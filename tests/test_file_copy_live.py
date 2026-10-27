"""file_copy backend against a live PostgreSQL server (skipped when none
answers on PGSQL_HOST:PGSQL_PORT). Exercises every verb through the public
API, including the connection parking a clone needs."""

import threading
import time

import pytest

from dblib import result_collector as rc
from dblib import file_copy as file_copy

DB = "bb_test_fc"

pytestmark = pytest.mark.skipif(not file_copy.server_up(), reason="no PostgreSQL server")


@pytest.fixture
def suite():
    file_copy.setup_database(DB)
    s = file_copy.FileCopyToolSuite.init_for_bench(rc.ResultCollector(), DB, DB)
    s.exec(["CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(20), n INT)",
            "CREATE TABLE h (a INT, b VARCHAR(10))",
            "INSERT INTO t VALUES (1,'a',1),(2,'b',2),(3,'c',3)",
            "INSERT INTO h VALUES (1,'x')"], refs=[DB])
    yield s
    s.close_connection()
    file_copy.drop_database(DB)


def rows(suite, ref, sql):
    return [tuple(r) for r in suite.exec([sql], refs=[ref])[0].rows]


def test_implementation_summary():
    impl = file_copy.FileCopyToolSuite.implementation()
    assert impl["branch"] == "simulated" and impl["merge"] == "simulated"
    assert impl["delete"] == "simulated" and impl["multi_ref_exec"] == "simulated"
    assert set(impl) >= {"branch", "commit", "diff", "log", "merge", "rebase", "revert",
                         "reset", "delete", "commit_refs", "multi_ref_exec"}


def test_commit_log_diff_reset_revert(suite):
    c1 = suite.commit(DB, "one")
    assert c1.ok and c1.value, c1.error
    suite.exec(["UPDATE t SET v='a2' WHERE id=1", "INSERT INTO t VALUES (4,'d',4)",
                "DELETE FROM t WHERE id=3"], refs=[DB])
    c2 = suite.commit(DB, "two")
    assert c2.ok, c2.error
    log = suite.log(DB, limit=10)
    assert log.ok and [e["message"] for e in log.value][:2] == ["two", "one"]

    d = suite.diff(f"{DB}@{c1.value}", f"{DB}@{c2.value}")
    assert d.ok, d.error
    assert (d.value["rows_added"], d.value["rows_deleted"], d.value["rows_modified"]) == (1, 1, 1)

    r = suite.revert(DB, c2.value)
    assert r.ok, r.error
    assert sorted(rows(suite, DB, "SELECT id, v FROM t")) == [(1, "a"), (2, "b"), (3, "c")]
    assert suite.log(DB, limit=1).value[0]["message"].startswith("revert ")

    suite.exec(["UPDATE t SET v='zz'"], refs=[DB])
    rs = suite.reset(DB, c2.value)
    assert rs.ok, rs.error
    assert sorted(rows(suite, DB, "SELECT id, v FROM t")) == [(1, "a2"), (2, "b"), (4, "d")]
    assert [e["message"] for e in suite.log(DB).value][0] == "two"


def test_commit_ref_is_read_only_and_branch_from_it_writable(suite):
    c1 = suite.commit(DB, "base").value
    r = suite.exec(["UPDATE t SET v='nope' WHERE id=1"], refs=[f"{DB}@{c1}"])[0]
    assert r.failed and "read-only" in r.error
    assert rows(suite, f"{DB}@{c1}", "SELECT v FROM t WHERE id=1") == [("a",)]
    assert suite.branch("child", from_ref=f"{DB}@{c1}").ok
    assert suite.exec(["UPDATE t SET v='yes' WHERE id=1"], refs=["child"])[0].ok
    assert rows(suite, "child", "SELECT v FROM t WHERE id=1") == [("yes",)]


def test_branch_from_commit_and_multi_ref_exec(suite):
    c1 = suite.commit(DB, "base").value
    suite.exec(["UPDATE t SET v='later' WHERE id=1"], refs=[DB])
    suite.commit(DB, "later")
    b = suite.branch("old", from_ref=f"{DB}@{c1}")
    assert b.ok, b.error
    assert rows(suite, "old", "SELECT v FROM t WHERE id=1") == [("a",)]
    assert rows(suite, DB, "SELECT v FROM t WHERE id=1") == [("later",)]
    res = suite.exec(lambda db: db.sql(f"SELECT COUNT(*) FROM {db.table('old', 't')} o "
                                       f"JOIN {db.table(DB, 't')} m ON o.id = m.id"),
                     refs=[DB, "old"], mode="multi")[0]
    assert res.ok, res.error
    assert [tuple(r) for r in res.value] == [(3,)]
    # A commit ref in a multi-ref script is a time-travel read.
    res = suite.exec(lambda db: db.sql(f"SELECT v FROM {db.table(f'{DB}@{c1}', 't')} WHERE id = 1"),
                     refs=[DB, f"{DB}@{c1}"], mode="multi")[0]
    assert res.ok and [tuple(r) for r in res.value] == [("a",)]
    assert rows(suite, f"{DB}@{c1}", "SELECT v FROM t WHERE id=1") == [("a",)]
    assert "old" in suite.list_branches()
    assert suite.delete("old").ok
    assert "old" not in suite.list_branches()


def test_three_way_merge_and_conflicts(suite):
    suite.commit(DB, "base")
    assert suite.branch("feat", DB).ok
    suite.exec(["UPDATE t SET v='feat' WHERE id=1", "INSERT INTO t VALUES (10,'ten',10)",
                "DELETE FROM t WHERE id=3", "INSERT INTO h VALUES (2,'y')",
                "ALTER TABLE t ADD COLUMN extra INT"], refs=["feat"])
    suite.commit("feat", "feature work SENTINEL")
    suite.exec(["UPDATE t SET v='spine' WHERE id=2", "UPDATE t SET n=99 WHERE id=1",
                "INSERT INTO t VALUES (20,'twenty',20)"], refs=[DB])
    suite.commit(DB, "spine work")

    m = suite.merge(DB, "feat", message="merge feat", on_conflict="theirs")
    assert m.ok, m.error
    assert m.value["conflicts"] == 1 and m.value["conflict_tables"] == ["t"]
    assert m.value["fast_forward"] is False
    assert "t.extra" in m.value["schema_changes"]["added_columns"]
    got = {r[0]: r[1:] for r in rows(suite, DB, "SELECT id, v, n FROM t")}
    assert got == {1: ("feat", 1), 2: ("spine", 2), 10: ("ten", 10), 20: ("twenty", 20)}
    assert sorted(rows(suite, DB, "SELECT a FROM h")) == [(1,), (2,)]
    msgs = [e["message"] for e in suite.log(DB, limit=10).value]
    assert msgs[0] == "merge feat" and any("SENTINEL" in x for x in msgs)

    assert suite.branch("ff", DB).ok
    suite.exec(["INSERT INTO t (id, v, n) VALUES (30,'thirty',30)"], refs=["ff"])
    suite.commit("ff", "ff work")
    m2 = suite.merge(DB, "ff")
    assert m2.ok, m2.error
    assert m2.value["fast_forward"] is True and m2.value["conflicts"] == 0


def test_merge_ours_keeps_target_rows(suite):
    suite.commit(DB, "base")
    assert suite.branch("b", DB).ok
    suite.exec(["UPDATE t SET n = 100 WHERE id = 1", "DELETE FROM t WHERE id = 2"], refs=["b"])
    suite.commit("b", "branch")
    suite.exec(["UPDATE t SET n = 200 WHERE id = 1", "UPDATE t SET n = 22 WHERE id = 2"], refs=[DB])
    m = suite.merge(DB, "b", on_conflict="ours")
    assert m.ok, m.error
    assert m.value["conflicts"] == 2
    assert dict(rows(suite, DB, "SELECT id, n FROM t")) == {1: 200, 2: 22, 3: 3}


def test_rebase_with_callable_resolver(suite):
    suite.commit(DB, "base")
    assert suite.branch("dev", DB).ok
    suite.exec(["UPDATE t SET n = n + 5 WHERE id=1"], refs=["dev"])
    suite.commit("dev", "dev adds 5")
    suite.exec(["UPDATE t SET n = n + 7 WHERE id=1", "INSERT INTO t VALUES (40,'f',40)",
                "DELETE FROM t WHERE id=2"], refs=[DB])
    suite.commit(DB, "spine adds 7")

    seen = {}

    def additive(db, conflicts):
        for cf in conflicts:
            seen[cf["table"]] = cf["rows"]
            for row in cf["rows"]:
                total = row["our_n"] + row["their_n"] - row["base_n"]
                db.sql("UPDATE t SET n = %s WHERE id = %s", (total, row["our_id"]))
            db.resolve(cf["table"])

    rb = suite.rebase("dev", DB, on_conflict=additive)
    assert rb.ok, rb.error
    assert rb.value["conflicts"] == 1 and seen["t"][0]["their_diff_type"] == "modified"
    got = {r[0]: r[1] for r in rows(suite, "dev", "SELECT id, n FROM t")}
    assert got == {1: 13, 3: 3, 40: 40}
    msgs = [e["message"] for e in suite.log("dev", limit=10).value]
    assert msgs[0].startswith("rebase onto") and "spine adds 7" in msgs and "dev adds 5" in msgs
    m = suite.merge(DB, "dev")
    assert m.ok, m.error
    assert m.value["conflicts"] == 0 and m.value["fast_forward"] is True
    assert {r[0]: r[1] for r in rows(suite, DB, "SELECT id, n FROM t")} == got


def test_rebase_theirs_keeps_branch_rows(suite):
    suite.commit(DB, "base")
    assert suite.branch("dev", DB).ok
    suite.exec(["UPDATE t SET v = 'branch' WHERE id = 1"], refs=["dev"])
    suite.commit("dev", "branch work")
    suite.exec(["UPDATE t SET v = 'spine' WHERE id = 1", "INSERT INTO t VALUES (50,'s',50)"], refs=[DB])
    suite.commit(DB, "spine work")
    rb = suite.rebase("dev", DB, on_conflict="theirs")
    assert rb.ok, rb.error
    assert rb.value["conflicts"] == 1 and rb.value["up_to_date"] is False
    got = dict(rows(suite, "dev", "SELECT id, v FROM t"))
    assert got[1] == "branch" and got[50] == "s"
    rb2 = suite.rebase("dev", DB, on_conflict="theirs")
    assert rb2.ok and rb2.value["up_to_date"] is True


def test_schema_conflicts_fail_without_touching_data(suite):
    suite.commit(DB, "base")
    assert suite.branch("pkchange", DB).ok
    r = suite.exec(["ALTER TABLE t DROP CONSTRAINT t_pkey", "ALTER TABLE t ADD PRIMARY KEY (id, n)"],
                   refs=["pkchange"])[0]
    assert r.ok, r.error
    suite.commit("pkchange", "pk change")
    m = suite.merge(DB, "pkchange")
    assert m.failed and "schema conflict" in m.error
    # The same column added with different types on both sides.
    assert suite.branch("typeclash", DB).ok
    suite.exec(["ALTER TABLE t ADD COLUMN tier VARCHAR(8)", "UPDATE t SET tier = 'gold' WHERE id = 1"], refs=["typeclash"])
    suite.commit("typeclash", "branch adds tier varchar")
    suite.exec(["ALTER TABLE t ADD COLUMN tier INT"], refs=[DB])
    m = suite.merge(DB, "typeclash", message="clash")
    assert m.failed and "schema conflict" in m.error.lower(), (m.status_name, m.error)
    assert rows(suite, DB, "SELECT data_type FROM information_schema.columns WHERE table_name = 't' AND column_name = 'tier'") == [("integer",)]
    assert rows(suite, "typeclash", "SELECT tier FROM t WHERE id = 1") == [("gold",)]


def test_branch_from_commit_has_old_schema(suite):
    c1 = suite.commit(DB, "before ddl").value
    suite.exec(["ALTER TABLE t ADD COLUMN later INT", "CREATE TABLE newer (id INT PRIMARY KEY)"], refs=[DB])
    suite.commit(DB, "after ddl")
    assert suite.branch("old", from_ref=f"{DB}@{c1}").ok
    cols = [r[0] for r in rows(suite, "old", "SELECT column_name FROM information_schema.columns "
                                              "WHERE table_schema = 'public' AND table_name = 't'")]
    assert "later" not in cols
    assert rows(suite, "old", "SELECT COUNT(*) FROM information_schema.tables "
                               "WHERE table_schema = 'public' AND table_name = 'newer'") == [(0,)]
    assert suite.exec(["ALTER TABLE t ADD COLUMN later INT"], refs=["old"])[0].ok


def test_revert_of_deleted_branch_commit_after_fast_forward(suite):
    """S2: a candidate's commit is fast-forwarded into the spine, the
    candidate is deleted, then the commit is reverted on the spine."""
    suite.commit(DB, "pre")
    assert suite.branch("cand", DB).ok
    suite.exec(["INSERT INTO t VALUES (7,'cand',7)", "UPDATE t SET v = 'sum' WHERE id = 1"], refs=["cand"])
    c = suite.commit("cand", "compaction")
    assert c.ok
    m = suite.merge(DB, "cand")
    assert m.ok and m.value["fast_forward"] is True
    assert suite.delete("cand").ok
    r = suite.revert(DB, c.value)
    assert r.ok, r.error
    got = dict(rows(suite, DB, "SELECT id, v FROM t"))
    assert 7 not in got and got[1] == "a"


def test_reset_to_inherited_commit_and_twice_to_older(suite):
    c = suite.commit(DB, "base").value
    suite.exec(["UPDATE t SET v = 'x' WHERE id = 1"], refs=[DB])
    suite.commit(DB, "later")
    assert suite.branch("b", DB).ok
    suite.exec(["INSERT INTO t VALUES (9,'nine',9)"], refs=["b"])
    suite.commit("b", "b work")
    r = suite.reset("b", c)
    assert r.ok, r.error
    assert dict(rows(suite, "b", "SELECT id, v FROM t")) == {1: "a", 2: "b", 3: "c"}

    assert suite.branch("twice", DB).ok
    suite.exec(["INSERT INTO t VALUES (10,'s1',1)"], refs=["twice"])
    c1 = suite.commit("twice", "s1")
    suite.exec(["INSERT INTO t VALUES (11,'s2',2)"], refs=["twice"])
    c2 = suite.commit("twice", "s2")
    suite.exec(["INSERT INTO t VALUES (12,'s3',3)"], refs=["twice"])
    assert suite.commit("twice", "s3").ok
    assert suite.reset("twice", c2.value).ok
    assert [r[0] for r in rows(suite, "twice", "SELECT id FROM t WHERE id >= 10 ORDER BY id")] == [10, 11]
    r1 = suite.reset("twice", c1.value)
    assert r1.ok, r1.error
    assert [r[0] for r in rows(suite, "twice", "SELECT id FROM t WHERE id >= 10 ORDER BY id")] == [10]
    assert suite.log("twice", limit=5).value[0]["message"] == "s1"
    assert suite.delete("twice").ok


def test_rebase_spine_onto_its_own_branch(suite):
    suite.commit(DB, "pre")
    assert suite.branch("cand", DB).ok
    suite.exec(["UPDATE t SET v = 'cand' WHERE id = 1", "INSERT INTO t VALUES (8,'c8',8)"], refs=["cand"])
    suite.commit("cand", "compaction")
    suite.exec(["INSERT INTO t VALUES (9,'m9',9)", "UPDATE t SET v = 'main' WHERE id = 1"], refs=[DB])
    suite.commit(DB, "work")
    rb = suite.rebase(DB, "cand", on_conflict="theirs")
    assert rb.ok, rb.error
    assert rb.value["conflicts"] == 1 and rb.value["up_to_date"] is False
    assert dict(rows(suite, DB, "SELECT id, v FROM t")) == {1: "main", 2: "b", 3: "c", 8: "c8", 9: "m9"}
    m = suite.merge(DB, "cand", message="fast-forward to alternative")
    assert m.ok, m.error
    assert m.value["fast_forward"] is True and m.value["conflicts"] == 0
    msgs = [e["message"] for e in suite.log(DB, limit=10).value]
    assert msgs[0] == "fast-forward to alternative" and "compaction" in msgs and "work" in msgs


def test_rebase_when_upstream_added_a_table_and_a_default(suite):
    suite.commit(DB, "base")
    assert suite.branch("dev", DB).ok
    suite.exec(["INSERT INTO t VALUES (50,'dev',50)"], refs=["dev"])
    suite.commit("dev", "dev work")
    suite.exec(["CREATE TABLE promo_1 (promo_id INT PRIMARY KEY, i_id INT)",
                "INSERT INTO promo_1 VALUES (1, 1)", "INSERT INTO t VALUES (60,'spine',60)",
                "ALTER TABLE t ADD COLUMN cat VARCHAR(16) DEFAULT 'general'"], refs=[DB])
    suite.commit(DB, "migration")
    r = suite.rebase("dev", DB, on_conflict="theirs")
    assert r.ok, r.error
    assert rows(suite, "dev", "SELECT COUNT(*) FROM promo_1") == [(1,)]
    assert sorted(k for (k,) in rows(suite, "dev", "SELECT id FROM t")) == [1, 2, 3, 50, 60]
    suite.exec(["INSERT INTO t (id, v, n) VALUES (70,'seventy',70)"], refs=["dev"])
    assert rows(suite, "dev", "SELECT cat FROM t WHERE id = 70") == [("general",)]
    m = suite.merge(DB, "dev")
    assert m.ok and m.value["conflicts"] == 0


def test_fork_parks_other_connections_to_the_template(suite):
    """Another suite's idle connection on the parent is closed for the
    clone and reopens on its next use; a busy one is waited for."""
    other = file_copy.FileCopyToolSuite.init_for_bench(rc.ResultCollector(), DB, DB)
    try:
        assert rows(other, DB, "SELECT COUNT(*) FROM t") == [(3,)]   # idle on the parent now
        b = suite.branch("parked", DB)
        assert b.ok, b.error
        assert other.conn.parked
        assert rows(other, DB, "SELECT COUNT(*) FROM t") == [(3,)]   # reconnected
        assert not other.conn.parked and other.conn.reconnects >= 1

        # Busy: a statement sleeping on the parent delays the clone.
        t0 = time.monotonic()
        th = threading.Thread(target=lambda: other.exec(["SELECT pg_sleep(1.5)"], refs=[DB]))
        th.start()
        time.sleep(0.3)
        b2 = suite.branch("waited", DB)
        th.join()
        assert b2.ok, b2.error
        assert time.monotonic() - t0 >= 1.4
        assert file_copy.FileCopyToolSuite.OBSERVED.get("quiesce_waits", 0) >= 1
    finally:
        other.close_connection()


def test_storage_shares_blocks_between_clones(suite):
    """A clone of a database adds far less storage than its size."""
    suite.exec(["CREATE TABLE big AS SELECT g AS id, md5(g::text) AS v FROM generate_series(1, 300000) g",
                "ALTER TABLE big ADD PRIMARY KEY (id)"], refs=[DB])
    suite.gc()
    before = suite._storage_bytes()
    size = int(rows(suite, DB, "SELECT pg_database_size(current_database())")[0][0])
    assert suite.branch("clone1", DB).ok and suite.branch("clone2", "clone1").ok
    suite.gc()
    after = suite._storage_bytes()
    if file_copy.PGSQL_DATA_DIR:
        assert after - before < size // 4, (before, after, size)
    assert rows(suite, "clone2", "SELECT COUNT(*) FROM big") == [(300000,)]


def test_schema_diff_reports_tables_and_columns(suite):
    main = DB
    c = suite.commit(main, "base")
    base = f"{main}@{c.value}"
    assert suite.branch("sd", main).ok
    r = suite.exec(["ALTER TABLE t ADD COLUMN w INT", "CREATE TABLE extra (id INT PRIMARY KEY, z VARCHAR(4))"],
                   refs=["sd"])[0]
    assert r.ok, r.error
    d = suite.diff(base, "sd")
    assert d.ok, d.error
    d.value = d.value["schema"]
    assert d.value["tables_added"] == ["extra"] and d.value["tables_dropped"] == []
    assert d.value["columns_added"] == {"t": ["w"]} and d.value["changed"]
    assert not d.value["columns_dropped"] and not d.value["columns_changed"]
    back = suite.diff("sd", base)
    assert back.ok and back.value["schema"]["tables_dropped"] == ["extra"] and back.value["schema"]["columns_dropped"] == {"t": ["w"]}
    same = suite.diff("sd", "sd")
    assert same.ok and not same.value["schema"]["changed"]
    assert suite.delete("sd").ok
