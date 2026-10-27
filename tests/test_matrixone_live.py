"""MatrixOne backend against a live server (skipped when none listens on
MO_HOST:MO_PORT). Exercises every verb through the public API."""

import socket

import pytest

from dblib import result_collector as rc
from dblib import matrixone as matrixone

DB = "bb_test_mo"


# Tests that use an operation the adapter simulates (log, revert, the
# diff-driven reset) or a resolver that reads the log; skipped in
# native-only mode, where those operations run as unsupported.
from dblib.db_api import native_only
needs_simulated = pytest.mark.skipif(native_only(), reason="uses a simulated operation; native-only mode")


def _server_up() -> bool:
    try:
        with socket.create_connection((matrixone.MO_HOST, matrixone.MO_PORT), timeout=1):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not _server_up(), reason="no MatrixOne server")


@pytest.fixture
def suite():
    matrixone.setup_database(DB)
    s = matrixone.MatrixOneToolSuite.init_for_bench(rc.ResultCollector(), DB)
    s.exec(["CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(20), n INT)",
            "CREATE TABLE h (a INT, b VARCHAR(10))",
            "INSERT INTO t VALUES (1,'a',1),(2,'b',2),(3,'c',3)",
            "INSERT INTO h VALUES (1,'x')"], refs=["main"])
    yield s
    s.close_connection()
    matrixone.drop_database(DB)


def rows(suite, ref, sql):
    return [tuple(r) for r in suite.exec([sql], refs=[ref])[0].rows]


@needs_simulated
def test_implementation_summary():
    impl = matrixone.MatrixOneToolSuite.implementation()
    assert impl["branch"] == "native" and impl["merge"] == "native" and impl["revert"] == "simulated"
    assert set(impl) >= {"branch", "commit", "diff", "log", "merge", "rebase", "revert",
                         "reset", "delete", "commit_refs", "multi_ref_exec"}


@needs_simulated
def test_commit_log_diff_reset_revert(suite):
    c1 = suite.commit("main", "one")
    assert c1.ok and c1.value, c1.error
    suite.exec(["UPDATE t SET v='a2' WHERE id=1", "INSERT INTO t VALUES (4,'d',4)",
                "DELETE FROM t WHERE id=3"], refs=["main"])
    c2 = suite.commit("main", "two")
    assert c2.ok, c2.error
    log = suite.log("main", limit=10)
    assert log.ok and [e["message"] for e in log.value][:2] == ["two", "one"]

    d = suite.diff(f"main@{c1.value}", f"main@{c2.value}")
    assert d.ok, d.error
    assert (d.value["rows_added"], d.value["rows_deleted"], d.value["rows_modified"]) == (1, 1, 1)

    r = suite.revert("main", c2.value)
    assert r.ok, r.error
    assert sorted(rows(suite, "main", "SELECT id, v FROM t")) == [(1, "a"), (2, "b"), (3, "c")]

    suite.exec(["UPDATE t SET v='zz'"], refs=["main"])
    rs = suite.reset("main", c2.value)
    assert rs.ok, rs.error
    assert sorted(rows(suite, "main", "SELECT id, v FROM t")) == [(1, "a2"), (2, "b"), (4, "d")]
    assert [e["message"] for e in suite.log("main").value][0] == "two"


def test_branch_from_commit_and_multi_ref_exec(suite):
    c1 = suite.commit("main", "base").value
    suite.exec(["UPDATE t SET v='later' WHERE id=1"], refs=["main"])
    suite.commit("main", "later")
    b = suite.branch("old", from_ref=f"main@{c1}")
    assert b.ok, b.error
    assert rows(suite, "old", "SELECT v FROM t WHERE id=1") == [("a",)]
    assert rows(suite, "main", "SELECT v FROM t WHERE id=1") == [("later",)]
    res = suite.exec(lambda db: db.sql(f"SELECT COUNT(*) FROM {db.table('old', 't')} o "
                                       f"JOIN {db.table('main', 't')} m ON o.id = m.id"),
                     refs=["main", "old"], mode="multi")[0]
    assert res.ok and [tuple(r) for r in res.value] == [(3,)]
    # A commit ref in a multi-ref script is a time-travel read.
    res = suite.exec(lambda db: db.sql(f"SELECT v FROM {db.table(f'main@{c1}', 't')} WHERE id = 1"),
                     refs=["main", f"main@{c1}"], mode="multi")[0]
    assert res.ok and [tuple(r) for r in res.value] == [("a",)]
    # exec() directly on a commit ref works through a temporary branch.
    assert rows(suite, f"main@{c1}", "SELECT v FROM t WHERE id=1") == [("a",)]
    assert "old" in suite.list_branches()
    assert suite.delete("old").ok


@needs_simulated
def test_three_way_merge_and_conflicts(suite):
    suite.commit("main", "base")
    assert suite.branch("feat", "main").ok
    suite.exec(["UPDATE t SET v='feat' WHERE id=1", "INSERT INTO t VALUES (10,'ten',10)",
                "DELETE FROM t WHERE id=3", "INSERT INTO h VALUES (2,'y')",
                "ALTER TABLE t ADD COLUMN extra INT"], refs=["feat"])
    suite.commit("feat", "feature work SENTINEL")
    # Spine meanwhile: modify 2 (no conflict), modify 1 (conflict), add 20.
    suite.exec(["UPDATE t SET v='spine' WHERE id=2", "UPDATE t SET n=99 WHERE id=1",
                "INSERT INTO t VALUES (20,'twenty',20)"], refs=["main"])
    suite.commit("main", "spine work")

    m = suite.merge("main", "feat", message="merge feat", on_conflict="theirs")
    assert m.ok, m.error
    assert m.value["conflicts"] == 1 and m.value["conflict_tables"] == ["t"]
    assert m.value["fast_forward"] is False
    assert "t.extra" in m.value["schema_changes"]["added_columns"]
    got = {r[0]: r[1:] for r in rows(suite, "main", "SELECT id, v, n FROM t")}
    assert got == {1: ("feat", 1), 2: ("spine", 2), 10: ("ten", 10), 20: ("twenty", 20)}
    assert sorted(rows(suite, "main", "SELECT a FROM h")) == [(1,), (2,)]
    msgs = [e["message"] for e in suite.log("main", limit=10).value]
    assert msgs[0] == "merge feat" and any("SENTINEL" in x for x in msgs)

    # Fast-forward: a branch merged back without spine changes.
    assert suite.branch("ff", "main").ok
    suite.exec(["INSERT INTO t (id, v, n) VALUES (30,'thirty',30)"], refs=["ff"])
    suite.commit("ff", "ff work")
    m2 = suite.merge("main", "ff")
    assert m2.ok, m2.error
    assert m2.value["fast_forward"] is True and m2.value["conflicts"] == 0


def test_merge_ours_keeps_target_rows(suite):
    suite.commit("main", "base")
    assert suite.branch("b", "main").ok
    suite.exec(["UPDATE t SET n = 100 WHERE id = 1", "DELETE FROM t WHERE id = 2"], refs=["b"])
    suite.commit("b", "branch")
    suite.exec(["UPDATE t SET n = 200 WHERE id = 1", "UPDATE t SET n = 22 WHERE id = 2"], refs=["main"])
    m = suite.merge("main", "b", on_conflict="ours")
    assert m.ok, m.error
    assert m.value["conflicts"] == 2
    got = dict(rows(suite, "main", "SELECT id, n FROM t"))
    assert got == {1: 200, 2: 22, 3: 3}


@needs_simulated
def test_rebase_with_callable_resolver(suite):
    suite.commit("main", "base")
    assert suite.branch("dev", "main").ok
    suite.exec(["UPDATE t SET n = n + 5 WHERE id=1"], refs=["dev"])
    suite.commit("dev", "dev adds 5")
    suite.exec(["UPDATE t SET n = n + 7 WHERE id=1", "INSERT INTO t VALUES (40,'f',40)",
                "DELETE FROM t WHERE id=2"], refs=["main"])
    suite.commit("main", "spine adds 7")

    seen = {}

    def additive(db, conflicts):
        for cf in conflicts:
            seen[cf["table"]] = cf["rows"]
            for row in cf["rows"]:
                total = row["our_n"] + row["their_n"] - row["base_n"]
                db.sql("UPDATE t SET n = %s WHERE id = %s", (total, row["our_id"]))
            db.resolve(cf["table"])

    rb = suite.rebase("dev", "main", on_conflict=additive)
    assert rb.ok, rb.error
    assert rb.value["conflicts"] == 1 and seen["t"][0]["their_diff_type"] == "modified"
    got = {r[0]: r[1] for r in rows(suite, "dev", "SELECT id, n FROM t")}
    assert got == {1: 13, 3: 3, 40: 40}  # 1+5+7, spine's delete and insert applied
    msgs = [e["message"] for e in suite.log("dev", limit=10).value]
    assert msgs[0] == "dev adds 5" and "spine adds 7" in msgs
    # Merging the rebased branch back is a fast-forward with no conflicts.
    m = suite.merge("main", "dev")
    assert m.ok, m.error
    assert m.value["conflicts"] == 0 and m.value["fast_forward"] is True
    assert {r[0]: r[1] for r in rows(suite, "main", "SELECT id, n FROM t")} == got


def test_rebase_theirs_keeps_branch_rows(suite):
    suite.commit("main", "base")
    assert suite.branch("dev", "main").ok
    suite.exec(["UPDATE t SET v = 'branch' WHERE id = 1"], refs=["dev"])
    suite.commit("dev", "branch work")
    suite.exec(["UPDATE t SET v = 'spine' WHERE id = 1", "INSERT INTO t VALUES (50,'s',50)"],
               refs=["main"])
    suite.commit("main", "spine work")
    rb = suite.rebase("dev", "main", on_conflict="theirs")
    assert rb.ok, rb.error
    assert rb.value["conflicts"] == 1 and rb.value["up_to_date"] is False
    got = dict(rows(suite, "dev", "SELECT id, v FROM t"))
    assert got[1] == "branch" and got[50] == "s"
    # Nothing new upstream: the second rebase is a no-op.
    rb2 = suite.rebase("dev", "main", on_conflict="theirs")
    assert rb2.ok and rb2.value["up_to_date"] is True


def test_schema_conflict_fails_without_touching_data(suite):
    suite.commit("main", "base")
    assert suite.branch("pkchange", "main").ok
    r = suite.exec(["ALTER TABLE t DROP PRIMARY KEY", "ALTER TABLE t ADD PRIMARY KEY (id, n)"],
                   refs=["pkchange"])[0]
    if not r.ok:
        pytest.skip(f"MatrixOne cannot change the primary key: {r.error}")
    suite.commit("pkchange", "pk change")
    m = suite.merge("main", "pkchange")
    assert m.failed and "schema conflict" in m.error


def test_branch_from_commit_has_old_schema(suite):
    c1 = suite.commit("main", "before ddl").value
    suite.exec(["ALTER TABLE t ADD COLUMN later INT", "CREATE TABLE newer (id INT PRIMARY KEY)"],
               refs=["main"])
    suite.commit("main", "after ddl")
    assert suite.branch("old", from_ref=f"main@{c1}").ok
    cols = [r[0] for r in rows(suite, "old", "SELECT column_name FROM information_schema.columns "
                                              "WHERE table_schema = DATABASE() AND table_name = 't'")]
    assert "later" not in cols
    assert rows(suite, "old", "SELECT COUNT(*) FROM information_schema.tables "
                               "WHERE table_schema = DATABASE() AND table_name = 'newer'") == [(0,)]
    assert suite.exec(["ALTER TABLE t ADD COLUMN later INT"], refs=["old"])[0].ok


@needs_simulated
def test_revert_of_deleted_branch_commit_after_fast_forward(suite):
    """S2: a candidate's commit is fast-forwarded into the spine, the
    candidate is deleted, then the commit is reverted on the spine."""
    suite.commit("main", "pre")
    assert suite.branch("cand", "main").ok
    suite.exec(["INSERT INTO t VALUES (7,'cand',7)", "UPDATE t SET v = 'sum' WHERE id = 1"],
               refs=["cand"])
    c = suite.commit("cand", "compaction")
    assert c.ok
    m = suite.merge("main", "cand")
    assert m.ok and m.value["fast_forward"] is True
    assert suite.delete("cand").ok
    r = suite.revert("main", c.value)
    assert r.ok, r.error
    got = dict(rows(suite, "main", "SELECT id, v FROM t"))
    assert 7 not in got and got[1] == "a"


@needs_simulated
def test_reset_to_inherited_commit(suite):
    c = suite.commit("main", "base").value
    suite.exec(["UPDATE t SET v = 'x' WHERE id = 1"], refs=["main"])
    suite.commit("main", "later")
    assert suite.branch("b", "main").ok
    suite.exec(["INSERT INTO t VALUES (9,'nine',9)"], refs=["b"])
    suite.commit("b", "b work")
    r = suite.reset("b", c)
    assert r.ok, r.error
    got = dict(rows(suite, "b", "SELECT id, v FROM t"))
    assert got == {1: "a", 2: "b", 3: "c"}


def test_rebase_carries_column_default(suite):
    suite.commit("main", "base")
    assert suite.branch("feat", "main").ok
    suite.exec(["ALTER TABLE t ADD COLUMN cat VARCHAR(16) DEFAULT 'general'",
                "INSERT INTO t (id, v, n) VALUES (40,'forty',40)"], refs=["main"])
    suite.commit("main", "migration")
    suite.exec(["INSERT INTO t (id, v, n) VALUES (50,'fifty',50)"], refs=["feat"])
    suite.commit("feat", "feature work")

    r = suite.rebase("feat", "main")
    assert r.ok, r.error
    assert rows(suite, "feat", "SELECT column_default FROM information_schema.columns "
                               "WHERE table_schema = DATABASE() AND table_name = 't' "
                               "AND column_name = 'cat'") == [("'general'",)]
    suite.exec(["INSERT INTO t (id, v, n) VALUES (60,'sixty',60)"], refs=["feat"])
    got = dict(rows(suite, "feat", "SELECT id, cat FROM t WHERE id IN (40, 50, 60)"))
    assert got[40] == "general" and got[60] == "general"


@needs_simulated
def test_rebase_spine_onto_its_own_branch(suite):
    """S2: the spine is rebased onto a candidate that was forked from it."""
    suite.commit("main", "pre")
    assert suite.branch("cand", "main").ok
    suite.exec(["UPDATE t SET v = 'cand' WHERE id = 1", "INSERT INTO t VALUES (8,'c8',8)"],
               refs=["cand"])
    suite.commit("cand", "compaction")
    suite.exec(["INSERT INTO t VALUES (9,'m9',9)", "UPDATE t SET v = 'main' WHERE id = 1"],
               refs=["main"])
    suite.commit("main", "work")
    rb = suite.rebase("main", "cand", on_conflict="theirs")
    assert rb.ok, rb.error
    assert rb.value["conflicts"] == 1 and rb.value["up_to_date"] is False
    got = dict(rows(suite, "main", "SELECT id, v FROM t"))
    assert got == {1: "main", 2: "b", 3: "c", 8: "c8", 9: "m9"}
    m = suite.merge("main", "cand", message="fast-forward to alternative")
    assert m.ok, m.error
    assert m.value["fast_forward"] is True and m.value["conflicts"] == 0
    msgs = [e["message"] for e in suite.log("main", limit=10).value]
    assert msgs[0] == "fast-forward to alternative" and "compaction" in msgs and "work" in msgs


def test_rebase_when_upstream_added_a_table(suite):
    """S4: a spine migration creates a table after the branch forked."""
    suite.commit("main", "base")
    assert suite.branch("dev", "main").ok
    suite.exec(["INSERT INTO t VALUES (50,'dev',50)"], refs=["dev"])
    suite.commit("dev", "dev work")
    suite.exec(["CREATE TABLE promo_1 (promo_id INT PRIMARY KEY, i_id INT)",
                "INSERT INTO promo_1 VALUES (1, 1)", "INSERT INTO t VALUES (60,'spine',60)"],
               refs=["main"])
    suite.commit("main", "migration")
    r = suite.rebase("dev", "main", on_conflict="theirs")
    assert r.ok, r.error
    assert rows(suite, "dev", "SELECT COUNT(*) FROM promo_1") == [(1,)]
    assert sorted(k for (k,) in rows(suite, "dev", "SELECT id FROM t")) == [1, 2, 3, 50, 60]
    m = suite.merge("main", "dev")
    assert m.ok and m.value["conflicts"] == 0


@needs_simulated
def test_reset_keeps_lineage_for_rebase_at_scale(suite):
    """S6: a batch resets to an earlier commit, continues, and is rebased
    and merged; the branch's delta must only hold its own changes."""
    suite.exec(["CREATE TABLE big (id INT PRIMARY KEY, v INT)"], refs=["main"])
    conn = suite.conn
    with conn.cursor() as cur:
        cur.executemany("INSERT INTO big VALUES (%s, %s)", [(i, i) for i in range(200000)])
    suite.commit("main", "seeded")
    assert suite.branch("batch", "main").ok
    with conn.cursor() as cur:
        cur.execute("USE " + suite._db_for("batch"))
        cur.executemany("INSERT INTO big VALUES (%s, %s)", [(i, i) for i in range(1000000, 1001000)])
        cur.execute("UPDATE big SET v = v + 1 WHERE id < 500")
    suite._current_db = None
    c1 = suite.commit("batch", "step 1")
    assert c1.ok
    suite.exec(["INSERT INTO big VALUES (2000000, 1)", "UPDATE big SET v = 0 WHERE id = 600"], refs=["batch"])
    suite.commit("batch", "step 2 (bad)")
    r = suite.reset("batch", c1.value)
    assert r.ok, r.error
    assert rows(suite, "batch", "SELECT COUNT(*) FROM big WHERE id = 2000000 OR (id = 600 AND v = 0)") == [(0,)]
    suite.exec(["INSERT INTO big VALUES (2000001, 1)"], refs=["batch"])
    suite.commit("batch", "step 2 (redo)")
    suite.exec(["INSERT INTO big VALUES (3000000, 3)", "UPDATE big SET v = -1 WHERE id = 100"], refs=["main"])
    suite.commit("main", "spine moved")
    rb = suite.rebase("batch", "main", on_conflict="ours")
    assert rb.ok, rb.error
    assert rb.value["conflicts"] == 1  # id 100: +1 on the batch, -1 on the spine
    m = suite.merge("main", "batch")
    assert m.ok, m.error
    got = dict(rows(suite, "main", "SELECT id, v FROM big WHERE id IN (100, 200, 600, 1000000, 2000001, 3000000)"))
    assert got == {100: -1, 200: 201, 600: 600, 1000000: 1000000, 2000001: 1, 3000000: 3}
    assert rows(suite, "main", "SELECT COUNT(*) FROM big") == [(201002,)]


@needs_simulated
def test_second_rebase_keeps_replayed_column_backfill(suite):
    """S4.4: a branch adds a column and backfills it (no other column
    changes, which MatrixOne's diff does not list), is rebased twice over a
    moving spine, and must still carry every backfilled row; the branch
    moves to a new generation per rebase and old commits stay readable."""
    suite.commit("main", "base")
    assert suite.branch("dev", "main").ok
    suite.exec(["ALTER TABLE t ADD COLUMN tier VARCHAR(8)",
                "UPDATE t SET tier = 'gold' WHERE id IN (1, 2)"], refs=["dev"])
    c1 = suite.commit("dev", "backfill 1")
    suite.exec(["UPDATE t SET n = n + 10 WHERE id = 1"], refs=["main"])
    suite.commit("main", "spine 1")
    rb1 = suite.rebase("dev", "main", on_conflict="theirs")
    assert rb1.ok, rb1.error
    assert rb1.value["generation"] == 1 and suite._db_for("dev").endswith("__g1")
    assert sorted(k for (k,) in rows(suite, "dev", "SELECT id FROM t WHERE tier = 'gold'")) == [1, 2]
    suite.exec(["UPDATE t SET tier = 'gold' WHERE id = 3"], refs=["dev"])
    suite.commit("dev", "backfill 2")
    suite.exec(["INSERT INTO t VALUES (7,'m7',7)"], refs=["main"])
    suite.commit("main", "spine 2")
    rb2 = suite.rebase("dev", "main", on_conflict="theirs")
    assert rb2.ok, rb2.error
    assert rb2.value["generation"] == 2
    assert sorted(k for (k,) in rows(suite, "dev", "SELECT id FROM t WHERE tier = 'gold'")) == [1, 2, 3]
    assert rows(suite, "dev", "SELECT n FROM t WHERE id = 7") == [(7,)]
    # The first backfill's commit, taken on generation 0, is still readable.
    assert rows(suite, f"dev@{c1.value}", "SELECT COUNT(*) FROM t WHERE tier = 'gold'") == [(2,)]
    msgs = [e["message"] for e in suite.log("dev", limit=10).value]
    assert msgs[:2] == ["backfill 2", "backfill 1"]
    assert suite.list_branches() == ["main", "dev"]
    m = suite.merge("main", "dev")
    assert m.ok and m.value["conflicts"] == 0, m.error
    assert rows(suite, "main", "SELECT COUNT(*) FROM t WHERE tier = 'gold'") == [(3,)]
    assert suite.delete("dev").ok
    assert suite.list_branches() == ["main"]


def test_generations_resolve_across_workers(suite):
    """Another suite on the same database sees the branch where a rebase
    moved it, and a readability probe raises on an unknown snapshot
    instead of reporting the table absent."""
    suite.commit("main", "base")
    assert suite.branch("dev", "main").ok
    other = matrixone.MatrixOneToolSuite.init_for_bench(rc.ResultCollector(), DB)
    try:
        assert other.list_branches() == ["main", "dev"]
        suite.exec(["INSERT INTO t VALUES (40,'d',40)"], refs=["dev"])
        suite.commit("dev", "dev work")
        other.exec(["INSERT INTO t VALUES (41,'m',41)"], refs=["main"])
        other.commit("main", "spine work")
        assert suite.rebase("dev", "main").ok
        assert sorted(k for (k,) in rows(other, "dev", "SELECT id FROM t WHERE id > 39")) == [40, 41]
        with pytest.raises(Exception, match="snapshot"):
            other._readable(matrixone._State(other._db_for("dev"), "bb_no_such_snapshot"), "t")
        assert other._readable(matrixone._State(other._db_for("dev")), "no_such_table") is False
    finally:
        other.close_connection()


def test_second_rebase_keeps_picked_rows_on_fk_table(suite):
    """A second rebase keeps the rows the first one replayed on a table
    with a foreign key (the case where v4.2.5's PICK, no longer used, lost
    them once the old generation was dropped)."""
    suite.exec(["CREATE TABLE parent (p INT PRIMARY KEY)", "INSERT INTO parent VALUES (1)",
                "CREATE TABLE child (id INT PRIMARY KEY, p INT, v INT, "
                "FOREIGN KEY (p) REFERENCES parent (p) ON DELETE CASCADE)",
                "INSERT INTO child VALUES " + ", ".join(f"({i}, 1, 0)" for i in range(1, 201))],
               refs=["main"])
    suite.commit("main", "base")
    assert suite.branch("dev", "main").ok
    suite.exec(["ALTER TABLE child ADD COLUMN tier VARCHAR(8)",
                "UPDATE child SET tier = 'gold' WHERE id <= 100"], refs=["dev"])
    suite.commit("dev", "backfill 1")
    suite.exec(["UPDATE child SET v = 1 WHERE id = 150"], refs=["main"])
    suite.commit("main", "spine 1")
    rb1 = suite.rebase("dev", "main", on_conflict="theirs")
    assert rb1.ok, rb1.error
    suite.exec(["UPDATE child SET tier = 'gold' WHERE id > 100"], refs=["dev"])
    suite.commit("dev", "backfill 2")
    suite.exec(["UPDATE child SET v = 2 WHERE id = 160"], refs=["main"])
    suite.commit("main", "spine 2")
    rb2 = suite.rebase("dev", "main", on_conflict="theirs")
    assert rb2.ok, rb2.error
    assert rows(suite, "dev", "SELECT COUNT(*) FROM child WHERE tier = 'gold'") == [(200,)]
    # Row 150 changed upstream before the first rebase and is carried; row
    # 160 changed upstream after it while the branch tiered it, and PICK
    # resolves per row, so the branch's whole row wins there (v back to 0).
    assert rows(suite, "dev", "SELECT v FROM child WHERE id = 150") == [(1,)]


def test_schema_diff_reports_tables_and_columns(suite):
    main = "main"
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
