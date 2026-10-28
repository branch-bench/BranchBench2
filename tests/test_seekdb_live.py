"""SeekDB backend against a live server (skipped when none listens on
SEEKDB_HOST:SEEKDB_PORT). Exercises every verb through the public API."""

import socket

import pytest

from dblib import result_collector as rc
from dblib.systems.seekdb import adapter as seekdb

DB = "bb_test_seekdb"


def _server_up() -> bool:
    try:
        with socket.create_connection((seekdb.SEEKDB_HOST, seekdb.SEEKDB_PORT), timeout=1):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not _server_up(), reason="no SeekDB server")


@pytest.fixture
def suite():
    seekdb.setup_database(DB)
    s = seekdb.SeekDBToolSuite.init_for_bench(rc.ResultCollector(), DB)
    s.exec(["CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(20), n INT)",
            "CREATE TABLE h (a INT, b VARCHAR(10))",
            "INSERT INTO t VALUES (1,'a',1),(2,'b',2),(3,'c',3)",
            "INSERT INTO h VALUES (1,'x')"], refs=["main"])
    yield s
    s.close_connection()
    seekdb.drop_database(DB)


def rows(suite, ref, sql):
    return [tuple(r) for r in suite.exec([sql], refs=[ref])[0].rows]


def test_commit_log_diff_reset_revert(suite):
    c1 = suite.commit("main", "one")
    assert c1.ok and c1.value
    suite.exec(["UPDATE t SET v='a2' WHERE id=1", "INSERT INTO t VALUES (4,'d',4)",
                "DELETE FROM t WHERE id=3"], refs=["main"])
    c2 = suite.commit("main", "two")
    assert c2.ok
    log = suite.log("main", limit=10)
    assert log.ok and [e["message"] for e in log.value][:2] == ["two", "one"]

    d = suite.diff(f"main@{c1.value}", f"main@{c2.value}")
    assert d.ok and (d.value["rows_added"], d.value["rows_deleted"], d.value["rows_modified"]) == (1, 1, 1)

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
    assert suite.branch("old", from_ref=f"main@{c1}").ok
    assert rows(suite, "old", "SELECT v FROM t WHERE id=1") == [("a",)]
    assert rows(suite, "main", "SELECT v FROM t WHERE id=1") == [("later",)]
    res = suite.exec(lambda db: db.sql(f"SELECT COUNT(*) FROM {db.table('old', 't')} o "
                                       f"JOIN {db.table('main', 't')} m ON o.id = m.id"),
                     refs=["main", "old"], mode="multi")[0]
    assert res.ok and [tuple(r) for r in res.value] == [(3,)]
    assert "old" in suite.list_branches()
    assert suite.delete("old").ok


def test_three_way_merge_and_conflicts(suite):
    suite.commit("main", "base")
    assert suite.branch("feat", "main").ok
    # Branch: modify 1, add 10, delete 3, add a row to the keyless table.
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
    assert m2.ok and m2.value["fast_forward"] is True and m2.value["conflicts"] == 0


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
    # Merging the rebased branch back is a fast-forward with no conflicts.
    m = suite.merge("main", "dev")
    assert m.ok and m.value["conflicts"] == 0 and m.value["fast_forward"] is True
    assert {r[0]: r[1] for r in rows(suite, "main", "SELECT id, n FROM t")} == got


def test_schema_conflict_fails_without_touching_data(suite):
    suite.commit("main", "base")
    assert suite.branch("pkchange", "main").ok
    suite.exec(["ALTER TABLE t DROP PRIMARY KEY", "ALTER TABLE t ADD PRIMARY KEY (id, n)"],
               refs=["pkchange"])
    suite.commit("pkchange", "pk change")
    m = suite.merge("main", "pkchange")
    assert m.failed and "schema conflict" in m.error


def test_branch_from_commit_drops_later_schema(suite):
    c1 = suite.commit("main", "before ddl").value
    suite.exec(["ALTER TABLE t ADD COLUMN later INT", "CREATE TABLE newer (id INT PRIMARY KEY)",
                "CREATE INDEX idx_v ON t (v)"], refs=["main"])
    suite.commit("main", "after ddl")
    assert suite.branch("old", from_ref=f"main@{c1}").ok
    cols = [r[0] for r in rows(suite, "old", "SELECT column_name FROM information_schema.columns "
                                              "WHERE table_schema = DATABASE() AND table_name = 't'")]
    assert "later" not in cols
    assert rows(suite, "old", "SELECT COUNT(*) FROM information_schema.tables "
                               "WHERE table_schema = DATABASE() AND table_name = 'newer'") == [(0,)]
    # The same step's DDL can now be replayed on the fork.
    assert suite.exec(["ALTER TABLE t ADD COLUMN later INT"], refs=["old"])[0].ok


def test_merge_survives_index_after_updates_on_branch(suite):
    """An index built after row updates makes the table's earlier flashback
    snapshots unreadable; the fork point lives in the parent, so a merge
    still has its base and stays three-way."""
    suite.commit("main", "base")
    assert suite.branch("idx", "main").ok
    suite.exec(["UPDATE t SET v = 'branch' WHERE id = 1", "CREATE INDEX idx_n ON t (n)"],
               refs=["idx"])
    c = suite.commit("idx", "branch work")
    assert c.ok
    suite.exec(["UPDATE t SET v = 'spine' WHERE id = 2"], refs=["main"])
    m = suite.merge("main", "idx")
    assert m.ok, m.error
    assert m.value["conflicts"] == 0 and "two_way_tables" not in m.value
    assert {r[0]: r[1] for r in rows(suite, "main", "SELECT id, v FROM t")} == {1: "branch", 2: "spine", 3: "c"}
    # The branch's own commit snapshot of t is gone: resetting to it keeps
    # the current rows for t and reports it rather than failing.
    suite.exec(["UPDATE t SET v = 'later' WHERE id = 3"], refs=["idx"])
    r = suite.reset("idx", c.value)
    assert r.ok, r.error


def test_rebase_carries_column_default(suite):
    """A column added with a DEFAULT on the spine reaches the branch with the
    same default, so rows the branch writes afterwards get the value."""
    suite.commit("main", "base")
    assert suite.branch("feat", "main").ok
    suite.exec(["ALTER TABLE t ADD COLUMN cat VARCHAR(16) DEFAULT 'general'",
                "INSERT INTO t (id, v, n) VALUES (40,'forty',40)"], refs=["main"])
    suite.commit("main", "migration")
    suite.exec(["INSERT INTO t (id, v, n) VALUES (50,'fifty',50)"], refs=["feat"])
    suite.commit("feat", "feature work")

    r = suite.rebase("feat", "main")
    assert r.ok, r.error
    assert "t.cat" in r.value["schema_changes"]["added_columns"]
    assert rows(suite, "feat", "SELECT column_default FROM information_schema.columns "
                               "WHERE table_schema = DATABASE() AND table_name = 't' "
                               "AND column_name = 'cat'") == [("general",)]
    suite.exec(["INSERT INTO t (id, v, n) VALUES (60,'sixty',60)"], refs=["feat"])
    got = dict(rows(suite, "feat", "SELECT id, cat FROM t WHERE id IN (40, 50, 60)"))
    assert got[40] == "general" and got[60] == "general"


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
