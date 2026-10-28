"""Neon backend against the real service. Billable: runs only with
DBFORK_LIVE_TESTS=1 and NEON_API_KEY_ORG set. Creates one scratch project,
exercises every verb, deletes the project."""

import os

import pytest

from dblib import result_collector as rc
from dblib.systems.neon import adapter as neon

pytestmark = pytest.mark.skipif(os.environ.get("DBFORK_LIVE_TESTS") != "1" or not neon.API_KEY,
                                reason="billable: needs DBFORK_LIVE_TESTS=1 and a Neon API key")

DB = "bb_test"


@pytest.fixture(scope="module")
def project():
    p = neon.create_project("project_macro_test_neon")
    pid = p["project"]["id"]
    uri = p["connection_uris"][0]["connection_uri"]
    import psycopg2
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
    c = psycopg2.connect(uri)
    c.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    c.cursor().execute(f"CREATE DATABASE {DB}")
    c.close()
    yield pid, p["branch"]["id"], p["branch"]["name"]
    neon.delete_project(pid)


@pytest.fixture
def suite(project):
    pid, bid, bname = project
    s = neon.NeonToolSuite.init_for_bench(rc.ResultCollector(), pid, bid, bname, DB)
    s.exec(["DROP TABLE IF EXISTS h", "DROP TABLE IF EXISTS t",
            "CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(20), n INT)",
            "CREATE TABLE h (a INT, b VARCHAR(10))",
            "INSERT INTO t VALUES (1,'a',1),(2,'b',2),(3,'c',3)",
            "INSERT INTO h VALUES (1,'x')"], refs=[bname])
    yield s
    for b in s.list_branches():
        if b != bname:
            s.delete(b)
    s.close_connection()


def rows(suite, ref, sql):
    return [tuple(r) for r in suite.exec([sql], refs=[ref])[0].rows]


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


def test_branch_from_commit_and_per_ref_exec(suite):
    c1 = suite.commit("main", "base").value
    suite.exec(["UPDATE t SET v='later' WHERE id=1"], refs=["main"])
    suite.commit("main", "later")
    b = suite.branch("old", from_ref=f"main@{c1}")
    assert b.ok, b.error
    assert rows(suite, "old", "SELECT v FROM t WHERE id=1") == [("a",)]
    assert rows(suite, "main", "SELECT v FROM t WHERE id=1") == [("later",)]
    res = suite.exec(["SELECT 1"], refs=["main", "old"], mode="multi")[0]
    assert res.unsupported
    assert rows(suite, f"main@{c1}", "SELECT v FROM t WHERE id=1") == [("a",)]
    assert "old" in suite.list_branches()
    assert suite.delete("old").ok


def test_three_way_merge_and_conflicts(suite):
    suite.commit("main", "base")
    assert suite.branch("feat", "main").ok
    suite.exec(["UPDATE t SET v='feat' WHERE id=1", "INSERT INTO t VALUES (10,'ten',10)",
                "DELETE FROM t WHERE id=3", "INSERT INTO h VALUES (2,'y')",
                "ALTER TABLE t ADD COLUMN extra INT"], refs=["feat"])
    suite.commit("feat", "feature work SENTINEL")
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

    assert suite.branch("ff", "main").ok
    suite.exec(["INSERT INTO t (id, v, n) VALUES (30,'thirty',30)"], refs=["ff"])
    suite.commit("ff", "ff work")
    m2 = suite.merge("main", "ff")
    assert m2.ok, m2.error
    assert m2.value["fast_forward"] is True and m2.value["conflicts"] == 0


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
    assert got == {1: 13, 3: 3, 40: 40}
    m = suite.merge("main", "dev")
    assert m.ok, m.error
    assert m.value["conflicts"] == 0 and m.value["fast_forward"] is True
    assert {r[0]: r[1] for r in rows(suite, "main", "SELECT id, n FROM t")} == got


def test_revert_of_deleted_branch_commit_after_fast_forward(suite):
    suite.commit("main", "pre")
    assert suite.branch("cand", "main").ok
    suite.exec(["INSERT INTO t VALUES (7,'cand',7)", "UPDATE t SET v = 'sum' WHERE id = 1"], refs=["cand"])
    c = suite.commit("cand", "compaction")
    m = suite.merge("main", "cand")
    assert m.ok and m.value["fast_forward"] is True
    assert suite.delete("cand").ok
    r = suite.revert("main", c.value)
    assert r.ok, r.error
    got = dict(rows(suite, "main", "SELECT id, v FROM t"))
    assert 7 not in got and got[1] == "a"


def test_delete_parent_is_deferred(suite):
    suite.commit("main", "base")
    assert suite.branch("p", "main").ok
    cp = suite.commit("p", "p work").value
    assert suite.branch("q", f"p@{cp}").ok
    d = suite.delete("p")
    assert d.ok  # deferred until q is gone
    assert suite.delete("q").ok
    assert "p" not in suite.list_branches()


def test_reset_twice_to_an_older_commit(suite):
    """A restore re-parents the branch under its backup at the restore
    LSN; a second reset to an older commit must restore from that
    ancestor (Neon refuses it from the branch itself)."""
    b = suite.branch("twice", "main")
    assert b.ok, b.error
    suite.exec(["INSERT INTO t VALUES (10,'s1',1)"], refs=["twice"])
    c1 = suite.commit("twice", "s1")
    suite.exec(["INSERT INTO t VALUES (11,'s2',2)"], refs=["twice"])
    c2 = suite.commit("twice", "s2")
    suite.exec(["INSERT INTO t VALUES (12,'s3',3)"], refs=["twice"])
    c3 = suite.commit("twice", "s3")
    assert c1.ok and c2.ok and c3.ok
    r2 = suite.reset("twice", c2.value)
    assert r2.ok, r2.error
    assert [r[0] for r in rows(suite, "twice", "SELECT id FROM t WHERE id >= 10 ORDER BY id")] == [10, 11]
    r1 = suite.reset("twice", c1.value)
    assert r1.ok, r1.error
    assert [r[0] for r in rows(suite, "twice", "SELECT id FROM t WHERE id >= 10 ORDER BY id")] == [10]
    log = suite.log("twice", limit=5)
    assert log.ok and log.value[0]["message"] == "s1"


def test_column_type_clash_is_a_schema_conflict(suite):
    """The same column added with different types on both sides: the
    merge fails naming a schema conflict and changes nothing."""
    b = suite.branch("typeclash", "main")
    assert b.ok, b.error
    suite.exec(["ALTER TABLE t ADD COLUMN tier VARCHAR(8)", "UPDATE t SET tier = 'gold' WHERE id = 1"], refs=["typeclash"])
    suite.commit("typeclash", "branch adds tier varchar")
    suite.exec(["ALTER TABLE t ADD COLUMN tier INT"], refs=["main"])
    suite.commit("main", "main adds tier int")
    m = suite.merge("main", "typeclash", message="clash")
    assert m.failed and "schema conflict" in m.error.lower(), (m.status_name, m.error)
    assert [r[0] for r in rows(suite, "main", "SELECT data_type FROM information_schema.columns WHERE table_name = 't' AND column_name = 'tier'")] == ["integer"]
    assert rows(suite, "typeclash", "SELECT tier FROM t WHERE id = 1") == [("gold",)]
