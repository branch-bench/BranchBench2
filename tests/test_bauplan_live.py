"""Bauplan backend against a live account (skipped unless BAUPLAN_S3_URI is
set, e.g. in .env, with Bauplan credentials). Exercises every verb through
the public API. Slow: every write is an S3 upload plus Bauplan import jobs
(tens of seconds), so the data is seeded once and each test works on its own
branch from the seeded commit."""

import itertools

import pytest

from dblib import bauplan as bp
from dblib import result_collector as rc

DB = "bb_test_bauplan"

pytestmark = pytest.mark.skipif(not bp.BAUPLAN_S3_URI, reason="no Bauplan S3 bucket configured")

_n = itertools.count(1)


@pytest.fixture(scope="module")
def seeded():
    bp.setup_database(DB)
    s = bp.BauplanToolSuite.init_for_bench(rc.ResultCollector(), DB)
    res = s.exec(["CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(20), n INT)",
                  "CREATE TABLE h (a INT, b VARCHAR(10))",
                  "INSERT INTO t VALUES (1,'a',1),(2,'b',2),(3,'c',3)",
                  "INSERT INTO h VALUES (1,'x')"], refs=["main"])[0]
    assert res.ok, res.error
    base = s.commit("main", "seed").value
    yield s, base
    s.close_connection()
    bp.drop_database(DB)


@pytest.fixture
def env(seeded):
    """(suite, branch): a fresh branch from the seeded commit."""
    s, base = seeded
    name = f"w{next(_n)}"
    assert s.branch(name, f"main@{base}").ok
    return s, name


def rows(suite, ref, sql):
    res = suite.exec([sql], refs=[ref])[0]
    assert res.ok, res.error
    return [tuple(r) for r in res.rows]


def run(suite, ref, *stmts):
    res = suite.exec(list(stmts), refs=[ref])[0]
    assert res.ok, res.error
    return res


def test_dml_reports_rows_affected(env):
    s, b = env
    res = run(s, b, "UPDATE t SET n = n + 1")
    assert res.rows is None
    assert sorted(rows(s, b, "SELECT id, n FROM t")) == [(1, 2), (2, 3), (3, 4)]
    # Reads in the same script see its own writes.
    res = s.exec(lambda db: (db.sql("UPDATE t SET v = 'mine' WHERE id = 1"),
                             db.sql("SELECT v FROM t WHERE id = 1"))[1], refs=[b])[0]
    assert res.ok and [tuple(r) for r in res.value] == [("mine",)]


def test_commit_log_diff_reset_revert(env):
    s, b = env
    c1 = s.commit(b, "one")
    assert c1.ok and c1.value
    run(s, b, "UPDATE t SET v='a2' WHERE id=1", "INSERT INTO t VALUES (4,'d',4)",
        "DELETE FROM t WHERE id=3")
    c2 = s.commit(b, "two LOGSENTINEL-x")
    log = s.log(b, limit=10)
    assert log.ok and [e["message"] for e in log.value][:2] == ["two LOGSENTINEL-x", "one"]

    d = s.diff(f"{b}@{c1.value}", f"{b}@{c2.value}")
    assert d.ok, d.error
    assert (d.value["rows_added"], d.value["rows_deleted"], d.value["rows_modified"]) == (1, 1, 1)

    r = s.revert(b, c2.value)
    assert r.ok, r.error
    assert sorted(rows(s, b, "SELECT id, v FROM t")) == [(1, "a"), (2, "b"), (3, "c")]

    run(s, b, "UPDATE t SET v='zz'")
    rs = s.reset(b, c2.value)
    assert rs.ok, rs.error
    assert sorted(rows(s, b, "SELECT id, v FROM t")) == [(1, "a2"), (2, "b"), (4, "d")]
    # Commit refs stay readable after the branch was replaced.
    assert rows(s, f"{b}@{c1.value}", "SELECT v FROM t WHERE id = 1") == [("a",)]


def test_branch_from_commit_and_commit_refs_are_read_only(env):
    s, b = env
    c1 = s.commit(b, "base").value
    run(s, b, "UPDATE t SET v='later' WHERE id=1")
    old = f"{b}_old"
    assert s.branch(old, from_ref=f"{b}@{c1}").ok
    assert rows(s, old, "SELECT v FROM t WHERE id=1") == [("a",)]
    assert rows(s, b, "SELECT v FROM t WHERE id=1") == [("later",)]
    res = s.exec(["UPDATE t SET v = 'x'"], refs=[f"{b}@{c1}"])[0]
    assert res.failed and "read-only" in res.error
    assert old in s.list_branches()
    assert s.delete(old).ok
    assert old not in s.list_branches()


def test_merge_native_and_with_conflicts(env):
    s, b = env
    feat = f"{b}_feat"
    assert s.branch(feat, b).ok
    run(s, feat, "UPDATE t SET v='feat' WHERE id=1", "INSERT INTO t VALUES (10,'ten',10)",
        "DELETE FROM t WHERE id=3", "INSERT INTO h VALUES (2,'y')",
        "ALTER TABLE t ADD COLUMN extra INT")
    s.commit(feat, "feature work SENTINEL")
    # The base branch changes t too, so the native merge conflicts on t.
    run(s, b, "UPDATE t SET v='spine' WHERE id=2", "UPDATE t SET n=99 WHERE id=1",
        "INSERT INTO t VALUES (20,'twenty',20)")
    m = s.merge(b, feat, message="merge feat", on_conflict="theirs")
    assert m.ok, m.error
    assert m.value["native"] is False and m.value["conflicts"] == 1
    assert m.value["conflict_tables"] == ["t"]
    got = {r[0]: r[1:] for r in rows(s, b, "SELECT id, v, n FROM t")}
    assert got == {1: ("feat", 1), 2: ("spine", 2), 10: ("ten", 10), 20: ("twenty", 20)}
    assert sorted(rows(s, b, "SELECT a FROM h")) == [(1,), (2,)]  # copied: only feat changed h
    assert any("SENTINEL" in e["message"] for e in s.log(b, limit=10).value)

    ff = f"{b}_ff"
    assert s.branch(ff, b).ok
    run(s, ff, "INSERT INTO t (id, v, n) VALUES (30,'thirty',30)")
    m2 = s.merge(b, ff)
    assert m2.ok and m2.value["native"] is True and m2.value["fast_forward"] is True


def test_rebase_with_callable_resolver(env):
    s, b = env
    dev = f"{b}_dev"
    assert s.branch(dev, b).ok
    run(s, dev, "UPDATE t SET n = n + 5 WHERE id=1")
    s.commit(dev, "dev adds 5")
    run(s, b, "UPDATE t SET n = n + 7 WHERE id=1", "INSERT INTO t VALUES (40,'f',40)",
        "DELETE FROM t WHERE id=2")

    seen = {}

    def additive(db, conflicts):
        for cf in conflicts:
            seen[cf["table"]] = cf["rows"]
            for row in cf["rows"]:
                total = row["our_n"] + row["their_n"] - row["base_n"]
                db.sql("UPDATE t SET n = %s WHERE id = %s", (total, row["our_id"]))
            db.resolve(cf["table"])

    rb = s.rebase(dev, b, on_conflict=additive)
    assert rb.ok, rb.error
    assert rb.value["conflicts"] == 1 and seen["t"][0]["their_diff_type"] == "modified"
    got = {r[0]: r[1] for r in rows(s, dev, "SELECT id, n FROM t")}
    assert got == {1: 13, 3: 3, 40: 40}
    assert any(e["message"] == "dev adds 5" for e in s.log(dev, limit=20).value)
    m = s.merge(b, dev)
    assert m.ok and m.value["conflicts"] == 0 and m.value["fast_forward"] is True
    assert {r[0]: r[1] for r in rows(s, b, "SELECT id, n FROM t")} == got


def test_rebase_conflict_sides_follow_dolt(env):
    s, b = env
    for name in (f"{b}_up", f"{b}_br"):
        assert s.branch(name, b).ok
        run(s, name, "UPDATE t SET v = 'branch' WHERE id = 1")
    run(s, b, "UPDATE t SET v = 'upstream' WHERE id = 1")
    assert s.rebase(f"{b}_up", b, on_conflict="ours").ok
    assert s.rebase(f"{b}_br", b, on_conflict="theirs").ok
    assert rows(s, f"{b}_up", "SELECT v FROM t WHERE id = 1") == [("upstream",)]
    assert rows(s, f"{b}_br", "SELECT v FROM t WHERE id = 1") == [("branch",)]


def test_schema_conflict_fails_without_touching_data(env):
    s, b = env
    pk = f"{b}_pk"
    assert s.branch(pk, b).ok
    run(s, pk, "DROP TABLE t", "CREATE TABLE t (id INT, v VARCHAR(20), n INT, PRIMARY KEY (id, n))",
        "INSERT INTO t VALUES (1,'a',1)")
    run(s, b, "UPDATE t SET v = 'mine' WHERE id = 2")
    m = s.merge(b, pk)
    assert m.failed and "schema conflict" in m.error
    assert rows(s, b, "SELECT v FROM t WHERE id = 1") == [("a",)]


def test_publish_conflict_reruns_the_script(env, seeded):
    s, b = env
    other = bp.BauplanToolSuite.init_for_bench(rc.ResultCollector(), DB)
    calls = []

    def script(db):
        calls.append(1)
        db.sql("UPDATE t SET v = 'A' WHERE id = 1")
        if len(calls) == 1:
            # Another writer changes the same table before this one publishes.
            assert other.exec(["UPDATE t SET n = 99 WHERE id = 2"], refs=[b])[0].ok

    res = s.exec(script, refs=[b])[0]
    assert res.ok, res.error
    assert len(calls) == 2
    got = {r[0]: r[1:] for r in rows(s, b, "SELECT id, v, n FROM t")}
    assert got[1][0] == "A" and got[2][1] == 99
    other.close_connection()


def test_nothing_left_after_drop():
    db = DB + "_drop"
    bp.setup_database(db)
    s = bp.BauplanToolSuite.init_for_bench(rc.ResultCollector(), db)
    run(s, "main", "CREATE TABLE t (id INT PRIMARY KEY)")
    c = s.commit("main", "c").value
    assert s.branch("b", "main").ok
    assert s.reset("main", c).ok
    s.close_connection()
    bp.drop_database(db)
    client = bp.new_client()
    root = bp.root_branch(db, client)
    assert [x.name for x in client.get_branches(name=root) if x.name.startswith(root)] == []
    assert [x.name for x in client.get_tags(filter_by_name=root) if x.name.startswith(root)] == []
