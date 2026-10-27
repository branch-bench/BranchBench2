"""chronos backend against a live PostgreSQL server (skipped when none
answers on PGSQL_HOST:PGSQL_PORT or chronos_core is not installed).
Exercises every verb through the public API plus the SQL shim."""

import pytest

from dblib import result_collector as rc
from dblib import chronos as chronos

DB = "bb_test_chronos"
MAIN = chronos.MAIN_BRANCH

pytestmark = pytest.mark.skipif(not chronos.server_up(), reason="no PostgreSQL server / chronos_core")


@pytest.fixture
def suite():
    chronos.setup_database(DB)
    s = chronos.ChronosToolSuite.init_for_bench(rc.ResultCollector(), DB, MAIN)
    s.exec(["CREATE TABLE t (id INT NOT NULL, v VARCHAR(20), n INT, PRIMARY KEY (id))",
            "CREATE TABLE h (a INT, b VARCHAR(10))",
            "CREATE TABLE u (id INT PRIMARY KEY, t_id INT REFERENCES t(id), x NUMERIC(10,2))",
            "INSERT INTO t VALUES (1,'a',1),(2,'b',2),(3,'c',3)",
            "INSERT INTO h VALUES (1,'x')",
            "INSERT INTO u VALUES (10,1,1.5)"], refs=[MAIN])
    yield s
    s.close_connection()
    chronos.drop_database(DB)


def rows(suite, ref, sql, vars=None):
    r = suite.exec([(sql, vars)] if vars else [sql], refs=[ref])[0]
    assert r.ok, r.error
    return [tuple(x) for x in r.rows]


def ok(res):
    assert res.ok, res.error
    return res


def test_implementation_summary():
    impl = chronos.ChronosToolSuite.implementation()
    assert impl["branch"] == "native" and impl["merge"] == "simulated"
    assert impl["delete"] == "native" and impl["commit_refs"] == "native"
    assert impl["multi_ref_exec"] == "simulated"
    assert set(impl) >= {"branch", "commit", "diff", "log", "merge", "rebase", "revert",
                         "reset", "delete", "commit_refs", "multi_ref_exec"}


def test_adoption_and_sql_shim(suite):
    # Plain mode until the first verb; the commit adopts every table.
    c0 = ok(suite.commit(MAIN, "seed")).value
    assert suite.adopted
    assert sorted(rows(suite, MAIN, "SELECT id, v FROM t ORDER BY id")) == [(1, "a"), (2, "b"), (3, "c")]
    # keyless table got a synthetic key; inserts keep working without naming it
    assert rows(suite, MAIN, "SELECT COUNT(*) FROM h") == [(1,)]
    ok(suite.exec([("INSERT INTO h (a, b) VALUES (%s, %s)", (2, "y")), "INSERT INTO h (a, b) VALUES (3, 'z')"],
                  refs=[MAIN])[0])
    assert rows(suite, MAIN, "SELECT COUNT(*) FROM h") == [(3,)]
    # positional params, expressions Chronos evaluates itself
    ok(suite.exec([("UPDATE t SET n = n + %s, v = %s WHERE id = %s", (10, "a2", 1))], refs=[MAIN])[0])
    assert rows(suite, MAIN, "SELECT n, v FROM t WHERE id = %s", (1,)) == [(11, "a2")]
    # statements Chronos rejects are simulated: NOW(), BETWEEN, subqueries, INSERT ... SELECT
    ok(suite.exec(["UPDATE t SET v = 'late' WHERE id BETWEEN 2 AND 3",
                   "UPDATE u SET x = x * 2 WHERE t_id IN (SELECT id FROM t WHERE n > 5)",
                   "INSERT INTO t (id, v, n) SELECT id + 100, v, n FROM t WHERE id = 1",
                   "DELETE FROM t WHERE id = (SELECT MAX(id) FROM t)"], refs=[MAIN])[0])
    assert rows(suite, MAIN, "SELECT id, v FROM t ORDER BY id") == [(1, "a2"), (2, "late"), (3, "late")]
    assert rows(suite, MAIN, "SELECT x FROM u") == [(3.0,)] or rows(suite, MAIN, "SELECT x::float FROM u") == [(3.0,)]
    assert suite.OBSERVED.get("simulated_update", 0) >= 1
    # DDL normalisation: table-level PK + NOT NULL + FK, index, view
    ok(suite.exec(["CREATE TABLE audit (audit_id INT NOT NULL, t_id INT REFERENCES t(id), note VARCHAR(20) "
                   "DEFAULT 'none', PRIMARY KEY (audit_id))",
                   "INSERT INTO audit (audit_id, t_id) VALUES (1, 1)",
                   "ALTER TABLE audit ADD COLUMN extra VARCHAR(8) NOT NULL DEFAULT 'e'",
                   "CREATE INDEX IF NOT EXISTS idx_audit ON audit (t_id)",
                   "CREATE VIEW audit_v AS SELECT audit_id, note FROM audit WHERE t_id = 1"], refs=[MAIN])[0])
    assert rows(suite, MAIN, "SELECT note, extra FROM audit") == [("none", "e")]
    assert rows(suite, MAIN, "SELECT COUNT(*) FROM audit_v") == [(1,)]
    # transactions: rollback undoes, commit keeps
    r = suite.exec("def run(db):\n    db.sql('BEGIN'); db.sql(\"UPDATE t SET v = 'tx' WHERE id = 1\")\n"
                   "    assert db.sql('SELECT v FROM t WHERE id = 1')[0][0] == 'tx'\n    db.sql('ROLLBACK')",
                   refs=[MAIN])[0]
    assert r.ok, r.error
    assert rows(suite, MAIN, "SELECT v FROM t WHERE id = 1") == [("a2",)]
    log = ok(suite.log(MAIN)).value
    assert log[0]["commit_hash"] == c0 and log[0]["message"] == "seed"


def test_commit_log_diff_reset_revert(suite):
    c1 = ok(suite.commit(MAIN, "one")).value
    suite.exec(["UPDATE t SET v='a2' WHERE id=1", "INSERT INTO t (id, v, n) VALUES (4,'d',4)",
                "DELETE FROM t WHERE id=3"], refs=[MAIN])
    c2 = ok(suite.commit(MAIN, "two")).value
    log = ok(suite.log(MAIN, limit=10)).value
    assert [e["message"] for e in log][:2] == ["two", "one"]
    d = ok(suite.diff(f"{MAIN}@{c1}", f"{MAIN}@{c2}")).value
    assert (d["rows_added"], d["rows_deleted"], d["rows_modified"]) == (1, 1, 1)
    ok(suite.revert(MAIN, c2))
    assert sorted(rows(suite, MAIN, "SELECT id, v FROM t")) == [(1, "a"), (2, "b"), (3, "c")]
    assert suite.log(MAIN, limit=1).value[0]["message"].startswith("revert ")
    suite.exec(["UPDATE t SET v='zz'", "CREATE TABLE later (id INT PRIMARY KEY)"], refs=[MAIN])
    ok(suite.reset(MAIN, c2))
    assert sorted(rows(suite, MAIN, "SELECT id, v FROM t")) == [(1, "a2"), (2, "b"), (4, "d")]
    assert [e["message"] for e in suite.log(MAIN).value][0] == "two"
    assert suite.exec(["SELECT * FROM later"], refs=[MAIN])[0].failed


def test_commit_ref_read_only_and_branch_from_it(suite):
    c1 = ok(suite.commit(MAIN, "base")).value
    r = suite.exec(["UPDATE t SET v='nope' WHERE id=1"], refs=[f"{MAIN}@{c1}"])[0]
    assert r.failed and "read-only" in r.error
    suite.exec(["UPDATE t SET v='moved' WHERE id=1"], refs=[MAIN])
    assert rows(suite, f"{MAIN}@{c1}", "SELECT v FROM t WHERE id=1") == [("a",)]
    ok(suite.branch("child", from_ref=f"{MAIN}@{c1}"))
    assert rows(suite, "child", "SELECT v FROM t WHERE id=1") == [("a",)]
    ok(suite.exec(["UPDATE t SET v='yes' WHERE id=1"], refs=["child"])[0])
    assert rows(suite, "child", "SELECT v FROM t WHERE id=1") == [("yes",)]
    assert rows(suite, MAIN, "SELECT v FROM t WHERE id=1") == [("moved",)]
    assert set(suite.list_branches()) == {MAIN, "child"}


def test_multi_ref_exec(suite):
    c1 = ok(suite.commit(MAIN, "base")).value
    ok(suite.branch("b1", from_ref=MAIN))
    suite.exec(["UPDATE t SET n = 100 WHERE id = 1"], refs=["b1"])
    suite.exec(["UPDATE t SET n = 50 WHERE id = 1"], refs=[MAIN])

    def script(db):
        a = db.sql(f"SELECT n FROM {db.table('b1', 't')} WHERE id = 1")[0][0]
        b = db.sql(f"SELECT n FROM {db.table(f'{MAIN}@{c1}', 't')} WHERE id = 1")[0][0]
        c = db.sql("SELECT n FROM t WHERE id = 1")[0][0]
        return (a, b, c)
    r = suite.exec(script, refs=[MAIN, "b1", f"{MAIN}@{c1}"], mode="multi")[0]
    assert r.ok, r.error
    assert r.value == (100, 1, 50)


def test_three_way_merge_with_conflicts(suite):
    ok(suite.commit(MAIN, "base"))
    ok(suite.branch("feat", from_ref=MAIN))
    suite.exec(["UPDATE t SET v = 'feat' WHERE id = 1", "INSERT INTO t (id, v, n) VALUES (5, 'e', 5)",
                "DELETE FROM t WHERE id = 3", "CREATE TABLE nt (id INT PRIMARY KEY, w INT)",
                "INSERT INTO nt (id, w) VALUES (1, 1)", "ALTER TABLE t ADD COLUMN c1 INT DEFAULT 7"], refs=["feat"])
    suite.exec(["UPDATE t SET v = 'main' WHERE id = 1", "UPDATE t SET n = 20 WHERE id = 2"], refs=[MAIN])
    m = ok(suite.merge(MAIN, "feat", "merge feat"))
    assert m.value["conflicts"] == 1 and m.value["resolved"] == "ours"
    assert sorted(rows(suite, MAIN, "SELECT id, v, n FROM t")) == [(1, "main", 1), (2, "b", 20), (5, "e", 5)]
    assert rows(suite, MAIN, "SELECT c1 FROM t WHERE id = 2") == [(7,)]
    assert rows(suite, MAIN, "SELECT w FROM nt") == [(1,)]
    assert suite.log(MAIN, limit=1).value[0]["message"] == "merge feat"
    # a second merge after both sides moved: base is the sync point
    suite.exec(["UPDATE t SET v = 'feat2' WHERE id = 5"], refs=["feat"])
    suite.exec(["UPDATE t SET v = 'main2' WHERE id = 2"], refs=[MAIN])
    m2 = ok(suite.merge(MAIN, "feat", "again", on_conflict="theirs"))
    assert m2.value["conflicts"] == 0
    assert sorted(rows(suite, MAIN, "SELECT id, v FROM t")) == [(1, "main"), (2, "main2"), (5, "feat2")]


def test_theirs_policy_and_callable_resolver(suite):
    ok(suite.commit(MAIN, "base"))
    ok(suite.branch("b", from_ref=MAIN))
    suite.exec(["UPDATE t SET n = 10 WHERE id = 1", "UPDATE t SET n = 10 WHERE id = 2"], refs=["b"])
    suite.exec(["UPDATE t SET n = 5 WHERE id = 1", "UPDATE t SET n = 5 WHERE id = 2"], refs=[MAIN])

    def resolver(db, conflicts):
        out = {}
        for cf in conflicts:
            assert cf["table"] == "t"
            for row in cf["rows"]:
                assert row["our_diff_type"] == "modified" and row["their_diff_type"] == "modified"
                total = int(row["our_n"]) + int(row["their_n"]) - int(row["base_n"])
                db.sql("UPDATE t SET n = %s WHERE id = %s", (total, row["our_id"]))
            db.resolve(cf["table"])
            out[cf["table"]] = len(cf["rows"])
        return out
    m = ok(suite.merge(MAIN, "b", on_conflict=resolver))
    assert m.value["conflicts"] == 2 and m.value["resolution"] == {"t": 2}
    assert sorted(rows(suite, MAIN, "SELECT id, n FROM t")) == [(1, 14), (2, 13), (3, 3)]
    ok(suite.branch("c", from_ref=MAIN))
    suite.exec(["UPDATE t SET n = 1 WHERE id = 3"], refs=["c"])
    suite.exec(["UPDATE t SET n = 2 WHERE id = 3"], refs=[MAIN])
    m = ok(suite.merge(MAIN, "c", on_conflict="theirs"))
    assert m.value["conflicts"] == 1
    assert rows(suite, MAIN, "SELECT n FROM t WHERE id = 3") == [(1,)]


def test_rebase(suite):
    ok(suite.commit(MAIN, "base"))
    ok(suite.branch("b", from_ref=MAIN))
    suite.exec(["UPDATE t SET v = 'branch' WHERE id = 1", "INSERT INTO t (id, v, n) VALUES (9, 'i', 9)"], refs=["b"])
    suite.exec(["UPDATE t SET v = 'up' WHERE id = 1", "UPDATE t SET v = 'up2' WHERE id = 2",
                "CREATE TABLE upt (id INT PRIMARY KEY)"], refs=[MAIN])
    ok(suite.commit(MAIN, "upstream"))
    # "theirs" keeps the branch's own change on a conflict
    r = ok(suite.rebase("b", MAIN, on_conflict="theirs"))
    assert r.value["conflicts"] == 1 and not r.value["up_to_date"]
    assert sorted(rows(suite, "b", "SELECT id, v FROM t")) == [(1, "branch"), (2, "up2"), (3, "c"), (9, "i")]
    assert rows(suite, "b", "SELECT COUNT(*) FROM upt") == [(0,)]
    assert [e["message"] for e in suite.log("b", limit=3).value][:2] == ["rebase onto main", "upstream"]
    r2 = ok(suite.rebase("b", MAIN))
    assert r2.value["up_to_date"]
    # "ours" (the upstream) wins
    suite.exec(["UPDATE t SET v = 'b3' WHERE id = 3"], refs=["b"])
    suite.exec(["UPDATE t SET v = 'm3' WHERE id = 3"], refs=[MAIN])
    r3 = ok(suite.rebase("b", MAIN, on_conflict="ours"))
    assert r3.value["conflicts"] == 1
    assert rows(suite, "b", "SELECT v FROM t WHERE id = 3") == [("m3",)]


def test_schema_conflict_fails_merge(suite):
    ok(suite.commit(MAIN, "base"))
    ok(suite.branch("b", from_ref=MAIN))
    suite.exec(["ALTER TABLE t ADD COLUMN tier INT"], refs=["b"])
    suite.exec(["ALTER TABLE t ADD COLUMN tier VARCHAR(8)"], refs=[MAIN])
    m = suite.merge(MAIN, "b")
    assert m.failed and "schema conflict" in m.error


def test_delete_defers_while_children_live(suite):
    ok(suite.commit(MAIN, "base"))
    ok(suite.branch("p", from_ref=MAIN))
    ok(suite.branch("q", from_ref="p"))
    suite.exec(["UPDATE t SET v = 'q' WHERE id = 1"], refs=["q"])
    ok(suite.delete("p"))
    assert set(suite.list_branches()) == {MAIN, "q"}
    assert rows(suite, "q", "SELECT v FROM t WHERE id = 1") == [("q",)]
    ok(suite.delete("q"))
    assert set(suite.list_branches()) == {MAIN}
    assert suite.OBSERVED.get("deferred_deletes_done", 0) >= 1
    assert suite.delete(MAIN).failed


def test_revert_of_commit_on_fork(suite):
    ok(suite.commit(MAIN, "base"))
    ok(suite.branch("b", from_ref=MAIN))
    suite.exec(["UPDATE t SET v = 'x' WHERE id = 1"], refs=["b"])
    c = ok(suite.commit("b", "change")).value
    suite.exec(["UPDATE t SET v = 'y' WHERE id = 2"], refs=["b"])
    ok(suite.revert("b", c))
    assert sorted(rows(suite, "b", "SELECT id, v FROM t")) == [(1, "a"), (2, "y"), (3, "c")]
    assert ok(suite.diff(MAIN, "b")).value["rows_modified"] == 1


def test_storage_and_gc(suite):
    ok(suite.commit(MAIN, "base"))
    assert suite._storage_bytes() > 0
    assert suite.gc().ok


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
