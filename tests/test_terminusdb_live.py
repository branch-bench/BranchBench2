"""TerminusDB backend. The parsing tests need no server; the rest run
against a live one (skipped when none answers at TERMINUSDB_URL)."""

import pytest

from dblib import result_collector as rc
from dblib import terminusdb
from dblib.terminusdb import NotSupportedError
from util import db_helpers as dbh

DB = "bb_test_terminusdb"

DUMP = """
-- a pg_dump-style file
SET client_encoding = 'UTF8';
CREATE TABLE public.t (
    id integer NOT NULL,
    v character varying(20),
    n numeric(6,2),
    at timestamp without time zone
);
CREATE TABLE public.pair (
    a integer NOT NULL,
    b integer NOT NULL,
    note text
);
CREATE TABLE h (x INT, y VARCHAR(10));
COPY public.t (id, v, n, at) FROM stdin;
1\ta\t1.50\t2024-01-02 03:04:05
2\tb\\tb\t\\N\t\\N
3\tc\t3.00\t2024-01-02 03:04:05.5
\\.
COPY public.pair (a, b, note) FROM stdin;
1\t1\tx
1\t2\ty
2\t1\tz
\\.
INSERT INTO h (x, y) VALUES (1, 'p'), (2, NULL);
ALTER TABLE ONLY public.t
    ADD CONSTRAINT t_pkey PRIMARY KEY (id);
ALTER TABLE pair ADD PRIMARY KEY (a, b);
"""

needs_server = pytest.mark.skipif(not terminusdb.server_up(), reason="no TerminusDB server")


# ---------------------------------------------------------------------------
# No server needed
# ---------------------------------------------------------------------------


def test_dump_is_read_as_tables_and_rows(tmp_path):
    path = tmp_path / "dump.sql"
    path.write_text(DUMP)
    tables, copies = {}, {}
    for kind, item in terminusdb._statements(str(path)):
        if kind == "copy":
            copies[item[0]] = [line.split("\t") for line in item[2]]
        else:
            table = terminusdb._parse_create(item)
            if table:
                tables[table.name] = table
    assert tables["t"].columns == [("id", "integer"), ("v", "character varying(20)"),
                                   ("n", "numeric(6,2)"), ("at", "timestamp without time zone")]
    assert tables["t"].xsd == {"id": "xsd:integer", "v": "xsd:string", "n": "xsd:decimal",
                               "at": "xsd:dateTime"}
    assert len(copies["t"]) == 3 and len(copies["pair"]) == 3
    assert terminusdb._copy_value("b\\tb") == "b\tb" and terminusdb._copy_value("\\N") is None


def test_statement_shapes_are_recognised():
    plan = terminusdb._parse("SELECT * FROM pair WHERE (a, b) >= (%(s0)s, %(s1)s) AND (a, b) <= (%(e0)s, %(e1)s);")
    assert (plan.kind, plan.table, plan.star) == ("select", "pair", True)
    assert plan.where == [(["a", "b"], ">=", [("param", "s0"), ("param", "s1")]),
                          (["a", "b"], "<=", [("param", "e0"), ("param", "e1")])]

    plan = terminusdb._parse("UPDATE t SET v = %(v)s, n = 3 WHERE id = %(id)s")
    assert plan.sets == [("v", ("param", "v")), ("n", ("lit", 3))]
    assert plan.where == [(["id"], "=", [("param", "id")])]

    plan = terminusdb._parse("DELETE FROM t WHERE id = %s")
    assert plan.where == [(["id"], "=", [("param", "_p0")])]


@pytest.mark.parametrize("sql", [
    "SELECT * FROM t JOIN pair ON t.id = pair.a",
    "SELECT v, COUNT(*) FROM t GROUP BY v",
    "SELECT * FROM t ORDER BY id",
    "UPDATE t SET n = n + 1 WHERE id = 1",
    "SELECT * FROM t WHERE v LIKE 'a%'",
    "CREATE INDEX i ON t (v)",
])
def test_statements_without_a_mapping_are_refused(sql):
    with pytest.raises(NotSupportedError):
        terminusdb._parse(sql)


# ---------------------------------------------------------------------------
# Live server
# ---------------------------------------------------------------------------


@pytest.fixture
def suite(tmp_path):
    path = tmp_path / "dump.sql"
    path.write_text(DUMP)
    terminusdb.setup_database(DB, str(path))
    s = terminusdb.TerminusDBToolSuite.init_for_bench(rc.ResultCollector(), DB, "main")
    yield s
    s.close_connection()
    terminusdb.drop_database(DB)


def rows(suite, ref, sql, params=None):
    result = suite.exec([(sql, params)], refs=[ref], timed=False)[0]
    result.raise_for_status()
    return sorted(result.rows) if result.rows is not None else None


@needs_server
def test_load_and_catalog(suite):
    conn = suite.get_current_connection()
    assert dbh.get_all_tables(conn) == ["h", "pair", "t"]
    assert dbh.get_pk_column_names(conn, "pair") == ["a", "b"]
    assert dbh.get_all_columns(conn, "t") == ["id", "v", "n", "at"]
    assert sorted(dbh.get_pk_values(conn, "pair")) == [(1, 1), (1, 2), (2, 1)]
    assert suite.get_table_schema("t").startswith("CREATE TABLE t (\n  id integer,")
    assert rows(suite, "main", "SELECT * FROM t WHERE id = %s", (2,)) == [(2, "b\tb", None, None)]
    assert rows(suite, "main", "SELECT COUNT(*) FROM h") == [(2,)]


@needs_server
def test_point_and_range_statements(suite):
    insert = "INSERT INTO t (id, v, n, at) VALUES (%(id)s, %(v)s, %(n)s, %(at)s)"
    assert rows(suite, "main", insert, {"id": 4, "v": "d", "n": 4, "at": None}) is None
    assert rows(suite, "main", "SELECT id, v FROM t WHERE id = %s", (4,)) == [(4, "d")]
    assert rows(suite, "main", "SELECT id FROM t WHERE id = %s", (99,)) == []

    # every non-key column given: a document replace
    rows(suite, "main", "UPDATE t SET v = %(v)s, n = %(n)s, at = %(at)s WHERE id = %(id)s",
         {"id": 4, "v": "D", "n": None, "at": None})
    assert rows(suite, "main", "SELECT v, n FROM t WHERE id = 4") == [("D", None)]
    # one column: a WOQL update of that property
    rows(suite, "main", "UPDATE t SET v = %s WHERE id = %s", ("dd", 4))
    assert rows(suite, "main", "SELECT v FROM t WHERE id = 4") == [("dd",)]

    span = {"s0": 1, "s1": 2, "e0": 2, "e1": 1}
    where = "(a, b) >= (%(s0)s, %(s1)s) AND (a, b) <= (%(e0)s, %(e1)s)"
    assert rows(suite, "main", f"SELECT a, b FROM pair WHERE {where}", span) == [(1, 2), (2, 1)]
    rows(suite, "main", f"UPDATE pair SET note = %(note)s WHERE {where}", {**span, "note": "r"})
    assert rows(suite, "main", "SELECT a, b, note FROM pair") == [(1, 1, "x"), (1, 2, "r"), (2, 1, "r")]

    rows(suite, "main", "DELETE FROM t WHERE id = %s", (4,))
    rows(suite, "main", "DELETE FROM pair WHERE a = %s", (1,))
    assert rows(suite, "main", "SELECT id FROM t") == [(1,), (2,), (3,)]
    assert rows(suite, "main", "SELECT a, b FROM pair") == [(2, 1)]


@needs_server
def test_statement_without_a_mapping_fails_the_exec(suite):
    result = suite.exec(["SELECT * FROM t ORDER BY id"], refs=["main"], timed=False)[0]
    assert result.failed and "NotSupportedError" in result.error


@needs_server
def test_branch_commit_log_diff_reset_revert_delete(suite):
    assert suite.branch("dev", "main").ok
    rows(suite, "dev", "INSERT INTO t (id, v) VALUES (%s, %s)", (10, "x"))
    assert rows(suite, "main", "SELECT id FROM t WHERE id = 10") == []
    c1 = suite.commit("dev", "one")
    assert c1.ok and c1.value

    rows(suite, "dev", "INSERT INTO t (id, v) VALUES (%s, %s)", (11, "y"))
    rows(suite, "dev", "UPDATE t SET v = %s WHERE id = %s", ("a2", 1))
    rows(suite, "dev", "DELETE FROM t WHERE id = %s", (3,))
    c2 = suite.commit("dev", "two")
    log = suite.log("dev", limit=3)
    assert log.ok and log.value[0]["hash"] == c2.value and len(log.value) == 3

    d = suite.diff(f"dev@{c1.value}", f"dev@{c2.value}")
    assert (d.value["rows_added"], d.value["rows_deleted"], d.value["rows_modified"]) == (1, 1, 1)
    # a commit ref reads the old state and refuses writes
    assert rows(suite, f"dev@{c1.value}", "SELECT id FROM t") == [(1,), (2,), (3,), (10,)]
    assert suite.exec([("DELETE FROM t WHERE id = %s", (1,))], refs=[f"dev@{c1.value}"], timed=False)[0].failed

    # revert undoes all three writes commit "two" covers
    assert suite.revert("dev", c2.value).ok
    assert rows(suite, "dev", "SELECT id, v FROM t") == [(1, "a"), (2, "b\tb"), (3, "c"), (10, "x")]

    assert suite.reset("dev", c2.value).ok
    assert rows(suite, "dev", "SELECT id, v FROM t") == [(1, "a2"), (2, "b\tb"), (10, "x"), (11, "y")]

    assert suite.branch("old", f"dev@{c1.value}").ok
    assert rows(suite, "old", "SELECT id FROM t") == [(1,), (2,), (3,), (10,)]
    assert suite.delete("old").ok and sorted(suite.list_branches()) == ["dev", "main"]


@needs_server
def test_merge_and_rebase(suite):
    suite.branch("dev", "main")
    rows(suite, "dev", "INSERT INTO t (id, v) VALUES (%s, %s)", (10, "dev"))
    rows(suite, "main", "INSERT INTO t (id, v) VALUES (%s, %s)", (20, "main"))
    m = suite.merge("main", "dev", "first")
    assert m.ok and m.value["conflicts"] == 0
    assert rows(suite, "main", "SELECT id FROM t") == [(1,), (2,), (3,), (10,), (20,)]

    # a second merge only carries what the source added since the first
    rows(suite, "dev", "INSERT INTO t (id, v) VALUES (%s, %s)", (11, "dev"))
    assert suite.merge("main", "dev").ok
    assert rows(suite, "main", "SELECT id FROM t WHERE id >= %s", (10,)) == [(10,), (11,), (20,)]

    rows(suite, "dev", "UPDATE t SET v = %s WHERE id = %s", ("dev", 1))
    rows(suite, "dev", "UPDATE t SET v = %s WHERE id = %s", ("dev", 2))
    rows(suite, "main", "UPDATE t SET v = %s WHERE id = %s", ("main", 1))
    ours = suite.merge("main", "dev", on_conflict="ours")
    assert ours.ok and ours.value["conflicts"] == 1
    assert rows(suite, "main", "SELECT id, v FROM t WHERE id <= %s", (2,)) == [(1, "main"), (2, "dev")]

    rows(suite, "dev", "UPDATE t SET v = %s WHERE id = %s", ("dev2", 2))
    rows(suite, "main", "UPDATE t SET v = %s WHERE id = %s", ("main2", 2))
    theirs = suite.merge("main", "dev", on_conflict="theirs")
    assert theirs.ok and theirs.value["conflicts"] == 1
    assert rows(suite, "main", "SELECT v FROM t WHERE id = 2") == [("dev2",)]

    suite.branch("feat", "main")
    rows(suite, "feat", "INSERT INTO t (id, v) VALUES (%s, %s)", (30, "feat"))
    rows(suite, "main", "INSERT INTO t (id, v) VALUES (%s, %s)", (31, "main"))
    rb = suite.rebase("feat", "main")
    assert rb.ok, rb.error
    assert rows(suite, "feat", "SELECT id FROM t WHERE id >= %s", (30,)) == [(30,), (31,)]
    assert rows(suite, "main", "SELECT id FROM t WHERE id >= %s", (30,)) == [(31,)]


@needs_server
def test_capabilities(suite):
    caps = suite.capabilities()
    assert all(caps[v] for v in ("branch", "commit", "diff", "log", "merge", "rebase",
                                 "revert", "reset", "delete", "commit_refs"))
    assert not caps["multi_ref_exec"] and not caps["exec_async"]
    impl = suite.implementation()
    assert (impl["commit"], impl["merge"], impl["revert"], impl["rebase"]) == (
        "simulated", "composed", "composed", "native")
