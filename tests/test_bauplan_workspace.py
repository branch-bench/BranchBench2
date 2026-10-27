"""The Bauplan backend's local parts (no account needed): SQL rewriting, the
DuckDB three-way merge, revert and diff."""

import datetime
import decimal

import duckdb
import pyarrow as pa

from dblib import bauplan as bp


def test_to_duckdb_placeholders():
    assert bp.to_duckdb("SELECT * FROM t WHERE a = %s AND b = %s", (1, 2)) == \
        ("SELECT * FROM t WHERE a = ? AND b = ?", [1, 2])
    assert bp.to_duckdb("UPDATE t SET v = %(v)s WHERE k LIKE 'x%%'", {"v": 3}) == \
        ("UPDATE t SET v = $v WHERE k LIKE 'x%'", {"v": 3})
    # Without parameters nothing is substituted, as with the DB-API drivers.
    assert bp.to_duckdb("SELECT '%s'", None) == ("SELECT '%s'", None)


def test_inline_literals():
    sql = bp.inline("SELECT %s, %s, %s, %s, %s, %s, %s",
                    (None, True, 3, decimal.Decimal("1.50"), "it's",
                     datetime.datetime(2026, 1, 2, 3, 4, 5), datetime.date(2026, 1, 2)))
    assert sql == ("SELECT NULL, TRUE, 3, 1.50, 'it''s', "
                   "TIMESTAMP '2026-01-02 03:04:05', DATE '2026-01-02'")
    utc = datetime.datetime(2026, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)
    assert bp.inline("SELECT %(t)s", {"t": utc}) == "SELECT TIMESTAMP '2026-01-02 03:04:05'"


def test_statement_tables():
    tables, target = bp.statement_tables(
        "WITH x AS (SELECT * FROM orders) SELECT * FROM x JOIN Customer c ON TRUE")
    assert tables == {"orders", "customer"} and target is None
    assert bp.statement_tables("UPDATE stock SET q = 1 WHERE i IN (SELECT i FROM item)")[1] == "stock"
    assert bp.statement_tables("INSERT INTO t (a) SELECT a FROM s")[1] == "t"
    assert bp.statement_tables("CREATE TABLE IF NOT EXISTS n (a INT, PRIMARY KEY (a))")[1] == "n"
    assert bp.statement_tables("ALTER TABLE t ADD COLUMN c INT")[1] == "t"
    assert bp.statement_tables("DROP TABLE IF EXISTS t")[1] == "t"


def test_normalize_timestamps_and_strings():
    t = pa.table({"ts": pa.array([datetime.datetime(2026, 1, 1, 12)],
                                 type=pa.timestamp("us", tz="UTC")),
                  "s": pa.array(["x"], type=pa.large_string())})
    n = bp.normalize(t)
    assert n.schema.field("ts").type == pa.timestamp("us")
    assert n.schema.field("s").type == pa.string()
    assert n.column("ts").to_pylist() == [datetime.datetime(2026, 1, 1, 12)]


SCHEMA = {"columns": [["id", "INTEGER"], ["v", "VARCHAR"], ["n", "INTEGER"]], "pk": ["id"]}


def table(rows):
    return pa.table({"id": [r[0] for r in rows], "v": [r[1] for r in rows],
                     "n": pa.array([r[2] for r in rows], type=pa.int32())})


def ours_db(rows, schema=SCHEMA):
    db = duckdb.connect()
    bp.create_local(db, "t", table(rows), schema)
    return db


def contents(db):
    return {r[0]: r[1:] for r in db.execute("SELECT id, v, n FROM t").fetchall()}


BASE = [(1, "a", 1), (2, "b", 2), (3, "c", 3)]


def test_create_local_enforces_primary_key():
    db = ours_db(BASE)
    assert bp.local_schema(db, "t")["pk"] == ["id"]
    try:
        db.execute("INSERT INTO t VALUES (1, 'dup', 0)")
        raise AssertionError("duplicate key accepted")
    except duckdb.ConstraintException:
        pass


def test_three_way_applies_non_conflicting_changes():
    # Ours changed 2; theirs changed 1, deleted 3 and added 4.
    db = ours_db([(1, "a", 1), (2, "ours", 2), (3, "c", 3)])
    theirs = table([(1, "theirs", 1), (2, "b", 2), (4, "d", 4)])
    info = bp.three_way_local(db, "t", table(BASE), theirs, ["id"], "ours")
    assert info["conflicts"] == 0
    assert contents(db) == {1: ("theirs", 1), 2: ("ours", 2), 4: ("d", 4)}


def test_three_way_conflicts_per_policy():
    ours = [(1, "ours", 1), (2, "b", 2), (3, "c", 3)]
    theirs = table([(1, "theirs", 1), (2, "b", 2), (3, "c", 3), (5, "e", 5)])
    db = ours_db(ours)
    info = bp.three_way_local(db, "t", table(BASE), theirs, ["id"], "ours")
    assert info["conflicts"] == 1 and contents(db)[1] == ("ours", 1) and 5 in contents(db)
    db = ours_db(ours)
    bp.three_way_local(db, "t", table(BASE), theirs, ["id"], "theirs")
    assert contents(db)[1] == ("theirs", 1)
    db = ours_db(ours)
    info = bp.three_way_local(db, "t", table(BASE), theirs, ["id"], lambda db, c: None)
    row = info["rows"][0]
    assert (row["base_v"], row["our_v"], row["their_v"]) == ("a", "ours", "theirs")
    assert row["our_diff_type"] == row["their_diff_type"] == "modified"
    assert contents(db)[1] == ("ours", 1)  # left for the callable


def test_three_way_same_change_is_not_a_conflict():
    db = ours_db([(1, "same", 1), (2, "b", 2), (3, "c", 3)])
    theirs = table([(1, "same", 1), (2, "b", 2), (3, "c", 3)])
    assert bp.three_way_local(db, "t", table(BASE), theirs, ["id"])["conflicts"] == 0


def test_three_way_delete_vs_update_conflicts():
    db = ours_db([(1, "a", 1), (2, "b", 2)])  # ours deleted 3
    theirs = table([(1, "a", 1), (2, "b", 2), (3, "changed", 3)])
    info = bp.three_way_local(db, "t", table(BASE), theirs, ["id"], "theirs")
    assert info["conflicts"] == 1 and contents(db)[3] == ("changed", 3)


def test_three_way_composite_key_and_missing_base_column():
    schema = {"columns": [["a", "INTEGER"], ["b", "INTEGER"], ["v", "VARCHAR"]], "pk": ["a", "b"]}
    db = duckdb.connect()
    bp.create_local(db, "t", pa.table({"a": [1, 1], "b": [1, 2], "v": ["x", "y"]}), schema)
    base = pa.table({"a": [1, 1], "b": [1, 2]})  # base predates column v
    theirs = pa.table({"a": [1, 1, 2], "b": [1, 2, 1], "v": ["x", "y", "z"]})
    bp.three_way_local(db, "t", base, theirs, ["a", "b"])
    assert sorted(db.execute("SELECT a, b, v FROM t").fetchall()) == [(1, 1, "x"), (1, 2, "y"), (2, 1, "z")]


def test_three_way_without_primary_key_adds_new_rows_only():
    db = ours_db(BASE, {"columns": SCHEMA["columns"], "pk": []})
    theirs = table(BASE + [(9, "new", 9)])
    info = bp.three_way_local(db, "t", table(BASE), theirs, [])
    assert info["no_primary_key"] and sorted(contents(db)) == [1, 2, 3, 9]


def test_revert_local():
    before = table(BASE)
    after = table([(1, "changed", 1), (2, "b", 2), (4, "added", 4)])  # 3 deleted
    db = ours_db([(1, "changed", 1), (2, "later", 2), (4, "added", 4)])
    bp.revert_local(db, "t", before, after, ["id"])
    assert contents(db) == {1: ("a", 1), 2: ("later", 2), 3: ("c", 3)}


def test_diff_counts():
    a = table(BASE)
    b = table([(1, "changed", 1), (2, "b", 2), (4, "d", 4)])
    assert bp.diff_counts(a, b, ["id"]) == (1, 1, 1)


def test_ref_names():
    assert bp._ref_name("samwe.run__x") == "samwe.run__x"
    for bad in ("1abc", "a b", "a..b", "a."):
        try:
            bp._ref_name(bad)
            raise AssertionError(f"accepted {bad!r}")
        except ValueError:
            pass


def test_deadline_client(monkeypatch):
    import time

    import pytest

    monkeypatch.setattr(bp, "BAUPLAN_CLIENT_TIMEOUT", 0.2)

    class Fake:
        name = "fake"

        def hang(self):
            time.sleep(5)

        def commits(self):
            yield from (1, 2, 3)

        def boom(self):
            raise KeyError("x")

        def named(self, name=None, label=None):
            return (name, label)

    c = bp._DeadlineClient(Fake())
    assert c.name == "fake"
    start = time.perf_counter()
    with pytest.raises(TimeoutError):
        c.hang()
    assert time.perf_counter() - start < 2
    assert c.commits() == [1, 2, 3]  # read inside the deadline
    # SDK keyword arguments may share names with the wrapper's own.
    assert c.named(name="n", label="l") == ("n", "l")
    with pytest.raises(KeyError):
        c.boom()


class _FakeClient:
    """Serves Arrow tables from memory, as scan() and query() would."""

    def __init__(self, tables):
        self.tables = tables

    def scan(self, table, ref=None, namespace=None):
        return self.tables[table]

    def query(self, sql, ref=None, namespace=None):
        db = duckdb.connect()
        for name, data in self.tables.items():
            db.register(name, data)
        return db.execute(sql).fetch_arrow_table()


class _FakeSuite:
    ns = "ns"

    def __init__(self, tables, schemas):
        self.client = _FakeClient(tables)
        self.schemas = schemas

    def _table_ids(self, ref):
        return {name: name for name in self.client.tables}

    def _schema_of(self, table_id):
        return self.schemas.get(table_id)

    def _warn_once(self, key, msg):
        pass


def _published(ws):
    """(tables, schemas) a publish of ws would leave on the branch."""
    tables = dict(ws.suite.client.tables)
    schemas = dict(ws.suite.schemas)
    for t in ws.dirty:
        tables[t] = ws.db.execute(f'SELECT * FROM "{t}"').fetch_arrow_table()
        schemas[t] = bp.local_schema(ws.db, t)
    return tables, schemas


def test_views_are_stored_as_definitions_and_recreated():
    ws = bp._Workspace(_FakeSuite({}, {}), "u.run", "u.run@h")
    ws.execute("CREATE TABLE turn (turn_id INT PRIMARY KEY, step INT, tokens INT)")
    ws.execute("INSERT INTO turn VALUES (1, 1, 10), (2, 2, 20), (3, 3, 30)")
    ws.execute("CREATE VIEW context_v_a AS SELECT turn_id, tokens FROM turn WHERE step >= %s", (2,))
    assert ws.execute("SELECT SUM(tokens) FROM context_v_a")[1] == [(50,)]
    # The view is published as a definition, not as a table.
    assert ws.dirty == {"turn", bp.VIEWS_TABLE}
    tables, schemas = _published(ws)
    ws.close()

    # A later script on the published branch reads the view: its definition
    # is loaded and the view recreated over the tables it reads.
    ws2 = bp._Workspace(_FakeSuite(tables, schemas), "u.run", "u.run@h2")
    assert ws2.execute("SELECT COUNT(*), SUM(tokens) FROM context_v_a")[1] == [(2, 50)]
    # Reads of plain tables still go to the remote query and load nothing.
    ws3 = bp._Workspace(_FakeSuite(tables, schemas), "u.run", "u.run@h3")
    assert ws3.execute("SELECT COUNT(*) FROM turn")[1] == [(3,)]
    assert ws3.loaded == set()
    ws3.execute("DROP VIEW context_v_a")
    assert ws3.dirty == {bp.VIEWS_TABLE}
    try:
        ws3.execute("SELECT * FROM context_v_a")
        raise AssertionError("dropped view still readable")
    except Exception as e:
        assert "context_v_a" in str(e)
    ws2.close()
    ws3.close()
