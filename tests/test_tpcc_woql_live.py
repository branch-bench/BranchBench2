"""macrobench/tpcc_woql.py against macrobench/tpcc.py: the same seeded CH
data is loaded into TerminusDB and into PostgreSQL, the same transactions
and CH queries run on both (WOQL on one, SQL on the other), and the
results and the final table contents must agree.

Skipped unless a TerminusDB server (TERMINUSDB_URL) and a PostgreSQL
server (PGSQL_USER/PGSQL_HOST/PGSQL_PORT) are both reachable."""

import random
import re
from datetime import datetime, timedelta
from decimal import Decimal

import psycopg2
import pytest

from dblib import result_collector as rc
from dblib.systems.file_copy import adapter as file_copy
from dblib.systems.terminusdb import adapter as terminusdb
from macrobench import tpcc, tpcc_woql
from macrobench.datagen.ch import CHGenerator, CHScale

DB = "bb_test_tpcc_woql"
SCHEMA = "db_setup/ch_benchmark_schema.sql"
SCALE = CHScale(warehouses=1, items=60, customers_per_district=8, orders_per_district=12,
                suppliers=5)
NOW = datetime(2026, 6, 15, 12, 0, 0)


def _pg(dbname: str):
    return psycopg2.connect(user=file_copy.PGSQL_USER, password=file_copy.PGSQL_PASSWORD,
                            host=file_copy.PGSQL_HOST, port=file_copy.PGSQL_PORT, dbname=dbname)


def _servers_up() -> bool:
    if not terminusdb.server_up():
        return False
    try:
        _pg("postgres").close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _servers_up(), reason="needs TerminusDB and PostgreSQL")


class SqlSession:
    """The part of a Session that tpcc.py uses, on a psycopg2 connection."""

    def __init__(self, conn):
        self.conn = conn

    def sql(self, query, params=None):
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall() if cur.description else None


def _norm(value):
    """Values as both systems should agree on them."""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%S")
    if isinstance(value, (Decimal, float)):
        return round(float(value), 2)
    if isinstance(value, str):
        if re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?Z", value):
            return value[:19]
        return value.rstrip()   # CHAR(n) is blank-padded in PostgreSQL
    return value


def _rows(rows):
    return sorted((tuple(_norm(v) for v in row) for row in rows), key=repr)


@pytest.fixture(scope="module")
def both():
    with open(SCHEMA) as f:
        ddl = f.read()
    admin = _pg("postgres")
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {DB}")
        cur.execute(f"CREATE DATABASE {DB}")
    pg = _pg(DB)
    pg.autocommit = True
    with pg.cursor() as cur:
        cur.execute(ddl)

    terminusdb.setup_database(DB, SCHEMA)
    suite = terminusdb.TerminusDBToolSuite.init_for_bench(rc.ResultCollector(), DB, "main")
    for table, columns, rows in CHGenerator(SCALE, seed=7, now=NOW).tables():
        rows = list(rows)
        with pg.cursor() as cur:
            marks = ", ".join(["%s"] * len(columns))
            cur.executemany(f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({marks})", rows)
        terminusdb.insert_rows(DB, table, columns, rows)

    yield SqlSession(pg), tpcc_woql.NativeSession(suite, "main"), suite
    suite.close_connection()
    terminusdb.drop_database(DB)
    pg.close()
    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {DB}")
    admin.close()


def _tables_agree(sql, suite):
    for table in suite.conn.catalog.tables():
        expected = _rows(sql.sql(f"SELECT * FROM {table}"))
        actual = _rows(suite.exec([f"SELECT * FROM {table}"], refs=["main"], timed=False)[0].rows)
        assert actual == expected, f"table {table} differs"


def test_seed_is_identical(both):
    sql, _, suite = both
    _tables_agree(sql, suite)


def test_transactions_agree(both, monkeypatch):
    sql, native, suite = both
    monkeypatch.setattr(tpcc, "_now", lambda: NOW)
    monkeypatch.setattr(tpcc_woql, "_now", lambda: NOW)
    plan = (["new_order"] * 6 + ["payment"] * 6 + ["delivery"] * 3 + ["order_status"] * 4
            + ["stock_level"] * 4 + ["new_order", "delivery", "stock_level", "order_status"])
    for i, name in enumerate(plan):
        kwargs = {"touch_c_data": True} if name == "payment" and i % 2 else {}
        expected = tpcc.TRANSACTIONS[name](sql, random.Random(i), SCALE, **kwargs)
        actual = tpcc_woql.TRANSACTIONS[name](native, random.Random(i), SCALE, **kwargs)
        assert actual == expected, f"{name} #{i}"
    _tables_agree(sql, suite)
    failed = [r for r in suite.result_collector.results if r.status != rc.rslt.OpStatus.OK]
    assert not failed


@pytest.mark.parametrize("name", ["q1", "q3", "q6", "q12", "q18"])
def test_ch_queries_agree(both, name):
    sql, native, _ = both
    since = NOW - timedelta(days=25)
    args = (since, NOW) if name == "q6" else (since,)
    expected = getattr(tpcc, f"ch_{name}")(sql, *args)
    actual = tpcc_woql.QUERIES[name](native, *args)
    if name == "q6":   # SQL SUM over no rows is NULL; WOQL's is 0
        expected = [(row[0] or 0,) for row in expected]
    assert len(expected) > 0 or name == "q3"
    assert _rows(actual) == _rows(expected)
