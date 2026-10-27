"""Helpers shared by the MySQL-protocol backends (dolt_mysql, seekdb)."""

import re

# Rows per multi-row INSERT when converting pg_dump COPY blocks.
_LOAD_BATCH_ROWS = 2000

# Postgres type names (as written by pg_dump) -> MySQL equivalents.
_PG_TYPE_MAP = [
    (re.compile(r"\bcharacter varying\b", re.I), "VARCHAR"),
    (re.compile(r"\bcharacter\b", re.I), "CHAR"),
    (re.compile(r"\btimestamp without time zone\b", re.I), "DATETIME(6)"),
    (re.compile(r"\bdouble precision\b", re.I), "DOUBLE"),
]

_COPY_RE = re.compile(r"^COPY\s+(?:public\.)?(\w+)\s*\(([^)]*)\)\s+FROM\s+stdin;", re.I)
_COPY_ESCAPES = {"t": "\t", "n": "\n", "r": "\r", "b": "\b", "f": "\f", "v": "\v", "\\": "\\"}

# Column metadata for DBToolSuite._get_table_columns. information_schema
# spans every database on the server, so filter to the current one;
# DATA_TYPE is MySQL's udt_name equivalent.
TABLE_COLUMNS_QUERY = """
SELECT
    column_name,
    data_type,
    is_nullable,
    character_maximum_length,
    numeric_precision,
    numeric_scale
FROM
    information_schema.columns
WHERE
    table_schema = DATABASE() AND table_name = %s
ORDER BY
    ordinal_position;
"""


def normalize_column_types(rows) -> list[tuple]:
    """Map TABLE_COLUMNS_QUERY rows to the Postgres type names the data
    generator knows: TIMESTAMP WITHOUT TIME ZONE columns (loaded as DATETIME)
    are reported as datetime."""
    return [
        (name, "timestamp" if dtype == "datetime" else dtype, *rest)
        for name, dtype, *rest in rows
    ]


def _unescape_copy_field(field: str):
    """Decode one field of pg_dump's COPY text format (\\N is NULL)."""
    if field == "\\N":
        return None
    if "\\" not in field:
        return field
    return re.sub(r"\\(.)", lambda m: _COPY_ESCAPES.get(m.group(1), m.group(1)), field)


def _pg_to_mysql_ddl(stmt: str) -> str:
    """Translate a pg_dump DDL statement to MySQL syntax."""
    stmt = stmt.replace("public.", "").replace("ALTER TABLE ONLY ", "ALTER TABLE ")
    for pattern, replacement in _PG_TYPE_MAP:
        stmt = pattern.sub(replacement, stmt)
    return stmt


def _is_pg_only(stmt: str) -> bool:
    """Session settings and psql meta-commands that have no MySQL meaning."""
    s = stmt.lstrip().upper()
    return s.startswith(("SET ", "SELECT PG_CATALOG.", "\\"))


def load_sql_dump(conn, sql_path: str) -> None:
    """Load a SQL file through conn, which must be open on the target
    database with CLIENT.MULTI_STATEMENTS. The caller closes conn.

    Accepts plain SQL (e.g. ch_benchmark_seed.sql) as well as pg_dump
    output (e.g. ch-w1.sql): COPY blocks become batched INSERTs, Postgres
    type names are translated, and Postgres-only settings are skipped.
    """
    cur = conn.cursor()
    try:
        stmt_lines = []
        with open(sql_path) as f:
            for line in f:
                copy_match = _COPY_RE.match(line) if not stmt_lines else None
                if copy_match:
                    table, cols = copy_match.groups()
                    _load_copy_block(cur, f, table, cols)
                    continue
                if not stmt_lines and (not line.strip() or line.startswith("--")):
                    continue
                stmt_lines.append(line)
                if line.rstrip().endswith(";"):
                    stmt = "".join(stmt_lines)
                    stmt_lines = []
                    if not _is_pg_only(stmt):
                        cur.execute(_pg_to_mysql_ddl(stmt))
                        while cur.nextset():
                            pass
    finally:
        cur.close()


def _load_copy_block(cur, f, table: str, cols: str) -> None:
    """Consume a COPY ... FROM stdin block from f and insert its rows."""
    num_cols = len(cols.split(","))
    placeholders = "(" + ", ".join(["%s"] * num_cols) + ")"
    sql = f"INSERT INTO {table} ({cols}) VALUES {placeholders}"
    batch = []
    for line in f:
        if line.rstrip("\n") == "\\.":
            break
        batch.append([_unescape_copy_field(v) for v in line.rstrip("\n").split("\t")])
        if len(batch) >= _LOAD_BATCH_ROWS:
            cur.executemany(sql, batch)
            batch = []
    if batch:
        cur.executemany(sql, batch)


from contextlib import asynccontextmanager as _asynccontextmanager


@_asynccontextmanager
async def aiomysql_pool_connection(pool):
    """Borrow a connection from an aiomysql pool (its acquire() API differs
    from psycopg_pool's connection())."""
    async with pool.acquire() as conn:
        yield conn
