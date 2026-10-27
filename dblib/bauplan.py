"""Bauplan backend.

Bauplan is a lakehouse (Iceberg tables over S3) with git-like branches,
commits, tags and merges in its catalog. It has no SQL writes: query() is
read-only, and data enters a table only from Parquet files in S3
(create_table / import_data) or from pipeline runs. The verbs map as:

    branch     create_branch(<user>.<db>__<name>, from_ref=<branch or tag>)
    commit     create_tag(<user>.<db>.c<id>[.m<base32 message>], from_ref=branch)
    log        get_commits(ref), keeping the commits our tags point at and the
               ids merges carried over (a merge commit's bb_commits property)
    diff       table ids compared per table; changed tables row-diffed locally
    merge      merge_branch; on a (table-level) MergeConflictError, a row-level
               three-way merge of the conflicting tables, computed locally
    rebase     a branch from the upstream gets the branch's changes (as merge),
               then replaces the branch (delete_branch + rename_branch)
    reset      a branch from the commit's tag replaces the branch
    revert     the commit's row changes, inverted locally and published
    delete     delete_branch

Names: a non-admin user can only write branches named "<user>.<name>", so a
run's root branch (the benchmark's "main") is <user>.<db_name>, created from
BAUPLAN_SOURCE_REF, and its tables live in their own namespace, bb_<db_name>.

exec() works on a local copy. Reads run on the branch's head as of the
script's first statement (<branch>@<hash>) through query(); a statement
that writes runs on local DuckDB copies of the tables it touches (pulled
with scan()), and reads of those tables then run locally too. When the
script ends, each changed table is written to S3 as Parquet and loaded with
create_table(replace=True) + import_data on a temporary branch, which is
merged into the branch: one atomic commit per exec(). If another writer
changed one of those tables in the meantime, the merge conflicts and the
script runs again from a fresh head. Writes are therefore visible to other
sessions when the script ends, not per statement.

Iceberg keeps neither primary keys nor declared types the way merge needs
them, so every table version (Bauplan gives a table a new id each time it is
replaced) has a sidecar JSON in S3, <BAUPLAN_S3_URI>/<db>/_schema/<id>.json,
with its columns and primary key. Table ids travel with merges and
revert_table, so the sidecar always matches the data.

A deleted or renamed branch's "<name>@<hash>" refs stop resolving, but tags
keep their commits readable, so commit refs resolve through tags.
"""

import base64
import contextlib
import datetime
import decimal
import json
import os
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache

import bauplan
import bauplan.exceptions as bpx
import duckdb
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import sqlglot
from sqlglot import exp

from dblib.db_api import DBToolSuite, Ref
import dblib.result_collector as rc
from util.sql_parse import get_sql_operation_keyword

# Credentials: an API key, or a profile in ~/.bauplan/config.yaml (the
# default profile when neither is set).
BAUPLAN_API_KEY = os.environ.get("BAUPLAN_API_KEY") or None
BAUPLAN_PROFILE = os.environ.get("BAUPLAN_PROFILE") or None
# Defaults to the API key's user (client.info()).
BAUPLAN_USERNAME = os.environ.get("BAUPLAN_USERNAME", "")
# s3://bucket/prefix the run's Parquet files and schema sidecars go to. The
# public sandbox can only import from a public, listable bucket.
BAUPLAN_S3_URI = os.environ.get("BAUPLAN_S3_URI", "").rstrip("/")
# Ref each run's root branch is created from.
BAUPLAN_SOURCE_REF = os.environ.get("BAUPLAN_SOURCE_REF", "main")
# Times a script is re-run after its publish lost a race with another writer.
BAUPLAN_PUBLISH_RETRIES = int(os.environ.get("BAUPLAN_PUBLISH_RETRIES", "5"))
# Seconds any one Bauplan request (including a table create or import job)
# may take before it fails.
BAUPLAN_CLIENT_TIMEOUT = int(os.environ.get("BAUPLAN_CLIENT_TIMEOUT", "600"))
# Commits walked for merge bases, log() and the commits a merge carries.
BAUPLAN_HISTORY_LIMIT = int(os.environ.get("BAUPLAN_HISTORY_LIMIT", "300"))

MAIN_BRANCH = "main"
_BRANCH_SEP = "__"
# Branch-name suffix of temporary branches (publishes, merges, resets).
_TEMP_PREFIX = "tmp_"
# Bauplan's reference-name rule.
_REF_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9._/-]*$")
# Bytes of a commit message kept in its tag name.
_MESSAGE_BYTES = 120
# Commit property listing the commit ids a merge carried over
# (merge_branch keeps commit properties but drops the commit body).
_CARRIED = "bb_commits"
_PUBLISH_THREADS = 8

_TXN_KEYWORDS = ("BEGIN", "START", "COMMIT", "END", "ROLLBACK", "ABORT")
_WRITE_KEYWORDS = ("INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE")
_CONSTRAINT_RE = re.compile(r"^\s*ALTER\s+TABLE\s+.*\bADD\s+(CONSTRAINT\s+\w+\s+)?"
                            r"(FOREIGN\s+KEY|CHECK)\b", re.I | re.S)
_VIEW_RE = re.compile(r"^\s*CREATE\s+(OR\s+REPLACE\s+)?VIEW\s+(IF\s+NOT\s+EXISTS\s+)?\"?(\w+)\"?", re.I)
_DROP_VIEW_RE = re.compile(r"^\s*DROP\s+VIEW\s+(IF\s+EXISTS\s+)?\"?(\w+)\"?", re.I)
# Bauplan stores tables only, so each branch keeps its view definitions in
# this table (published, branched and merged like any other).
VIEWS_TABLE = "_bb_views"
_VIEWS_SCHEMA = {"columns": [["name", "VARCHAR"], ["sql", "VARCHAR"]], "pk": ["name"]}
_RENAME_TABLE_RE = re.compile(r"^\s*ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?\"?(\w+)\"?\s+"
                              r"RENAME\s+TO\s+\"?(\w+)\"?\s*;?\s*$", re.I)


class PublishConflict(Exception):
    """Another writer changed a table this script wrote; run it again."""


def _with_deadline(label: str, fn, /, *args, **kwargs):
    """Call fn on its own thread and wait at most BAUPLAN_CLIENT_TIMEOUT.

    The SDK's client_timeout does not cover every call (a query or scan on
    a connection that died while the machine slept blocks forever), so the
    deadline is enforced here; a call that overruns is abandoned on its
    daemon thread. Lazy iterators (get_commits, get_tags, ...) are read to
    the end inside the deadline, since their network reads happen while
    iterating."""
    box = {}

    def run():
        try:
            value = fn(*args, **kwargs)
            if hasattr(value, "__next__"):
                value = list(value)
            box["value"] = value
        except BaseException as e:
            box["error"] = e

    worker = threading.Thread(target=run, name=f"bauplan-{label}", daemon=True)
    worker.start()
    worker.join(BAUPLAN_CLIENT_TIMEOUT)
    if worker.is_alive():
        raise TimeoutError(f"Bauplan {label}() did not return within "
                           f"{BAUPLAN_CLIENT_TIMEOUT}s (BAUPLAN_CLIENT_TIMEOUT)")
    if "error" in box:
        raise box["error"]
    return box["value"]


class _DeadlineClient:
    """bauplan.Client whose calls each have a deadline (see _with_deadline)."""

    def __init__(self, client: bauplan.Client):
        self._client = client

    def __getattr__(self, name):
        attr = getattr(self._client, name)
        if not callable(attr):
            return attr

        def call(*args, **kwargs):
            return _with_deadline(name, attr, *args, **kwargs)
        return call


def new_client() -> "_DeadlineClient":
    kwargs = {"client_timeout": BAUPLAN_CLIENT_TIMEOUT}
    if BAUPLAN_API_KEY:
        client = bauplan.Client(api_key=BAUPLAN_API_KEY, **kwargs)
    elif BAUPLAN_PROFILE:
        client = bauplan.Client(profile=BAUPLAN_PROFILE, **kwargs)
    else:
        client = bauplan.Client(**kwargs)
    return _DeadlineClient(client)


_user_lock = threading.Lock()
_user = None


def username(client: bauplan.Client = None) -> str:
    global _user
    if BAUPLAN_USERNAME:
        return BAUPLAN_USERNAME
    with _user_lock:
        if _user is None:
            _user = (client or new_client()).info().user.username
        return _user


def _ref_name(name: str) -> str:
    if not _REF_NAME_RE.match(name) or name.endswith((".", "/")) or ".." in name:
        raise ValueError(f"'{name}' is not a valid Bauplan branch or tag name "
                         "(letters, digits and . / _ - only)")
    return name


def root_branch(db_name: str, client: bauplan.Client = None) -> str:
    return _ref_name(f"{username(client)}.{db_name}")


def namespace(db_name: str) -> str:
    return f"bb_{db_name}"


# ----------------------------------------------------------------------
# S3: Parquet files and schema sidecars
# ----------------------------------------------------------------------

_s3_lock = threading.Lock()
_s3 = None


def _s3_fs():
    """(filesystem, bucket/prefix path) for BAUPLAN_S3_URI. The region is
    looked up from the bucket, so a wrong AWS_REGION does not matter."""
    global _s3
    with _s3_lock:
        if _s3 is None:
            if not BAUPLAN_S3_URI.startswith("s3://"):
                raise RuntimeError("BAUPLAN_S3_URI must be set to s3://<bucket>/<prefix> "
                                   "(see the Bauplan backend section of the README)")
            path = BAUPLAN_S3_URI[len("s3://"):]
            bucket = path.split("/")[0]
            try:
                region = pafs.resolve_s3_region(bucket)
            except Exception:
                region = os.environ.get("AWS_REGION", "us-east-1")
            _s3 = (pafs.S3FileSystem(region=region), path)
        return _s3


def _upload(db_name: str, table: pa.Table) -> str:
    """Write table as one Parquet file; returns the import search URI."""
    fs, base = _s3_fs()
    folder = f"{base}/{db_name}/data/{uuid.uuid4().hex}"
    pq.write_table(table, f"{folder}/part.parquet", filesystem=fs)
    return f"s3://{folder}/*.parquet"


def _write_sidecar(db_name: str, table_id, schema: dict) -> None:
    fs, base = _s3_fs()
    with fs.open_output_stream(f"{base}/{db_name}/_schema/{table_id}.json") as f:
        f.write(json.dumps(schema).encode())


def _read_sidecar(db_name: str, table_id):
    fs, base = _s3_fs()
    try:
        with fs.open_input_stream(f"{base}/{db_name}/_schema/{table_id}.json") as f:
            return json.loads(f.read().decode())
    except (FileNotFoundError, OSError):
        return None


def setup_database(db_name: str, sql_path: str = None) -> None:
    """(Re)create the run's root branch and namespace."""
    if sql_path:
        raise NotImplementedError(
            "The Bauplan backend does not load SQL dumps yet; "
            "use database_setup { generated {} }")
    drop_database(db_name)
    client = new_client()
    root = root_branch(db_name, client)
    client.create_branch(root, from_ref=BAUPLAN_SOURCE_REF)
    client.create_namespace(namespace(db_name), root)
    print(f"Bauplan branch '{root}' created from {BAUPLAN_SOURCE_REF}.")


def drop_database(db_name: str) -> None:
    """Delete the run's branches, tags and S3 files."""
    client = new_client()
    root = root_branch(db_name, client)
    for tag in client.get_tags(filter_by_name=root):
        if tag.name.startswith(root + "."):
            client.delete_tag(tag.name, if_exists=True)
    for branch in client.get_branches(name=root):
        if branch.name == root or branch.name.startswith(root + _BRANCH_SEP):
            client.delete_branch(branch.name, if_exists=True)
    fs, base = _s3_fs()
    path = f"{base}/{db_name}"
    if fs.get_file_info(path).type != pafs.FileType.NotFound:
        fs.delete_dir(path)
    print(f"Bauplan run '{root}' deleted.")


# ----------------------------------------------------------------------
# SQL helpers
# ----------------------------------------------------------------------

_PLACEHOLDER_RE = re.compile(r"%\((\w+)\)s|%s|%%")


def to_duckdb(sql: str, vars=None):
    """pyformat SQL (%s, %(name)s) -> DuckDB SQL (?, $name) and parameters.
    As with the DB-API drivers, nothing is substituted when vars is None."""
    if vars is None:
        return sql, None

    def sub(m):
        if m.group(0) == "%%":
            return "%"
        return f"${m.group(1)}" if m.group(1) else "?"
    params = dict(vars) if isinstance(vars, dict) else list(vars)
    return _PLACEHOLDER_RE.sub(sub, sql), params


def literal(value) -> str:
    """SQL literal for a parameter value (Bauplan's query() takes no
    parameters)."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, decimal.Decimal)):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, datetime.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return f"TIMESTAMP '{value.isoformat(sep=' ')}'"
    if isinstance(value, datetime.date):
        return f"DATE '{value.isoformat()}'"
    return "'" + str(value).replace("'", "''") + "'"


def inline(sql: str, vars=None) -> str:
    """pyformat SQL with its parameters rendered as literals."""
    if vars is None:
        return sql
    if isinstance(vars, dict):
        values = vars
    else:
        it = iter(vars)

    def sub(m):
        if m.group(0) == "%%":
            return "%"
        if m.group(1):
            return literal(values[m.group(1)])
        return literal(next(it))
    return _PLACEHOLDER_RE.sub(sub, sql)


@lru_cache(maxsize=4096)
def statement_tables(duck_sql: str):
    """(tables referenced, table written) of a DuckDB statement, lower case;
    CTE names are not tables. (None, None) if it does not parse."""
    try:
        tree = sqlglot.parse_one(duck_sql, read="duckdb")
    except Exception:
        return None, None
    ctes = {c.alias_or_name.lower() for c in tree.find_all(exp.CTE)}
    tables = frozenset(t.name.lower() for t in tree.find_all(exp.Table) if t.name) - ctes
    target = None
    if isinstance(tree, (exp.Insert, exp.Update, exp.Delete, exp.Create, exp.Drop,
                         exp.Alter, exp.Merge)):
        first = tree.this if isinstance(tree.this, exp.Table) else tree.find(exp.Table)
        if first is not None and first.name:
            target = first.name.lower()
    return tables, target


def normalize(table: pa.Table) -> pa.Table:
    """Arrow from Bauplan in the shape the other backends return: naive UTC
    timestamps and plain strings."""
    cols = []
    for field, col in zip(table.schema, table.columns):
        t = field.type
        if pa.types.is_timestamp(t) and t.tz is not None:
            col = col.cast(pa.timestamp(t.unit))
        elif pa.types.is_large_string(t):
            col = col.cast(pa.string())
        cols.append(col)
    return pa.table(cols, names=table.column_names)


def arrow_rows(table: pa.Table) -> list:
    if table.num_columns == 0:
        return []
    return list(zip(*[c.to_pylist() for c in table.columns]))


def _q(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def _cols(alias, cols) -> str:
    return ", ".join(f"{alias}.{_q(c)}" if alias else _q(c) for c in cols)


def _join(a, b, cols) -> str:
    return " AND ".join(f"{a}.{_q(c)} = {b}.{_q(c)}" for c in cols)


def _same(a, b, cols) -> str:
    if not cols:
        return "TRUE"
    return " AND ".join(f"{a}.{_q(c)} IS NOT DISTINCT FROM {b}.{_q(c)}" for c in cols)


def local_schema(db, table: str) -> dict:
    """{"columns": [[name, type]], "pk": [names]} of a DuckDB table."""
    cols = db.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = ? ORDER BY ordinal_position", [table]).fetchall()
    pk = db.execute(
        "SELECT constraint_column_names FROM duckdb_constraints() "
        "WHERE table_name = ? AND constraint_type = 'PRIMARY KEY'", [table]).fetchall()
    return {"columns": [[c, t] for c, t in cols], "pk": list(pk[0][0]) if pk else []}


def create_local(db, table: str, data: pa.Table, schema: dict = None) -> None:
    """Create DuckDB table ``table`` holding ``data``, with the declared
    types and primary key of ``schema`` when given."""
    view = f"__bb_src_{uuid.uuid4().hex}"
    db.register(view, data)
    try:
        if schema and schema.get("columns"):
            defs = [f"{_q(c)} {t}" for c, t in schema["columns"]]
            if schema.get("pk"):
                defs.append(f"PRIMARY KEY ({_cols(None, schema['pk'])})")
            db.execute(f"CREATE TABLE {_q(table)} ({', '.join(defs)})")
            have = set(data.column_names)
            cols = [c for c, _ in schema["columns"] if c in have]
            if cols:
                db.execute(f"INSERT INTO {_q(table)} ({_cols(None, cols)}) "
                           f"SELECT {_cols(None, cols)} FROM {view}")
        else:
            db.execute(f"CREATE TABLE {_q(table)} AS SELECT * FROM {view}")
    finally:
        db.unregister(view)


def _project(db, name: str, data: pa.Table, cols: list, like: str) -> None:
    """Temp table ``name`` with exactly ``cols`` of ``data`` (NULL, typed as
    in table ``like``, for the ones data lacks)."""
    view = f"__bb_src_{uuid.uuid4().hex}"
    db.register(view, data)
    try:
        have = set(data.column_names)
        types = dict(db.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = ?", [like]).fetchall())
        sel = [_q(c) if c in have else f"CAST(NULL AS {types.get(c, 'VARCHAR')}) AS {_q(c)}"
               for c in cols]
        db.execute(f"CREATE OR REPLACE TEMP TABLE {_q(name)} AS SELECT {', '.join(sel)} FROM {view}")
    finally:
        db.unregister(view)


def three_way_local(db, table: str, base: pa.Table, theirs: pa.Table, pk: list,
                    on_conflict="ours") -> dict:
    """Apply theirs' changes since base to local DuckDB table ``table``
    (ours), matching rows by ``pk``. Rows both sides changed differently
    are conflicts: "ours" keeps ours, "theirs" takes theirs, and for a
    callable the conflict rows (Dolt's base_*/our_*/their_* shape) are
    returned under "rows" for the caller to resolve. Without a primary key,
    only theirs' new rows are added."""
    cols = [c for c, _ in local_schema(db, table)["columns"] if c in set(theirs.column_names)]
    _project(db, "__bb_t", theirs, cols, table)
    _project(db, "__bb_b", base, cols, table)
    db.execute(f"CREATE OR REPLACE TEMP TABLE __bb_o AS SELECT {_cols(None, cols)} FROM {_q(table)}")
    info = {"ours_changes": 0, "theirs_changes": 0, "conflicts": 0, "rows": []}
    if not pk:
        n = db.execute(f"INSERT INTO {_q(table)} ({_cols(None, cols)}) "
                       f"SELECT * FROM __bb_t EXCEPT ALL SELECT * FROM __bb_b").fetchall()[0][0]
        info["theirs_changes"] = n
        info["no_primary_key"] = True
        return info
    rest = [c for c in cols if c not in pk]
    keys = _cols(None, pk)
    for side, src in (("t", "__bb_t"), ("o", "__bb_o")):
        db.execute(
            f"CREATE OR REPLACE TEMP TABLE __bb_{side}k AS "
            f"SELECT {_cols('x', pk)}, 'u' AS side FROM {src} x WHERE NOT EXISTS "
            f"(SELECT 1 FROM __bb_b b WHERE {_join('b', 'x', pk)} AND {_same('b', 'x', rest)}) "
            f"UNION ALL SELECT {_cols('b', pk)}, 'd' FROM __bb_b b WHERE NOT EXISTS "
            f"(SELECT 1 FROM {src} x WHERE {_join('b', 'x', pk)})")
    info["theirs_changes"] = db.execute("SELECT COUNT(*) FROM __bb_tk").fetchall()[0][0]
    info["ours_changes"] = db.execute("SELECT COUNT(*) FROM __bb_ok").fetchall()[0][0]
    db.execute(
        f"CREATE OR REPLACE TEMP TABLE __bb_ck AS SELECT {_cols('a', pk)} FROM __bb_tk a "
        f"JOIN __bb_ok b ON {_join('a', 'b', pk)} "
        f"LEFT JOIN __bb_o o ON {_join('a', 'o', pk)} LEFT JOIN __bb_t t ON {_join('a', 't', pk)} "
        f"WHERE NOT (a.side = 'd' AND b.side = 'd') "
        f"AND NOT (a.side = 'u' AND b.side = 'u' AND {_same('o', 't', rest)})")
    info["conflicts"] = db.execute("SELECT COUNT(*) FROM __bb_ck").fetchall()[0][0]

    def apply(conflicting: bool):
        pick = "IN" if conflicting else "NOT IN"
        db.execute(
            f"DELETE FROM {_q(table)} WHERE ({keys}) IN "
            f"(SELECT {keys} FROM __bb_tk WHERE side = 'd') "
            f"AND ({keys}) {pick} (SELECT {keys} FROM __bb_ck)")
        upsert = (f"INSERT INTO {_q(table)} ({_cols(None, cols)}) "
                  f"SELECT {_cols('t', cols)} FROM __bb_t t WHERE ({_cols('t', pk)}) IN "
                  f"(SELECT {keys} FROM __bb_tk WHERE side = 'u') "
                  f"AND ({_cols('t', pk)}) {pick} (SELECT {keys} FROM __bb_ck) "
                  f"ON CONFLICT ({keys}) DO ")
        sets = ", ".join(f"{_q(c)} = excluded.{_q(c)}" for c in rest)
        db.execute(upsert + (f"UPDATE SET {sets}" if rest else "NOTHING"))

    if info["conflicts"] and callable(on_conflict):
        info["rows"] = _conflict_rows(db, cols, pk)
    apply(conflicting=False)
    if info["conflicts"] and on_conflict == "theirs":
        apply(conflicting=True)
    return info


def _conflict_rows(db, cols, pk) -> list:
    p0 = _q(pk[0])
    rows = db.execute(
        f"SELECT {_cols('b', cols)}, {_cols('o', cols)}, {_cols('t', cols)}, "
        f"b.{p0} IS NULL, o.{p0} IS NULL, t.{p0} IS NULL FROM __bb_ck c "
        f"LEFT JOIN __bb_b b ON {_join('c', 'b', pk)} "
        f"LEFT JOIN __bb_o o ON {_join('c', 'o', pk)} "
        f"LEFT JOIN __bb_t t ON {_join('c', 't', pk)}").fetchall()
    lower = [c.lower() for c in cols]
    names = [f"base_{c}" for c in lower] + [f"our_{c}" for c in lower] + [f"their_{c}" for c in lower]
    out = []
    for r in rows:
        d = dict(zip(names, r[:len(names)]))
        no_base, no_ours, no_theirs = r[len(names):]

        def diff_type(missing):
            if missing:
                return "removed"
            return "added" if no_base else "modified"
        d["our_diff_type"] = diff_type(no_ours)
        d["their_diff_type"] = diff_type(no_theirs)
        out.append(d)
    return out


def revert_local(db, table: str, before: pa.Table, after: pa.Table, pk: list) -> None:
    """Undo, in local table ``table``, the row changes between ``before``
    and ``after`` (rows added are deleted, rows changed or deleted are put
    back as they were)."""
    cols = [c for c, _ in local_schema(db, table)["columns"]]
    _project(db, "__bb_s", before, cols, table)
    _project(db, "__bb_e", after, cols, table)
    rest = [c for c in cols if c not in pk]
    keys = _cols(None, pk)
    db.execute(f"DELETE FROM {_q(table)} WHERE ({keys}) IN (SELECT {_cols('e', pk)} FROM __bb_e e "
               f"WHERE NOT EXISTS (SELECT 1 FROM __bb_s s WHERE {_join('s', 'e', pk)}))")
    sets = ", ".join(f"{_q(c)} = excluded.{_q(c)}" for c in rest)
    db.execute(
        f"INSERT INTO {_q(table)} ({_cols(None, cols)}) SELECT {_cols('s', cols)} FROM __bb_s s "
        f"WHERE NOT EXISTS (SELECT 1 FROM __bb_e e WHERE {_join('s', 'e', pk)} AND {_same('s', 'e', rest)}) "
        f"ON CONFLICT ({keys}) DO " + (f"UPDATE SET {sets}" if rest else "NOTHING"))


def diff_counts(a: pa.Table, b: pa.Table, pk: list) -> tuple:
    """(added, deleted, modified) rows from a to b."""
    db = duckdb.connect()
    try:
        cols = [c for c in a.column_names if c in set(b.column_names)]
        db.register("a_src", a)
        db.register("b_src", b)
        db.execute(f"CREATE TABLE a AS SELECT TRUE AS __p, {_cols(None, cols)} FROM a_src")
        db.execute(f"CREATE TABLE b AS SELECT TRUE AS __p, {_cols(None, cols)} FROM b_src")
        keys = [c for c in pk if c in cols]
        rest = [c for c in cols if c not in keys]
        on = _join("x", "y", keys) if keys else _same("x", "y", cols)
        modified = (f"COUNT(*) FILTER (WHERE x.__p AND y.__p AND NOT ({_same('x', 'y', rest)}))"
                    if keys and rest else "0")
        return tuple(int(v) for v in db.execute(
            f"SELECT COUNT(*) FILTER (WHERE x.__p IS NULL), COUNT(*) FILTER (WHERE y.__p IS NULL), "
            f"{modified} FROM a x FULL OUTER JOIN b y ON {on}").fetchall()[0])
    finally:
        db.close()


# ----------------------------------------------------------------------
# The DB-API shim exec() sessions use
# ----------------------------------------------------------------------

class _Cursor:
    def __init__(self, suite):
        self._suite = suite
        self.description = None
        self.rowcount = -1
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass

    def execute(self, query, vars=None):
        self.description, self._rows, self.rowcount = self._suite._workspace().execute(query, vars)
        return self

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def close(self):
        pass


class _Connection:
    """What exec() sessions get as their connection: statements go to the
    suite's workspace for the ref being run."""

    def __init__(self, suite):
        self._suite = suite

    def cursor(self):
        return _Cursor(self._suite)

    def close(self):
        pass


class _Workspace:
    """One exec() script's view of a ref. Reads are pinned to the ref's head
    as of the first statement; writes go to local DuckDB copies of the
    tables they touch and are published when the script ends."""

    def __init__(self, suite, branch: str, read_ref: str):
        self.suite = suite
        self.branch = branch      # None for a commit ref (read-only)
        self.read_ref = read_ref  # <branch>@<hash> or a tag
        self.db = duckdb.connect()
        self.remote = suite._table_ids(read_ref)  # table -> id at read_ref
        self.loaded = set()
        self.dirty = set()
        self.dropped = set()
        self._saved = None        # (dirty, dropped) at BEGIN
        self._views = None        # view name -> CREATE VIEW statement
        self._local_views = set() # views created in DuckDB

    def close(self) -> None:
        self.db.close()

    @property
    def changed(self) -> bool:
        return bool(self.dirty or self.dropped)

    def load(self, table: str) -> None:
        """Copy table into DuckDB on first use (if it exists at read_ref)."""
        if table in self.loaded or table not in self.remote or table in self.dropped:
            return
        data = normalize(self.suite.client.scan(table, ref=self.read_ref,
                                                namespace=self.suite.ns))
        create_local(self.db, table, data, self.suite._schema_of(self.remote[table]))
        self.loaded.add(table)

    def _view_defs(self, names=frozenset()) -> dict:
        """The branch's view definitions. Read only when needed: when a
        statement names something that is not a known table."""
        if self._views is None:
            unknown = set(names) - set(self.remote) - self.loaded - self.dirty
            if not unknown or VIEWS_TABLE not in self.remote:
                return {}
            self._load_views()
        return self._views

    def _load_views(self) -> None:
        if self._views is not None:
            return
        if VIEWS_TABLE in self.remote:
            self.load(VIEWS_TABLE)
        else:
            create_local(self.db, VIEWS_TABLE, pa.table({"name": pa.array([], pa.string()),
                                                         "sql": pa.array([], pa.string())}),
                         _VIEWS_SCHEMA)
            self.loaded.add(VIEWS_TABLE)
        self._views = dict(self.db.execute(f"SELECT name, sql FROM {VIEWS_TABLE}").fetchall())

    def _prepare(self, names) -> None:
        """Make the tables and views a statement names available locally."""
        views = self._view_defs(names)
        for name in names:
            if name in views:
                self._ensure_view(name)
            else:
                self.load(name)

    def _ensure_view(self, name: str) -> None:
        if name in self._local_views:
            return
        sql = self._views[name]
        deps, _ = statement_tables(sql)
        self._prepare((deps or frozenset()) - {name})
        self.db.execute(sql)
        self._local_views.add(name)

    def _view_statement(self, sql, vars, duck_sql, params, tables):
        """CREATE VIEW / DROP VIEW: kept in VIEWS_TABLE."""
        if self.branch is None:
            raise ValueError("a commit ref is read-only")
        create = _VIEW_RE.match(sql)
        name = (create.group(3) if create else _DROP_VIEW_RE.match(sql).group(2)).lower()
        self._load_views()
        exists = name in self._views
        if create:
            if exists and create.group(2):
                return None, [], -1  # IF NOT EXISTS
            if exists and not create.group(1):
                raise ValueError(f'view "{name}" already exists')
            self._prepare(tables - {name})
            if exists:
                self._ensure_view(name)
            # DuckDB cannot bind parameters in a view, so they are rendered
            # into the definition, which is what is stored and recreated.
            definition = inline(sql, vars)
            self.db.execute(definition)
            self.db.execute(f"INSERT OR REPLACE INTO {VIEWS_TABLE} VALUES (?, ?)", [name, definition])
            self._views[name] = definition
            self._local_views.add(name)
        else:
            if not exists:
                if _DROP_VIEW_RE.match(sql).group(1):
                    return None, [], -1  # IF EXISTS
                raise ValueError(f'view "{name}" does not exist')
            if name in self._local_views:
                self.db.execute(f"DROP VIEW {_q(name)}")
                self._local_views.discard(name)
            self.db.execute(f"DELETE FROM {VIEWS_TABLE} WHERE name = ?", [name])
            del self._views[name]
        self.dirty.add(VIEWS_TABLE)
        return None, [], -1

    def _local(self, duck_sql, params):
        cur = self.db.execute(duck_sql, params) if params is not None else self.db.execute(duck_sql)
        return cur

    def execute(self, sql: str, vars=None):
        """Run one statement; returns (description, rows, rowcount)."""
        kw = get_sql_operation_keyword(sql) or ""
        duck_sql, params = to_duckdb(sql, vars)
        if kw in _TXN_KEYWORDS:
            self._transaction(kw)
            self._local(duck_sql, params)
            return None, [], -1
        tables, target = statement_tables(duck_sql)
        tables = tables or frozenset()
        if _VIEW_RE.match(sql) or _DROP_VIEW_RE.match(sql):
            return self._view_statement(sql, vars, duck_sql, params, tables)
        if kw == "SELECT":
            if (tables & (self.loaded | self.dropped | self.dirty)
                    or tables & set(self._view_defs(tables))):
                return self._select_local(duck_sql, params, tables)
            try:
                data = self.suite.client.query(inline(sql, vars), ref=self.read_ref,
                                               namespace=self.suite.ns)
            except bpx.BauplanQueryError as e:
                # Not every Postgres construct plans on Bauplan's engine;
                # run it on local copies instead (once per statement shape).
                self.suite._warn_once(("query", duck_sql),
                                      f"Bauplan query failed, ran locally: {str(e)[:160]}")
                return self._select_local(duck_sql, params, tables)
            data = normalize(data)
            return [(n, None, None, None, None, None, None) for n in data.column_names], \
                arrow_rows(data), data.num_rows
        if kw == "ALTER" and _CONSTRAINT_RE.match(sql):
            # Bauplan tables have no constraints to add.
            self.suite._warn_once(("constraint",), "Bauplan: FOREIGN KEY/CHECK constraints are not kept")
            return None, [], 0
        if kw in ("CREATE", "DROP") and re.match(r"^\s*(CREATE|DROP)\s+(UNIQUE\s+)?INDEX\b", sql, re.I):
            raise NotImplementedError("Bauplan tables have no secondary indexes")
        if kw in _WRITE_KEYWORDS or kw in ("CREATE", "ALTER", "DROP"):
            if self.branch is None:
                raise ValueError("a commit ref is read-only")
            if target is None and len(tables) == 1:
                target = next(iter(tables))
            if (kw == "CREATE" and target and target in self.remote
                    and target not in self.loaded and target not in self.dropped):
                # The table exists on the branch: don't copy it to find out.
                if re.search(r"\bIF\s+NOT\s+EXISTS\b", sql, re.I):
                    return None, [], -1
                raise ValueError(f'table "{target}" already exists')
            self._prepare(tables)
            rename = _RENAME_TABLE_RE.match(sql)
            cur = self._local(duck_sql, params)
            count = cur.fetchall() if cur.description else []
            if kw == "DROP" and target:
                self.loaded.discard(target)
                self.dirty.discard(target)
                if target in self.remote:
                    self.dropped.add(target)
            elif rename:
                old, new = rename.group(1).lower(), rename.group(2).lower()
                self.loaded.discard(old)
                self.dirty.discard(old)
                if old in self.remote:
                    self.dropped.add(old)
                self.loaded.add(new)
                self.dirty.add(new)
            elif target:
                self.loaded.add(target)
                self.dirty.add(target)
                self.dropped.discard(target)
            n = count[0][0] if kw in _WRITE_KEYWORDS and count and isinstance(count[0][0], int) else -1
            return None, [], n
        # Anything else (SET, CHECKPOINT, ...) runs on the local session.
        cur = self._local(duck_sql, params)
        rows = cur.fetchall() if cur.description else []
        return cur.description, rows, len(rows)

    def _select_local(self, duck_sql, params, tables):
        self._prepare(tables)
        cur = self._local(duck_sql, params)
        return cur.description, cur.fetchall(), -1

    def _transaction(self, kw: str) -> None:
        if kw in ("BEGIN", "START"):
            self._saved = (set(self.dirty), set(self.dropped), set(self.loaded))
        elif kw in ("ROLLBACK", "ABORT") and self._saved is not None:
            self.dirty, self.dropped, self.loaded = (set(s) for s in self._saved)
            self._saved = None
        else:
            self._saved = None

    def publish(self, message: str, properties: dict = None) -> bool:
        """Write the changed tables to a temporary branch from read_ref and
        merge it into the branch: one commit. Returns False if there was
        nothing to publish; raises PublishConflict if another writer changed
        one of the tables since read_ref."""
        if not self.changed:
            return False
        suite = self.suite
        client = suite.client
        tmp = suite._temp_branch(self.branch)
        client.create_branch(tmp, from_ref=self.read_ref)
        try:
            tables = sorted(self.dirty)
            data = {t: self.db.execute(f"SELECT * FROM {_q(t)}").fetch_arrow_table() for t in tables}
            schemas = {t: local_schema(self.db, t) for t in tables}
            if tables:
                with ThreadPoolExecutor(min(len(tables), _PUBLISH_THREADS)) as pool:
                    list(pool.map(lambda t: suite._write_table(tmp, t, data[t], schemas[t],
                                                               t in self.remote), tables))
            for t in sorted(self.dropped):
                client.delete_table(t, tmp, namespace=suite.ns, if_exists=True)
            try:
                client.merge_branch(tmp, self.branch, commit_message=message,
                                    commit_properties=properties)
            except (bpx.MergeConflictError, bpx.BranchHeadChangedError, bpx.UpdateConflictError) as e:
                raise PublishConflict(str(e)) from e
        finally:
            try:
                client.delete_branch(tmp, if_exists=True)
            except Exception:
                pass
        return True


class BauplanToolSuite(DBToolSuite):
    BACKEND_NAME = "bauplan"
    SUPPORTS_COMMIT_REFS = True
    # query() reads one ref.
    SUPPORTS_MULTI_REF_EXEC = False
    IMPLEMENTATION = {
        "branch": "native",
        "commit": "native",
        "log": "native",
        "diff": "simulated",
        "merge": "native",
        "rebase": "native",
        "revert": "simulated",
        "reset": "native",
        "delete": "native",
        "commit_refs": "native",
    }
    IMPLEMENTATION_NOTES = {
        "branch": "create_branch",
        "commit": "create_tag on the branch head (every Bauplan write is already a commit)",
        "log": "get_commits, keeping the commits benchmark tags point at",
        "diff": "table ids compared per table; changed tables row-diffed locally over scan()",
        "merge": ("merge_branch; on a table-level conflict, row-level three-way merge "
                  "computed locally and published"),
        "rebase": "branch from the upstream + the branch's changes (as merge), then renamed",
        "revert": "the commit's row changes inverted locally and published",
        "reset": "branch from the commit's tag, then renamed",
        "delete": "delete_branch",
        "commit_refs": "tags (a deleted or renamed branch's name@hash no longer resolves)",
    }

    @classmethod
    def get_default_connection_uri(cls) -> str:
        return "bauplan://"

    @classmethod
    def get_initial_connection_uri(cls, db_name: str) -> str:
        return f"bauplan://{db_name}"

    @classmethod
    def init_for_bench(cls, collector: rc.ResultCollector, db_name: str,
                       default_branch_name: str = MAIN_BRANCH,
                       measure_storage: bool = False):
        return cls(new_client(), collector, db_name, default_branch_name, measure_storage)

    def __init__(self, client: bauplan.Client, collector: rc.ResultCollector,
                 db_name: str, default_branch_name: str = MAIN_BRANCH,
                 measure_storage: bool = False):
        super().__init__(None, collector, measure_storage)
        self.client = client
        self.db_name = db_name
        self.ns = namespace(db_name)
        self.root = root_branch(db_name, client)
        self.default_branch = default_branch_name
        self.conn = _Connection(self)
        self._ws = None
        self._tags: dict = {}       # commit id -> tag name
        self._schemas: dict = {}    # table id -> sidecar
        self._warned: set = set()
        self._connect_impl(Ref(default_branch_name))
        self._current_ref = Ref(default_branch_name)

    # ------------------------------------------------------------------
    # Names and refs
    # ------------------------------------------------------------------

    def _branch(self, name: str) -> str:
        if name == MAIN_BRANCH:
            return self.root
        return _ref_name(f"{self.root}{_BRANCH_SEP}{name}")

    def _temp_branch(self, branch: str) -> str:
        return _ref_name(f"{branch}{_BRANCH_SEP}{_TEMP_PREFIX}{uuid.uuid4().hex[:12]}")

    def _tag_prefix(self) -> str:
        return f"{self.root}.c"

    def _tag_name(self, commit_id: str, message: str) -> str:
        name = f"{self._tag_prefix()}{commit_id}"
        if message:
            enc = base64.b32encode(message.encode()[:_MESSAGE_BYTES]).decode().rstrip("=")
            name += f".m{enc}"
        return _ref_name(name)

    def _parse_tag(self, name: str):
        """(commit id, message) of one of the run's commit tags, or None."""
        prefix = self._tag_prefix()
        if not name.startswith(prefix):
            return None
        rest = name[len(prefix):]
        commit_id, _, enc = rest.partition(".m")
        message = ""
        if enc:
            enc += "=" * (-len(enc) % 8)
            try:
                message = base64.b32decode(enc).decode(errors="replace")
            except Exception:
                message = ""
        return commit_id, message

    def _tag_for(self, commit_id: str) -> str:
        if commit_id not in self._tags:
            want = f"{self._tag_prefix()}{commit_id}"
            names = [t.name for t in self.client.get_tags(filter_by_name=want)
                     if t.name == want or t.name.startswith(want + ".m")]
            if not names:
                raise ValueError(f"unknown commit '{commit_id}'")
            self._tags[commit_id] = names[0]
        return self._tags[commit_id]

    def _ref_name(self, ref: Ref) -> str:
        """Bauplan ref for a benchmark ref: the branch, or the commit's tag
        (a raw 64-hex Bauplan hash is used as <branch>@<hash>)."""
        if not ref.commit:
            return self._branch(ref.branch)
        if re.fullmatch(r"[0-9a-f]{64}", ref.commit):
            return f"{self._branch(ref.branch)}@{ref.commit}"
        return self._tag_for(ref.commit)

    def _pin(self, ref: Ref) -> str:
        """A ref that does not move: <branch>@<head hash>, or the tag."""
        if ref.commit:
            return self._ref_name(ref)
        branch = self._branch(ref.branch)
        return f"{branch}@{self.client.get_branch(branch).hash}"

    def _warn_once(self, key, msg: str) -> None:
        if key not in self._warned:
            self._warned.add(key)
            print(f"Warning: {msg}")

    # ------------------------------------------------------------------
    # Tables
    # ------------------------------------------------------------------

    def _table_ids(self, ref_name: str) -> dict:
        """{table: id} of the run's namespace at a ref."""
        return {t.name.lower(): str(t.id)
                for t in self.client.get_tables(ref_name, filter_by_namespace=self.ns)
                if (t.namespace or self.ns) == self.ns}

    def _schema_of(self, table_id: str):
        if table_id not in self._schemas:
            self._schemas[table_id] = _read_sidecar(self.db_name, table_id)
        return self._schemas[table_id]

    def _write_table(self, branch: str, table: str, data: pa.Table, schema: dict,
                     exists: bool) -> None:
        """Replace (or create) a table on branch with data, and record its
        schema sidecar."""
        client = new_client()  # one per publish thread
        uri = _upload(self.db_name, data)
        client.create_table(table, uri, branch=branch, namespace=self.ns,
                            replace=True if exists else None)
        if data.num_rows:
            client.import_data(table, uri, branch=branch, namespace=self.ns)
        table_id = str(client.get_table(table, ref=branch, namespace=self.ns).id)
        _write_sidecar(self.db_name, table_id, schema)
        self._schemas[table_id] = schema

    def _scan(self, table: str, ref_name: str, ids: dict):
        """Arrow for table at a ref, or None if it does not exist there."""
        if table not in ids:
            return None
        return normalize(self.client.scan(table, ref=ref_name, namespace=self.ns))

    # ------------------------------------------------------------------
    # exec(): the workspace
    # ------------------------------------------------------------------

    def _workspace(self) -> _Workspace:
        if self._ws is None:
            ref = self._current_ref
            if ref is None:
                raise ValueError("no ref selected")
            branch = None if ref.commit else self._branch(ref.branch)
            self._ws = _Workspace(self, branch, self._pin(ref))
        return self._ws

    def _exec_finish(self, session) -> None:
        ws, self._ws = self._ws, None
        if ws is None:
            return
        try:
            ws.publish(f"exec {session.label}".strip())
        finally:
            ws.close()

    def _exec_abort(self, session) -> None:
        ws, self._ws = self._ws, None
        if ws is not None:
            ws.close()

    def _exec_retry(self, exc: Exception, attempt: int) -> bool:
        if not isinstance(exc, PublishConflict) or attempt >= BAUPLAN_PUBLISH_RETRIES:
            return False
        time.sleep(min(0.5 * 2 ** attempt, 8.0))
        return True

    # ------------------------------------------------------------------
    # History
    # ------------------------------------------------------------------

    def _commits(self, ref_name: str) -> list:
        return list(self.client.get_commits(ref_name, limit=BAUPLAN_HISTORY_LIMIT))

    def _run_tags(self) -> dict:
        """{hash: [(commit id, message)]} of the run's commit tags."""
        out = {}
        for t in self.client.get_tags(filter_by_name=self._tag_prefix()):
            parsed = self._parse_tag(t.name)
            if parsed:
                self._tags.setdefault(parsed[0], t.name)
                out.setdefault(t.hash, []).append(parsed)
        for entries in out.values():
            entries.sort(reverse=True)
        return out

    @staticmethod
    def _carried(commit) -> list:
        ids = (commit.properties or {}).get(_CARRIED, "")
        return [c for c in ids.split(",") if c]

    def _history(self, ref_name: str, tags: dict = None) -> list:
        """[(commit id, message, date)] of the benchmark commits reachable
        from ref, newest first: tagged commits, and the ids merges carried."""
        tags = self._run_tags() if tags is None else tags
        messages = {cid: msg for entries in tags.values() for cid, msg in entries}
        out, seen = [], set()
        for c in self._commits(ref_name):
            date = c.committed_date or c.authored_date
            for cid, msg in tags.get(c.ref.hash, []):
                if cid not in seen:
                    seen.add(cid)
                    out.append((cid, msg, date))
            for cid in self._carried(c):
                if cid not in seen:
                    seen.add(cid)
                    out.append((cid, messages.get(cid, ""), date))
        return out

    def _carry_properties(self, source: str, target: str):
        """Commit properties for a commit bringing source's benchmark commits that
        target does not have yet (None if there are none)."""
        tags = self._run_tags()
        have = {cid for cid, _, _ in self._history(target, tags)}
        new = [cid for cid, _, _ in self._history(source, tags) if cid not in have]
        return {_CARRIED: ",".join(new)} if new else None

    def _merge_base(self, a: str, b: str):
        """A ref for the newest commit a and b share (None if none is found
        within BAUPLAN_HISTORY_LIMIT). Merge commits count the source head
        they merged, so a branch merged before has a recent base."""
        seen = set()
        for c in self._commits(a):
            seen.add(c.ref.hash)
            merged = getattr(c, "parent_merge_ref", None)
            if merged is not None and getattr(merged, "hash", None):
                seen.add(merged.hash)
        # The shared commit is in both histories, so either side's branch
        # can name it (a tag cannot: only a branch takes @hash).
        branch = next((r.split("@")[0] for r in (a, b) if "@" in r), None)
        if branch is None:
            return None
        for c in self._commits(b):
            if c.ref.hash in seen:
                return f"{branch}@{c.ref.hash}"
        return None

    # ------------------------------------------------------------------
    # Hooks
    # ------------------------------------------------------------------

    def _storage_bytes(self) -> int:
        """Table sizes on the run's root branch (Bauplan reports per table)."""
        return int(sum(t.size or 0 for t in self.client.get_tables(
            self.root, filter_by_namespace=self.ns)))

    def _connect_impl(self, ref: Ref) -> None:
        """Bauplan has no sessions: check the ref exists."""
        if self._ws is not None:
            self._ws.close()
            self._ws = None
        if ref.commit:
            self._ref_name(ref)
        elif not self.client.has_branch(self._branch(ref.branch)):
            raise ValueError(f"branch '{ref.branch}' does not exist")

    def list_branches(self) -> list:
        prefix = self.root + _BRANCH_SEP
        names = [b.name[len(prefix):] for b in self.client.get_branches(name=self.root)
                 if b.name.startswith(prefix)]
        # Temporary branches end in __tmp_<id>, under any branch.
        return [MAIN_BRANCH] + [n for n in names
                                if not (_BRANCH_SEP + n).count(_BRANCH_SEP + _TEMP_PREFIX)]

    def _branch_impl(self, name: str, from_ref: Ref) -> None:
        self.client.create_branch(self._branch(name), from_ref=self._ref_name(from_ref))

    def _commit_impl(self, ref: Ref, message: str) -> str:
        commit_id = f"{time.time_ns():x}"
        tag = self._tag_name(commit_id, message)
        self.client.create_tag(tag, from_ref=self._branch(ref.branch))
        self._tags[commit_id] = tag
        return commit_id

    def _log_impl(self, ref: Ref, limit: int) -> list:
        return [
            {"commit_hash": cid, "kind": "commit", "committer": "bauplan", "email": "",
             "date": date, "message": msg}
            for cid, msg, date in self._history(self._ref_name(ref))[:limit]
        ]

    def _diff_impl(self, ref_a: Ref, ref_b: Ref) -> dict:
        a, b = self._pin(ref_a), self._pin(ref_b)
        ids_a, ids_b = self._table_ids(a), self._table_ids(b)
        out = []
        for table in sorted(set(ids_a) | set(ids_b)):
            if table.startswith("_bb_"):
                continue  # bookkeeping (view definitions)
            entry = {"table_name": table, "rows_added": 0, "rows_deleted": 0, "rows_modified": 0}
            if ids_a.get(table) != ids_b.get(table):
                ta, tb = self._scan(table, a, ids_a), self._scan(table, b, ids_b)
                if ta is None:
                    entry["rows_added"] = tb.num_rows
                elif tb is None:
                    entry["rows_deleted"] = ta.num_rows
                else:
                    schema = self._schema_of(ids_b[table]) or self._schema_of(ids_a[table]) or {}
                    added, deleted, modified = diff_counts(ta, tb, schema.get("pk", []))
                    entry.update(rows_added=added, rows_deleted=deleted, rows_modified=modified)
            out.append(entry)
        return {
            "tables": out,
            "rows_added": sum(t["rows_added"] for t in out),
            "rows_deleted": sum(t["rows_deleted"] for t in out),
            "rows_modified": sum(t["rows_modified"] for t in out),
        }

    def _replace_branch(self, branch: str, from_ref: str) -> None:
        """Make branch a new branch at from_ref (tags keep the old one's
        commits readable)."""
        tmp = self._temp_branch(branch)
        self.client.create_branch(tmp, from_ref=from_ref)
        try:
            self.client.delete_branch(branch)
        except BaseException:
            self.client.delete_branch(tmp, if_exists=True)
            raise
        self.client.rename_branch(tmp, branch)

    def _reset_impl(self, ref: Ref, to: str) -> None:
        self._replace_branch(self._branch(ref.branch), self._ref_name(Ref(ref.branch, to)))

    def _revert_impl(self, ref: Ref, commit: str) -> None:
        branch = self._branch(ref.branch)
        after_ref = self._ref_name(Ref(ref.branch, commit))
        tags = self._run_tags()
        commits = self._commits(after_ref)
        before_ref = None
        for c in commits[1:]:
            if c.ref.hash in tags:
                before_ref = self._tag_for(tags[c.ref.hash][0][0])
                break
        if before_ref is None:
            raise ValueError(f"commit '{commit}' has no earlier commit to revert to")
        ids_before, ids_after = self._table_ids(before_ref), self._table_ids(after_ref)
        ws = _Workspace(self, branch, self._pin(Ref(ref.branch)))
        try:
            for table in sorted(set(ids_before) | set(ids_after)):
                if ids_before.get(table) == ids_after.get(table) or table not in ws.remote:
                    continue
                ws.load(table)
                pk = local_schema(ws.db, table)["pk"]
                if not pk:
                    continue
                empty = ws.db.execute(f"SELECT * FROM {_q(table)} LIMIT 0").fetch_arrow_table()
                before = self._scan(table, before_ref, ids_before)
                after = self._scan(table, after_ref, ids_after)
                revert_local(ws.db, table, before if before is not None else empty,
                             after if after is not None else empty, pk)
                ws.dirty.add(table)
            ws.publish(f"revert {commit}")
        finally:
            ws.close()
        self._commit_impl(ref, f"revert {commit}")

    # ------------------------------------------------------------------
    # Merge and rebase
    # ------------------------------------------------------------------

    def _apply_changes(self, ws: _Workspace, base, theirs: str, on_conflict,
                       session_ref: Ref, properties: dict = None) -> dict:
        """Bring theirs' changes since base into the workspace's branch
        (ours): tables only theirs changed are copied with revert_table,
        tables both changed are merged row by row locally. Publishes."""
        client = self.client
        ids_base = self._table_ids(base) if base else {}
        ids_ours, ids_theirs = ws.remote, self._table_ids(theirs)
        info = {"conflicts": 0, "conflict_tables": [], "ours_changes": 0, "theirs_changes": 0,
                "merged_tables": [], "copied_tables": [], "two_way": base is None}
        conflicts = []
        for table in sorted(set(ids_base) | set(ids_ours) | set(ids_theirs)):
            b, o, t = ids_base.get(table), ids_ours.get(table), ids_theirs.get(table)
            ours_changed, theirs_changed = o != b, t != b
            info["ours_changes"] += int(ours_changed)
            if not theirs_changed or t == o:
                continue
            if not ours_changed:
                # Only theirs changed it: take their table as is.
                if t is None:
                    client.delete_table(table, ws.branch, namespace=self.ns, if_exists=True)
                else:
                    client.revert_table(table, namespace=self.ns, source_ref=theirs,
                                        into_branch=ws.branch, replace=True if o else None,
                                        commit_properties=properties)
                info["copied_tables"].append(table)
                info["theirs_changes"] += 1
                continue
            if t is None or o is None:
                continue  # deleted on one side, changed on the other: keep ours
            ws.load(table)
            ours_schema = local_schema(ws.db, table)
            their_schema = self._schema_of(t) or {}
            if their_schema.get("pk") and their_schema["pk"] != ours_schema["pk"]:
                raise RuntimeError(f"schema conflict on {table}: primary keys differ")
            have = {c for c, _ in ours_schema["columns"]}
            for col, ctype in their_schema.get("columns", []):
                if col not in have:
                    ws.db.execute(f"ALTER TABLE {_q(table)} ADD COLUMN {_q(col)} {ctype}")
            theirs_data = self._scan(table, theirs, ids_theirs)
            base_data = self._scan(table, base, ids_base) if base else None
            if base_data is None:
                base_data = theirs_data.slice(0, 0)
            r = three_way_local(ws.db, table, base_data, theirs_data, ours_schema["pk"], on_conflict)
            ws.dirty.add(table)
            info["merged_tables"].append(table)
            info["theirs_changes"] += int(r["theirs_changes"] > 0)
            if r["conflicts"]:
                info["conflicts"] += r["conflicts"]
                info["conflict_tables"].append(table)
                if r["rows"]:
                    conflicts.append({"table": table, "rows": r["rows"]})
        if conflicts:
            self._ws = ws
            try:
                info["resolution"] = on_conflict(self._conflict_session(session_ref), conflicts)
            finally:
                self._ws = None
            info["resolved"] = "custom"
        elif info["conflicts"]:
            info["resolved"] = on_conflict
        ws.publish("resolve merge", properties)
        return info

    def _merge_impl(self, into: Ref, source: Ref, message: str, on_conflict="ours") -> dict:
        target = self._branch(into.branch)
        theirs = self._pin(source)
        if theirs.split("@")[0] == target:
            raise ValueError(f"Cannot merge branch '{source.branch}' into itself")
        message = message or f"Merge {source} into {into}"
        props = self._carry_properties(theirs, target)
        head = self.client.get_branch(target).hash
        try:
            self.client.merge_branch(theirs, target, commit_message=message,
                                     commit_properties=props)
            theirs_hashes = {c.ref.hash for c in self._commits(theirs)}
            return {"fast_forward": head in theirs_hashes, "conflicts": 0,
                    "conflict_tables": [], "native": True}
        except bpx.MergeConflictError as e:
            native_error = str(e)
        # Table-level conflict: merge the conflicting tables row by row on a
        # branch from the target's head, then merge that branch in.
        ours = f"{target}@{head}"
        base = self._merge_base(ours, theirs)
        tmp = self._temp_branch(target)
        self.client.create_branch(tmp, from_ref=ours)
        try:
            ws = _Workspace(self, tmp, f"{tmp}@{self.client.get_branch(tmp).hash}")
            try:
                info = self._apply_changes(ws, base, theirs, on_conflict, into, props)
            finally:
                ws.close()
            self.client.merge_branch(tmp, target, commit_message=message,
                                     commit_properties=props)
        finally:
            self.client.delete_branch(tmp, if_exists=True)
        info.update(fast_forward=False, native=False, native_conflict=native_error[:300])
        return info

    def _rebase_impl(self, ref: Ref, onto: Ref, on_conflict="ours") -> dict:
        branch = self._branch(ref.branch)
        upstream = self._pin(onto)
        if upstream.split("@")[0] == branch:
            raise ValueError(f"Cannot rebase branch '{ref.branch}' onto itself")
        mine = self._pin(ref)
        base = self._merge_base(upstream, mine)
        props = self._carry_properties(mine, upstream)
        tmp = self._temp_branch(branch)
        self.client.create_branch(tmp, from_ref=upstream)
        try:
            # The upstream is "ours" (as in Dolt); the branch's changes are
            # "theirs".
            ws = _Workspace(self, tmp, f"{tmp}@{self.client.get_branch(tmp).hash}")
            try:
                info = self._apply_changes(ws, base, mine, on_conflict, ref, props)
            finally:
                ws.close()
            self.client.delete_branch(branch)
        except BaseException:
            self.client.delete_branch(tmp, if_exists=True)
            raise
        self.client.rename_branch(tmp, branch)
        info["up_to_date"] = info["ours_changes"] == 0
        return info

    def _delete_impl(self, ref: Ref) -> None:
        self.client.delete_branch(self._branch(ref.branch))
        if self._current_ref is not None and self._current_ref.branch == ref.branch:
            self._current_ref = None

    def close_connection(self) -> None:
        if self._ws is not None:
            self._ws.close()
            self._ws = None
        self._current_ref = None
