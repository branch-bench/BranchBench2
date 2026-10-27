"""SeekDB backend.

SeekDB (OceanBase) is MySQL-compatible and branches by forking whole
databases (copy-on-write, milliseconds) rather than keeping named branches
inside one database. Each of our branches is therefore its own database:

    main branch      -> <db_name>
    branch "X"       -> <db_name>__X

The mapping is derived from the names alone, so every worker (each with its
own connection) resolves the same branch to the same database with no shared
state.

SeekDB has no commits of its own. The verbs that need them are built from
two primitives it does have:

* ``current_scn()`` and flashback reads (``<table> AS OF SNAPSHOT <scn>``),
  which read a table as it was at an earlier SCN (kept for ``undo_retention``
  seconds; raise it with ``ALTER SYSTEM SET undo_retention = 86400``);
* ``FORK DATABASE`` / ``DROP DATABASE``.

A commit is a row in the branch database's ``_bb_commits`` table recording
the SCN the branch was at; forks copy that table, so a branch inherits its
parent's history. Each row also names the database whose flashback holds the
snapshot (a fork's tables have no history before the fork).

    branch     FORK DATABASE <parent db> TO <branch db>; from a commit,
               the fork's tables are then restored to that snapshot
    commit     INSERT INTO _bb_commits (id = current_scn())
    log        SELECT FROM _bb_commits
    diff       anti-joins between the two states (snapshots or heads)
    reset      restore every table from the commit's snapshot, in place
    revert     apply the inverse of the commit's (before, after) snapshots
    merge      SQL three-way merge: base = source's fork point, ours = the
               target database, theirs = the source database
    rebase     same three-way merge written into the branch, with base = the
               branch's fork point and theirs = the upstream branch
    delete     DROP DATABASE <branch db>

The three-way merge works per table with primary keys (rows are matched by
key; tables without one, like ``history``, only receive the other side's new
rows). Conflicts are rows both sides changed differently; ``on_conflict``
decides ("ours" | "theirs" | callable, see dblib.db_api). A table whose
columns were added on one side gets those columns on the other; a differing
primary key is a schema conflict and the verb fails without touching data.

Native-only mode (``DBFORK_NATIVE_ONLY=1``) switches merge() to SeekDB's own
``MERGE TABLE ... STRATEGY OURS|THEIRS`` per table, which has no common
ancestor (every differing row is a conflict and the source's deletes are
never applied) but measures the native primitive.

Because every branch is a database on the same server, a multi-ref script
can address another branch as `<db>__<branch>`.`<table>`.
"""

import contextlib
import json
import os
import time

import aiomysql
import pymysql
from pymysql.constants import CLIENT

from dblib.db_api import DBToolSuite, Ref, native_only
from dblib import util as dbutil, mysql_common
import dblib.result_collector as rc

SEEKDB_USER = os.environ.get("SEEKDB_USER", "root")
SEEKDB_PASSWORD = os.environ.get("SEEKDB_PASSWORD", "")
SEEKDB_HOST = os.environ.get("SEEKDB_HOST") or os.environ.get("DBFORK_SERVER_HOST", "127.0.0.1")
SEEKDB_PORT = int(os.environ.get("SEEKDB_PORT", "2881"))


def _default_data_dir() -> str:
    """Where the SeekDB server keeps its data: SEEKDB_DATA_DIR, else the
    first of the source build's `~/seekdb/store` and the brew install's
    data dir that exists."""
    env = os.environ.get("SEEKDB_DATA_DIR")
    if env:
        return os.path.expanduser(env)
    for cand in ("~/seekdb/store", "/opt/homebrew/var/seekdb/data"):
        path = os.path.expanduser(cand)
        if os.path.isdir(path):
            return path
    return os.path.expanduser("~/seekdb/store")


SEEKDB_DATA_DIR = _default_data_dir()

# Use SeekDB's own MERGE TABLE instead of the SQL three-way merge.
SEEKDB_NATIVE_MERGE = native_only()
# Use SeekDB's own DIFF TABLE instead of the anti-joins between flashback reads.
SEEKDB_NATIVE_DIFF = native_only()

# OceanBase's default 10s query timeout is too short for seeding and for the
# anti-joins behind merge/rebase/reset; one hour, in microseconds.
_SESSION_INIT = ("SET SESSION ob_query_timeout = 3600000000, "
                 "ob_trx_timeout = 3600000000, ob_trx_idle_timeout = 3600000000")

# Branch name that maps to the root database itself.
MAIN_BRANCH = "main"

# Separates the root database name from the branch name in a branch database.
_BRANCH_SEP = "__"

# Prefix (after the separator) of the temporary databases that materialise
# a commit ref for exec().
_TEMP_PREFIX = "tmp_"

# MySQL's limit on database name length.
_MAX_DB_NAME_LEN = 64

# Error SeekDB returns when MERGE TABLE's / DIFF TABLE's two tables have
# different columns or primary keys ("Schema error").
_ER_SCHEMA_MISMATCH = 4029
# MERGE TABLE with STRATEGY FAIL (the default) when rows conflict.
_ER_MERGE_CONFLICTS = 4179
# MERGE TABLE / DIFF TABLE on a table without a primary key.
_ER_NO_PRIMARY_KEY = 1235
# Flashback read of a table at an SCN before the table existed.
_ER_SNAPSHOT_BEFORE_TABLE = 1412

# Per-database bookkeeping tables; never merged, diffed or restored.
META_PREFIX = "_bb_"
COMMITS_TABLE = "_bb_commits"
KEYS_TABLE = "_bb_keys"

_COMMITS_DDL = f"""
CREATE TABLE IF NOT EXISTS `{COMMITS_TABLE}` (
    seq BIGINT NOT NULL,
    id VARCHAR(40) NOT NULL,
    kind VARCHAR(8) NOT NULL,
    message TEXT,
    db VARCHAR(64) NOT NULL,
    scn_start BIGINT NOT NULL,
    scn_end BIGINT NOT NULL,
    parent VARCHAR(64),
    alt_db VARCHAR(64),
    alt_scn BIGINT,
    schema_json LONGTEXT,
    ts DATETIME(6),
    PRIMARY KEY (seq),
    KEY (id)
)
"""

# Scratch rows for the three-way merge: (tag, table, key string, side).
_KEYS_DDL = f"""
CREATE TABLE IF NOT EXISTS `{KEYS_TABLE}` (
    tag VARCHAR(24) NOT NULL,
    tbl VARCHAR(64) NOT NULL,
    k VARCHAR(512) NOT NULL,
    side CHAR(1) NOT NULL,
    PRIMARY KEY (tag, tbl, k)
)
"""


# How long to retry a flashback read that fails with 1412 for a table the
# commit recorded, in case an index build is still in progress.
_UNREADABLE_WAIT_SEC = 1


def connect(db_name: str = None, autocommit: bool = True, **kwargs):
    """Open a PyMySQL connection to the SeekDB server."""
    kwargs.setdefault("init_command", _SESSION_INIT)
    return pymysql.connect(
        host=SEEKDB_HOST,
        port=SEEKDB_PORT,
        user=SEEKDB_USER,
        password=SEEKDB_PASSWORD,
        database=db_name,
        autocommit=autocommit,
        **kwargs,
    )


async def create_pool_async(db_name: str, size: int, autocommit: bool = True):
    """Open an aiomysql pool of `size` connections to the SeekDB server."""
    return await aiomysql.create_pool(
        minsize=size,
        maxsize=size,
        host=SEEKDB_HOST,
        port=SEEKDB_PORT,
        user=SEEKDB_USER,
        password=SEEKDB_PASSWORD,
        db=db_name,
        autocommit=autocommit,
        init_command=_SESSION_INIT,
    )


def _quote(ident: str) -> str:
    """Quote a MySQL identifier (database, table or column name)."""
    return "`" + ident.replace("`", "``") + "`"


def _qt(db: str, table: str) -> str:
    return f"{_quote(db)}.{_quote(table)}"


def branch_db_name(db_name: str, branch_name: str) -> str:
    """Database that holds branch_name of the root database db_name."""
    if branch_name == MAIN_BRANCH:
        return db_name
    name = f"{db_name}{_BRANCH_SEP}{branch_name}"
    if len(name) > _MAX_DB_NAME_LEN:
        raise ValueError(
            f"SeekDB database name '{name}' for branch '{branch_name}' is longer "
            f"than {_MAX_DB_NAME_LEN} characters"
        )
    return name


def _branch_databases(cur, db_name: str) -> list:
    """Names of all databases forked from db_name (branches and commit-ref
    temporaries), not db_name itself."""
    prefix = db_name + _BRANCH_SEP
    cur.execute("SHOW DATABASES;")
    return [name for (name,) in cur.fetchall() if name.startswith(prefix)]


def load_sql_dump(db_name: str, sql_path: str) -> None:
    """Load a SQL file (plain SQL or pg_dump output) into db_name."""
    conn = connect(db_name, client_flag=CLIENT.MULTI_STATEMENTS)
    try:
        mysql_common.load_sql_dump(conn, sql_path)
    finally:
        conn.close()


def setup_database(db_name: str, sql_path: str = None) -> None:
    """(Re)create db_name, with no branches, and load sql_path (if given)
    into it."""
    drop_database(db_name)
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE {_quote(db_name)};")
        print("Database created successfully.")
    finally:
        conn.close()
    if sql_path:
        load_sql_dump(db_name, sql_path)


def drop_database(db_name: str) -> None:
    """Drop db_name and every database forked from it."""
    conn = connect()
    try:
        with conn.cursor() as cur:
            for branch_db in _branch_databases(cur, db_name):
                cur.execute(f"DROP DATABASE IF EXISTS {_quote(branch_db)};")
            cur.execute(f"DROP DATABASE IF EXISTS {_quote(db_name)};")
        print(f"Database '{db_name}' deleted successfully.")
    finally:
        conn.close()


def _commit_id(scn: int) -> str:
    return f"{int(scn):x}"


class _State:
    """A ref resolved to a database and, for commit refs, an SCN, plus the
    {table: [columns]} fingerprint recorded with the commit (None for a
    branch head)."""

    __slots__ = ("db", "scn", "schema", "alt")

    def __init__(self, db: str, scn: int = None, schema: dict = None,
                 alt: "_State" = None):
        self.db = db
        self.scn = scn
        self.schema = schema
        # Fallback snapshot of the same state held elsewhere (a fork point
        # also exists in the parent at the SCN just before the fork).
        self.alt = alt

    def had_table(self, table: str):
        """True/False per the fingerprint, None when unknown."""
        if self.schema is None:
            return None
        return table in self.schema

    def src(self, table: str) -> str:
        """Table expression for a FROM clause: the table itself for a branch
        head, or a flashback read for a snapshot. The flashback read is
        wrapped in a derived table that pins the primary key as the access
        path: SeekDB plans a flashback read like any other query and may
        pick a secondary index, and an index built after the snapshot makes
        the read fail with 1412 ("table definition has changed") although
        the rows are still readable through the primary (found 2026-10-14,
        S1.5 on historical forks). Callers add their own alias after it."""
        expr = _qt(self.db, table)
        if self.scn:
            return (f"(SELECT /*+ INDEX(fb PRIMARY) */ * FROM {expr} "
                    f"AS OF SNAPSHOT {int(self.scn)} fb)")
        return expr


class SeekDBToolSuite(DBToolSuite):
    BACKEND_NAME = "seekdb"
    STORAGE_SCOPE = "server"
    SUPPORTS_COMMIT_REFS = True
    SCHEMA_DIALECT = "mysql"
    SUPPORTS_MULTI_REF_EXEC = True
    # For the report: FORK/DROP DATABASE and cross-database queries are
    # native; everything else is built on current_scn() + flashback reads.
    IMPLEMENTATION = {
        "branch": "native",
        "commit": "native",
        "diff": "native" if SEEKDB_NATIVE_DIFF else "simulated",
        "log": "simulated",
        "merge": "native" if SEEKDB_NATIVE_MERGE else "simulated",
        "rebase": "simulated",
        "revert": "simulated",
        "reset": "simulated",
        "delete": "native",
        "commit_refs": "native",
        "multi_ref_exec": "native",
    }
    IMPLEMENTATION_NOTES = {
        "branch": "FORK DATABASE (restored to the snapshot for a commit ref)",
        "commit": "current_scn() recorded in a _bb_commits row (flashback snapshot)",
        "diff": ("DIFF TABLE per table (rows present on one side only, grouped by key; keyless or schema-differing tables skipped)"
                 if SEEKDB_NATIVE_DIFF else "anti-joins between flashback reads"),
        "log": "SELECT from the _bb_commits bookkeeping table",
        "merge": ("MERGE TABLE ... STRATEGY OURS|THEIRS per table (no common ancestor)"
                  if SEEKDB_NATIVE_MERGE else "SQL three-way merge against the fork SCN"),
        "rebase": "SQL three-way merge of the upstream into the branch",
        "revert": "inverse of the commit's (before, after) flashback snapshots applied with SQL",
        "reset": "rows restored with SQL from the flashback snapshot",
        "delete": "DROP DATABASE",
        "commit_refs": "flashback reads AS OF SNAPSHOT scn",
        "multi_ref_exec": "cross-database queries",
    }

    @classmethod
    def get_default_connection_uri(cls) -> str:
        return cls.get_initial_connection_uri("")

    @classmethod
    def get_initial_connection_uri(cls, db_name: str) -> str:
        return f"mysql://{SEEKDB_USER}:{SEEKDB_PASSWORD}@{SEEKDB_HOST}:{SEEKDB_PORT}/{db_name}"

    @classmethod
    def init_for_bench(
        cls,
        collector: rc.ResultCollector,
        db_name: str,
        default_branch_name: str = MAIN_BRANCH,
        measure_storage: bool = False,
    ):
        return cls(connect(db_name), collector, db_name, default_branch_name,
                   measure_storage)

    def __init__(
        self,
        connection,
        collector: rc.ResultCollector,
        db_name: str,
        default_branch_name: str = MAIN_BRANCH,
        measure_storage: bool = False,
    ):
        super().__init__(connection, collector, measure_storage)
        # Root database; every branch database name is derived from it.
        self.db_name = db_name
        self.default_branch = default_branch_name
        self._current_db = None
        # Commit-ref temporaries this suite created: Ref string -> database.
        self._temp_dbs: dict = {}
        # (table, reason) pairs already warned about, so a table skipped on
        # every merge is reported once.
        self._merge_skip_warned = set()
        self._unreadable_warned = set()
        self._ensure_meta(self._db_for(default_branch_name))
        self._connect_impl(Ref(default_branch_name))
        self._current_ref = Ref(default_branch_name)

    # ------------------------------------------------------------------
    # Branch name <-> database name
    # ------------------------------------------------------------------

    def _db_for(self, branch: str) -> str:
        return branch_db_name(self.db_name, branch)

    def _use(self, db: str) -> None:
        self._execute(f"USE {_quote(db)};")
        self._current_db = db

    def _on(self, ref: Ref) -> str:
        """Point the connection at the branch's database; returns it."""
        db = self._db_for(ref.branch)
        if self._current_db != db:
            self._use(db)
        self._current_ref = Ref(ref.branch)
        return db

    def _get_table_columns(self, table_name: str) -> list:
        rows = self._execute(mysql_common.TABLE_COLUMNS_QUERY, (table_name,))
        return mysql_common.normalize_column_types(rows)

    def list_branches(self) -> list:
        with self.conn.cursor() as cur:
            branch_dbs = _branch_databases(cur, self.db_name)
        prefix = self.db_name + _BRANCH_SEP
        names = [db[len(prefix):] for db in branch_dbs]
        return [MAIN_BRANCH] + [n for n in names if not n.startswith(_TEMP_PREFIX)]

    def _scn(self) -> int:
        return int(self._execute("SELECT current_scn();")[0][0])

    # ------------------------------------------------------------------
    # Commit log
    # ------------------------------------------------------------------

    def _ensure_meta(self, db: str) -> None:
        self._execute(_COMMITS_DDL.replace(f"`{COMMITS_TABLE}`", _qt(db, COMMITS_TABLE)))
        self._execute(_KEYS_DDL.replace(f"`{KEYS_TABLE}`", _qt(db, KEYS_TABLE)))
        n = self._execute(f"SELECT COUNT(*) FROM {_qt(db, COMMITS_TABLE)};")[0][0]
        if not n:
            scn = self._scn()
            self._add_commit(db, "root", "initial state", db, scn, scn)

    def _add_commit(self, db: str, kind: str, message: str, snap_db: str,
                    scn_start: int, scn_end: int, commit_id: str = None,
                    parent: str = None, schema: dict = None,
                    alt: _State = None) -> str:
        """Append a row to db's log; returns the commit id. ``parent`` is
        the database a fork/rebase row descends from; ``schema`` is the
        {table: [columns]} fingerprint of snap_db at scn_end (captured
        when not given); ``alt`` is a fallback copy of the same state."""
        commit_id = commit_id or _commit_id(scn_end)
        if schema is None:
            schema = self._fingerprint(snap_db)
        seq = int(scn_end)
        for _ in range(5):
            try:
                self._execute(
                    f"INSERT INTO {_qt(db, COMMITS_TABLE)} "
                    "(seq, id, kind, message, db, scn_start, scn_end, parent, "
                    "alt_db, alt_scn, schema_json, ts) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(6))",
                    (seq, commit_id, kind, message or "", snap_db,
                     int(scn_start), int(scn_end), parent,
                     alt.db if alt else None, alt.scn if alt else None,
                     json.dumps(schema)),
                )
                return commit_id
            except pymysql.err.IntegrityError:
                seq += 1  # two writers recorded the same SCN
        raise RuntimeError(f"could not record commit {commit_id} on {db}")

    def _schema_at(self, ref: Ref) -> dict:
        """A commit's schema is the typed fingerprint recorded with it (a
        fork restored to the commit would cost a full restore); a branch
        head is read from the catalog."""
        if ref.commit:
            row = self._find_commit(self._db_for(ref.branch), ref.commit)
            fp = json.loads(row["schema_json"]) if row.get("schema_json") else None
            if fp and all(isinstance(v, dict) for v in fp.values()):
                return {t: {c: str(ty).lower() for c, ty in cols.items()} for t, cols in fp.items()}
        return super()._schema_at(ref)

    def _fingerprint(self, db: str) -> dict:
        """{table: {column: type}} of db's data tables (older rows hold
        {table: [columns]}; consumers only need the names from those)."""
        rows = self._execute(
            "SELECT c.table_name, c.column_name, c.column_type FROM information_schema.columns c "
            "JOIN information_schema.tables t ON t.table_schema = c.table_schema "
            "AND t.table_name = c.table_name AND t.table_type = 'BASE TABLE' "
            "WHERE c.table_schema = %s AND c.table_name NOT LIKE %s "
            "ORDER BY c.table_name, c.ordinal_position",
            (db, META_PREFIX.replace("_", r"\_") + "%"))
        out = {}
        for table, col, ctype in rows or []:
            out.setdefault(table, {})[col] = str(ctype).lower()
        return out

    @staticmethod
    def _row_state(row: dict) -> _State:
        schema = row.get("schema_json")
        return _State(row["db"], int(row["scn_end"]),
                      json.loads(schema) if schema else None)

    def _log_rows(self, db: str, limit: int = None, max_seq: int = None) -> list:
        sql = (f"SELECT seq, id, kind, message, db, scn_start, scn_end, ts "
               f"FROM {_qt(db, COMMITS_TABLE)}")  # parent is read by _fork_parent
        args = []
        if max_seq is not None:
            sql += " WHERE seq <= %s"
            args.append(int(max_seq))
        sql += " ORDER BY seq DESC"
        if limit:
            sql += f" LIMIT {int(limit)}"
        rows = self._execute(sql, tuple(args) if args else None) or []
        keys = ("seq", "id", "kind", "message", "db", "scn_start", "scn_end", "ts")
        return [dict(zip(keys, r)) for r in rows]

    def _find_commit(self, db: str, commit_id: str) -> dict:
        rows = self._execute(
            f"SELECT seq, id, kind, message, db, scn_start, scn_end, ts, schema_json "
            f"FROM {_qt(db, COMMITS_TABLE)} WHERE id = %s ORDER BY seq DESC LIMIT 1",
            (str(commit_id),),
        )
        if not rows:
            raise ValueError(f"unknown commit '{commit_id}' on {db}")
        keys = ("seq", "id", "kind", "message", "db", "scn_start", "scn_end", "ts", "schema_json")
        return dict(zip(keys, rows[0]))

    def _head(self, db: str) -> dict:
        return self._log_rows(db, limit=1)[0]

    def _base(self, db: str) -> _State:
        """The branch's fork point (or last rebase): the base of a
        three-way merge involving it."""
        rows = self._execute(
            f"SELECT db, scn_end, schema_json, alt_db, alt_scn FROM {_qt(db, COMMITS_TABLE)} "
            "WHERE kind IN ('fork', 'rebase', 'root') ORDER BY seq DESC LIMIT 1"
        )
        if not rows:
            raise ValueError(f"{db} has no fork point recorded")
        snap_db, scn, schema, alt_db, alt_scn = rows[0]
        schema = json.loads(schema) if schema else None
        alt = _State(alt_db, int(alt_scn), schema) if alt_db else None
        return _State(snap_db, int(scn), schema, alt)

    def _fork_parent(self, db: str):
        """Database the branch was last forked from or rebased onto."""
        rows = self._execute(
            f"SELECT parent FROM {_qt(db, COMMITS_TABLE)} "
            "WHERE kind IN ('fork', 'rebase') ORDER BY seq DESC LIMIT 1")
        return rows[0][0] if rows else None

    def _merge_base(self, ours_db: str, theirs_db: str) -> _State:
        """Common ancestor of the two databases: the fork point of
        whichever descends from the other (theirs by default)."""
        if self._fork_parent(theirs_db) != ours_db and self._fork_parent(ours_db) == theirs_db:
            return self._base(ours_db)
        return self._base(theirs_db)

    def _state(self, ref: Ref) -> _State:
        db = self._db_for(ref.branch)
        if not ref.commit:
            return _State(db)
        return self._row_state(self._find_commit(db, ref.commit))

    # ------------------------------------------------------------------
    # Table metadata
    # ------------------------------------------------------------------

    def _tables(self, db: str) -> list:
        rows = self._execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = %s AND table_type = 'BASE TABLE' "
            "AND table_name NOT LIKE %s ORDER BY table_name",
            (db, META_PREFIX.replace("_", r"\_") + "%"),
        )
        return [r[0] for r in rows or []]

    def _columns(self, db: str, table: str) -> list:
        """[(name, column_type, is_nullable, column_default)] in column order."""
        rows = self._execute(
            "SELECT column_name, column_type, is_nullable, column_default "
            "FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
            (db, table),
        )
        return [(r[0], r[1], r[2], r[3]) for r in rows or []]

    def _pk(self, db: str, table: str) -> list:
        rows = self._execute(
            "SELECT column_name FROM information_schema.key_column_usage "
            "WHERE table_schema = %s AND table_name = %s AND constraint_name = 'PRIMARY' "
            "ORDER BY ordinal_position",
            (db, table),
        )
        return [r[0] for r in rows or []]

    def _tables_with_pk_flag(self, db: str) -> dict:
        rows = self._execute(
            """
            SELECT t.table_name, c.constraint_name IS NOT NULL
            FROM information_schema.tables t
            LEFT JOIN information_schema.table_constraints c
                ON c.table_schema = t.table_schema
                AND c.table_name = t.table_name
                AND c.constraint_type = 'PRIMARY KEY'
            WHERE t.table_schema = %s AND t.table_type = 'BASE TABLE'
                AND t.table_name NOT LIKE %s
            ORDER BY t.table_name;
            """,
            (db, META_PREFIX.replace("_", r"\_") + "%"),
        )
        return {table: bool(has_pk) for table, has_pk in rows or []}

    def _snapshot_exists(self, state: _State, table: str):
        """True when the table can be read at the snapshot, False when it
        did not exist there, None when it existed (per the commit's
        fingerprint) but its history cannot be read: SeekDB raises 1412 for
        a flashback read planned through an index built after the snapshot
        (``_State.src`` pins the primary key, so this is now only expected
        while an index build is in progress). Callers degrade for None."""
        if not state.scn:
            return True
        expected = state.had_table(table)
        if expected is False:
            return False
        deadline = time.monotonic() + (_UNREADABLE_WAIT_SEC if expected else 0)
        while True:
            try:
                self._execute(f"SELECT 1 FROM {state.src(table)} s LIMIT 1")
                return True
            except pymysql.MySQLError as e:
                if not (e.args and e.args[0] == _ER_SNAPSHOT_BEFORE_TABLE):
                    raise
                if time.monotonic() >= deadline:
                    if not expected:
                        return False
                    key = (state.db, table, state.scn)
                    if key not in self._unreadable_warned:
                        self._unreadable_warned.add(key)
                        print(f"Warning: SeekDB flashback of {state.db}.{table} at "
                              f"{state.scn} is unreadable (DDL after the snapshot)")
                    return None
                time.sleep(0.5)

    # ------------------------------------------------------------------
    # SQL fragments
    # ------------------------------------------------------------------

    @staticmethod
    def _cols(alias: str, cols: list) -> str:
        return ", ".join(f"{alias}.{_quote(c)}" for c in cols)

    @staticmethod
    def _same(a: str, b: str, cols: list) -> str:
        if not cols:
            return "TRUE"
        return " AND ".join(f"{a}.{_quote(c)} <=> {b}.{_quote(c)}" for c in cols)

    @staticmethod
    def _join(a: str, b: str, pk: list) -> str:
        return " AND ".join(f"{a}.{_quote(c)} = {b}.{_quote(c)}" for c in pk)

    @staticmethod
    def _key(alias: str, pk: list) -> str:
        parts = ", ".join(f"CAST({alias}.{_quote(c)} AS CHAR)" for c in pk)
        return f"CONCAT_WS('|', {parts})"

    @staticmethod
    def _upsert(dst: str, cols: list, pk: list, select: str) -> str:
        """INSERT ... SELECT that updates rows whose key exists (no
        delete-and-reinsert, so concurrent readers never miss the row)."""
        rest = [c for c in cols if c not in pk]
        head = f"INSERT INTO {dst} ({', '.join(map(_quote, cols))}) {select}"
        if not rest:
            return head.replace("INSERT INTO", "INSERT IGNORE INTO", 1)
        updates = ", ".join(f"{_quote(c)} = VALUES({_quote(c)})" for c in rest)
        return f"{head} ON DUPLICATE KEY UPDATE {updates}"

    @contextlib.contextmanager
    def _bulk(self):
        """One transaction with foreign-key checks off: the row-level
        rewrites behind reset/revert/merge/rebase touch tables in name
        order, so parents and children are only consistent at the end,
        and a failure must leave the branch untouched."""
        self._execute("SET SESSION foreign_key_checks = 0")
        self._execute("BEGIN")
        try:
            yield
        except BaseException:
            try:
                self._execute("ROLLBACK")
            finally:
                self._execute("SET SESSION foreign_key_checks = 1")
            raise
        self._execute("COMMIT")
        self._execute("SET SESSION foreign_key_checks = 1")

    # ------------------------------------------------------------------
    # Hooks
    # ------------------------------------------------------------------

    def _storage_bytes(self) -> int:
        """Size of the server's data directory. SeekDB keeps all databases
        in one store, so this covers the whole server."""
        return dbutil.get_directory_size_bytes(SEEKDB_DATA_DIR, ssh=dbutil.ssh_target("SEEKDB_SSH"))

    def _connect_impl(self, ref: Ref) -> None:
        if ref.commit:
            self._use(self._temp_db_for(ref))
            return
        self._use(self._db_for(ref.branch))

    def _temp_db_for(self, ref: Ref) -> str:
        """A database holding ``ref``'s branch as of its commit, created on
        first use and dropped when the connection closes."""
        key = str(ref)
        db = self._temp_dbs.get(key)
        if db:
            return db
        src = self._db_for(ref.branch)
        row = self._find_commit(src, ref.commit)
        db = branch_db_name(self.db_name, f"{_TEMP_PREFIX}{ref.commit}_{os.getpid() % 10000}_{len(self._temp_dbs)}")
        self._execute(f"DROP DATABASE IF EXISTS {_quote(db)};")
        self._execute(f"FORK DATABASE {_quote(src)} TO {_quote(db)};")
        self._restore(db, self._row_state(row))
        self._temp_dbs[key] = db
        return db

    def _branch_impl(self, name: str, from_ref: Ref) -> None:
        src = self._db_for(from_ref.branch)
        db = branch_db_name(self.db_name, name)
        # The fork point is the fork's own content (its SCN right after the
        # fork, exact); the parent at the SCN just before the fork is kept
        # as a fallback for tables whose history DDL on the branch later
        # makes unreadable (it may miss rows written in between).
        scn_before = self._scn()
        self._execute(f"FORK DATABASE {_quote(src)} TO {_quote(db)};")
        if from_ref.commit:
            row = self._find_commit(src, from_ref.commit)
            base = self._row_state(row)
            self._restore(db, base)
            self._execute(f"DELETE FROM {_qt(db, COMMITS_TABLE)} WHERE seq > %s",
                          (int(row["seq"]),))
            self._add_commit(db, "fork", f"fork from {from_ref}", base.db, base.scn,
                             base.scn, parent=src, schema=base.schema)
            return
        scn = self._scn()
        self._add_commit(db, "fork", f"fork from {from_ref}", db, scn, scn,
                         parent=src, alt=_State(src, scn_before))

    def _commit_impl(self, ref: Ref, message: str) -> str:
        db = self._on(ref)
        prev = int(self._head(db)["scn_end"])
        scn = self._scn()
        return self._add_commit(db, "commit", message, db, prev, scn)

    def _log_impl(self, ref: Ref, limit: int) -> list:
        db = self._db_for(ref.branch)
        max_seq = None
        if ref.commit:
            max_seq = int(self._find_commit(db, ref.commit)["seq"])
        return [
            {"commit_hash": r["id"], "kind": r["kind"], "committer": "seekdb",
             "email": "", "date": r["ts"], "message": r["message"]}
            for r in self._log_rows(db, limit=limit, max_seq=max_seq)
        ]

    def _diff_impl(self, ref_a: Ref, ref_b: Ref) -> dict:
        if SEEKDB_NATIVE_DIFF:
            return self._native_diff(ref_a, ref_b)
        a, b = self._state(ref_a), self._state(ref_b)
        tables_a = set(self._tables(a.db))
        tables_b = set(self._tables(b.db))
        out = []
        for table in sorted(tables_a | tables_b):
            in_a = table in tables_a and self._snapshot_exists(a, table)
            in_b = table in tables_b and self._snapshot_exists(b, table)
            if in_a is None or in_b is None:
                out.append({"table_name": table, "rows_added": 0, "rows_deleted": 0,
                            "rows_modified": 0, "unreadable": True})
                continue
            if in_a and not in_b:
                n = self._execute(f"SELECT COUNT(*) FROM {a.src(table)} s")[0][0]
                out.append({"table_name": table, "rows_added": 0, "rows_deleted": int(n),
                            "rows_modified": 0})
                continue
            if in_b and not in_a:
                n = self._execute(f"SELECT COUNT(*) FROM {b.src(table)} s")[0][0]
                out.append({"table_name": table, "rows_added": int(n), "rows_deleted": 0,
                            "rows_modified": 0})
                continue
            if not (in_a or in_b):
                continue
            cols_a = [c[0] for c in self._columns(a.db, table)]
            cols_b = {c[0] for c in self._columns(b.db, table)}
            cols = [c for c in cols_a if c in cols_b]
            pk = [c for c in self._pk(a.db, table) if c in cols] or cols
            rest = [c for c in cols if c not in pk]
            added = self._execute(
                f"SELECT COUNT(*) FROM {b.src(table)} y LEFT JOIN {a.src(table)} x "
                f"ON {self._join('x', 'y', pk)} WHERE x.{_quote(pk[0])} IS NULL")[0][0]
            deleted = self._execute(
                f"SELECT COUNT(*) FROM {a.src(table)} x LEFT JOIN {b.src(table)} y "
                f"ON {self._join('x', 'y', pk)} WHERE y.{_quote(pk[0])} IS NULL")[0][0]
            modified = 0
            if rest:
                modified = self._execute(
                    f"SELECT COUNT(*) FROM {a.src(table)} x JOIN {b.src(table)} y "
                    f"ON {self._join('x', 'y', pk)} WHERE NOT ({self._same('x', 'y', rest)})")[0][0]
            out.append({"table_name": table, "rows_added": int(added),
                        "rows_deleted": int(deleted), "rows_modified": int(modified)})
        return {
            "tables": out,
            "rows_added": sum(t["rows_added"] for t in out),
            "rows_deleted": sum(t["rows_deleted"] for t in out),
            "rows_modified": sum(t["rows_modified"] for t in out),
        }

    # ------------------------------------------------------------------
    # Snapshot restore (reset, revert, branch from commit)
    # ------------------------------------------------------------------

    def _restore(self, db: str, target: _State) -> dict:
        """Make every table in ``db`` equal to its state in ``target``
        (a database, possibly db itself, at a snapshot). Tables that did
        not exist at the snapshot are dropped; tables that existed there
        but not in db are recreated."""
        here = set(self._tables(db))
        there = set(self._tables(target.db))
        readd = []
        if target.schema is not None:
            there &= set(target.schema)
            # Upfront schema comparison: what the target had and the branch
            # has since dropped is new to the engine when it comes back,
            # and a flashback read returns NULL for a re-added column
            # (probed 2026-10-17). A dropped column is re-added with its
            # recorded type and its rows come back NULL, which the
            # scenario's checks then report (S5.4); a dropped table has no
            # recorded definition beyond its columns and is refused.
            lost, readd = [], []
            for table, fp in target.schema.items():
                if table not in here:
                    if target.db == db or table not in set(self._tables(target.db)):
                        lost.append(f"table {table}")
                    continue
                now = {col for col, _, _, _ in self._columns(db, table)}
                types = fp if isinstance(fp, dict) else {}
                for c in fp:
                    if c not in now:
                        if types.get(c):
                            readd.append((table, c, types[c]))
                        else:
                            lost.append(f"{table}.{c}")
            if lost:
                raise self._unsupported("restore to a state before a dropped table",
                                        "flashback versions rows under the current schema only; dropped since "
                                        f"the target and not restorable: {', '.join(lost)}")
            for table, col, ctype in readd:
                self._execute(f"ALTER TABLE {_qt(db, table)} ADD COLUMN {_quote(col)} {ctype} NULL")
        dropped, restored, created, skipped = [], [], [], []
        retyped: dict = {}
        for table in sorted(here | there):
            if table in there:
                readable = self._snapshot_exists(target, table)
                if readable is None:
                    skipped.append(table)  # history gone: keep the current rows
                    continue
                if not readable:
                    there.discard(table)
            if table in here and table not in there:
                self._execute(f"DROP TABLE {_qt(db, table)}")
                dropped.append(table)
                continue
            if table not in here:
                self._execute(f"CREATE TABLE {_qt(db, table)} LIKE {_qt(target.db, table)}")
                created.append(table)
            if target.schema is not None:
                # Columns added after the commit are not part of its state;
                # columns dropped or retyped since are put back (re-added
                # with the recorded type: the row restore refills them).
                fp = target.schema.get(table, ())
                keep = set(fp)
                types = fp if isinstance(fp, dict) else {}
                now = {col: str(ctype).lower() for col, ctype, _, _ in self._columns(db, table)}
                for col in now:
                    if col not in keep:
                        self._execute(f"ALTER TABLE {_qt(db, table)} DROP COLUMN {_quote(col)}")
                # A retyped column keeps its identity: rows are restored
                # first (flashback still reads it), then the type is put back.
                retyped[table] = [(c, t) for c, t in types.items() if c in now and now[c] != t]
            restored.append(table)
        with self._bulk():
            for table in restored:
                self._restore_table(db, table, target)
        for table, cols in retyped.items():
            for col, ctype in cols:
                self._execute(f"ALTER TABLE {_qt(db, table)} MODIFY {_quote(col)} {ctype}")
        return {"restored": restored, "dropped": dropped, "created": created,
                "not_restored": skipped,
                "readded_columns_null": [f"{t}.{c}" for t, c, _ in readd]}

    def _restore_table(self, db: str, table: str, target: _State) -> None:
        cols_here = [c[0] for c in self._columns(db, table)]
        cols_there = {c[0] for c in self._columns(target.db, table)}
        if target.schema is not None:
            cols_there &= set(target.schema.get(table, ()))
        cols = [c for c in cols_here if c in cols_there]
        pk = [c for c in self._pk(db, table) if c in cols]
        dst, src = _qt(db, table), target.src(table)
        if not pk:
            self._execute(f"DELETE FROM {dst}")
            self._execute(f"INSERT INTO {dst} ({', '.join(map(_quote, cols))}) "
                          f"SELECT {self._cols('s', cols)} FROM {src} s")
            return
        rest = [c for c in cols if c not in pk]
        keys = ", ".join(map(_quote, pk))
        self._execute(
            f"DELETE FROM {dst} WHERE ({keys}) NOT IN "
            f"(SELECT {self._cols('s', pk)} FROM {src} s)")
        differs = f"x.{_quote(pk[0])} IS NULL"
        if rest:
            differs += f" OR NOT ({self._same('x', 's', rest)})"
        self._execute(self._upsert(
            dst, cols, pk,
            f"SELECT {self._cols('s', cols)} FROM {src} s "
            f"LEFT JOIN {dst} x ON {self._join('x', 's', pk)} WHERE {differs}"))

    def _reset_impl(self, ref: Ref, to: str) -> None:
        db = self._on(ref)
        row = self._find_commit(db, to)
        self._restore(db, self._row_state(row))
        self._execute(
            f"DELETE FROM {_qt(db, COMMITS_TABLE)} WHERE seq > %s "
            "AND kind NOT IN ('fork', 'rebase', 'root')", (int(row["seq"]),))

    def _revert_impl(self, ref: Ref, commit: str) -> None:
        db = self._on(ref)
        row = self._find_commit(db, commit)
        after = self._row_state(row)
        before = _State(row["db"], int(row["scn_start"]))
        if before.scn == after.scn:
            return
        scn0 = self._scn()
        with self._bulk():
            self._revert_tables(db, row, before, after)
        scn1 = self._scn()
        self._add_commit(db, "commit", f"revert {commit}", db, scn0, scn1)

    def _revert_tables(self, db: str, row: dict, before: _State, after: _State) -> None:
        for table in self._tables(db):
            if not (self._snapshot_exists(after, table) and self._snapshot_exists(before, table)):
                continue  # absent at the commit, or history unreadable
            cols = [c[0] for c in self._columns(db, table)]
            there = {c[0] for c in self._columns(row["db"], table)}
            cols = [c for c in cols if c in there]
            pk = [c for c in self._pk(db, table) if c in cols]
            if not pk:
                continue
            rest = [c for c in cols if c not in pk]
            dst = _qt(db, table)
            keys = ", ".join(map(_quote, pk))
            # Rows the commit added: delete them.
            self._execute(
                f"DELETE FROM {dst} WHERE ({keys}) IN "
                f"(SELECT {self._cols('e', pk)} FROM {after.src(table)} e "
                f"LEFT JOIN {before.src(table)} s ON {self._join('s', 'e', pk)} "
                f"WHERE s.{_quote(pk[0])} IS NULL)")
            # Rows the commit deleted or modified: put the earlier version back.
            differs = f"e.{_quote(pk[0])} IS NULL"
            if rest:
                differs += f" OR NOT ({self._same('s', 'e', rest)})"
            self._execute(self._upsert(
                dst, cols, pk,
                f"SELECT {self._cols('s', cols)} FROM {before.src(table)} s "
                f"LEFT JOIN {after.src(table)} e ON {self._join('s', 'e', pk)} "
                f"WHERE {differs}"))

    # ------------------------------------------------------------------
    # Three-way merge (merge and rebase)
    # ------------------------------------------------------------------

    def _reconcile_schema(self, ours_db: str, theirs_db: str) -> dict:
        """Give ``ours_db`` the tables and columns ``theirs_db`` has and it
        lacks. A differing primary key is a schema conflict (raises before
        anything is changed)."""
        ours = set(self._tables(ours_db))
        theirs = set(self._tables(theirs_db))
        plan_tables, plan_cols = [], []
        for table in sorted(theirs - ours):
            plan_tables.append(table)
        for table in sorted(ours & theirs):
            if self._pk(ours_db, table) != self._pk(theirs_db, table):
                raise RuntimeError(
                    f"schema conflict on {table}: primary keys differ between "
                    f"{ours_db} and {theirs_db}")
            have = {c[0]: c[1] for c in self._columns(ours_db, table)}
            for name, ctype, nullable, default in self._columns(theirs_db, table):
                if name not in have:
                    plan_cols.append((table, name, ctype, default))
                elif str(ctype).lower() != str(have[name]).lower():
                    raise RuntimeError(f"schema conflict on {table}.{name}: types differ "
                                       f"(ours {have[name]}, theirs {ctype})")
        for table in plan_tables:
            self._execute(f"CREATE TABLE {_qt(ours_db, table)} LIKE {_qt(theirs_db, table)}")
        for table, name, ctype, default in plan_cols:
            # Carry the column default so rows written later on this side
            # get the same value as on theirs.
            self._execute(f"ALTER TABLE {_qt(ours_db, table)} ADD COLUMN {_quote(name)} {ctype} NULL"
                          + (" DEFAULT %s" if default is not None else ""),
                          (default,) if default is not None else None)
        return {"added_tables": plan_tables,
                "added_columns": [f"{t}.{c}" for t, c, _, _ in plan_cols]}

    def _three_way(self, ours_db: str, theirs: _State, base: _State,
                   on_conflict, ref: Ref, tag: str, new_tables: list) -> dict:
        """Apply theirs' changes since base onto ours_db, in place.

        Returns conflict counts; rows both sides changed differently are
        resolved per ``on_conflict`` ("ours" keeps ours_db's version)."""
        info = {"conflicts": 0, "conflict_tables": [], "ours_changes": 0,
                "theirs_changes": 0, "merged_tables": [], "skipped_tables": {}}
        conflicts = []  # (table, rows) for a callable
        with self._bulk():
            self._three_way_tables(ours_db, theirs, base, on_conflict, ref, tag,
                                   new_tables, info, conflicts)
            if conflicts:
                info["resolution"] = on_conflict(self._conflict_session(ref), conflicts)
                info["resolved"] = "custom"
            elif info["conflicts"]:
                info["resolved"] = on_conflict
        return info

    def _three_way_tables(self, ours_db, theirs, base, on_conflict, ref, tag,
                          new_tables, info, conflicts) -> None:
        keys_t = _qt(ours_db, KEYS_TABLE)
        tag_t, tag_o, tag_c = f"{tag}t", f"{tag}o", f"{tag}c"
        try:
            for table in sorted(set(self._tables(ours_db)) & set(self._tables(theirs.db))):
                if table in new_tables:
                    # Created on their side: it was copied whole.
                    info["merged_tables"].append(table)
                    continue
                cols_o = [c[0] for c in self._columns(ours_db, table)]
                cols_t = {c[0] for c in self._columns(theirs.db, table)}
                cols = [c for c in cols_o if c in cols_t]
                pk = [c for c in self._pk(ours_db, table) if c in cols]
                O, T = _qt(ours_db, table), theirs.src(table)
                B, mode = self._base_src(base, table, T)
                if mode == "fallback":
                    info.setdefault("fallback_base_tables", []).append(table)
                elif mode == "none":
                    info.setdefault("two_way_tables", []).append(table)
                if not pk:
                    # No key to match rows on: take their new rows only.
                    self._execute(
                        f"INSERT INTO {O} ({', '.join(map(_quote, cols))}) "
                        f"SELECT {self._cols('t', cols)} FROM {T} t WHERE NOT EXISTS "
                        f"(SELECT 1 FROM {B} b WHERE {self._same('b', 't', cols)})")
                    info["merged_tables"].append(table)
                    info["skipped_tables"][table] = "no primary key: new rows only"
                    continue
                rest = [c for c in cols if c not in pk]
                kt, ko, kb = self._key("t", pk), self._key("o", pk), self._key("b", pk)
                differs_tb = f"b.{_quote(pk[0])} IS NULL"
                differs_ob = f"b.{_quote(pk[0])} IS NULL"
                if rest:
                    differs_tb += f" OR NOT ({self._same('t', 'b', rest)})"
                    differs_ob += f" OR NOT ({self._same('o', 'b', rest)})"
                # Their changes since base.
                self._execute(
                    f"INSERT INTO {keys_t} (tag, tbl, k, side) "
                    f"SELECT %s, %s, {kt}, 'u' FROM {T} t LEFT JOIN {B} b "
                    f"ON {self._join('t', 'b', pk)} WHERE {differs_tb}", (tag_t, table))
                self._execute(
                    f"INSERT INTO {keys_t} (tag, tbl, k, side) "
                    f"SELECT %s, %s, {kb}, 'd' FROM {B} b LEFT JOIN {T} t "
                    f"ON {self._join('t', 'b', pk)} WHERE t.{_quote(pk[0])} IS NULL", (tag_t, table))
                # Our changes since base.
                self._execute(
                    f"INSERT INTO {keys_t} (tag, tbl, k, side) "
                    f"SELECT %s, %s, {ko}, 'u' FROM {O} o LEFT JOIN {B} b "
                    f"ON {self._join('o', 'b', pk)} WHERE {differs_ob}", (tag_o, table))
                self._execute(
                    f"INSERT INTO {keys_t} (tag, tbl, k, side) "
                    f"SELECT %s, %s, {kb}, 'd' FROM {B} b LEFT JOIN {O} o "
                    f"ON {self._join('o', 'b', pk)} WHERE o.{_quote(pk[0])} IS NULL", (tag_o, table))
                n_t = self._execute(f"SELECT COUNT(*) FROM {keys_t} WHERE tag = %s AND tbl = %s",
                                    (tag_t, table))[0][0]
                n_o = self._execute(f"SELECT COUNT(*) FROM {keys_t} WHERE tag = %s AND tbl = %s",
                                    (tag_o, table))[0][0]
                info["theirs_changes"] += int(n_t)
                info["ours_changes"] += int(n_o)
                if not n_t:
                    continue
                # Conflicts: keys both changed, unless both made the same change.
                same_ot = self._same("o", "t", rest) if rest else "TRUE"
                self._execute(
                    f"INSERT INTO {keys_t} (tag, tbl, k, side) "
                    f"SELECT %s, %s, a.k, 'c' FROM {keys_t} a JOIN {keys_t} b "
                    f"ON b.tag = %s AND b.tbl = a.tbl AND b.k = a.k "
                    f"LEFT JOIN {O} o ON {ko} = a.k LEFT JOIN {T} t ON {kt} = a.k "
                    f"WHERE a.tag = %s AND a.tbl = %s "
                    f"AND NOT (a.side = 'd' AND b.side = 'd') "
                    f"AND NOT (a.side = 'u' AND b.side = 'u' AND {same_ot})",
                    (tag_c, table, tag_o, tag_t, table))
                n_c = int(self._execute(
                    f"SELECT COUNT(*) FROM {keys_t} WHERE tag = %s AND tbl = %s",
                    (tag_c, table))[0][0])
                not_conflict = (f"NOT EXISTS (SELECT 1 FROM {keys_t} c WHERE c.tag = '{tag_c}' "
                                f"AND c.tbl = '{table}' AND c.k = s.k)")
                # Apply their non-conflicting changes.
                self._execute(
                    f"DELETE FROM {O} WHERE {self._key(O, pk)} IN "
                    f"(SELECT s.k FROM {keys_t} s WHERE s.tag = %s AND s.tbl = %s "
                    f"AND s.side = 'd' AND {not_conflict})", (tag_t, table))
                self._execute(self._upsert(
                    O, cols, pk,
                    f"SELECT {self._cols('t', cols)} FROM {T} t WHERE {kt} IN "
                    f"(SELECT s.k FROM {keys_t} s WHERE s.tag = %s AND s.tbl = %s "
                    f"AND s.side = 'u' AND {not_conflict})"), (tag_t, table))
                info["merged_tables"].append(table)
                if n_c:
                    info["conflicts"] += n_c
                    info["conflict_tables"].append(table)
                    if callable(on_conflict):
                        conflicts.append({"table": table, "rows": self._conflict_rows(
                            ours_db, table, theirs, base, cols, pk, tag_c)})
                    elif on_conflict == "theirs":
                        self._apply_theirs(ours_db, table, theirs, cols, pk, tag_t, tag_c)
        finally:
            self._execute(f"DELETE FROM {keys_t} WHERE tag IN (%s, %s, %s)",
                          (tag_t, tag_o, tag_c))

    def _base_src(self, base: _State, table: str, theirs_expr: str):
        """(table expression, mode) for the merge base of ``table``: the
        exact snapshot, its fallback copy, or an empty relation ("none":
        every row then counts as added on both sides, so neither side's
        deletes propagate)."""
        readable = self._snapshot_exists(base, table)
        if readable:
            return base.src(table), "exact"
        if readable is None and base.alt is not None and self._snapshot_exists(base.alt, table):
            return base.alt.src(table), "fallback"
        return f"(SELECT * FROM {theirs_expr} z WHERE 1 = 0)", ("none" if readable is None else "absent")

    def _apply_theirs(self, ours_db, table, theirs, cols, pk, tag_t, tag_c):
        keys_t = _qt(ours_db, KEYS_TABLE)
        O, T = _qt(ours_db, table), theirs.src(table)
        kt = self._key("t", pk)
        conflict = (f"EXISTS (SELECT 1 FROM {keys_t} c WHERE c.tag = '{tag_c}' "
                    f"AND c.tbl = '{table}' AND c.k = s.k)")
        self._execute(
            f"DELETE FROM {O} WHERE {self._key(O, pk)} IN "
            f"(SELECT s.k FROM {keys_t} s WHERE s.tag = %s AND s.tbl = %s "
            f"AND s.side = 'd' AND {conflict})", (tag_t, table))
        self._execute(self._upsert(
            O, cols, pk,
            f"SELECT {self._cols('t', cols)} FROM {T} t WHERE {kt} IN "
            f"(SELECT s.k FROM {keys_t} s WHERE s.tag = %s AND s.tbl = %s "
            f"AND s.side = 'u' AND {conflict})"), (tag_t, table))

    def _conflict_rows(self, ours_db, table, theirs, base, cols, pk, tag_c) -> list:
        """Conflict rows in Dolt's dolt_conflicts_<table> shape: base_*,
        our_*, their_* columns plus our_diff_type / their_diff_type."""
        keys_t = _qt(ours_db, KEYS_TABLE)
        O, T = _qt(ours_db, table), theirs.src(table)
        B, _ = self._base_src(base, table, T)
        select = ", ".join(
            [f"b.{_quote(c)} AS {_quote('base_' + c)}" for c in cols]
            + [f"o.{_quote(c)} AS {_quote('our_' + c)}" for c in cols]
            + [f"t.{_quote(c)} AS {_quote('their_' + c)}" for c in cols])
        rows = self._execute(
            f"SELECT {select}, b.{_quote(pk[0])} IS NULL, o.{_quote(pk[0])} IS NULL, "
            f"t.{_quote(pk[0])} IS NULL FROM {keys_t} c "
            f"LEFT JOIN {B} b ON {self._key('b', pk)} = c.k "
            f"LEFT JOIN {O} o ON {self._key('o', pk)} = c.k "
            f"LEFT JOIN {T} t ON {self._key('t', pk)} = c.k "
            f"WHERE c.tag = %s AND c.tbl = %s", (tag_c, table))
        names = [f"base_{c}" for c in cols] + [f"our_{c}" for c in cols] + [f"their_{c}" for c in cols]
        out = []
        for r in rows or []:
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

    def _copy_log(self, into_db: str, from_db: str, max_seq: int = None) -> None:
        """Add from_db's commits that into_db does not have to its log."""
        sql = (f"INSERT INTO {_qt(into_db, COMMITS_TABLE)} "
               "(seq, id, kind, message, db, scn_start, scn_end, parent, alt_db, alt_scn, "
               "schema_json, ts) "
               f"SELECT s.seq, s.id, s.kind, s.message, s.db, s.scn_start, s.scn_end, s.parent, "
               f"s.alt_db, s.alt_scn, s.schema_json, s.ts "
               f"FROM {_qt(from_db, COMMITS_TABLE)} s WHERE s.kind = 'commit' "
               f"AND NOT EXISTS (SELECT 1 FROM {_qt(into_db, COMMITS_TABLE)} d "
               "WHERE d.seq = s.seq OR d.id = s.id)")
        if max_seq is not None:
            sql += f" AND s.seq <= {int(max_seq)}"
        self._execute(sql)

    def _merge_impl(self, into: Ref, source: Ref, message: str,
                    on_conflict="ours") -> dict:
        if SEEKDB_NATIVE_MERGE:
            return self._native_merge(into, source, message, on_conflict)
        ours_db = self._on(into)
        source_db = self._db_for(source.branch)
        if source_db == ours_db:
            raise ValueError(f"Cannot merge branch '{source.branch}' into itself")
        base = self._merge_base(ours_db, source_db)
        theirs = self._state(source)
        scn0 = self._scn()
        schema = self._reconcile_schema(ours_db, source_db)
        info = self._three_way(ours_db, theirs, base, on_conflict, into,
                               f"m{scn0 % 1000000007}", schema["added_tables"])
        self._copy_log(ours_db, source_db)
        scn1 = self._scn()
        head = self._head(source_db)
        commit_id = head["id"] if head["kind"] == "commit" else None
        info["hash"] = self._add_commit(
            ours_db, "merge", message or f"Merge {source.branch} into {into.branch}",
            ours_db, scn0, scn1, commit_id)
        info["fast_forward"] = info["ours_changes"] == 0
        info["schema_changes"] = schema
        return info

    def _rebase_impl(self, ref: Ref, onto: Ref, on_conflict="ours") -> dict:
        ours_db = self._on(ref)
        onto_db = self._db_for(onto.branch)
        if onto_db == ours_db:
            raise ValueError(f"Cannot rebase branch '{ref.branch}' onto itself")
        base = self._merge_base(ours_db, onto_db)
        # Read the upstream at one SCN so a concurrently written spine is
        # seen consistently; that SCN becomes the branch's new fork point.
        scn_onto = self._scn() if not onto.commit else None
        theirs = self._state(onto)
        if scn_onto:
            theirs = _State(onto_db, scn_onto)
        schema = self._reconcile_schema(ours_db, onto_db)
        info = self._three_way(ours_db, theirs, base, on_conflict, ref,
                               f"r{theirs.scn % 1000000007}", schema["added_tables"])
        self._copy_log(ours_db, onto_db, max_seq=theirs.scn)
        info["up_to_date"] = info["theirs_changes"] == 0 and not schema["added_tables"] \
            and not schema["added_columns"]
        info["schema_changes"] = schema
        self._add_commit(ours_db, "rebase", f"rebase onto {onto}", theirs.db,
                         theirs.scn, theirs.scn, parent=onto_db)
        return info

    # ------------------------------------------------------------------
    # Native MERGE TABLE (native-only mode)
    # ------------------------------------------------------------------

    def _native_merge(self, into: Ref, source: Ref, message: str,
                      on_conflict="ours") -> dict:
        """Merge source into target one table at a time with SeekDB's
        MERGE TABLE. It compares the two tables directly, with no common
        ancestor: every differing row is a conflict, resolved by STRATEGY
        OURS (keep the target's row) or THEIRS (take the source's row);
        the source's deletes are never applied. Tables without a primary
        key, with a different schema, or missing on one side are skipped.
        A callable on_conflict is applied as "ours"."""
        source_db = self._db_for(source.branch)
        target_db = self._on(into)
        if source_db == target_db:
            raise ValueError(f"Cannot merge branch '{source.branch}' into itself")
        strategy = "THEIRS" if on_conflict == "theirs" else "OURS"
        source_tables = self._tables_with_pk_flag(source_db)
        target_tables = self._tables_with_pk_flag(target_db)
        scn0 = self._scn()
        merged, skipped, conflicts = [], {}, 0
        for table, source_has_pk in source_tables.items():
            if table not in target_tables:
                skipped[table] = "not in the target branch"
            elif not (source_has_pk and target_tables[table]):
                skipped[table] = "no primary key"
            else:
                n = self._native_conflicts(source_db, target_db, table)
                if n is None:
                    skipped[table] = "schemas differ between branches"
                    continue
                conflicts += n
                self._execute(
                    f"MERGE TABLE {_qt(source_db, table)} INTO {_qt(target_db, table)} "
                    f"STRATEGY {strategy};")
                merged.append(table)
        for table, reason in skipped.items():
            if (table, reason) not in self._merge_skip_warned:
                self._merge_skip_warned.add((table, reason))
                print(f"Warning: SeekDB merge skips table '{table}' ({reason})")
        self._copy_log(target_db, source_db)
        scn1 = self._scn()
        head = self._head(source_db)
        commit_id = self._add_commit(
            target_db, "merge", message or f"Merge {source.branch} into {into.branch}",
            target_db, scn0, scn1, head["id"] if head["kind"] == "commit" else None)
        return {
            "fast_forward": False,
            "conflicts": conflicts,
            "hash": commit_id,
            "merged_tables": merged,
            "skipped_tables": skipped,
            "native": True,
        }

    def _native_diff(self, ref_a: Ref, ref_b: Ref) -> dict:
        """Native-only mode: SeekDB's ``DIFF TABLE b AGAINST a`` per
        table, which lists every row present on one side only, tagged with
        its side; rows are grouped by primary key into added (b only),
        deleted (a only) and modified (both). A commit ref is read through
        the database that materialises it (a fork restored to the commit).
        DIFF TABLE refuses keyless tables and differing schemas; those
        tables are skipped with a warning, as the native merge does."""
        db_a = self._temp_db_for(ref_a) if ref_a.commit else self._db_for(ref_a.branch)
        db_b = self._temp_db_for(ref_b) if ref_b.commit else self._db_for(ref_b.branch)
        tables_a, tables_b = set(self._tables(db_a)), set(self._tables(db_b))
        out = []
        for table in sorted(tables_a | tables_b):
            if table in tables_a and table not in tables_b:
                n = self._execute(f"SELECT COUNT(*) FROM {_qt(db_a, table)}")[0][0]
                out.append({"table_name": table, "rows_added": 0, "rows_deleted": int(n), "rows_modified": 0})
                continue
            if table in tables_b and table not in tables_a:
                n = self._execute(f"SELECT COUNT(*) FROM {_qt(db_b, table)}")[0][0]
                out.append({"table_name": table, "rows_added": int(n), "rows_deleted": 0, "rows_modified": 0})
                continue
            pk = self._pk(db_a, table)
            reason = None if pk else "no primary key"
            rows, cols = [], []
            if not reason:
                try:
                    with self.conn.cursor() as cur:
                        cur.execute(f"DIFF TABLE {_qt(db_b, table)} AGAINST {_qt(db_a, table)}")
                        cols = [d[0] for d in cur.description]
                        rows = cur.fetchall()
                except pymysql.MySQLError as e:
                    code = e.args[0] if e.args else None
                    if code == _ER_SCHEMA_MISMATCH:
                        reason = "schemas differ between the sides"
                    elif code == _ER_NO_PRIMARY_KEY:
                        reason = "no primary key"
                    else:
                        raise
            if reason:
                if (table, reason) not in self._merge_skip_warned:
                    self._merge_skip_warned.add((table, reason))
                    print(f"Warning: SeekDB DIFF TABLE skips table '{table}' ({reason})")
                out.append({"table_name": table, "rows_added": 0, "rows_deleted": 0,
                            "rows_modified": 0, "skipped": reason})
                continue
            kpos = [cols.index(c) for c in pk]
            b_tag = f"{db_b}.{table}"
            sides: dict = {}
            for r in rows:
                side = str(r[0], "utf-8") if isinstance(r[0], (bytes, bytearray)) else str(r[0])
                key = tuple(r[i] for i in kpos)
                sides.setdefault(key, set()).add("b" if side == b_tag else "a")
            added = sum(1 for v in sides.values() if v == {"b"})
            deleted = sum(1 for v in sides.values() if v == {"a"})
            modified = sum(1 for v in sides.values() if v == {"a", "b"})
            out.append({"table_name": table, "rows_added": added, "rows_deleted": deleted,
                        "rows_modified": modified})
        return {
            "tables": out,
            "rows_added": sum(t["rows_added"] for t in out),
            "rows_deleted": sum(t["rows_deleted"] for t in out),
            "rows_modified": sum(t["rows_modified"] for t in out),
        }

    def _native_conflicts(self, source_db: str, target_db: str, table: str):
        """Conflicting rows MERGE TABLE ... STRATEGY FAIL reports; None if
        the schemas differ."""
        try:
            self._execute(
                f"MERGE TABLE {_qt(source_db, table)} INTO {_qt(target_db, table)} "
                "STRATEGY FAIL;")
            return 0
        except pymysql.MySQLError as e:
            code = e.args[0] if e.args else None
            if code == _ER_SCHEMA_MISMATCH:
                return None
            if code == _ER_MERGE_CONFLICTS:
                msg = str(e.args[1]) if len(e.args) > 1 else ""
                digits = "".join(ch if ch.isdigit() else " " for ch in msg).split()
                return int(digits[0]) if digits else 1
            raise

    # ------------------------------------------------------------------

    def _delete_impl(self, ref: Ref) -> None:
        """Drop the branch's database. Forks don't depend on the database
        they were forked from, so even main can be dropped."""
        db = self._db_for(ref.branch)
        if db == self._current_db:
            self._use(self._db_for(self.default_branch))
            self._current_ref = Ref(self.default_branch)
        self._execute(f"DROP DATABASE {_quote(db)};")

    def _qualified_table(self, ref: Ref, table: str) -> str:
        db = self._temp_db_for(ref) if ref.commit else self._db_for(ref.branch)
        return f"{_quote(db)}.{_quote(table)}"

    def close_connection(self) -> None:
        if self.conn and self._temp_dbs:
            for db in list(self._temp_dbs.values()):
                try:
                    self._execute(f"DROP DATABASE IF EXISTS {_quote(db)};")
                except Exception:
                    pass
            self._temp_dbs.clear()
        super().close_connection()

    # ------------------------------------------------------------------
    # Async: an aiomysql pool; each pool connection remembers its database.
    # ------------------------------------------------------------------

    async def open_async_pool(self, size: int) -> None:
        self.async_pool = await create_pool_async(self.db_name, size)

    def _pool_connection(self):
        return mysql_common.aiomysql_pool_connection(self.async_pool)

    async def close_async_pool(self) -> None:
        if self.async_pool:
            self.async_pool.close()
            await self.async_pool.wait_closed()
            self.async_pool = None

    async def _connect_impl_async(self, conn, ref: Ref):
        db = self._temp_db_for(ref) if ref.commit else self._db_for(ref.branch)
        if getattr(conn, "_seekdb_db", None) != db:
            async with conn.cursor() as cur:
                await cur.execute(f"USE {_quote(db)};")
            conn._seekdb_db = db
        return conn
