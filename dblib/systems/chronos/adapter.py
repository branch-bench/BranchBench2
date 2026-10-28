"""Chronos backend: bolt-on interval branching over PostgreSQL.

Chronos (https://github.com/mitdbg/chronos, arXiv:2609.14889) is a Python
library, not a server. Every registered table lives once in a physical
interval table (``_chronos_b_interval_<t>``) whose rows carry a half-open
visibility interval ``[live_lo, live_hi)``; a branch owns an interval and
reads the versions containing its branch point, so a fork is a metadata
transaction and a read is a range predicate. The library speaks its own
API (``ChronosBranchContext``: ``create_branch``, ``checkout`` sessions
with ``query``/``execute``, ``create_checkpoint``, ``diff``, ``merge_apply``,
``delete_branch``), so this backend is a shim that maps the suite's verbs
and the workload's SQL onto that API. What is native, composed or simulated
is listed in ``IMPLEMENTATION`` / ``IMPLEMENTATION_NOTES`` below.

Setup ("adoption"): the run's schema and seed data are loaded as ordinary
PostgreSQL tables (full SQL); the first versioning verb registers every
table with Chronos (its rows are copied into the interval table, the
original is dropped) -- the integration path the Chronos docs describe.
From then on every statement goes through a Chronos branch session.

The backend runs Chronos with ``enable_schema_branching=True`` because the
scenarios create tables and columns on branches. Chronos refuses its native
merge for tables under schema branching, so merge/rebase/revert/reset are
built on top of its native diff and bulk upsert/delete primitives here. In
native-only mode (``DBFORK_NATIVE_ONLY``) merge calls the engine's
merge_apply instead, which then reports the refusal as unsupported.

Environment: ``PGSQL_USER``, ``PGSQL_PASSWORD``, ``PGSQL_HOST``, ``PGSQL_PORT``
(the same PostgreSQL server the file_copy backend uses); one database per
run.
"""

import contextlib
import datetime
import decimal
import os
import random
import json
import re
import threading
import time
from collections import OrderedDict
from typing import Any, Optional

import psycopg

import dblib.result_collector as rc
from dblib.db_api import DBToolSuite, Ref, Session, UnsupportedOperation, native_only

# Native-only mode: merge goes to the engine's merge_apply (see IMPLEMENTATION_NOTES).
CHRONOS_NATIVE_MERGE = native_only()

try:
    from chronos_core.branching import (
        BranchingError,
        BranchNotFoundError,
        ChronosBranchContext,
        DuplicateKeyError,
        TableNotRegisteredError,
        UnsupportedSQLError,
    )
    CHRONOS_AVAILABLE = True
except ImportError:  # pragma: no cover - the backend is optional
    CHRONOS_AVAILABLE = False

    class BranchingError(Exception):
        pass

    class BranchNotFoundError(BranchingError):
        pass

    class DuplicateKeyError(BranchingError):
        pass

    class TableNotRegisteredError(BranchingError):
        pass

    class UnsupportedSQLError(BranchingError):
        pass

    ChronosBranchContext = None

PGSQL_USER = os.getenv("PGSQL_USER", "elaineang")
PGSQL_PASSWORD = os.getenv("PGSQL_PASSWORD", "")
# CHRONOS_HOST/CHRONOS_PORT when Chronos's Postgres is not the one file_copy uses.
PGSQL_HOST = (os.getenv("CHRONOS_HOST") or os.getenv("PGSQL_HOST")
              or os.getenv("DBFORK_SERVER_HOST", "localhost"))
PGSQL_PORT = os.getenv("CHRONOS_PORT") or os.getenv("PGSQL_PORT", "5433")

MAIN_BRANCH = "main"
LOG_TABLE = "_bb_log"
VIEWS_TABLE = "_bb_views"
ROWID_TABLES = "_bb_rowid_tables"
TOMBSTONES = "_bb_tombstones"
ROWID_COL = "_bb_rowid"
TEMP_PREFIX = "bbtmp_"
_SESSION_CACHE = 16
_CHUNK = 500

_LOG_DDL = f"""
CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
    log_branch TEXT NOT NULL, seq BIGINT NOT NULL, id TEXT NOT NULL, kind TEXT NOT NULL,
    message TEXT, snap TEXT, prev_snap TEXT, prev_branch TEXT,
    ts TIMESTAMPTZ NOT NULL DEFAULT now(), schema_json TEXT, PRIMARY KEY (log_branch, seq));
CREATE TABLE IF NOT EXISTS {VIEWS_TABLE} (branch TEXT NOT NULL, name TEXT NOT NULL, sql TEXT NOT NULL,
    PRIMARY KEY (branch, name));
CREATE TABLE IF NOT EXISTS {ROWID_TABLES} (name TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS {TOMBSTONES} (branch TEXT PRIMARY KEY);
"""
_LOG_COLS = ("seq", "id", "kind", "message", "snap", "prev_snap", "prev_branch", "ts", "schema_json")


def _uri(db: str, app: str = "") -> str:
    pw = f":{PGSQL_PASSWORD}" if PGSQL_PASSWORD else ""
    tail = f"?application_name={app}" if app else ""
    return f"postgresql://{PGSQL_USER}{pw}@{PGSQL_HOST}:{PGSQL_PORT}/{db}{tail}"


def _q(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def _unq(ident: str) -> str:
    ident = ident.strip()
    if ident.startswith('"') and ident.endswith('"'):
        return ident[1:-1].replace('""', '"')
    return ident.lower()


# ----------------------------------------------------------------------
# SQL plumbing: parameters, statement heads, DDL normalisation
# ----------------------------------------------------------------------

def _conv(v):
    """Parameter values the Chronos native binding accepts."""
    if isinstance(v, datetime.datetime):
        return v.isoformat(sep=" ")
    if isinstance(v, datetime.date):
        return v.isoformat()
    return v


def _to_named(sql: str, vars) -> tuple:
    """psycopg-style ``%s`` / ``%(name)s`` placeholders -> Chronos ``:name``."""
    if vars is None:
        return sql, {}
    out, params = [], {}
    i, n, k = 0, len(sql), 0
    seq = None if isinstance(vars, dict) else list(vars)
    in_str = False
    while i < n:
        ch = sql[i]
        if in_str:
            out.append(ch)
            if ch == "'":
                in_str = False
            i += 1
            continue
        if ch == "'":
            in_str = True
            out.append(ch)
            i += 1
            continue
        if ch == "%" and i + 1 < n:
            nxt = sql[i + 1]
            if nxt == "%":
                out.append("%")
                i += 2
                continue
            if nxt == "s" and seq is not None:
                out.append(f":p{k}")
                params[f"p{k}"] = _conv(seq[k]) if k < len(seq) else None
                k += 1
                i += 2
                continue
            if nxt == "(":
                j = sql.find(")s", i)
                if j > 0:
                    name = sql[i + 2:j]
                    out.append(f":{name}")
                    params[name] = _conv(vars[name]) if isinstance(vars, dict) else None
                    i = j + 2
                    continue
        out.append(ch)
        i += 1
    return "".join(out), params


_PYFORMAT = re.compile(r"%\((\w+)\)s")


def _pyformat_to_named(sql: str) -> str:
    """sqlglot renders ``:p0`` as ``%(p0)s``; turn it back."""
    return _PYFORMAT.sub(lambda m: ":" + m.group(1), sql)


def _head(sql: str) -> str:
    s = sql.lstrip()
    while s.startswith("--") or s.startswith("/*"):
        if s.startswith("--"):
            s = s.split("\n", 1)[1] if "\n" in s else ""
        else:
            s = s.split("*/", 1)[1] if "*/" in s else ""
        s = s.lstrip()
    m = re.match(r"(\w+)", s)
    return m.group(1).upper() if m else ""


def _split_top(body: str) -> list:
    items, depth, cur, in_str = [], 0, [], False
    for ch in body:
        if in_str:
            cur.append(ch)
            if ch == "'":
                in_str = False
            continue
        if ch == "'":
            in_str = True
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            items.append("".join(cur))
            cur = []
            continue
        cur.append(ch)
    if "".join(cur).strip():
        items.append("".join(cur))
    return items


_CONST_DEFAULT = re.compile(r"^(-?\d+(\.\d+)?|'(?:[^']|'')*'|NULL|TRUE|FALSE)$", re.I)


def _strip_column_constraints(rest: str) -> tuple:
    """Drop the column constraints Chronos rejects (NOT NULL, UNIQUE, CHECK,
    REFERENCES, non-constant DEFAULT). Returns (rest, had_inline_pk)."""
    inline_pk = re.search(r"\bPRIMARY\s+KEY\b", rest, re.I) is not None
    rest = re.sub(r"\bPRIMARY\s+KEY\b", " ", rest, flags=re.I)
    rest = re.sub(r"\bNOT\s+NULL\b|\bNULL\b|\bUNIQUE\b|\bAUTO_INCREMENT\b", " ", rest, flags=re.I)
    rest = re.sub(r"\bREFERENCES\s+[\w\".]+\s*\([^)]*\)(\s+ON\s+(DELETE|UPDATE)\s+\w+(\s+\w+)?)*", " ", rest, flags=re.I)
    rest = re.sub(r"\bCHECK\s*\(.*\)", " ", rest, flags=re.I | re.S)
    dm = re.search(r"\bDEFAULT\s+(.+)$", rest, re.I | re.S)
    if dm and not _CONST_DEFAULT.match(dm.group(1).strip()):
        rest = rest[:dm.start()]
    return re.sub(r"\s+", " ", rest).strip(), inline_pk


_CREATE_TABLE = re.compile(r"\s*CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?([\w\".]+)\s*\((.*)\)\s*;?\s*$", re.I | re.S)


def _normalize_create_table(sql: str) -> tuple:
    """Rewrite CREATE TABLE into the subset Chronos accepts: column defs with
    at most an inline PRIMARY KEY and a constant DEFAULT. A table-level
    PRIMARY KEY becomes inline; FOREIGN KEY / UNIQUE / CHECK are dropped; a
    keyless table gets a synthetic ``_bb_rowid`` key.
    Returns (sql, table, if_not_exists, keyless)."""
    m = _CREATE_TABLE.match(sql)
    if not m:
        raise UnsupportedSQLError(f"chronos: cannot normalise CREATE TABLE: {sql[:80]}")
    ine, table, body = bool(m.group(1)), m.group(2), m.group(3)
    pk, cols = [], []
    for item in _split_top(body):
        s = item.strip()
        u = s.upper()
        if u.startswith("PRIMARY KEY"):
            inner = s[s.find("(") + 1:s.rfind(")")]
            pk += [_unq(c) for c in inner.split(",")]
        elif u.startswith(("CONSTRAINT", "FOREIGN KEY", "UNIQUE", "CHECK", "KEY ", "INDEX ")):
            continue
        elif s:
            cols.append(s)
    out, have_pk = [], False
    for c in cols:
        nm = re.match(r'("[^"]+"|\w+)\s*(.*)$', c, re.S)
        name, rest = nm.group(1), nm.group(2)
        rest, inline = _strip_column_constraints(rest)
        if inline or _unq(name) in pk:
            rest += " PRIMARY KEY"
            have_pk = True
        out.append(f"{name} {rest}")
    keyless = not have_pk
    if keyless:
        out.append(f"{ROWID_COL} BIGINT PRIMARY KEY")
    return f"CREATE TABLE {table} ({', '.join(out)})", _unq(table), ine, keyless


def _normalize_alter(sql: str) -> str:
    m = re.match(r"(\s*ALTER\s+TABLE\s+[\w\".]+\s+ADD\s+(COLUMN\s+)?)(.*)$", sql, re.I | re.S)
    if not m:
        return sql
    rest, _ = _strip_column_constraints(m.group(3))
    return m.group(1) + rest


_UNSUPPORTED_MARKERS = ("unsupported", "only supports", "must specify", "requires a set-based",
                        "does not support", "must use a simple", "not supported", "native branch executor",
                        "arity mismatch", "unknown column in expression", "subquery", "expected ")


def _is_unsupported(e: Exception) -> bool:
    """Chronos's own executor rejected the statement's shape (as opposed to
    a database error, a missing table or a key violation)."""
    if isinstance(e, UnsupportedSQLError):
        return True
    if not isinstance(e, RuntimeError) or isinstance(e, BranchingError):
        return False
    msg = str(e)
    if msg.startswith("ERROR:") or "branch not found" in msg or "not registered" in msg:
        return False
    return any(s in msg for s in _UNSUPPORTED_MARKERS)


def _rowid() -> int:
    return random.getrandbits(62)


# ----------------------------------------------------------------------
# DB-API shim the suite's Session talks to
# ----------------------------------------------------------------------

class _Cursor:
    def __init__(self, conn):
        self._conn = conn
        self.description = None
        self.rowcount = -1
        self._rows = None

    def execute(self, sql, vars=None):
        self.description, self.rowcount, self._rows = None, -1, None
        self._conn.suite._run_statement(self, sql, vars)

    def fetchall(self):
        return list(self._rows or [])

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _Conn:
    def __init__(self, suite):
        self.suite = suite

    def cursor(self):
        return _Cursor(self)

    def cancel(self):
        self.suite._cancel_backends()

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


# Databases this process has adopted (registered with Chronos).
_ADOPTED: set = set()
_ADOPT_LOCK = threading.Lock()


class _Gates:
    """Process-wide gate per branch. A Chronos session holds a FOR SHARE
    lock on its branch's metadata row for every write transaction, and PG
    lets new share lockers join ahead of a waiting UPDATE, so a fork,
    checkpoint or delete on a branch with continuous writers (the spine
    under load) starves. Branch management raises the branch's gate: new
    statements on it wait, in-flight ones drain, the operation runs."""

    def __init__(self):
        self.cond = threading.Condition()
        self.active: dict = {}
        self.gated: dict = {}

    def enter(self, branch: str, check_cancel=None) -> None:
        with self.cond:
            waited = False
            while self.gated.get(branch):
                waited = True
                self.cond.wait(0.5)
                if check_cancel:
                    check_cancel()
            self.active[branch] = self.active.get(branch, 0) + 1
        if waited:
            ChronosToolSuite.observe("gate_waits")

    def leave(self, branch: str) -> None:
        with self.cond:
            self.active[branch] = max(0, self.active.get(branch, 0) - 1)
            self.cond.notify_all()

    @contextlib.contextmanager
    def hold(self, branches, check_cancel=None):
        branches = [b for b in dict.fromkeys(branches) if b]
        t0 = time.perf_counter()
        with self.cond:
            for b in branches:
                self.gated[b] = self.gated.get(b, 0) + 1
            try:
                while any(self.active.get(b) for b in branches):
                    self.cond.wait(0.5)
                    if check_cancel:
                        check_cancel()
            except BaseException:
                for b in branches:
                    self.gated[b] -= 1
                self.cond.notify_all()
                raise
        waited = time.perf_counter() - t0
        if waited > 0.001:
            ChronosToolSuite.observe("gate_hold_waits")
            ChronosToolSuite.observe("gate_hold_wait_sec", round(waited, 3))
        try:
            yield
        finally:
            with self.cond:
                for b in branches:
                    self.gated[b] -= 1
                self.cond.notify_all()


_GATES = _Gates()


class ChronosToolSuite(DBToolSuite):
    BACKEND_NAME = "chronos"
    STORAGE_SCOPE = "database"
    SUPPORTS_COMMIT_REFS = True
    SUPPORTS_MULTI_REF_EXEC = True

    IMPLEMENTATION = {
        "branch": "native",
        "commit": "native",
        "diff": "native",
        "log": "simulated",
        "merge": "native" if CHRONOS_NATIVE_MERGE else "simulated",
        "rebase": "simulated",
        "revert": "simulated",
        "reset": "simulated",
        "delete": "native",
        "commit_refs": "native",
        "multi_ref_exec": "simulated",
    }
    IMPLEMENTATION_NOTES = {
        "branch": "create_checkpoint on the parent (the fork base, kept for merges) + "
                  "create_branch_from_checkpoint: two metadata transactions, no data copied; "
                  "a commit ref forks from its checkpoint directly. Log rows copied in _bb_log.",
        "commit": "create_checkpoint (a read-only interval split, no data copied) + a _bb_log row",
        "diff": "native ChronosBranchContext.diff between two branches; a commit ref is read "
                "through a temporary branch forked from its checkpoint",
        "log": "SELECT from the _bb_log bookkeeping table (Chronos keeps no history of commits)",
        "merge": "three-way merge in the shim: native diff(base, source) and diff(base, target) "
                 "against the fork/sync checkpoint, conflicts compared by key, changes applied "
                 "with native upsert_rows/delete_keys, schema additions replayed as DDL. In "
                 "native-only mode the engine's merge_apply is called instead (and branches are "
                 "forked from the parent branch so it has a shared fork base); Chronos refuses "
                 "it for every table once schema branching is enabled, which every scenario "
                 "needs, and the refusal is reported as unsupported.",
        "rebase": "the same three-way merge with the upstream as the source; the base moves to a "
                  "sync checkpoint on the upstream",
        "revert": "inverse of the native diff between the commit's checkpoint and its "
                  "predecessor's, applied with upsert_rows/delete_keys, then a commit",
        "reset": "native diff(branch, checkpoint) applied back onto the branch with "
                 "upsert_rows/delete_keys (Chronos cannot move a branch's read point)",
        "delete": "delete_branch (metadata delete, versions reclaimed by Chronos's background "
                  "GC); deleting a branch deletes its subtree in Chronos, so a branch with live "
                  "children is tombstoned and deleted once they are gone",
        "commit_refs": "checkpoints: read-only sessions on the checkpoint's interval",
        "multi_ref_exec": "temporary views over the interval tables at each ref's branch point "
                          "(the visibility predicate Chronos itself uses), the SELECT rewritten by "
                          "the session for its own branch and run on a plain connection",
        "sql": "Chronos executes DML itself over a parsed SQL subset (INSERT ... VALUES with a "
               "column list, UPDATE with simple assignments, DELETE; no INSERT ... SELECT, "
               "RETURNING, NOW(), scalar subqueries). A statement it rejects is simulated: the "
               "shim evaluates it as a SELECT on the branch (PostgreSQL computes the new values) "
               "and applies the rows with upsert_rows/delete_keys. CREATE TABLE is normalised "
               "(table-level PRIMARY KEY made inline, NOT NULL/UNIQUE/CHECK/FOREIGN KEY dropped, "
               "keyless tables get a _bb_rowid key); CREATE VIEW is simulated by inlining the "
               "view's query; UNIQUE indexes become plain ones. SELECTs run as Chronos rewrites "
               "them (each table replaced by its visible-version subquery).",
    }

    OBSERVED: dict = {}
    _OBS_LOCK = threading.Lock()

    @classmethod
    def observe(cls, key: str, n: int = 1) -> None:
        with cls._OBS_LOCK:
            cls.OBSERVED[key] = cls.OBSERVED.get(key, 0) + n

    _SUITE_SEQ = 0

    @classmethod
    def init_for_bench(cls, collector: rc.ResultCollector, db_name: str, default_branch_name: str,
                       measure_storage: bool = False):
        return cls(None, collector, db_name, default_branch_name or MAIN_BRANCH, measure_storage)

    def __init__(self, connection, collector: rc.ResultCollector, db_name: str,
                 default_branch_name: str = MAIN_BRANCH, measure_storage: bool = False):
        if not CHRONOS_AVAILABLE:
            raise RuntimeError("chronos_core is not installed (see README: Chronos backend)")
        super().__init__(None, collector, measure_storage)
        self.db_name = db_name
        self.default_branch = default_branch_name or MAIN_BRANCH
        ChronosToolSuite._SUITE_SEQ += 1
        self._suite_id = ChronosToolSuite._SUITE_SEQ
        self._tag = f"bb_chronos_{os.getpid()}_{self._suite_id}"
        self._pg = psycopg.connect(_uri(db_name, self._tag), autocommit=True)
        self._admin = None
        self._ctx = None
        self._sessions: OrderedDict = OrderedDict()
        self._session = None          # BranchSession of the current ref (None in plain mode)
        self._tx = None               # open transaction context on the current session
        self._tx_branch = None
        self._tx_depth = 0
        self._views: dict = {}        # branch -> {view: select sql}
        self._rowid_tables: set = set()
        self._mviews: dict = {}       # temp view name -> (ref, table) for multi-ref exec
        self._tmp_seq = 0
        self.conn = _Conn(self)
        self._current_ref = Ref(self.default_branch)
        if self._db_adopted():
            self._mark_adopted()

    # ------------------------------------------------------------------
    # Plain connections
    # ------------------------------------------------------------------

    def _pgx(self, sql: str, args=None):
        with self._pg.cursor() as cur:
            cur.execute(sql, args)
            if cur.description is not None:
                return cur.fetchall()
            return None

    def _admin_conn(self):
        if self._admin is None or self._admin.closed:
            self._admin = psycopg.connect(_uri("postgres", self._tag + "_admin"), autocommit=True)
        return self._admin

    def _cancel_backends(self):
        try:
            with self._admin_conn().cursor() as cur:
                cur.execute("SELECT pg_cancel_backend(pid) FROM pg_stat_activity "
                            "WHERE datname = %s AND application_name = %s AND pid <> pg_backend_pid()",
                            (self.db_name, self._tag))
        except Exception as e:
            print(f"Warning: cancel failed: {e}")

    def cancel_current_op(self) -> None:
        super().cancel_current_op()

    def _unstick_gc(self) -> Optional[BaseException]:
        """Chronos's background interval-GC thread exits on its first error
        and a later delete re-flags GC as pending with nobody to run it, so
        ``close()`` and ``wait_for_background_work()`` would wait forever.
        Clear the flags when the worker is gone; return its error."""
        be = getattr(self._ctx, "_backend", None)
        th = getattr(be, "_interval_gc_thread", None)
        if th is None or th.is_alive():
            return None
        with be._interval_gc_condition:
            be._interval_gc_pending = False
            be._interval_gc_running = False
            err = be._interval_gc_error
        if err is not None:
            self.observe("gc_worker_died")
            print(f"Warning: Chronos interval GC worker died: {type(err).__name__}: {str(err)[:200]}")
        return err

    def close_connection(self) -> None:
        for s in list(self._sessions.values()):
            try:
                s.close()
            except Exception:
                pass
        self._sessions.clear()
        self._session = None
        if self._ctx is not None:
            try:
                self._unstick_gc()
                self._ctx.close()
            except Exception:
                pass
            self._ctx = None
        for c in (self._pg, self._admin):
            if c is not None:
                try:
                    c.close()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Chronos context, sessions, adoption
    # ------------------------------------------------------------------

    def _retry(self, what: str, fn, attempts: int = 400):
        """Run a Chronos branch-management call, retrying while another
        session barrier on the same branch is in progress (Chronos raises
        instead of waiting; forks from a busy spine collide often)."""
        delay = 0.02
        for attempt in range(attempts):
            try:
                return fn()
            except Exception as e:
                msg = str(e)
                if "chronos_session_barrier_in_progress" not in msg or attempt == attempts - 1:
                    raise
                self._check_cancel()
                self.observe(f"barrier_retries_{what}")
                time.sleep(delay)
                delay = min(delay * 1.5, 0.5)

    @property
    def ctx(self) -> "ChronosBranchContext":
        if self._ctx is None:
            self._ctx = ChronosBranchContext.connect(_uri(self.db_name, self._tag),
                                                     enable_schema_branching=True)
        return self._ctx

    def _db_adopted(self) -> bool:
        return bool(self._pgx(f"SELECT to_regclass(%s)", (LOG_TABLE,))[0][0])

    def _mark_adopted(self):
        with _ADOPT_LOCK:
            _ADOPTED.add(self.db_name)
        self._refresh_meta()

    @property
    def adopted(self) -> bool:
        return self.db_name in _ADOPTED

    def _refresh_meta(self):
        self._rowid_tables = {r[0] for r in self._pgx(f"SELECT name FROM {ROWID_TABLES}") or []}

    def _adopt(self) -> None:
        """Register every table with Chronos (first versioning verb)."""
        if self.adopted:
            return
        key = f"bb_adopt_{self.db_name}"
        self._pgx("SELECT pg_advisory_lock(hashtext(%s))", (key,))
        try:
            if self._db_adopted():
                self._mark_adopted()
                return
            t0 = time.perf_counter()
            tables = [r[0] for r in self._pgx(
                "SELECT tablename FROM pg_tables WHERE schemaname = 'public' "
                "AND tablename NOT LIKE '\\_chronos%' AND tablename NOT LIKE '\\_bb\\_%' ORDER BY 1")]
            for v in self._pgx("SELECT viewname FROM pg_views WHERE schemaname = 'public'") or []:
                self._pgx(f"DROP VIEW IF EXISTS {_q(v[0])} CASCADE")
            self._pgx(_LOG_DDL)
            ctx = self.ctx
            rows_total = 0
            for t in tables:
                pk = [r[0] for r in self._pgx(
                    "SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid "
                    "AND a.attnum = ANY(i.indkey) WHERE i.indrelid = %s::regclass AND i.indisprimary "
                    "ORDER BY array_position(i.indkey, a.attnum)", (_q(t),))]
                if not pk:
                    self._pgx(f"ALTER TABLE {_q(t)} ADD COLUMN {ROWID_COL} BIGINT")
                    self._pgx(f"UPDATE {_q(t)} SET {ROWID_COL} = s.rn FROM (SELECT ctid, "
                              f"row_number() OVER () AS rn FROM {_q(t)}) s WHERE {_q(t)}.ctid = s.ctid")
                    self._pgx(f"INSERT INTO {ROWID_TABLES} (name) VALUES (%s) ON CONFLICT DO NOTHING", (t,))
                    pk = [ROWID_COL]
                    self.observe("adopt_keyless_tables")
                ctx.register_table(t, pk)
                rows_total += int(self._pgx(f"SELECT COUNT(*) FROM {_q(t)}")[0][0])
            for t in tables:
                self._pgx(f"DROP TABLE {_q(t)} CASCADE")
            seq = time.time_ns()
            self._pgx(f"INSERT INTO {LOG_TABLE} (log_branch, seq, id, kind, message) VALUES (%s, %s, %s, %s, %s)",
                      (MAIN_BRANCH, seq, f"{seq:x}", "root", "initial state"))
            self.observe("adopt_tables", len(tables))
            self.observe("adopt_rows", rows_total)
            self.observe("adopt_sec", round(time.perf_counter() - t0, 3))
            print(f"Chronos: adopted {len(tables)} tables ({rows_total:,} rows) in "
                  f"{time.perf_counter() - t0:.1f}s")
            self._mark_adopted()
        finally:
            self._pgx("SELECT pg_advisory_unlock(hashtext(%s))", (key,))
        # The plain session the suite was on is now a Chronos session.
        if self._current_ref is not None:
            self._session = self._get_session(self._current_ref)

    def _get_session(self, ref: Ref):
        key = str(ref)
        s = self._sessions.get(key)
        if s is not None:
            self._sessions.move_to_end(key)
            return s
        if ref.commit:
            s = self.ctx.checkout_checkpoint(self._snapshot_of(ref.branch, ref.commit))
        else:
            s = self.ctx.checkout(ref.branch)
        self._sessions[key] = s
        while len(self._sessions) > _SESSION_CACHE:
            _, old = self._sessions.popitem(last=False)
            try:
                old.close()
            except Exception:
                pass
        return s

    def _drop_sessions(self, branch: str):
        for key in [k for k in self._sessions if Ref.parse(k).branch == branch]:
            s = self._sessions.pop(key)
            try:
                s.close()
            except Exception:
                pass
        if self._current_ref is not None and self._current_ref.branch == branch:
            self._session = None
            self._current_ref = None

    def _connect_impl(self, ref: Ref) -> None:
        if self._tx is not None:
            self._tx_end(False)
        if not self.adopted:
            if ref.commit or ref.branch != self.default_branch:
                self._adopt()
            else:
                self._session = None
                return
        self._session = self._get_session(ref)
        self._views[ref.branch] = self._load_views(ref.branch)

    def _on(self, ref: Ref):
        """Point the suite's session at ``ref`` (untimed)."""
        self._connect(ref, 0, False, "", False)
        if self._current_ref != ref:
            raise RuntimeError(f"could not connect to {ref}")
        return self._session

    def _begin_op(self, op_name, ref, label):
        return super()._begin_op(op_name, ref, label)

    def _end_op(self, token, error):
        if self._mviews:
            for name in list(self._mviews):
                try:
                    self._pgx(f"DROP VIEW IF EXISTS {name}")
                except Exception:
                    pass
            self._mviews.clear()
        return super()._end_op(token, error)

    # ------------------------------------------------------------------
    # Statement execution
    # ------------------------------------------------------------------

    def _run_statement(self, cur: _Cursor, sql: str, vars) -> None:
        head = _head(sql)
        if self._session is None:
            if self.adopted and self._current_ref is not None:
                self._session = self._get_session(self._current_ref)
            else:
                self._plain(cur, sql, vars)
                return
        sess = self._session
        if head in ("BEGIN", "START"):
            self._tx_begin(sess)
            return
        if head in ("COMMIT", "END"):
            self._tx_end(True)
            return
        if head == "ROLLBACK":
            self._tx_end(False)
            return
        if head in ("SELECT", "WITH", "VALUES", "EXPLAIN", "TABLE"):
            self._select(cur, sess, sql, vars)
            return
        if head in ("CHECKPOINT", "VACUUM", "ANALYZE", "SET", "SHOW", "RESET"):
            self._plain(cur, sql, vars)
            return
        nsql, params = _to_named(sql, params_or_none(vars))
        # A write outside an explicit transaction locks the branch row for
        # its own statement: pass the branch's gate for its duration.
        branch = self._gate_branch() if self._tx is None else None
        if branch:
            _GATES.enter(branch, self._check_cancel)
        try:
            if head == "INSERT":
                cur.rowcount = self._insert(sess, nsql, params)
            elif head == "UPDATE":
                cur.rowcount = self._update(sess, nsql, params)
            elif head == "DELETE":
                cur.rowcount = self._delete(sess, nsql, params)
            elif head in ("CREATE", "ALTER", "DROP", "TRUNCATE"):
                cur.rowcount = self._ddl(sess, head, nsql, params)
            else:
                raise UnsupportedSQLError(f"chronos: statement not supported by the shim: {head}")
        finally:
            if branch:
                _GATES.leave(branch)

    def _gate_branch(self) -> Optional[str]:
        ref = self._current_ref
        if ref is None or ref.commit:
            return None
        return ref.branch

    def _plain(self, cur: _Cursor, sql: str, vars):
        with self._pg.cursor() as c:
            c.execute(sql, vars)
            cur.description = c.description
            cur.rowcount = c.rowcount
            cur._rows = c.fetchall() if c.description is not None else None

    def _tx_begin(self, sess):
        if self._tx is not None:
            self._tx_depth += 1
            return
        branch = self._gate_branch()
        if branch:
            _GATES.enter(branch, self._check_cancel)
        self._tx_branch = branch
        try:
            self._tx = sess.transaction()
            self._tx.__enter__()
        except BaseException:
            if branch:
                _GATES.leave(branch)
            raise
        self._tx_depth = 1

    def _tx_end(self, commit: bool):
        if self._tx is None:
            return
        self._tx_depth -= 1
        if self._tx_depth > 0 and commit:
            return
        tx, self._tx, self._tx_depth = self._tx, None, 0
        branch, self._tx_branch = getattr(self, "_tx_branch", None), None
        try:
            if commit:
                tx.__exit__(None, None, None)
            else:
                exc = RuntimeError("rollback")
                try:
                    tx.__exit__(RuntimeError, exc, None)
                except RuntimeError as e:
                    if e is not exc:
                        raise
        finally:
            if branch:
                _GATES.leave(branch)

    @staticmethod
    def _dict_rows(rows: list) -> tuple:
        if not rows:
            return [], []
        keys = list(rows[0].keys())
        return [(k,) for k in keys], [tuple(r[k] for k in keys) for r in rows]

    def _select(self, cur: _Cursor, sess, sql: str, vars):
        branch = self._current_ref.branch if self._current_ref else None
        views = self._views.get(branch) or {}
        if views:
            sql = self._inline_views(sql, views)
        if self._tx is not None and not self._mviews:
            nsql, params = _to_named(sql, params_or_none(vars))
            cur.description, cur._rows = self._dict_rows(sess.query(nsql, params))
            cur.rowcount = len(cur._rows)
            return
        try:
            rewritten = sess.rewrite_query(sql)
        except Exception as e:
            if "chronos_table_not_registered" in str(e):
                raise TableNotRegisteredError(str(e)) from e
            raise
        self._plain(cur, rewritten, vars)

    # -- views (simulated) ----------------------------------------------

    def _load_views(self, branch: str) -> dict:
        return {r[0]: r[1] for r in self._pgx(f"SELECT name, sql FROM {VIEWS_TABLE} WHERE branch = %s",
                                              (branch,)) or []}

    @staticmethod
    def _inline_views(sql: str, views: dict, depth: int = 0) -> str:
        if depth > 3:
            return sql
        changed = False
        for name, vsql in views.items():
            pat = re.compile(rf'(?i)\b(FROM|JOIN)\s+("?{re.escape(name)}"?)(?=\s|$|,|\))')
            if pat.search(sql):
                sql = pat.sub(lambda m: f"{m.group(1)} ({vsql}) AS {name}", sql)
                changed = True
        return ChronosToolSuite._inline_views(sql, views, depth + 1) if changed else sql

    # -- DML --------------------------------------------------------------

    def _meta(self, branch: str, table: str):
        try:
            return self.ctx._backend.table_meta_for_branch(branch, table)
        except TableNotRegisteredError:
            return None

    def _cur_meta(self, table: str):
        branch = self._current_ref.branch if self._current_ref else MAIN_BRANCH
        meta = self._meta(branch, table)
        if meta is None:
            raise TableNotRegisteredError(table)
        return meta

    def _insert(self, sess, nsql: str, params: dict) -> int:
        m = re.match(r"\s*INSERT\s+INTO\s+([\w\".]+)", nsql, re.I)
        table = _unq(m.group(1)) if m else ""
        if table in self._rowid_tables:
            nsql, params = self._inject_rowid(nsql, params)
        try:
            return sess.execute(nsql, params).rowcount
        except Exception as e:
            if not _is_unsupported(e):
                raise
        self.observe("simulated_insert")
        return self._insert_simulated(sess, nsql, params, table)

    def _inject_rowid(self, nsql: str, params: dict) -> tuple:
        """Add the synthetic key to an INSERT into a keyless table; the
        values travel as bound parameters (a big integer literal would be
        read as a float by the Chronos evaluator)."""
        import sqlglot
        from sqlglot import exp
        tree = sqlglot.parse_one(nsql, read="postgres")
        if not isinstance(tree, exp.Insert) or not isinstance(tree.expression, exp.Values):
            return nsql, params
        cols = tree.this.expressions if isinstance(tree.this, exp.Schema) else None
        if cols is None:
            return nsql, params  # the simulated path fills the column list
        if any(c.name == ROWID_COL for c in cols):
            return nsql, params
        cols.append(exp.to_identifier(ROWID_COL))
        params = dict(params)
        for i, tup in enumerate(tree.expression.expressions):
            tup.expressions.append(exp.Placeholder(this=f"bbrid{i}"))
            params[f"bbrid{i}"] = _rowid()
        return _pyformat_to_named(tree.sql(dialect="postgres")), params

    def _insert_simulated(self, sess, nsql: str, params: dict, table: str) -> int:
        import sqlglot
        from sqlglot import exp
        tree = sqlglot.parse_one(nsql, read="postgres")
        if not isinstance(tree, exp.Insert):
            raise UnsupportedSQLError(f"chronos: cannot simulate: {nsql[:80]}")
        meta = self._cur_meta(table)
        if isinstance(tree.this, exp.Schema):
            cols = [c.name for c in tree.this.expressions]
        else:
            cols = [c for c in meta.columns if c != ROWID_COL]
        src = tree.expression
        if isinstance(src, exp.Values):
            alias = ", ".join(_q(c) for c in cols)
            vals = src.sql(dialect="postgres")
            q = f"SELECT * FROM ({vals}) AS v({alias})"
        elif isinstance(src, exp.Select):
            q = src.sql(dialect="postgres")
        else:
            raise UnsupportedSQLError(f"chronos: cannot simulate INSERT source: {nsql[:80]}")
        rows = sess.query(_pyformat_to_named(q), params)
        out = []
        defaults = self._defaults(meta)
        for r in rows:
            vals = list(r.values())
            d = dict(zip(cols, vals))
            for c in meta.columns:
                if c not in d:
                    d[c] = _rowid() if c == ROWID_COL else defaults.get(c)
            out.append(d)
        if not out:
            return 0
        conflict = re.search(r"\bON\s+CONFLICT\b.*\bDO\s+(NOTHING|UPDATE)", nsql, re.I | re.S)
        if conflict is None or conflict.group(1).upper() == "NOTHING":
            existing = self._existing_keys(sess, meta, [self._key(meta, d) for d in out])
            if existing:
                if conflict is None:
                    raise DuplicateKeyError(f"duplicate key value in {table}")
                out = [d for d in out if self._key(meta, d) not in existing]
        if out:
            sess.upsert_rows(table, out)
        return len(out)

    @staticmethod
    def _defaults(meta) -> dict:
        out = {}
        for c, d in zip(meta.columns, meta.column_defs):
            m = re.search(r"\bDEFAULT\s+(.+)$", d, re.I)
            if m:
                v = m.group(1).strip()
                if v.upper() == "NULL":
                    out[c] = None
                elif v.startswith("'"):
                    out[c] = v[1:-1].replace("''", "'")
                elif v.upper() in ("TRUE", "FALSE"):
                    out[c] = v.upper() == "TRUE"
                else:
                    try:
                        out[c] = int(v) if re.match(r"^-?\d+$", v) else decimal.Decimal(v)
                    except Exception:
                        out[c] = v
        return out

    @staticmethod
    def _key(meta, row: dict) -> tuple:
        return tuple(row.get(c) for c in meta.pk_columns)

    def _existing_keys(self, sess, meta, keys: list) -> set:
        found = set()
        pk = list(meta.pk_columns)
        for i in range(0, len(keys), _CHUNK):
            chunk = keys[i:i + _CHUNK]
            params, preds = {}, []
            for j, k in enumerate(chunk):
                conds = []
                for c, v in zip(pk, k):
                    params[f"k{j}_{c}"] = _conv(v)
                    conds.append(f"{_q(c)} = :k{j}_{c}")
                preds.append("(" + " AND ".join(conds) + ")")
            rows = sess.query(f"SELECT {', '.join(_q(c) for c in pk)} FROM {_q(meta.name)} "
                              f"WHERE {' OR '.join(preds)}", params)
            for r in rows:
                found.add(tuple(_norm(r[c]) for c in pk))
        return {k for k in [tuple(_norm(v) for v in key) for key in keys] if k in found} and found

    def _update(self, sess, nsql: str, params: dict) -> int:
        try:
            return sess.execute(nsql, params).rowcount
        except Exception as e:
            if not _is_unsupported(e):
                raise
        self.observe("simulated_update")
        import sqlglot
        from sqlglot import exp
        tree = sqlglot.parse_one(nsql, read="postgres")
        if not isinstance(tree, exp.Update):
            raise UnsupportedSQLError(f"chronos: cannot simulate: {nsql[:80]}")
        table = _unq(tree.this.sql(dialect="postgres"))
        sets = [(e.this.name, e.expression.sql(dialect="postgres")) for e in tree.expressions]
        where = tree.args.get("where")
        q = f"SELECT *, " + ", ".join(f"{expr} AS __bb_{i}" for i, (_, expr) in enumerate(sets)) + \
            f" FROM {_q(table)}"
        if where is not None:
            q += " WHERE " + where.this.sql(dialect="postgres")
        rows = sess.query(_pyformat_to_named(q), params)
        new = []
        for r in rows:
            d = {k: v for k, v in r.items() if not k.startswith("__bb_")}
            for i, (col, _) in enumerate(sets):
                d[col] = r[f"__bb_{i}"]
            new.append(d)
        for i in range(0, len(new), _CHUNK):
            sess.upsert_rows(table, new[i:i + _CHUNK])
        return len(new)

    def _delete(self, sess, nsql: str, params: dict) -> int:
        try:
            return sess.execute(nsql, params).rowcount
        except Exception as e:
            if not _is_unsupported(e):
                raise
        self.observe("simulated_delete")
        import sqlglot
        from sqlglot import exp
        tree = sqlglot.parse_one(nsql, read="postgres")
        if not isinstance(tree, exp.Delete):
            raise UnsupportedSQLError(f"chronos: cannot simulate: {nsql[:80]}")
        table = _unq(tree.this.sql(dialect="postgres"))
        where = tree.args.get("where")
        return self._delete_where(sess, table, where.this.sql(dialect="postgres") if where is not None else None,
                                  params)

    def _delete_where(self, sess, table: str, where_sql: Optional[str], params: dict) -> int:
        meta = self._cur_meta(table)
        pk = list(meta.pk_columns)
        q = f"SELECT {', '.join(_q(c) for c in pk)} FROM {_q(table)}"
        if where_sql:
            q += " WHERE " + where_sql
        rows = sess.query(_pyformat_to_named(q), params)
        keys = [{c: r[c] for c in pk} for r in rows]
        for i in range(0, len(keys), _CHUNK):
            sess.delete_keys(table, keys[i:i + _CHUNK])
        return len(keys)

    # -- DDL --------------------------------------------------------------

    def _ddl(self, sess, head: str, nsql: str, params: dict) -> int:
        u = nsql.lstrip().upper()
        branch = self._current_ref.branch if self._current_ref else MAIN_BRANCH
        if head == "CREATE":
            if re.match(r"CREATE\s+(OR\s+REPLACE\s+)?VIEW", u):
                m = re.match(r"\s*CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+([\w\".]+)\s+AS\s+(.*)$", nsql, re.I | re.S)
                name = _unq(m.group(1))
                self._pgx(f"INSERT INTO {VIEWS_TABLE} (branch, name, sql) VALUES (%s, %s, %s) "
                          "ON CONFLICT (branch, name) DO UPDATE SET sql = EXCLUDED.sql",
                          (branch, name, m.group(2).strip().rstrip(";")))
                self._views[branch] = self._load_views(branch)
                self.observe("simulated_views")
                return 0
            if re.match(r"CREATE\s+(UNIQUE\s+)?INDEX", u):
                if "UNIQUE" in u.split("INDEX")[0]:
                    nsql = re.sub(r"(?i)\bUNIQUE\s+", "", nsql, count=1)
                    self.observe("unique_index_downgraded")
                try:
                    return sess.execute(nsql, params).rowcount
                except Exception as e:
                    if "already exists" in str(e) and "IF NOT EXISTS" in u:
                        return 0
                    raise
            if re.match(r"CREATE\s+(TEMP|TEMPORARY\s+)?TABLE", u):
                ddl, table, ine, keyless = _normalize_create_table(nsql)
                try:
                    sess.execute(ddl, params)
                except BranchingError as e:
                    if ine and "already exists" in str(e):
                        return 0
                    raise
                if keyless:
                    self._pgx(f"INSERT INTO {ROWID_TABLES} (name) VALUES (%s) ON CONFLICT DO NOTHING", (table,))
                    self._rowid_tables.add(table)
                    self.observe("keyless_tables")
                return 0
            raise UnsupportedSQLError(f"chronos: unsupported DDL: {nsql[:60]}")
        if head == "ALTER":
            if re.search(r"\bADD\s+(CONSTRAINT|FOREIGN|PRIMARY|UNIQUE|CHECK)\b", u):
                raise UnsupportedSQLError("chronos: table constraints are not supported")
            return sess.execute(_normalize_alter(nsql), params).rowcount
        if head == "DROP":
            if re.match(r"DROP\s+VIEW", u):
                m = re.match(r"\s*DROP\s+VIEW\s+(IF\s+EXISTS\s+)?([\w\".]+)", nsql, re.I)
                self._pgx(f"DELETE FROM {VIEWS_TABLE} WHERE branch = %s AND name = %s", (branch, _unq(m.group(2))))
                self._views[branch] = self._load_views(branch)
                return 0
            try:
                return sess.execute(nsql, params).rowcount
            except TableNotRegisteredError:
                if "IF EXISTS" in u:
                    return 0
                raise
        if head == "TRUNCATE":
            m = re.match(r"\s*TRUNCATE\s+(TABLE\s+)?([\w\".]+)", nsql, re.I)
            self.observe("simulated_truncate")
            return self._delete_where(sess, _unq(m.group(2)), None, {})
        raise UnsupportedSQLError(f"chronos: unsupported statement: {nsql[:60]}")

    # ------------------------------------------------------------------
    # Bookkeeping: log, checkpoints, temp branches
    # ------------------------------------------------------------------

    def _log_rows(self, branch: str, limit: int = None, max_seq: int = None, kinds=None) -> list:
        sql = f"SELECT {', '.join(_LOG_COLS)} FROM {LOG_TABLE} WHERE log_branch = %s"
        args = [branch]
        if max_seq is not None:
            sql += " AND seq <= %s"
            args.append(int(max_seq))
        if kinds:
            sql += f" AND kind IN ({', '.join(['%s'] * len(kinds))})"
            args += list(kinds)
        sql += " ORDER BY seq DESC"
        if limit:
            sql += f" LIMIT {int(limit)}"
        return [dict(zip(_LOG_COLS, r)) for r in self._pgx(sql, tuple(args)) or []]

    def _add_row(self, branch: str, kind: str, message: str, snap: str = None, prev_snap: str = None,
                 prev_branch: str = None, commit_id: str = None, seq: int = None) -> tuple:
        seq = seq or time.time_ns()
        commit_id = commit_id or f"{seq:x}"
        for _ in range(16):
            try:
                self._pgx(f"INSERT INTO {LOG_TABLE} (log_branch, seq, id, kind, message, snap, prev_snap, prev_branch) "
                          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                          (branch, seq, commit_id, kind, message or "", snap, prev_snap, prev_branch))
                return commit_id, seq
            except psycopg.errors.UniqueViolation:
                seq += 1
        raise RuntimeError("could not record commit")

    def _copy_log(self, into: str, src: str, max_seq: int = None) -> int:
        sql = (f"INSERT INTO {LOG_TABLE} (log_branch, seq, id, kind, message, snap, prev_snap, prev_branch, ts) "
               f"SELECT %s, s.seq, s.id, s.kind, s.message, s.snap, s.prev_snap, s.prev_branch, s.ts "
               f"FROM {LOG_TABLE} s WHERE s.log_branch = %s AND NOT EXISTS "
               f"(SELECT 1 FROM {LOG_TABLE} d WHERE d.log_branch = %s AND (d.id = s.id OR d.seq = s.seq))")
        args = [into, src, into]
        if max_seq is not None:
            sql += " AND s.seq <= %s"
            args.append(int(max_seq))
        with self._pg.cursor() as cur:
            cur.execute(sql, tuple(args))
            return cur.rowcount

    def _find_commit(self, branch: str, commit_id: str) -> dict:
        rows = self._pgx(f"SELECT {', '.join(_LOG_COLS)} FROM {LOG_TABLE} WHERE log_branch = %s AND id = %s "
                         "ORDER BY (snap IS NOT NULL) DESC, seq DESC LIMIT 1", (branch, str(commit_id)))
        if not rows:
            raise ValueError(f"unknown commit '{commit_id}' on {branch}")
        return dict(zip(_LOG_COLS, rows[0]))

    @staticmethod
    def _state(row: dict) -> Optional[str]:
        """Checkpoint holding the branch's state right after ``row``."""
        if row["kind"] in ("commit", "sync", "revert"):
            return row["snap"]
        if row["kind"] == "fork":
            return row["prev_snap"]
        return None

    def _snapshot_of(self, branch: str, commit: str) -> str:
        row = self._find_commit(branch, commit)
        st = self._state(row)
        if not st:
            raise ValueError(f"commit '{commit}' on {branch} has no checkpoint ({row['kind']} row)")
        return st

    def _prev_state(self, branch: str, seq: int) -> Optional[str]:
        for row in self._log_rows(branch, max_seq=seq - 1):
            st = self._state(row)
            if st:
                return st
        return None

    def _merge_base(self, a: str, b: str, b_max_seq: int = None) -> Optional[str]:
        """Checkpoint of the latest log row the two branches share."""
        rows_b = {r["id"]: r for r in self._log_rows(b, max_seq=b_max_seq)}
        for row in self._log_rows(a):
            other = rows_b.get(row["id"])
            if other is None:
                continue
            st = self._state(row) or self._state(other)
            if st:
                return st
        return None

    def _cp_branch(self, checkpoint: str) -> Optional[str]:
        try:
            return self.ctx.get_checkpoint(checkpoint).branch_id
        except Exception:
            return None

    @contextlib.contextmanager
    def _temp_branch(self, checkpoint: str):
        self._tmp_seq += 1
        name = f"{TEMP_PREFIX}{os.getpid()}_{self._suite_id}_{self._tmp_seq}_{random.getrandbits(16):x}"
        owner = self._cp_branch(checkpoint)
        with _GATES.hold([owner], self._check_cancel):
            self._retry("fork", lambda: self.ctx.create_branch_from_checkpoint(name, checkpoint))
        try:
            yield name
        finally:
            try:
                with _GATES.hold([owner], self._check_cancel):
                    self._retry("delete", lambda: self.ctx.delete_branch(name))
            except Exception as e:
                print(f"Warning: could not drop temporary branch {name}: {e}")

    @contextlib.contextmanager
    def _reading(self, ref: Ref):
        """A branch name whose state is ``ref`` (a temporary one for a commit)."""
        if ref.commit:
            with self._temp_branch(self._snapshot_of(ref.branch, ref.commit)) as name:
                yield name
        else:
            yield ref.branch

    def _schema_of_branch(self, branch: str) -> dict:
        """{table: {"cols": {column: type}, "pk": [columns]}} from Chronos's
        per-branch table metadata (the hidden row id left out)."""
        out = {}
        for t, meta in self._branch_tables(branch).items():
            out[t] = {"cols": {c: _type_of(d).lower() for c, d in zip(meta.columns, meta.column_defs)
                               if c != ROWID_COL},
                      "pk": [c for c in meta.pk_columns if c != ROWID_COL]}
        return out

    @staticmethod
    def _fp_columns(fp: dict) -> dict:
        """{table: {column: type}} view of a fingerprint (either shape)."""
        return {t: (spec["cols"] if isinstance(spec, dict) and "cols" in spec else spec)
                for t, spec in fp.items()}

    def _reconcile_to_fingerprint(self, sess, branch: str, fp: dict) -> None:
        """Give the branch exactly the fingerprint's tables and columns:
        extra tables and columns dropped, missing tables created (empty),
        missing or retyped columns (re)added with the recorded type."""
        now = self._branch_tables(branch)
        for t in sorted(set(now) - set(fp)):
            sess.execute(f"DROP TABLE {_q(t)}")
        for t in sorted(set(fp) - set(now)):
            spec = fp[t]
            cols = spec["cols"] if "cols" in spec else spec
            pk = set(spec.get("pk", ())) if "cols" in spec else set()
            defs = [f"{_q(c)} {ty}" + (" PRIMARY KEY" if c in pk else "") for c, ty in cols.items()]
            sess.execute(f"CREATE TABLE {_q(t)} ({', '.join(defs)})")
        for t in sorted(set(now) & set(fp)):
            spec = fp[t]
            cols = spec["cols"] if "cols" in spec else spec
            meta = now[t]
            pk = set(meta.pk_columns)
            have = {c: _type_of(d).lower() for c, d in zip(meta.columns, meta.column_defs) if c != ROWID_COL}
            for c in list(have):
                if c not in cols and c not in pk:
                    sess.execute(f"ALTER TABLE {_q(t)} DROP COLUMN {_q(c)}")
                    have.pop(c)
            for c, ty in cols.items():
                if c in have and have[c] != ty and c not in pk:
                    sess.execute(f"ALTER TABLE {_q(t)} DROP COLUMN {_q(c)}")
                    have.pop(c)
                if c not in have:
                    sess.execute(f"ALTER TABLE {_q(t)} ADD COLUMN {_q(c)} {ty}")

    def _schema_at(self, ref: Ref) -> dict:
        """Chronos keeps a table's definition per branch, not per
        checkpoint; a commit's schema is the fingerprint the adapter stored
        with its log row (any branch's copy of the row carries it)."""
        if not ref.commit:
            return self._fp_columns(self._schema_of_branch(ref.branch))
        rows = self._pgx(f"SELECT schema_json FROM {LOG_TABLE} WHERE id = %s AND schema_json IS NOT NULL "
                         "ORDER BY (log_branch = %s) DESC LIMIT 1", (str(ref.commit), ref.branch))
        if not rows:
            raise self._unsupported("diff", f"no schema recorded at commit {ref.commit}")
        return self._fp_columns(json.loads(rows[0][0]))

    def _branch_tables(self, branch: str) -> dict:
        out = {}
        for t in self.ctx._backend.diff_tables():
            meta = self._meta(branch, t)
            if meta is not None:
                out[t] = meta
        return out

    def _diff_map(self, left: str, right: str) -> dict:
        """{(table, key tuple): RowDiff} of right relative to left. A row
        that differs only in columns the left side lacks (a column added
        with a DEFAULT) is not a change: the DDL carries it."""
        out = {}
        lt, rt = self._branch_tables(left), self._branch_tables(right)
        for d in self.ctx.diff(left, right).changes:
            if d.change == "modified":
                # The diff projects both rows onto the union of the two
                # schemas (None for a missing column): compare the shared ones.
                cols = None
                if d.table in lt and d.table in rt:
                    cols = set(lt[d.table].columns) & set(rt[d.table].columns)
                if not _changed_cols(d.before, d.after, cols):
                    continue
            out[(d.table, tuple(_norm(d.key[c]) for c in sorted(d.key)))] = d
        return out

    def _verify_side(self, branch: str, dmap: dict) -> None:
        """Replace the ``after`` rows of ``dmap`` (a _diff_map of ``branch``)
        by the rows the branch shows now; a missing row becomes a delete."""
        if not dmap:
            return
        sess = self._get_session(Ref(branch))
        metas = self._branch_tables(branch)
        by_table = {}
        for (table, key), d in dmap.items():
            by_table.setdefault(table, []).append((key, d))
        stale = 0
        for table, entries in by_table.items():
            meta = metas.get(table)
            if meta is None:
                continue
            pk = list(meta.pk_columns)
            for i in range(0, len(entries), _CHUNK):
                chunk = entries[i:i + _CHUNK]
                params, preds = {}, []
                for j, (key, d) in enumerate(chunk):
                    conds = []
                    for c in pk:
                        params[f"k{j}_{c}"] = _conv(d.key[c])
                        conds.append(f"{_q(c)} = :k{j}_{c}")
                    preds.append("(" + " AND ".join(conds) + ")")
                live = {tuple(_norm(r[c]) for c in sorted(pk)): r
                        for r in sess.query(f"SELECT * FROM {_q(table)} WHERE {' OR '.join(preds)}", params)}
                for key, d in chunk:
                    row = live.get(key)
                    if row is None:
                        if d.change != "deleted":
                            stale += 1
                            dmap[(table, key)] = type(d)(d.table, d.key, "deleted", d.before, None)
                    else:
                        if d.change == "deleted" or not _same(d.after, row):
                            stale += 1
                        change = d.change if d.change != "deleted" else "modified"
                        dmap[(table, key)] = type(d)(d.table, d.key, change, d.before, dict(row))
        if stale:
            self.observe("diff_values_stale", stale)

    def _all_rows(self, branch: str, table: str) -> list:
        s = self._get_session(Ref(branch))
        return s.query(f"SELECT * FROM {_q(table)}")

    def _apply(self, sess, branch: str, by_table: dict) -> int:
        """by_table: table -> {"upsert": [row dict], "delete": [key dict]}."""
        n = 0
        metas = self._branch_tables(branch)
        for table, ops in by_table.items():
            meta = metas.get(table)
            if meta is None:
                continue
            self._check_cancel()
            keys = ops.get("delete") or []
            for i in range(0, len(keys), _CHUNK):
                self._bulk(sess, table, meta, keys[i:i + _CHUNK], delete=True)
            n += len(keys)
            rows = ops.get("upsert") or []
            if rows:
                cols = list(meta.columns)
                extra = [c for c in cols if c not in rows[0]]
                if extra:
                    # The target has columns the source lacks: keep its values.
                    current = {}
                    pk = list(meta.pk_columns)
                    for i in range(0, len(rows), _CHUNK):
                        chunk = rows[i:i + _CHUNK]
                        params, preds = {}, []
                        for j, r in enumerate(chunk):
                            conds = []
                            for c in pk:
                                params[f"k{j}_{c}"] = _conv(r[c])
                                conds.append(f"{_q(c)} = :k{j}_{c}")
                            preds.append("(" + " AND ".join(conds) + ")")
                        for r in sess.query(f"SELECT * FROM {_q(table)} WHERE {' OR '.join(preds)}", params):
                            current[tuple(_norm(r[c]) for c in pk)] = r
                    merged = []
                    for r in rows:
                        cur = current.get(tuple(_norm(r[c]) for c in pk), {})
                        d = {c: cur.get(c) for c in cols}
                        d.update({c: v for c, v in r.items() if c in d})
                        merged.append(d)
                    rows = merged
                else:
                    rows = [{c: r.get(c) for c in cols} for r in rows]
                for i in range(0, len(rows), _CHUNK):
                    self._bulk(sess, table, meta, rows[i:i + _CHUNK])
                n += len(rows)
        return n

    def _bulk(self, sess, table: str, meta, rows: list, delete: bool = False) -> None:
        """Chronos's bulk upsert/delete, falling back to one statement per
        row through its DML executor when the bulk path raises a duplicate
        version key (it inserts a version at the segment's bound and does
        not splice an existing one)."""
        try:
            if delete:
                sess.delete_keys(table, rows)
            else:
                sess.upsert_rows(table, rows)
            return
        except Exception as e:
            if "duplicate key" not in str(e).lower():
                raise
        self.observe("bulk_write_fallback_rows", len(rows))
        pk = list(meta.pk_columns)
        for r in rows:
            params = {f"k{i}": _conv(r[c]) for i, c in enumerate(pk)}
            where = " AND ".join(f"{_q(c)} = :k{i}" for i, c in enumerate(pk))
            if delete:
                sess.execute(f"DELETE FROM {_q(table)} WHERE {where}", params)
                continue
            sets = [c for c in meta.columns if c not in pk and c in r]
            params.update({f"v{i}": _conv(r[c]) for i, c in enumerate(sets)})
            n = sess.execute(f"UPDATE {_q(table)} SET " + ", ".join(f"{_q(c)} = :v{i}" for i, c in enumerate(sets))
                             + f" WHERE {where}", params).rowcount
            if not n:
                cols = pk + sets
                vals = [f":k{i}" for i in range(len(pk))] + [f":v{i}" for i in range(len(sets))]
                sess.execute(f"INSERT INTO {_q(table)} ({', '.join(_q(c) for c in cols)}) VALUES ({', '.join(vals)})",
                             params)

    # ------------------------------------------------------------------
    # Schema reconciliation (merge/rebase/reset)
    # ------------------------------------------------------------------

    def _create_like(self, sess, meta) -> None:
        defs = []
        for c, d in zip(meta.columns, meta.column_defs):
            if c in meta.pk_columns:
                d = d + " PRIMARY KEY"
            defs.append(d)
        sess.execute(f"CREATE TABLE {_q(meta.name)} ({', '.join(defs)})")

    def _reconcile_schema(self, sess, ours: str, theirs: str, drop_extra: bool = False) -> dict:
        """Give ``ours`` the tables and columns ``theirs`` has (data of new
        tables copied over); with ``drop_extra`` also drop what theirs lacks
        (reset). A differing primary key or column type is a schema conflict."""
        ours_t, theirs_t = self._branch_tables(ours), self._branch_tables(theirs)
        added_tables, added_cols, dropped = [], [], []
        for t, tm in theirs_t.items():
            om = ours_t.get(t)
            if om is None:
                self._create_like(sess, tm)
                rows = self._all_rows(theirs, t)
                for i in range(0, len(rows), _CHUNK):
                    sess.upsert_rows(t, rows[i:i + _CHUNK])
                added_tables.append(t)
                continue
            if tuple(om.pk_columns) != tuple(tm.pk_columns):
                raise RuntimeError(f"schema conflict on {t}: primary keys differ")
            have = dict(zip(om.columns, om.column_defs))
            for c, d in zip(tm.columns, tm.column_defs):
                if c not in have:
                    sess.execute(f"ALTER TABLE {_q(t)} ADD COLUMN {d}")
                    added_cols.append(f"{t}.{c}")
                elif _type_of(d) != _type_of(have[c]):
                    raise RuntimeError(f"schema conflict on {t}.{c}: types differ "
                                       f"(ours {_type_of(have[c])}, theirs {_type_of(d)})")
            if drop_extra:
                for c in om.columns:
                    if c not in tm.columns and c not in om.pk_columns:
                        sess.execute(f"ALTER TABLE {_q(t)} DROP COLUMN {_q(c)}")
                        dropped.append(f"{t}.{c}")
        if drop_extra:
            for t in ours_t:
                if t not in theirs_t:
                    sess.execute(f"DROP TABLE {_q(t)}")
                    dropped.append(t)
        return {"added_tables": added_tables, "added_columns": added_cols, "dropped": dropped}

    # ------------------------------------------------------------------
    # Three-way merge (shared by merge and rebase)
    # ------------------------------------------------------------------

    def _three_way(self, sess, ours: str, theirs: str, base_cp: Optional[str], on_conflict,
                   keep_ours: bool, ref: Ref, swap: bool) -> dict:
        """Apply theirs' changes since the base onto ours. ``keep_ours``: a
        conflicting key keeps ours' version (else theirs'); a callable gets
        the conflicts with base_/our_/their_ columns (sides swapped for a
        rebase, where the branch's own change is "theirs")."""
        info = {"conflicts": 0, "conflict_tables": [], "ours_changes": 0, "theirs_changes": 0,
                "two_way": base_cp is None}
        schema = self._reconcile_schema(sess, ours, theirs)
        info["schema_changes"] = schema
        new_tables = set(schema["added_tables"])
        if base_cp is None:
            print(f"Warning: {theirs} and {ours} share no fork point: merging two-way")
            d_theirs = self._diff_map(ours, theirs)
            d_ours = {}
        else:
            with self._temp_branch(base_cp) as base:
                d_theirs = self._diff_map(base, theirs)
                d_ours = self._diff_map(base, ours)
        info["theirs_changes"], info["ours_changes"] = len(d_theirs), len(d_ours)
        # Trust the diff for *which* keys changed, not for their values: the
        # rows are re-read from each branch before they are applied (a diff
        # value found stale is counted in backend_observations).
        self._verify_side(theirs, d_theirs)
        if base_cp is not None:
            self._verify_side(ours, d_ours)
        by_table, conflicts = {}, {}
        for k, t in d_theirs.items():
            table = k[0]
            if table in new_tables:
                continue  # copied whole with the table
            o = d_ours.get(k) if base_cp is not None else None
            if base_cp is None and keep_ours and t.change != "added":
                continue
            if o is not None:
                if _same(o.after, t.after):
                    continue
                conflicts.setdefault(table, []).append((o, t))
                if keep_ours:
                    continue
            ops = by_table.setdefault(table, {"upsert": [], "delete": []})
            if t.change == "deleted":
                ops["delete"].append(dict(t.key))
            else:
                ops["upsert"].append(dict(t.after))
        info["conflicts"] = sum(len(v) for v in conflicts.values())
        info["conflict_tables"] = sorted(conflicts)
        if conflicts and os.environ.get("CHRONOS_DEBUG_CONFLICTS"):
            for table, pairs in conflicts.items():
                kinds = {}
                for o, t in pairs:
                    kinds[(o.change, t.change)] = kinds.get((o.change, t.change), 0) + 1
                print(f"[conflicts] {table}: {len(pairs)} rows, (ours, theirs) change kinds {kinds}")
                for o, t in pairs[:3]:
                    print(f"   key={t.key} base={o.before} ours={o.after} theirs={t.after}")
        info["applied"] = self._apply(sess, ours, by_table)
        if conflicts and callable(on_conflict):
            rows_out = []
            for table, pairs in conflicts.items():
                rows = []
                for o, t in pairs:
                    base_row = o.before or t.before or {}
                    our, their = (t.after, o.after) if swap else (o.after, t.after)
                    our_type, their_type = (_dtype(t), _dtype(o)) if swap else (_dtype(o), _dtype(t))
                    d = {}
                    for c, v in base_row.items():
                        d[f"base_{c}"] = v
                    for c in (our or base_row):
                        d[f"our_{c}"] = (our or {}).get(c)
                    for c in (their or base_row):
                        d[f"their_{c}"] = (their or {}).get(c)
                    d["our_diff_type"], d["their_diff_type"] = our_type, their_type
                    rows.append(d)
                rows_out.append({"table": table, "rows": rows})
            info["resolved"] = "custom"
            info["resolution"] = on_conflict(self._conflict_session(ref), rows_out)
        elif conflicts:
            info["resolved"] = on_conflict
        return info

    # ------------------------------------------------------------------
    # Verbs
    # ------------------------------------------------------------------

    def list_branches(self) -> list:
        self._adopt()
        dead = {r[0] for r in self._pgx(f"SELECT branch FROM {TOMBSTONES}") or []}
        return [b.branch_id for b in self.ctx.list_branches()
                if b.branch_id not in dead and not b.branch_id.startswith(TEMP_PREFIX)]

    def _branch_impl(self, name: str, from_ref: Ref) -> None:
        self._adopt()
        if name.startswith(TEMP_PREFIX):
            raise ValueError(f"reserved branch name: {name}")
        parent = from_ref.branch
        if from_ref.commit:
            row = self._find_commit(parent, from_ref.commit)
            base_cp, max_seq = self._snapshot_of(parent, from_ref.commit), int(row["seq"])
            with _GATES.hold([self._cp_branch(base_cp)], self._check_cancel):
                self._retry("fork", lambda: self.ctx.create_branch_from_checkpoint(name, base_cp))
            self._copy_log(name, parent, max_seq=max_seq)
            self._add_row(name, "fork", f"fork from {from_ref}", prev_snap=base_cp, prev_branch=parent)
        else:
            seq = time.time_ns()
            base_cp = f"bb_f{seq:x}"
            with _GATES.hold([parent], self._check_cancel):
                self._retry("checkpoint", lambda: self.ctx.create_checkpoint(base_cp, parent, {"kind": "fork", "child": name}))
                try:
                    if CHRONOS_NATIVE_MERGE:
                        # The engine's merge needs the branch forked from its
                        # parent ("no shared fork base" otherwise); the
                        # checkpoint is still taken as the fork point record.
                        self._retry("fork", lambda: self.ctx.create_branch(name, parent))
                    else:
                        self._retry("fork", lambda: self.ctx.create_branch_from_checkpoint(name, base_cp))
                except BranchingError as e:
                    if "already exists" in str(e):
                        raise ValueError(f"Cannot create branch {name}, already exists") from e
                    raise
            self._copy_log(name, parent)
            cid, seq = self._add_row(parent, "sync", f"fork point of {name}", snap=base_cp, seq=seq)
            self._add_row(name, "fork", f"fork from {from_ref}", prev_snap=base_cp, prev_branch=parent,
                          commit_id=cid, seq=seq + 1)
        self._pgx(f"INSERT INTO {VIEWS_TABLE} (branch, name, sql) SELECT %s, name, sql FROM {VIEWS_TABLE} "
                  "WHERE branch = %s ON CONFLICT DO NOTHING", (name, parent))

    def _commit_impl(self, ref: Ref, message: str) -> str:
        self._adopt()
        if ref.commit:
            raise ValueError("cannot commit on a commit ref")
        seq = time.time_ns()
        cid = f"{seq:x}"
        cp = f"bb_c{cid}"
        with _GATES.hold([ref.branch], self._check_cancel):
            self._retry("checkpoint", lambda: self.ctx.create_checkpoint(cp, ref.branch, {"message": message or "", "id": cid}))
        self._add_row(ref.branch, "commit", message, snap=cp, commit_id=cid, seq=seq)
        # Chronos keeps no schema history, so the commit records the
        # branch's schema for schema_diff at this commit.
        self._pgx(f"UPDATE {LOG_TABLE} SET schema_json = %s WHERE log_branch = %s AND id = %s",
                  (json.dumps(self._schema_of_branch(ref.branch)), ref.branch, cid))
        return cid

    def _log_impl(self, ref: Ref, limit: int) -> list:
        self._adopt()
        max_seq = int(self._find_commit(ref.branch, ref.commit)["seq"]) if ref.commit else None
        out = []
        for r in self._log_rows(ref.branch, max_seq=max_seq):
            if r["kind"] == "sync":
                continue
            out.append({"commit_hash": r["id"], "kind": r["kind"], "committer": "chronos", "email": "",
                        "date": r["ts"], "message": r["message"], "snapshot": r["snap"]})
            if limit and len(out) >= limit:
                break
        return out

    def _diff_impl(self, ref_a: Ref, ref_b: Ref) -> dict:
        self._adopt()
        with self._reading(ref_a) as a, self._reading(ref_b) as b:
            if a == b:
                return {"tables": [], "rows_added": 0, "rows_deleted": 0, "rows_modified": 0}
            per = {}
            for d in self.ctx.diff(a, b).changes:
                e = per.setdefault(d.table, {"table_name": d.table, "rows_added": 0, "rows_deleted": 0,
                                             "rows_modified": 0})
                e["rows_added" if d.change == "added" else "rows_deleted" if d.change == "deleted"
                  else "rows_modified"] += 1
            ta, tb = self._branch_tables(a), self._branch_tables(b)
            for t in set(ta) ^ set(tb):
                n = len(self._all_rows(b if t in tb else a, t))
                per[t] = {"table_name": t, "rows_added": n if t in tb else 0,
                          "rows_deleted": n if t in ta else 0, "rows_modified": 0}
        tables = [per[t] for t in sorted(per)]
        return {"tables": tables, "rows_added": sum(x["rows_added"] for x in tables),
                "rows_deleted": sum(x["rows_deleted"] for x in tables),
                "rows_modified": sum(x["rows_modified"] for x in tables)}

    def _merge_impl(self, into: Ref, source: Ref, message: str, on_conflict="ours") -> dict:
        self._adopt()
        if into.commit:
            raise ValueError("cannot merge into a commit ref")
        if into.branch == source.branch:
            raise ValueError(f"Cannot merge branch '{source.branch}' into itself")
        if CHRONOS_NATIVE_MERGE:
            return self._native_merge(into, source, message)
        sess = self._on(into)
        src_max = int(self._find_commit(source.branch, source.commit)["seq"]) if source.commit else None
        base_cp = self._merge_base(into.branch, source.branch, b_max_seq=src_max)
        keep_ours = callable(on_conflict) or on_conflict == "ours"
        with self._reading(source) as theirs:
            info = self._three_way(sess, into.branch, theirs, base_cp, on_conflict, keep_ours, into, swap=False)
        self._copy_log(into.branch, source.branch, max_seq=src_max)
        if source.commit:
            commit_id = source.commit
        else:
            seq = time.time_ns()
            cp = f"bb_s{seq:x}"
            with _GATES.hold([source.branch], self._check_cancel):
                self._retry("checkpoint", lambda: self.ctx.create_checkpoint(cp, source.branch, {"kind": "sync", "into": into.branch}))
            commit_id, seq = self._add_row(source.branch, "sync", f"merged into {into.branch}", snap=cp, seq=seq)
            self._add_row(into.branch, "sync", f"merge point of {source.branch}", snap=cp, commit_id=commit_id,
                          seq=seq + 1)
        info["hash"], _ = self._add_row(into.branch, "merge", message or f"Merge {source} into {into.branch}",
                                        prev_branch=source.branch)
        info["fast_forward"] = info.get("ours_changes", 0) == 0 or info.get("theirs_changes", 0) == 0
        return info

    def _native_merge(self, into: Ref, source: Ref, message: str) -> dict:
        """Native-only mode: the engine's own merge_apply (abort-on-conflict;
        the scenario's resolver is not consulted). Chronos refuses it with
        chronos_native_merge_unsupported for every table under schema
        branching, which is reported as unsupported."""
        if source.commit:
            raise self._unsupported("merge", "the native merge takes a branch head, not a commit")
        self._drop_sessions(into.branch)
        self._drop_sessions(source.branch)
        try:
            with _GATES.hold([into.branch, source.branch], self._check_cancel):
                res = self._retry("merge", lambda: self.ctx.merge_apply(source.branch, into.branch))
        except Exception as e:
            msg = str(e)
            if "chronos_native_merge_unsupported" in msg:
                raise self._unsupported("merge", msg.split("chronos_native_merge_unsupported:", 1)[-1].strip()
                                        or msg) from e
            raise
        self._copy_log(into.branch, source.branch)
        info = {"conflicts": 0, "fast_forward": False, "native_result": str(res)[:200]}
        info["hash"], _ = self._add_row(into.branch, "merge", message or f"Merge {source} into {into.branch}",
                                        prev_branch=source.branch)
        return info

    def _rebase_impl(self, ref: Ref, onto: Ref, on_conflict="ours") -> dict:
        self._adopt()
        if ref.commit:
            raise ValueError("cannot rebase a commit ref")
        if ref.branch == onto.branch:
            raise ValueError(f"Cannot rebase branch '{ref.branch}' onto itself")
        sess = self._on(ref)
        onto_max = int(self._find_commit(onto.branch, onto.commit)["seq"]) if onto.commit else None
        base_cp = self._merge_base(ref.branch, onto.branch, b_max_seq=onto_max)
        # git: "ours" is the upstream, "theirs" the branch's own commits.
        keep_branch = on_conflict == "theirs" or callable(on_conflict)
        with self._reading(onto) as upstream:
            info = self._three_way(sess, ref.branch, upstream, base_cp, on_conflict, keep_branch, ref, swap=True)
        info["up_to_date"] = info.get("theirs_changes", 0) == 0 and not info["schema_changes"]["added_tables"] \
            and not info["schema_changes"]["added_columns"]
        self._copy_log(ref.branch, onto.branch, max_seq=onto_max)
        if not onto.commit:
            seq = time.time_ns()
            cp = f"bb_s{seq:x}"
            with _GATES.hold([onto.branch], self._check_cancel):
                self._retry("checkpoint", lambda: self.ctx.create_checkpoint(cp, onto.branch, {"kind": "sync", "rebased": ref.branch}))
            cid, seq = self._add_row(onto.branch, "sync", f"rebase point of {ref.branch}", snap=cp, seq=seq)
            self._add_row(ref.branch, "sync", f"rebased onto {onto.branch}", snap=cp, commit_id=cid, seq=seq + 1)
        self._add_row(ref.branch, "rebase", f"rebase onto {onto}", prev_branch=onto.branch)
        return info

    def _revert_impl(self, ref: Ref, commit: str) -> None:
        self._adopt()
        sess = self._on(ref)
        row = self._find_commit(ref.branch, commit)
        after_cp = self._state(row)
        before_cp = self._prev_state(ref.branch, int(row["seq"])) if after_cp else None
        if not after_cp or not before_cp:
            raise ValueError(f"commit {commit} has no checkpoint pair to revert ({row['kind']} row)")
        by_table = {}
        with self._temp_branch(before_cp) as b, self._temp_branch(after_cp) as a:
            for (table, _), d in self._diff_map(b, a).items():
                ops = by_table.setdefault(table, {"upsert": [], "delete": []})
                if d.change == "added":
                    ops["delete"].append(dict(d.key))
                else:
                    ops["upsert"].append(dict(d.before))
        changed = self._apply(sess, ref.branch, by_table)
        if changed:
            self._commit_impl(ref, f"revert {commit}")

    def _reset_impl(self, ref: Ref, to: str) -> None:
        self._adopt()
        sess = self._on(ref)
        row = self._find_commit(ref.branch, to)
        cp = self._state(row)
        if not cp:
            raise ValueError(f"commit {to} has no checkpoint to reset to ({row['kind']} row)")
        by_table = {}
        with self._temp_branch(cp) as target:
            # The schema the commit recorded is what the reset returns to
            # (a retyped column is dropped and re-added, then refilled);
            # without a fingerprint, reconcile against the temporary
            # branch, which fails on a type difference.
            if row.get("schema_json"):
                self._reconcile_to_fingerprint(sess, ref.branch, json.loads(row["schema_json"]))
            else:
                self._reconcile_schema(sess, ref.branch, target, drop_extra=True)
            cols_now = {t: set(m.columns) for t, m in self._branch_tables(ref.branch).items()}
            for (table, _), d in self._diff_map(ref.branch, target).items():
                if table not in cols_now:
                    continue
                ops = by_table.setdefault(table, {"upsert": [], "delete": []})
                if d.change == "deleted":
                    ops["delete"].append(dict(d.key))
                else:
                    ops["upsert"].append({c: v for c, v in dict(d.after).items() if c in cols_now[table]})
            self._apply(sess, ref.branch, by_table)
        self._pgx(f"DELETE FROM {LOG_TABLE} WHERE log_branch = %s AND seq > %s", (ref.branch, int(row["seq"])))

    def _delete_impl(self, ref: Ref) -> None:
        self._adopt()
        b = ref.branch
        if b == MAIN_BRANCH or b == self.default_branch:
            raise ValueError("the default branch cannot be deleted")
        if self._tx is not None and self._current_ref is not None and self._current_ref.branch == b:
            self._tx_end(False)
        self._drop_sessions(b)
        children = self._pgx("SELECT child_count FROM _chronos_branch_interval_branches WHERE branch_id = %s", (b,))
        if not children:
            raise BranchNotFoundError(b)
        self._pgx(f"DELETE FROM {LOG_TABLE} WHERE log_branch = %s", (b,))
        self._pgx(f"DELETE FROM {VIEWS_TABLE} WHERE branch = %s", (b,))
        if int(children[0][0]) > 0:
            self._pgx(f"INSERT INTO {TOMBSTONES} (branch) VALUES (%s) ON CONFLICT DO NOTHING", (b,))
            self.observe("deferred_deletes")
            return
        self._delete_cascade(b)

    def _delete_cascade(self, b: str) -> None:
        """Delete ``b``, then every tombstoned ancestor left without children."""
        while b:
            parent = self._pgx("SELECT parent_branch_id FROM _chronos_branch_interval_branches WHERE branch_id = %s",
                               (b,))
            parent = parent[0][0] if parent else None
            with _GATES.hold([b, parent], self._check_cancel):
                self._retry("delete", lambda: self.ctx.delete_branch(b))
            self._pgx(f"DELETE FROM {TOMBSTONES} WHERE branch = %s", (b,))
            if not parent:
                return
            row = self._pgx(f"SELECT t.branch FROM {TOMBSTONES} t JOIN _chronos_branch_interval_branches c "
                            "ON c.branch_id = t.branch WHERE t.branch = %s AND c.child_count = 0", (parent,))
            if not row:
                return
            self.observe("deferred_deletes_done")
            b = parent

    def _gc_impl(self) -> None:
        """Chronos's version reclamation (synchronously, so a dead background
        worker does not leave garbage behind) then VACUUM."""
        if self._ctx is not None:
            self._unstick_gc()
            try:
                self._ctx.wait_for_background_work()
            except BranchingError as e:
                print(f"Warning: {e}")
            store = getattr(self._ctx._backend, "_native_branch_store", None)
            if store is not None:
                reclaimed = store.collect_interval_garbage()
                self.observe("gc_segments_reclaimed", int(reclaimed or 0))
        self._pgx("VACUUM")

    def _storage_bytes(self) -> int:
        return int(self._pgx("SELECT pg_database_size(current_database())")[0][0])

    # ------------------------------------------------------------------
    # Multi-ref exec: temporary views at each ref's branch point
    # ------------------------------------------------------------------

    def _segment_for(self, ref: Ref):
        be = self.ctx._backend
        if ref.commit:
            return be._segment(int(self.ctx.get_checkpoint(self._snapshot_of(ref.branch, ref.commit)).ref))
        return be._current_segment(ref.branch)

    def _qualified_table(self, ref: Ref, table: str) -> str:
        self._adopt()
        seg = self._segment_for(ref)
        meta = self.ctx._backend._meta_for_segment(seg, table)
        name = f"bb_mv_{len(self._mviews)}"
        p = int(seg.branch_point)
        cols = ", ".join(_q(c) for c in meta.columns)
        self._pgx(f"CREATE OR REPLACE TEMP VIEW {name} AS SELECT {cols} FROM {_q(meta.physical_name)} "
                  f"WHERE live_lo <= {p} AND {p} < live_hi AND deleted = FALSE")
        self._mviews[name] = (str(ref), table)
        return name

    # ------------------------------------------------------------------
    # Run-level helpers (harness)
    # ------------------------------------------------------------------

    @classmethod
    def get_default_connection_uri(cls) -> str:
        return _uri("postgres")

    @classmethod
    def get_initial_connection_uri(cls, db_name: str) -> str:
        return _uri(db_name)

    class ChronosInfo:
        """Run-level state: the run's database (created fresh)."""

        def __init__(self, db_name: str):
            self.db_name = db_name
            self.branches = set()
            self.branches_lock = threading.Lock()
            drop_database(db_name)
            conn = psycopg.connect(_uri("postgres"), autocommit=True)
            try:
                conn.execute(f"CREATE DATABASE {_q(db_name)}")
            finally:
                conn.close()

    @classmethod
    def cleanup(cls, info) -> None:
        if info is not None:
            drop_database(info.db_name)
        with _ADOPT_LOCK:
            _ADOPTED.discard(getattr(info, "db_name", None))


def params_or_none(vars):
    return None if vars is None else vars


def _norm(v):
    """Comparable form of a value coming from a diff (strings) or a query."""
    if isinstance(v, (decimal.Decimal, float)):
        return str(decimal.Decimal(str(v)).normalize())
    if isinstance(v, str):
        try:
            return str(decimal.Decimal(v).normalize())
        except Exception:
            return v
    return v


def _changed_cols(before, after, cols=None) -> set:
    """Columns of ``after`` that differ from ``before`` (within ``cols``)."""
    if before is None or after is None:
        return set(after or before or {})
    return {c for c in after if c in before and (cols is None or c in cols)
            and _norm(before[c]) != _norm(after[c])}


def _same(a, b) -> bool:
    """The two sides reached the same state (on the columns both have)."""
    if a is None or b is None:
        return a is b
    common = set(a) & set(b)
    return all(_norm(a[c]) == _norm(b[c]) for c in common)


def _dtype(d) -> str:
    return {"added": "added", "deleted": "removed"}.get(d.change, "modified")


def _type_of(column_def: str) -> str:
    m = re.match(r'\s*(?:"[^"]+"|\w+)\s+(.*?)(\s+DEFAULT\b.*)?$', column_def, re.I | re.S)
    return re.sub(r"\s+", " ", m.group(1)).strip().upper() if m else column_def.upper()


def drop_database(db_name: str) -> int:
    conn = psycopg.connect(_uri("postgres"), autocommit=True)
    try:
        exists = conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,)).fetchone()
        if exists:
            conn.execute(f"DROP DATABASE {_q(db_name)} WITH (FORCE)")
            return 1
        return 0
    finally:
        conn.close()


def setup_database(db_name: str) -> None:
    """Create an empty run database (used by the tests)."""
    ChronosToolSuite.ChronosInfo(db_name)
    with _ADOPT_LOCK:
        _ADOPTED.discard(db_name)


def server_up() -> bool:
    if not CHRONOS_AVAILABLE:
        return False
    try:
        psycopg.connect(_uri("postgres"), connect_timeout=2).close()
        return True
    except Exception:
        return False
