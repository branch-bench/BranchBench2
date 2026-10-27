"""PostgreSQL backend where a branch is a database and a commit is a clone.

Plain PostgreSQL has one branching primitive, ``CREATE DATABASE ...
TEMPLATE``. With ``STRATEGY = FILE_COPY`` and ``file_copy_method = clone``
(PostgreSQL 18) the data files are reflinked, so on a copy-on-write file
system (XFS with reflink, Btrfs, APFS) a clone shares its blocks with the
template until either side writes. Everything else the API wants is built
on that primitive, on postgres_fdw (one database reading another's tables)
and on plain SQL:

    branch     CREATE DATABASE <child> TEMPLATE <parent> STRATEGY = FILE_COPY
               (native); a fork from a branch head also clones the child
               once more as the merge base, since the parent moves on
    commit     a row in the branch's _bb_commits table, then a clone of the
               branch database that stands in for the commit (readable
               through branch@commit)
    log        SELECT FROM _bb_commits
    diff       per-table, per-key-bucket row hashes on each side, only the
               differing buckets pulled over postgres_fdw and compared
    merge      SQL three-way merge on the target database: base = the fork
               (or last rebase) snapshot of whichever side descends from the
               other, theirs = the source database over postgres_fdw
    rebase     a fresh clone of the upstream becomes the branch's new base;
               the upstream's changes since the old base are merged into
               the branch three-way (lineage is not rewritten)
    revert     inverse of (commit's predecessor snapshot -> commit snapshot)
               applied to the branch, recorded as a new commit
    reset      DROP DATABASE <branch>; CREATE DATABASE <branch> TEMPLATE
               <commit snapshot>
    delete     DROP DATABASE of the branch, its bases and its snapshots
               (a snapshot another branch's log still references is kept)
    multi-ref  postgres_fdw foreign tables: ``db.table(ref, t)`` imports the
    exec       other branch's table into the session's database

A template must have no other connection while it is copied. The backend
therefore keeps every connection it opens in a process-wide registry: a
clone closes the registered connections to its template that are idle
(a parked connection reopens on its next use; the CONNECT is recorded),
waits for the busy ones (a verb or exec in flight) to finish, and holds
new connections to the template back until the copy is done. The wait is
part of the clone's latency, as it would be in production.

Environment: ``PGSQL_USER``, ``PGSQL_PASSWORD``, ``PGSQL_HOST``,
``PGSQL_PORT``, ``PGSQL_DATA_DIR`` (the mount
point of a volume dedicated to the server: storage is then the volume's
usage minus WAL, which is right under copy-on-write; without it, storage
is the sum of pg_database_size over the run's databases, which counts
shared blocks once per clone), ``FILE_COPY_STRATEGY`` (FILE_COPY or
WAL_LOG) and ``FILE_COPY_METHOD`` (clone or copy; the server setting the
run switches to).
"""

import contextlib
import hashlib
import os
import threading
import time

import psycopg2
import psycopg2.errors
import psycopg2.extensions
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from dblib.db_api import DBToolSuite, OperationCancelled, Ref
from dblib.pg_merge import META_PREFIX, PgMergeMixin, _q, empty_diff
import dblib.result_collector as rc
from dblib import util as dbutil

PGSQL_USER = os.environ.get("PGSQL_USER", "elaineang")
PGSQL_PASSWORD = os.environ.get("PGSQL_PASSWORD", "")
# FILE_COPY_HOST/FILE_COPY_PORT when this Postgres is not the one Chronos uses.
PGSQL_HOST = (os.environ.get("FILE_COPY_HOST") or os.environ.get("PGSQL_HOST")
              or os.environ.get("DBFORK_SERVER_HOST", "localhost"))
PGSQL_PORT = int(os.environ.get("FILE_COPY_PORT") or os.environ.get("PGSQL_PORT", "5433"))
PGSQL_DATA_DIR = os.environ.get("PGSQL_DATA_DIR", "")
# CREATE DATABASE strategy and the server's file_copy_method for it.
FILE_COPY_STRATEGY = os.environ.get("FILE_COPY_STRATEGY", "FILE_COPY").upper()
FILE_COPY_METHOD = os.environ.get("FILE_COPY_METHOD", "clone")
# How long a clone waits for busy connections to its template to finish.
PARK_TIMEOUT_SEC = float(os.environ.get("FILE_COPY_PARK_TIMEOUT_SEC", "900"))
# Database names are limited to 63 bytes: a branch database is kept short
# enough for its snapshot suffixes ("__c<16 hex>", "__b<16 hex>").
_MAX_BRANCH_DB = 40

COMMITS_TABLE = META_PREFIX + "commits"
_COMMITS_DDL = f"""
CREATE TABLE IF NOT EXISTS "{COMMITS_TABLE}" (
    seq BIGINT PRIMARY KEY,
    id TEXT NOT NULL,
    kind TEXT NOT NULL,
    message TEXT,
    snap TEXT,
    branch TEXT,
    prev_snap TEXT,
    prev_branch TEXT,
    ts TIMESTAMPTZ DEFAULT now()
)
"""
_LOG_COLS = ("seq", "id", "kind", "message", "snap", "branch", "prev_snap", "prev_branch", "ts")


def _uri(db: str) -> str:
    return dbutil.format_db_uri(PGSQL_USER, PGSQL_PASSWORD, PGSQL_HOST, PGSQL_PORT, db)


def _short(name: str, n: int = 8) -> str:
    return hashlib.sha1(name.encode()).hexdigest()[:n]


# ----------------------------------------------------------------------------
# Process-wide connection registry: who is connected to which database
# ----------------------------------------------------------------------------


class _Registry:
    def __init__(self):
        self.cond = threading.Condition(threading.Lock())
        self.handles: dict = {}     # database -> set of _Handle
        self.gates: dict = {}       # database -> clones in progress (connects wait)
        self.snapshots: dict = {}   # snapshot db -> branch dbs whose log references it
        self.orphans: set = set()   # snapshots whose branch is gone but are referenced
        self.branches: dict = {}    # run db_name -> set of branch names


_REG = _Registry()


class _Cursor:
    """psycopg2 cursor proxy that releases the handle's busy count."""

    def __init__(self, cur, handle):
        self._cur = cur
        self._handle = handle
        self._open = True

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()

    def __iter__(self):
        return iter(self._cur)

    def execute(self, query, vars=None):
        return self._cur.execute(query, vars)

    def executemany(self, query, seq):
        return self._cur.executemany(query, seq)

    def fetchall(self):
        return self._cur.fetchall()

    def fetchone(self):
        return self._cur.fetchone()

    def fetchmany(self, n=None):
        return self._cur.fetchmany(n) if n is not None else self._cur.fetchmany()

    @property
    def description(self):
        return self._cur.description

    @property
    def rowcount(self):
        return self._cur.rowcount

    def close(self):
        if not self._open:
            return
        self._open = False
        try:
            self._cur.close()
        finally:
            self._handle._unclaim()


class _Handle:
    """A lazily (re)connected autocommit connection to one database, kept
    in the process-wide registry so a clone of that database can park it.

    ``busy`` counts open cursors and verb/exec holds; a handle is parked
    (its connection closed) only at zero. A connect waits while a clone of
    the database is in progress."""

    def __init__(self, db: str):
        self.db = db
        self.raw = None
        self.busy = 0
        self.parked = False
        self.connecting = False   # claimed while unconnected: about to connect
        self.reconnects = 0
        self.fdw_used = False
        self.closed = False
        with _REG.cond:
            _REG.handles.setdefault(db, set()).add(self)

    # -- registry side --------------------------------------------------

    def _wait_gate_locked(self, timeout: float = PARK_TIMEOUT_SEC) -> None:
        deadline = time.monotonic() + timeout
        while _REG.gates.get(self.db, 0) > 0 and time.monotonic() < deadline:
            _REG.cond.wait(0.05)

    def _claim(self) -> None:
        with _REG.cond:
            if self.busy == 0:
                # Starting an operation: a clone waiting for this database
                # goes first (a client running back-to-back operations would
                # otherwise never be caught idle), and reconnecting waits
                # for it to finish.
                if _REG.gates.get(self.db, 0) > 0:
                    self._park_locked()
                    self._wait_gate_locked()
            elif self.raw is None or self.raw.closed:
                self._wait_gate_locked()
            self.busy += 1
            if self.raw is None or self.raw.closed:
                # Between here and the connect a clone cannot see a session
                # yet; it waits for this flag instead.
                self.connecting = True

    def _unclaim(self) -> None:
        with _REG.cond:
            self.busy = max(0, self.busy - 1)
            if self.busy == 0:
                self.connecting = False
                _REG.cond.notify_all()

    def _tx_idle(self) -> bool:
        raw = self.raw
        if raw is None or raw.closed:
            return True
        try:
            return raw.get_transaction_status() == psycopg2.extensions.TRANSACTION_STATUS_IDLE
        except Exception:
            return True

    def _park_locked(self) -> None:
        """Close the connection (registry lock held, caller checked idle)."""
        raw, self.raw = self.raw, None
        if raw is not None:
            self.parked = True
            try:
                raw.close()
            except Exception:
                pass

    # -- owner side -----------------------------------------------------

    def _ensure(self) -> None:
        try:
            if self.raw is None or self.raw.closed:
                raw = psycopg2.connect(_uri(self.db))
                raw.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                self.raw = raw
                if self.parked:
                    self.parked = False
                    self.reconnects += 1
                    FileCopyToolSuite.observe("reconnects_after_park")
        finally:
            self.connecting = False

    def hold(self) -> None:
        self._claim()

    def release(self) -> None:
        self._unclaim()

    def cursor(self):
        self._claim()
        try:
            self._ensure()
            return _Cursor(self.raw.cursor(), self)
        except BaseException:
            self._unclaim()
            raise

    def connect(self) -> None:
        """Open the connection now (CONNECT timing), waiting out a clone."""
        self._claim()
        try:
            self._ensure()
        finally:
            self._unclaim()

    def switch(self, db: str) -> None:
        """Point the handle at another database (connection reopened)."""
        with _REG.cond:
            was_parked = self.parked   # a reconnect after a park counts as one
            self._park_locked()
            self.parked = was_parked
            _REG.handles.get(self.db, set()).discard(self)
            self.db = db
            self._wait_gate_locked()
            _REG.handles.setdefault(db, set()).add(self)
        self.connect()

    def park(self) -> None:
        """Close the connection from the owner's side (no cursor open)."""
        with _REG.cond:
            self._park_locked()
            self.connecting = False

    def cancel(self) -> None:
        raw = self.raw
        if raw is not None and not raw.closed:
            try:
                raw.cancel()
            except Exception:
                pass

    def close(self) -> None:
        with _REG.cond:
            self._park_locked()
            self.parked = False
            self.closed = True
            _REG.handles.get(self.db, set()).discard(self)


# ----------------------------------------------------------------------------


class FileCopyToolSuite(PgMergeMixin, DBToolSuite):
    BACKEND_NAME = "file_copy"
    STORAGE_SCOPE = "server" if PGSQL_DATA_DIR else "database"
    SUPPORTS_COMMIT_REFS = True
    SCHEMA_DIALECT = "postgres"
    SUPPORTS_MULTI_REF_EXEC = True
    IMPLEMENTATION = {
        "branch": "simulated",
        "commit": "simulated",
        "diff": "simulated",
        "log": "simulated",
        "merge": "simulated",
        "rebase": "simulated",
        "revert": "simulated",
        "reset": "simulated",
        "delete": "simulated",
        "commit_refs": "simulated",
        "multi_ref_exec": "simulated",
    }
    IMPLEMENTATION_NOTES = {
        "branch": f"CREATE DATABASE TEMPLATE <parent> STRATEGY = {FILE_COPY_STRATEGY} "
                  f"(file_copy_method = {FILE_COPY_METHOD}); a fork from a branch head also clones "
                  "the child as the merge base (a fork from a commit uses the commit's snapshot). "
                  "The template may have no other connection: idle connections to it are closed "
                  "(reopened on their next use), busy ones are waited for, inside the verb",
        "commit": "_bb_commits row, then a clone of the branch database as the commit's snapshot "
                  "(two checkpoints and one reflink per data file; the branch's connections are "
                  "parked meanwhile). Before any clone, tables of the template that were never "
                  "analyzed are ANALYZEd, since the copy carries pg_statistic and kills the "
                  "template's autovacuum worker",
        "log": "SELECT from the _bb_commits bookkeeping table",
        "diff": "per-table row hashes per key bucket on each database, differing buckets pulled "
                "over postgres_fdw and compared on the target",
        "merge": "SQL three-way merge on the target database; base = the fork or rebase snapshot, "
                 "theirs read over postgres_fdw",
        "rebase": "a fresh clone of the upstream becomes the branch's base and is merged in "
                  "three-way; commits are not replayed",
        "revert": "inverse of (predecessor snapshot -> commit snapshot) applied through "
                  "postgres_fdw, recorded as a commit (with its own snapshot)",
        "reset": "DROP DATABASE of the branch, then CREATE DATABASE TEMPLATE <commit snapshot>",
        "delete": "DROP DATABASE of the branch, its base clones and its snapshots (snapshots other "
                  "branches' logs reference are kept until the run's cleanup)",
        "commit_refs": "a commit ref connects to the commit's snapshot database (read-only)",
        "multi_ref_exec": "postgres_fdw: db.table(ref, t) imports the other database's table into "
                          "the session's database (re-imported per exec; the foreign server "
                          "connection is dropped when the exec ends)",
    }

    # Facts observed during a run (shared by every suite of the process).
    OBSERVED: dict = {}
    _OBS_LOCK = threading.Lock()
    _SNAP_CACHE: dict = {}   # (branch db, commit id) -> snapshot db (immutable)

    @classmethod
    def observe(cls, key: str, amount=1) -> None:
        with cls._OBS_LOCK:
            cls.OBSERVED[key] = cls.OBSERVED.get(key, 0) + amount

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    def get_default_connection_uri(cls) -> str:
        return _uri("postgres")

    @classmethod
    def get_branch_uri(cls, branch_name) -> str:
        return _uri(branch_name)

    @classmethod
    def get_initial_connection_uri(cls, db_name: str) -> str:
        return _uri(db_name)

    @classmethod
    def init_for_bench(cls, collector: rc.ResultCollector, db_name: str, default_branch_name: str,
                       shared_branches: set = None, shared_branches_lock=None, create_db_lock=None,
                       measure_storage: bool = False):
        return cls(None, collector, db_name, default_branch_name or db_name, measure_storage)

    def __init__(self, connection, collector: rc.ResultCollector, db_name: str,
                 default_branch_name: str, measure_storage: bool = False):
        super().__init__(None, collector, measure_storage)
        self.db_name = db_name
        self.default_branch = default_branch_name or db_name
        self._admin_conn = None
        self._fdw_ready: set = set()
        self._fdw_imported: dict = {}
        with _REG.cond:
            _REG.branches.setdefault(db_name, set()).add(self.default_branch)
        self.conn = _Handle(self._db_for(self.default_branch))
        self._ensure_meta(self.conn.db)
        self.conn.connect()
        self._current_ref = Ref(self.default_branch)

    # ------------------------------------------------------------------
    # Names
    # ------------------------------------------------------------------

    def _db_for(self, branch: str) -> str:
        if branch == self.default_branch:
            return self.db_name
        name = f"{self.db_name}__{branch}"
        if len(name) > _MAX_BRANCH_DB:
            name = f"{name[:_MAX_BRANCH_DB - 9]}_{_short(branch)}"
        return name

    def _snapshot_name(self, branch_db: str, kind: str, seq: int) -> str:
        return f"{branch_db}__{kind}{seq:x}"

    def list_branches(self) -> list:
        with _REG.cond:
            return sorted(_REG.branches.get(self.db_name, {self.default_branch}))

    # ------------------------------------------------------------------
    # Connections
    # ------------------------------------------------------------------

    def _admin(self, sql: str, args=None):
        """Run a statement on a connection to the "postgres" database
        (CREATE/DROP DATABASE, server-wide queries)."""
        if self._admin_conn is None or self._admin_conn.closed:
            self._admin_conn = psycopg2.connect(_uri("postgres"))
            self._admin_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with self._admin_conn.cursor() as cur:
            cur.execute(sql, args)
            if cur.description is not None:
                return cur.fetchall()
            return None

    def _exec(self, conn, sql: str, args=None):
        self._check_cancel()
        with conn.cursor() as cur:
            cur.execute(sql, args)
            if cur.description is not None:
                return cur.fetchall()
            return None

    def _handle(self, db: str, fresh: bool = False) -> _Handle:
        """A held connection to ``db`` for the duration of a verb (released
        with _release_handle); self.conn when it is already there unless
        ``fresh``. The verbs' own sessions may write (temp tables, foreign
        tables) even on a read-only snapshot database."""
        if not fresh and self.conn is not None and self.conn.db == db and not self.conn.closed:
            return self.conn
        h = _Handle(db)
        h.hold()
        try:
            h._ensure()
            with h.raw.cursor() as cur:
                cur.execute("SET default_transaction_read_only = off")
        except BaseException:
            h.release()
            h.close()
            raise
        return h

    def _release_handle(self, h) -> None:
        if h is None or h is self.conn:
            return
        try:
            self._fdw_disconnect(h)
        finally:
            h.release()
            h.close()

    def _connect_impl(self, ref: Ref) -> None:
        self.conn.switch(self._reading_db(ref))

    def _connect(self, ref: Ref, exec_id: int, timed: bool, label: str, fallback: bool):
        """A connection a clone parked is reopened by the next exec and
        recorded as a CONNECT like any other switch."""
        if self.conn is not None and (self.conn.parked or self.conn.raw is None):
            self._current_ref = None
        return super()._connect(ref, exec_id, timed, label, fallback)

    def _on(self, ref: Ref) -> str:
        """Point self.conn at the branch head; returns its database."""
        db = self._db_for(ref.branch)
        if self._current_ref != Ref(ref.branch) or self.conn.db != db:
            self.conn.switch(db)
            self._current_ref = Ref(ref.branch)
        return db

    def _begin_op(self, op_name: str, ref: str, label: str) -> int:
        token = super()._begin_op(op_name, ref, label)
        self.conn.hold()
        self._fdw_imported = {}
        return token

    def _end_op(self, token: int, error: str) -> str:
        try:
            if self.conn.fdw_used:
                self._fdw_disconnect(self.conn)
        finally:
            self.conn.release()
        return super()._end_op(token, error)

    def cancel_current_op(self) -> None:
        super().cancel_current_op()
        admin = self._admin_conn
        if admin is not None and not admin.closed:
            try:
                admin.cancel()
            except Exception:
                pass

    def close_connection(self) -> None:
        if self.conn is not None:
            self.conn.close()
        if self._admin_conn is not None:
            try:
                self._admin_conn.close()
            except Exception:
                pass
            self._admin_conn = None
        super().close_connection()

    # ------------------------------------------------------------------
    # Cloning: exclusive access to the template, then CREATE DATABASE
    # ------------------------------------------------------------------

    @contextlib.contextmanager
    def _exclusive(self, db: str):
        """Close the registry's idle connections to ``db``, wait for the
        busy ones to finish, and keep new ones out until the block ends.
        The caller must not hold an open connection to ``db`` itself."""
        with _REG.cond:
            _REG.gates[db] = _REG.gates.get(db, 0) + 1
        try:
            self._quiesce(db)
            yield
        finally:
            with _REG.cond:
                n = _REG.gates.get(db, 0) - 1
                if n > 0:
                    _REG.gates[db] = n
                else:
                    _REG.gates.pop(db, None)
                _REG.cond.notify_all()

    def _quiesce(self, db: str) -> None:
        """With the gate on ``db`` closed: park its idle registered
        connections and wait for the busy (or connecting) ones to finish.
        Bounded by PARK_TIMEOUT_SEC in total per exclusive section."""
        t0 = time.monotonic()
        parked = 0
        with _REG.cond:
            while True:
                waiting = 0
                for h in list(_REG.handles.get(db, ())):
                    if h.raw is None or h.raw.closed:
                        if h.connecting:
                            waiting += 1
                        continue
                    if h.busy == 0 and h._tx_idle():
                        h._park_locked()
                        parked += 1
                    else:
                        waiting += 1
                if not waiting:
                    break
                if time.monotonic() - t0 > PARK_TIMEOUT_SEC:
                    raise RuntimeError(f"{waiting} connection(s) to {db} stayed busy for "
                                       f"{PARK_TIMEOUT_SEC:.0f}s; cannot copy it")
                _REG.cond.release()
                try:
                    self._check_cancel()
                finally:
                    _REG.cond.acquire()
                _REG.cond.wait(0.05)
        waited = time.monotonic() - t0
        if parked:
            self.observe("parked_connections", parked)
        if waited > 0.01:
            self.observe("quiesce_wait_sec", waited)
            self.observe("quiesce_waits")

    def _analyze_unanalyzed(self, db: str) -> None:
        """ANALYZE the tables of ``db`` that have never been analyzed
        (pg_class.reltuples = -1), so that the clone carries planner
        statistics: CREATE DATABASE copies pg_statistic, and the copy
        itself terminates any autovacuum worker on the template, so a
        freshly loaded table could otherwise stay unanalyzed on the
        template and on every clone of it (a nested-loop plan over an
        unanalyzed 300k-row table ran for over ten minutes)."""
        h = self._handle(db, fresh=True)
        try:
            rows = self._exec(h, "SELECT relname FROM pg_class WHERE relkind = 'r' AND reltuples < 0 "
                                 "AND relnamespace = 'public'::regnamespace AND relname NOT LIKE %s",
                              (META_PREFIX.replace("_", r"\_") + "%",)) or []
            for (t,) in rows:
                self._exec(h, f"ANALYZE {_q(t)}")
            if rows:
                self.observe("analyze_before_clone_tables", len(rows))
                self.observe("analyze_before_clone")
        finally:
            self._release_handle(h)

    def _clone(self, template: str, target: str, exclusive: bool = True) -> None:
        sql = f"CREATE DATABASE {_q(target)} TEMPLATE {_q(template)} STRATEGY = {FILE_COPY_STRATEGY}"
        if not exclusive:
            self._admin(sql)
            return
        self._analyze_unanalyzed(template)
        with self._exclusive(template):
            t0 = time.monotonic()
            attempt = 0
            while True:
                self._check_cancel()
                try:
                    self._admin(sql)
                    return
                except psycopg2.errors.ObjectInUse as e:
                    # A session the registry did not know (or one that
                    # connected before the gate closed) is on the template:
                    # the server waited 5 s for it. Quiesce again, which
                    # waits for registered sessions to finish, then retry.
                    attempt += 1
                    self.observe("clone_retries")
                    if time.monotonic() - t0 > PARK_TIMEOUT_SEC:
                        raise RuntimeError(f"could not copy {template} within {PARK_TIMEOUT_SEC:.0f}s: {e}")
                    time.sleep(min(2.0, 0.2 * attempt))
                    self._quiesce(template)

    def _mark_snapshot(self, db: str) -> None:
        """A snapshot is read-only (the setting is not inherited by clones
        made from it)."""
        self._admin(f"ALTER DATABASE {_q(db)} SET default_transaction_read_only = on")

    def _drop_db(self, db: str, force_after: int = 3) -> None:
        for attempt in range(force_after + 1):
            try:
                with self._exclusive(db):
                    if attempt < force_after:
                        self._admin(f"DROP DATABASE IF EXISTS {_q(db)}")
                    else:
                        self._admin(f"DROP DATABASE IF EXISTS {_q(db)} WITH (FORCE)")
                return
            except psycopg2.errors.ObjectInUse:
                self.observe("drop_retries")
                time.sleep(0.2 * (attempt + 1))

    def _park_self(self) -> None:
        """Close this suite's own connection before it clones or drops the
        database it is on (it reopens lazily)."""
        self.conn.park()

    # ------------------------------------------------------------------
    # Bookkeeping (commit log)
    # ------------------------------------------------------------------

    def _ensure_meta(self, db: str) -> None:
        h = self._handle(db)
        try:
            self._exec(h, _COMMITS_DDL)
            n = self._exec(h, f'SELECT COUNT(*) FROM "{COMMITS_TABLE}"')[0][0]
            if not n:
                self._add_row(h, "root", "initial state", branch=self.default_branch)
        finally:
            self._release_handle(h)

    def _add_row(self, conn, kind: str, message: str, branch: str, snap: str = None,
                 prev_snap: str = None, prev_branch: str = None, commit_id: str = None,
                 seq: int = None) -> tuple:
        seq = seq or time.time_ns()
        commit_id = commit_id or f"{seq:x}"
        for _ in range(16):
            try:
                self._exec(conn, f'INSERT INTO "{COMMITS_TABLE}" (seq, id, kind, message, snap, branch, '
                                 "prev_snap, prev_branch) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                           (seq, commit_id, kind, message or "", snap, branch, prev_snap, prev_branch))
                return commit_id, seq
            except psycopg2.IntegrityError:
                seq += 1
        raise RuntimeError("could not record commit")

    def _log_rows(self, conn, limit: int = None, max_seq: int = None) -> list:
        sql = f'SELECT {", ".join(_LOG_COLS)} FROM "{COMMITS_TABLE}"'
        args = []
        if max_seq is not None:
            sql += " WHERE seq <= %s"
            args.append(int(max_seq))
        sql += " ORDER BY seq DESC"
        if limit:
            sql += f" LIMIT {int(limit)}"
        rows = self._exec(conn, sql, tuple(args) if args else None) or []
        return [dict(zip(_LOG_COLS, r)) for r in rows]

    def _find_commit(self, conn, commit_id: str) -> dict:
        # A merge row may share the id of the source commit it brought in;
        # the row with a snapshot is the one that names a state.
        rows = self._exec(conn, f'SELECT {", ".join(_LOG_COLS)} FROM "{COMMITS_TABLE}" '
                                "WHERE id = %s ORDER BY (snap IS NOT NULL) DESC, seq DESC LIMIT 1",
                          (str(commit_id),))
        if not rows:
            raise ValueError(f"unknown commit '{commit_id}'")
        return dict(zip(_LOG_COLS, rows[0]))

    def _base_row(self, conn) -> dict:
        rows = self._exec(conn, f'SELECT {", ".join(_LOG_COLS)} FROM "{COMMITS_TABLE}" '
                                "WHERE kind IN ('fork', 'rebase', 'root') ORDER BY seq DESC LIMIT 1")
        return dict(zip(_LOG_COLS, rows[0]))

    @staticmethod
    def _state_db(row: dict):
        """The database holding the branch's state right after ``row``:
        a commit's snapshot, or a fork's base clone."""
        if row["kind"] == "commit":
            return row["snap"]
        if row["kind"] == "fork":
            return row["prev_snap"]
        return None

    def _register_refs(self, conn, owner_db: str) -> None:
        """Remember which snapshots ``owner_db``'s log names, so a branch
        delete keeps the ones other branches still need."""
        rows = self._exec(conn, f'SELECT snap, prev_snap FROM "{COMMITS_TABLE}"') or []
        with _REG.cond:
            for snap, prev in rows:
                for s in (snap, prev):
                    # "<branch db>__c<hex>" / "__b<hex>": owned by that branch.
                    if s and s.rsplit("__", 1)[0] != owner_db:
                        _REG.snapshots.setdefault(s, set()).add(owner_db)

    def _snapshot_of(self, branch: str, commit: str) -> str:
        """Snapshot database of ``branch@commit``."""
        bdb = self._db_for(branch)
        key = (bdb, commit)
        snap = self._SNAP_CACHE.get(key)
        if snap:
            return snap
        h = self._handle(bdb)
        try:
            row = self._find_commit(h, commit)
        finally:
            self._release_handle(h)
        if not row["snap"]:
            raise ValueError(f"commit '{commit}' on {branch} has no snapshot ({row['kind']} row)")
        self._SNAP_CACHE[key] = row["snap"]
        return row["snap"]

    def _reading_db(self, ref: Ref) -> str:
        """Database holding ``ref``: the branch, or a commit's snapshot."""
        if ref.commit:
            return self._snapshot_of(ref.branch, ref.commit)
        return self._db_for(ref.branch)

    # ------------------------------------------------------------------
    # postgres_fdw: read another database's tables from this connection
    # ------------------------------------------------------------------

    def _fdw_names(self, remote_db: str) -> tuple:
        tag = f"{remote_db[:20]}_{_short(remote_db)}"
        return f"bb_srv_{tag}", f"bb_{tag}"

    def _fdw_server(self, conn, remote_db: str) -> str:
        if conn.db not in self._fdw_ready:
            self._exec(conn, "CREATE EXTENSION IF NOT EXISTS postgres_fdw")
            self._fdw_ready.add(conn.db)
        srv, _ = self._fdw_names(remote_db)
        self._exec(conn, f"CREATE SERVER IF NOT EXISTS {_q(srv)} FOREIGN DATA WRAPPER postgres_fdw "
                         f"OPTIONS (host %s, port %s, dbname %s, fetch_size '10000')",
                   (PGSQL_HOST, str(PGSQL_PORT), remote_db))
        self._exec(conn, f"CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER SERVER {_q(srv)} "
                         f"OPTIONS (user %s, password %s)", (PGSQL_USER, PGSQL_PASSWORD))
        conn.fdw_used = True
        return srv

    def _fdw_schema(self, conn, local_db: str, remote_db: str, tables: list) -> str:
        """Import ``tables`` of ``remote_db`` into a fresh schema of the
        database ``conn`` is on; returns the schema name."""
        srv = self._fdw_server(conn, remote_db)
        _, schema = self._fdw_names(remote_db)
        self._exec(conn, f"DROP SCHEMA IF EXISTS {_q(schema)} CASCADE")
        self._exec(conn, f"CREATE SCHEMA {_q(schema)}")
        if tables:
            names = ", ".join(_q(t) for t in tables)
            self._exec(conn, f"IMPORT FOREIGN SCHEMA public LIMIT TO ({names}) FROM SERVER {_q(srv)} INTO {_q(schema)}")
        return schema

    def _fdw_cleanup(self, conn, remote_db: str) -> None:
        srv, schema = self._fdw_names(remote_db)
        for sql in (f"DROP SCHEMA IF EXISTS {_q(schema)} CASCADE", f"DROP SERVER IF EXISTS {_q(srv)} CASCADE"):
            try:
                self._exec(conn, sql)
            except Exception:
                pass
        self._fdw_disconnect(conn)

    def _fdw_disconnect(self, conn) -> None:
        """Drop the session's cached connections to other databases (they
        count as connections to those databases, which a clone there
        would wait for)."""
        if not getattr(conn, "fdw_used", False):
            return
        try:
            if conn.raw is not None and not conn.raw.closed:
                with conn.raw.cursor() as cur:
                    cur.execute("SELECT postgres_fdw_disconnect_all()")
        except Exception:
            pass
        conn.fdw_used = False

    def _qualified_table(self, ref: Ref, table: str) -> str:
        """Multi-ref exec: the table on the session's own database, or a
        foreign table imported from the other ref's database."""
        remote = self._reading_db(ref)
        conn = self.conn
        if remote == conn.db:
            return f"public.{_q(table)}"
        key = (conn.db, remote, table)
        if key not in self._fdw_imported:
            ro = self._exec(conn, "SHOW default_transaction_read_only")[0][0] == "on"
            if ro:
                self._exec(conn, "SET default_transaction_read_only = off")
            try:
                srv = self._fdw_server(conn, remote)
                _, schema = self._fdw_names(remote)
                self._exec(conn, f"CREATE SCHEMA IF NOT EXISTS {_q(schema)}")
                self._exec(conn, f"DROP FOREIGN TABLE IF EXISTS {_q(schema)}.{_q(table)}")
                self._exec(conn, f"IMPORT FOREIGN SCHEMA public LIMIT TO ({_q(table)}) "
                                 f"FROM SERVER {_q(srv)} INTO {_q(schema)}")
            finally:
                if ro:
                    self._exec(conn, "SET default_transaction_read_only = on")
            self._fdw_imported[key] = schema
            self.observe("fdw_table_imports")
        return f"{_q(self._fdw_imported[key])}.{_q(table)}"

    # ------------------------------------------------------------------
    # Storage
    # ------------------------------------------------------------------

    def _storage_bytes(self) -> int:
        if PGSQL_DATA_DIR:
            used = dbutil.get_volume_usage_bytes(PGSQL_DATA_DIR, ssh=dbutil.ssh_target("FILE_COPY_SSH"))
            try:
                wal = int(self._admin("SELECT COALESCE(SUM(size), 0) FROM pg_ls_waldir()")[0][0])
            except Exception:
                wal = 0
            return max(0, used - wal)
        rows = self._admin("SELECT COALESCE(SUM(pg_database_size(datname)), 0) FROM pg_database "
                           "WHERE datname = %s OR datname LIKE %s",
                           (self.db_name, self.db_name.replace("_", r"\_") + r"\_\_%"))
        return int(rows[0][0]) if rows else 0

    def _gc_impl(self) -> None:
        self._admin("CHECKPOINT")

    # ------------------------------------------------------------------
    # Verbs
    # ------------------------------------------------------------------

    def _branch_impl(self, name: str, from_ref: Ref) -> None:
        with _REG.cond:
            exists = name in _REG.branches.get(self.db_name, set())
        if exists:
            raise ValueError(f"Cannot create branch {name}, already exists")
        src_db = self._reading_db(from_ref)
        child = self._db_for(name)
        if self.conn.db == src_db:
            self._park_self()
        try:
            self._clone(src_db, child)
        except psycopg2.errors.DuplicateDatabase as e:
            raise ValueError(f"Cannot create branch {name}, already exists: {e}")
        if from_ref.commit:
            # The snapshot is immutable: it is the fork state itself.
            base = src_db
        else:
            # The parent moves on; keep the fork state for later merges.
            base = self._snapshot_name(child, "b", time.time_ns())
            self._clone(child, base, exclusive=False)
            self._mark_snapshot(base)
        h = self._handle(child)
        try:
            self._add_row(h, "fork", f"fork from {from_ref}", branch=name, prev_snap=base,
                          prev_branch=from_ref.branch)
            self._register_refs(h, child)
        finally:
            self._release_handle(h)
        with _REG.cond:
            _REG.branches.setdefault(self.db_name, set()).add(name)

    def _commit_impl(self, ref: Ref, message: str) -> str:
        bdb = self._on(ref)
        head = self._log_rows(self.conn, limit=1)[0]
        seq = time.time_ns()
        snap = self._snapshot_name(bdb, "c", seq)
        commit_id, seq = self._add_row(self.conn, "commit", message, branch=ref.branch, snap=snap,
                                       prev_snap=self._state_db(head), prev_branch=head["branch"],
                                       commit_id=f"{seq:x}", seq=seq)
        self._park_self()
        try:
            self._clone(bdb, snap)
        except BaseException:
            try:
                self._exec(self.conn, f'DELETE FROM "{COMMITS_TABLE}" WHERE seq = %s', (seq,))
            except Exception:
                pass
            raise
        self._mark_snapshot(snap)
        self._SNAP_CACHE[(bdb, commit_id)] = snap
        return commit_id

    def _log_impl(self, ref: Ref, limit: int) -> list:
        h = self._handle(self._db_for(ref.branch))
        try:
            max_seq = int(self._find_commit(h, ref.commit)["seq"]) if ref.commit else None
            return [{"commit_hash": r["id"], "kind": r["kind"], "committer": "file_copy", "email": "",
                     "date": r["ts"], "message": r["message"], "snapshot": r["snap"]}
                    for r in self._log_rows(h, limit=limit, max_seq=max_seq)]
        finally:
            self._release_handle(h)

    # -- diff ----------------------------------------------------------------

    def _diff_impl(self, ref_a: Ref, ref_b: Ref) -> dict:
        a_db, b_db = self._reading_db(ref_a), self._reading_db(ref_b)
        if a_db == b_db:
            return empty_diff()
        # Fresh sessions: side b gets temp and foreign tables, which the
        # suite's own session could not create on a read-only snapshot.
        ca, cb = self._handle(a_db, fresh=True), self._handle(b_db, fresh=True)
        try:
            return self._diff_tables(ca, a_db, cb, b_db)
        finally:
            self._release_handle(ca)
            self._release_handle(cb)

    # -- merge / rebase ------------------------------------------------------

    def _merge_base(self, ours_branch: str, ours_conn, theirs_branch: str, theirs_conn):
        """Snapshot of the fork/rebase point of whichever side descends from
        the other (the most recent one), or None."""
        rt = self._base_row(theirs_conn)
        ro = self._base_row(ours_conn)
        cands = []
        if rt["kind"] in ("fork", "rebase") and rt["prev_branch"] == ours_branch and rt["prev_snap"]:
            cands.append((int(rt["seq"]), rt["prev_snap"]))
        if ro["kind"] in ("fork", "rebase") and ro["prev_branch"] == theirs_branch and ro["prev_snap"]:
            cands.append((int(ro["seq"]), ro["prev_snap"]))
        if not cands:
            return None
        return max(cands)[1]

    def _copy_log(self, into_conn, into_db: str, from_conn, max_seq=None) -> None:
        rows = self._log_rows(from_conn)
        have = {r["id"] for r in self._log_rows(into_conn)}
        for r in reversed(rows):
            if r["kind"] not in ("commit", "merge") or r["id"] in have:
                continue
            if max_seq is not None and int(r["seq"]) > max_seq:
                continue
            self._add_row(into_conn, r["kind"], r["message"], branch=r["branch"], snap=r["snap"],
                          prev_snap=r["prev_snap"], prev_branch=r["prev_branch"],
                          commit_id=r["id"], seq=int(r["seq"]))
        self._register_refs(into_conn, into_db)

    def _two_way(self, ours, ours_db, theirs, theirs_db, keep_ours: bool, skip: list) -> dict:
        """No common ancestor: everything theirs has is "their change"."""
        info = {"conflicts": 0, "conflict_tables": [], "ours_changes": 0, "theirs_changes": 0,
                "merged_tables": [], "skipped_tables": {}}
        for t in sorted(set(self._tables(ours)) & set(self._tables(theirs)) - set(skip)):
            cols_o = [c[0] for c in self._columns(ours, t)]
            cols = [c for c in cols_o if c in {c2[0] for c2 in self._columns(theirs, t)}]
            pk = [c for c in self._pk(ours, t) if c in cols]
            s_t = self._fdw_schema(ours, ours_db, theirs_db, [t])
            if pk:
                cl = ", ".join(_q(c) for c in cols)
                if keep_ours:
                    self._exec(ours, f"INSERT INTO {_q(t)} ({cl}) SELECT {cl} FROM {_q(s_t)}.{_q(t)} ON CONFLICT DO NOTHING")
                else:
                    self._exec(ours, self._upsert(_q(t), cols, pk, f"SELECT {cl} FROM {_q(s_t)}.{_q(t)}"))
            info["merged_tables"].append(t)
        return info

    def _merge_impl(self, into: Ref, source: Ref, message: str, on_conflict="ours") -> dict:
        theirs_db = self._reading_db(source)
        ours_db = self._on(into)
        if theirs_db == ours_db:
            raise ValueError(f"Cannot merge branch '{source.branch}' into itself")
        ours = self.conn
        theirs = self._handle(theirs_db)
        base_conn, base_db = None, None
        try:
            base_db = self._merge_base(into.branch, ours, source.branch, theirs)
            info = {"two_way": base_db is None}
            if base_db is not None:
                base_conn = self._handle(base_db)
            else:
                print(f"Warning: {source.branch} and {into.branch} share no fork point: merging two-way")
            schema = self._reconcile_schema(ours, ours_db, theirs, theirs_db)
            keep_ours = callable(on_conflict) or on_conflict == "ours"
            if base_db is None:
                tw_info, conflicts = self._two_way(ours, ours_db, theirs, theirs_db, keep_ours,
                                                   schema["added_tables"]), []
            else:
                tw_info, conflicts = self._three_way(ours, ours_db, theirs, theirs_db, base_conn, base_db,
                                                     on_conflict, into, schema["added_tables"], keep_ours)
            info.update(tw_info)
            if conflicts:
                info["resolved"] = "custom"
                info["resolution"] = on_conflict(self._conflict_session(into), conflicts)
            elif info.get("conflicts"):
                info["resolved"] = on_conflict
            src_rows = self._log_rows(theirs, limit=1)
            src_head = (self._find_commit(theirs, source.commit) if source.commit else src_rows[0])
            self._copy_log(ours, ours_db, theirs, max_seq=int(src_head["seq"]))
            commit_id = src_head["id"] if src_head["kind"] == "commit" else None
            head = self._log_rows(ours, limit=1)[0]
            info["hash"], _ = self._add_row(ours, "merge", message or f"Merge {source.branch} into {into.branch}",
                                            branch=into.branch, prev_snap=self._state_db(head),
                                            prev_branch=head["branch"], commit_id=commit_id)
            info["fast_forward"] = info.get("ours_changes", 0) == 0 or info.get("theirs_changes", 0) == 0
            info["schema_changes"] = schema
            return info
        finally:
            self._release_handle(theirs)
            self._release_handle(base_conn)
            for db in {theirs_db, base_db}:
                if db:
                    self._fdw_cleanup(ours, db)

    def _rebase_impl(self, ref: Ref, onto: Ref, on_conflict="ours") -> dict:
        ours_db = self._on(ref)
        ours = self.conn
        up_db = self._db_for(onto.branch)
        if up_db == ours_db:
            raise ValueError(f"Cannot rebase branch '{ref.branch}' onto itself")
        # Read the upstream at one point: a commit's snapshot, or a fresh
        # clone of the upstream head (a spine under load keeps moving).
        # That snapshot becomes the branch's new base.
        if onto.commit:
            theirs_db = self._snapshot_of(onto.branch, onto.commit)
        else:
            theirs_db = self._snapshot_name(ours_db, "b", time.time_ns())
            self._clone(up_db, theirs_db)
            self._mark_snapshot(theirs_db)
        theirs = self._handle(theirs_db)
        base_conn, base_db = None, None
        try:
            base_db = self._merge_base(ref.branch, ours, onto.branch, theirs)
            if base_db is None:
                print(f"Warning: {ref.branch} and {onto.branch} share no fork point: upstream conflicts not detected")
            else:
                base_conn = self._handle(base_db)
            info = {"up_to_date": False}
            schema = self._reconcile_schema(ours, ours_db, theirs, theirs_db)
            # git: "ours" is the upstream, "theirs" the branch's own commits.
            keep_branch = (on_conflict == "theirs")
            if base_conn is not None:
                tw_info, conflicts = self._three_way(ours, ours_db, theirs, theirs_db, base_conn, base_db,
                                                     on_conflict, ref, schema["added_tables"],
                                                     keep_ours=keep_branch or callable(on_conflict))
            else:
                tw_info, conflicts = {"conflicts": 0, "conflict_tables": [], "ours_changes": 0, "theirs_changes": 0,
                                      "merged_tables": [], "skipped_tables": {}}, []
            # Swap sides for the callable: our_* = upstream, their_* = branch.
            for cf in conflicts:
                for row in cf["rows"]:
                    for c in list(row):
                        if c.startswith("our_") and not c.endswith("_diff_type"):
                            row[c], row["their_" + c[4:]] = row["their_" + c[4:]], row[c]
                    row["our_diff_type"], row["their_diff_type"] = row["their_diff_type"], row["our_diff_type"]
            info.update(tw_info)
            if conflicts:
                info["resolved"] = "custom"
                info["resolution"] = on_conflict(self._conflict_session(ref), conflicts)
            elif info.get("conflicts"):
                info["resolved"] = on_conflict
            info["up_to_date"] = (info.get("theirs_changes", 0) == 0 and not schema["added_tables"]
                                  and not schema["added_columns"])
            max_seq = int(self._find_commit(theirs, onto.commit)["seq"]) if onto.commit else None
            self._copy_log(ours, ours_db, theirs, max_seq=max_seq)
            self._add_row(ours, "rebase", f"rebase onto {onto}", branch=ref.branch, prev_snap=theirs_db,
                          prev_branch=onto.branch)
            info["schema_changes"] = schema
            return info
        finally:
            self._release_handle(theirs)
            self._release_handle(base_conn)
            for db in {theirs_db, base_db}:
                if db:
                    self._fdw_cleanup(ours, db)

    # -- reset / revert ------------------------------------------------------

    def _reset_impl(self, ref: Ref, to: str) -> None:
        bdb = self._on(ref)
        row = self._find_commit(self.conn, to)
        if not row["snap"]:
            raise ValueError(f"commit {to} has no snapshot to reset to ({row['kind']} row)")
        snap = row["snap"]
        self._park_self()
        # The branch database is replaced by a clone of the snapshot (whose
        # log ends at the commit); later snapshots stay until the branch is
        # deleted.
        with self._exclusive(bdb):
            for attempt in range(6):
                self._check_cancel()
                try:
                    self._admin(f"DROP DATABASE {_q(bdb)}")
                    break
                except psycopg2.errors.ObjectInUse:
                    self.observe("drop_retries")
                    time.sleep(0.2 * (attempt + 1))
            else:
                self._admin(f"DROP DATABASE {_q(bdb)}")
            self._clone(snap, bdb)

    def _revert_impl(self, ref: Ref, commit: str) -> None:
        bdb = self._on(ref)
        ours = self.conn
        row = self._find_commit(ours, commit)
        a_db, b_db = row["snap"], row["prev_snap"]
        if not a_db or not b_db:
            raise ValueError(f"commit {commit} has no snapshot pair to revert ({row['kind']} row; "
                             f"after {a_db}, before {b_db})")
        ca, cb = self._handle(a_db), self._handle(b_db)
        try:
            changed = self._revert_apply(ours, bdb, ca, a_db, cb, b_db)
        finally:
            self._release_handle(ca)
            self._release_handle(cb)
        if not changed:
            return
        self._commit_impl(ref, f"revert {commit}")

    # -- delete --------------------------------------------------------------

    def _delete_impl(self, ref: Ref) -> None:
        bdb = self._db_for(ref.branch)
        if bdb == self.db_name:
            raise ValueError("the default branch cannot be deleted")
        if self.conn.db == bdb or (self._current_ref and self._current_ref.branch == ref.branch):
            self._park_self()
            self._current_ref = None
        pattern = bdb.replace("_", r"\_") + r"\_\_%"
        rows = self._admin("SELECT datname FROM pg_database WHERE datname LIKE %s", (pattern,)) or []
        snaps = [r[0] for r in rows]
        self._drop_db(bdb)
        with _REG.cond:
            _REG.branches.get(self.db_name, set()).discard(ref.branch)
            keep, drop = [], []
            for s in snaps:
                refs = _REG.snapshots.get(s, set()) - {bdb}
                (keep if refs else drop).append(s)
            for s in keep:
                _REG.orphans.add(s)
            # Orphans this branch was the last to reference are free now.
            freed = []
            for s in list(_REG.orphans):
                refs = _REG.snapshots.get(s, set())
                refs.discard(bdb)
                if not refs:
                    _REG.orphans.discard(s)
                    _REG.snapshots.pop(s, None)
                    freed.append(s)
        for s in drop + freed:
            self._drop_db(s)
        if keep:
            self.observe("snapshots_kept_for_other_branches", len(keep))

    # ------------------------------------------------------------------
    # Async: a psycopg pool on the branch the suite is on when opened; a
    # script on another branch gets its own connection. Pool connections
    # are not in the registry, so a clone of their database waits for
    # them (microbench only; the macrobench spine load uses sync exec).
    # ------------------------------------------------------------------

    async def open_async_pool(self, size: int) -> None:
        from psycopg_pool import AsyncConnectionPool

        if self._current_ref is None:
            raise ValueError("Not connected to a branch")
        self._pool_branch = self._current_ref.branch
        self.async_pool = AsyncConnectionPool(
            _uri(self._db_for(self._pool_branch)),
            min_size=size, max_size=size, kwargs={"autocommit": True}, open=False,
        )
        await self.async_pool.open(wait=True)

    async def _connect_impl_async(self, conn, ref: Ref):
        if ref.branch == self._pool_branch and not ref.commit:
            return conn
        import psycopg

        return await psycopg.AsyncConnection.connect(_uri(self._reading_db(ref)), autocommit=True)

    # ------------------------------------------------------------------
    # Setup / cleanup helpers used by the runners
    # ------------------------------------------------------------------

    @classmethod
    def drop_run_databases(cls, db_name: str) -> int:
        """Drop the run's database and every branch, base and snapshot
        database derived from it (``<db_name>__...``). Returns the count."""
        conn = psycopg2.connect(_uri("postgres"))
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        n = 0
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT datname FROM pg_database WHERE datname = %s OR datname LIKE %s "
                            "ORDER BY length(datname) DESC",
                            (db_name, db_name.replace("_", r"\_") + r"\_\_%"))
                names = [r[0] for r in cur.fetchall()]
                for name in names:
                    cur.execute(f"DROP DATABASE IF EXISTS {_q(name)} WITH (FORCE)")
                    n += 1
        finally:
            conn.close()
        with _REG.cond:
            _REG.branches.pop(db_name, None)
        return n

    @classmethod
    def cleanup(cls, info):
        try:
            n = cls.drop_run_databases(info.db_name)
            print(f"Database '{info.db_name}' deleted successfully ({n} databases dropped).")
        except Exception as e:
            print(f"Error deleting database: {e}")
        finally:
            info.change_file_copy_method(info.prev_method)

    class FileCopyInfo:
        """Run-level state shared by every worker's suite: the run's name,
        the server's previous file_copy_method, and (for the microbench
        runner's interface) the branch set and locks it expects."""

        def __init__(self, db_name: str):
            self.branches = set()
            self.db_name = db_name
            self.prev_method = ""
            self.branches_lock = threading.Lock()
            self.create_db_lock = threading.Lock()
            self.change_file_copy_method(FILE_COPY_METHOD)
            try:
                stale = FileCopyToolSuite.drop_run_databases(db_name)
                if stale:
                    print(f"Dropped {stale} stale database(s) of an earlier '{db_name}' run")
            except Exception as e:
                print(f"Warning: could not drop stale databases: {e}")

        def change_file_copy_method(self, method: str) -> None:
            """Set file_copy_method server-wide, remembering the old value."""
            if not method:
                return
            conn = None
            try:
                conn = psycopg2.connect(_uri("postgres"))
                conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                with conn.cursor() as cur:
                    cur.execute("SHOW file_copy_method;")
                    self.prev_method = cur.fetchone()[0]
                    if self.prev_method != method:
                        cur.execute(f"ALTER SYSTEM SET file_copy_method = '{method}';")
                        cur.execute("SELECT pg_reload_conf();")
                        print(f"Changed file copy method to {method} (was {self.prev_method})")
                    else:
                        print(f"File copy method: {method}")
            except Exception as e:
                print(f"Error changing file copy method: {e}")
            finally:
                if conn:
                    conn.close()


def setup_database(db_name: str) -> None:
    """Create an empty run database (used by the tests)."""
    FileCopyToolSuite.drop_run_databases(db_name)
    conn = psycopg2.connect(_uri("postgres"))
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE {_q(db_name)}")
    finally:
        conn.close()


def drop_database(db_name: str) -> int:
    return FileCopyToolSuite.drop_run_databases(db_name)


def server_up() -> bool:
    try:
        conn = psycopg2.connect(_uri("postgres"), connect_timeout=2)
        conn.close()
        return True
    except Exception:
        return False
