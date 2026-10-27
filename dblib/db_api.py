"""Git-like API over branchable databases.

A backend subclasses DBToolSuite and implements the protected ``_*_impl``
hooks it supports. The public verbs (branch, commit, diff, log, merge,
rebase, revert, reset, delete) wrap those hooks with timing, storage
measurement and result recording, and ``exec()`` runs a workload script on
one or more branches.

Status handling
---------------
Every verb returns an OpResult instead of raising. A hook the backend did
not override raises UnsupportedOperation, which the verb turns into an
UNSUPPORTED row (zero latency, no storage delta) so the workload keeps
running. A hook that raised anything else produces a FAILED row carrying
the error. Pass ``raise_on_error=True`` to get the exception instead.

Refs
----
Operations name their target with a Ref: a branch name, or ``branch@commit``
on backends with commits (SUPPORTS_COMMIT_REFS). On other backends a commit
ref falls back to the branch head with a warning, and the row is flagged
with ``commit_ref_fallback``. The suite keeps no notion of a "current
branch" beyond knowing which ref its connection is on, so that exec() can
skip a redundant switch.

exec()
------
``exec(script, refs, mode)`` runs ``script`` on each ref in turn
(``mode="per_ref"``), or once with every ref addressable from one session
(``mode="multi"``, only on backends with multi-branch query semantics). A
script is one of:

* a list of SQL statements, each a string or ``(sql, params)``;
* Python source. It runs with ``db`` (the Session), ``params`` and ``suite``
  in scope. If it defines ``run(db)`` that function is called with the
  session (``async def run(db)`` in exec_async) and its return value becomes
  the EXEC row's value;
* a callable taking the session.

Each statement the script issues through ``db.sql()`` is timed and recorded
as its own row (READ/INSERT/UPDATE/DELETE_ROWS/DDL). Per ref, exec() also records a
CONNECT row when it had to switch the connection, and one EXEC row with the
script's total latency and the storage delta. Every statement autocommits.

Conflicts
---------
``merge()`` and ``rebase()`` take ``on_conflict``: "ours" (default, keep the
target branch's version), "theirs", or a callable ``resolve(db, conflicts)``
that the backend calls on the half-merged working set with a Session and a
list of ``{"table": name, "rows": [...]}`` entries (row dicts carry the
backend's base/our/their columns). The callable resolves conflicts with SQL
through ``db.sql()`` and calls ``db.resolve(table)`` for each table it
settled; whatever it leaves unresolved is resolved as "ours".
Schema conflicts cannot be resolved this way: the backend aborts the
operation and the verb is recorded as FAILED with the reason.
"""

import inspect
import contextlib
import os
import time
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Union

import dblib.result_collector as rc
from dblib import result_pb2 as rslt

OpStatus = rslt.OpStatus
OpType = rslt.OpType

VERBS = (
    "branch",
    "commit",
    "diff",
    "log",
    "merge",
    "rebase",
    "revert",
    "reset",
    "delete",
)

_VERB_OP_TYPES = {
    "branch": OpType.BRANCH,
    "commit": OpType.COMMIT,
    "diff": OpType.DIFF,
    "log": OpType.LOG,
    "merge": OpType.MERGE,
    "rebase": OpType.REBASE,
    "revert": OpType.REVERT,
    "reset": OpType.RESET,
    "delete": OpType.DELETE,
}


class OperationCancelled(RuntimeError):
    """The operation was aborted (a stall watchdog cancelled it)."""


# Native-only mode: every verb an adapter simulates is treated as
# unsupported, so a run shows what the engine does on its own. Read at call
# time so tests can toggle it.
NATIVE_ONLY = os.environ.get("DBFORK_NATIVE_ONLY", "").lower() in ("1", "true", "yes")


def native_only() -> bool:
    return NATIVE_ONLY


class UnsupportedOperation(Exception):
    """The backend cannot perform this operation at all."""

    def __init__(self, op: str, backend: str = "", reason: str = ""):
        self.op = op
        self.backend = backend
        self.reason = reason
        msg = f"{backend or 'backend'} does not support {op}"
        if reason:
            msg += f": {reason}"
        super().__init__(msg)


@dataclass(frozen=True)
class Ref:
    """A branch, optionally pinned to a commit ("branch@commit")."""

    branch: str
    commit: Optional[str] = None

    @classmethod
    def parse(cls, value: Union[str, "Ref"]) -> "Ref":
        if isinstance(value, Ref):
            return value
        if not isinstance(value, str) or not value:
            raise ValueError(f"Invalid ref: {value!r}")
        if "@" in value:
            branch, commit = value.split("@", 1)
            return cls(branch, commit or None)
        return cls(value)

    def head(self) -> "Ref":
        return Ref(self.branch)

    def __str__(self) -> str:
        return f"{self.branch}@{self.commit}" if self.commit else self.branch


RefLike = Union[str, Ref]

# "ours" | "theirs" | resolve(db: Session, conflicts: list[dict]) -> dict | None
ConflictPolicy = Union[str, Callable[[Any, list], Any]]
CONFLICT_POLICIES = ("ours", "theirs")


def check_conflict_policy(on_conflict: ConflictPolicy) -> ConflictPolicy:
    if callable(on_conflict) or on_conflict in CONFLICT_POLICIES:
        return on_conflict
    raise ValueError(
        f"on_conflict must be one of {CONFLICT_POLICIES} or a callable, "
        f"got {on_conflict!r}"
    )


@dataclass
class OpResult:
    """Outcome of one verb, one statement, or one exec() on one ref."""

    op: str
    status: int = OpStatus.OK
    ref: str = ""
    latency: float = 0.0
    storage_before: int = 0
    storage_after: int = 0
    value: Any = None
    error: str = ""
    note: str = ""

    @property
    def ok(self) -> bool:
        return self.status == OpStatus.OK

    @property
    def unsupported(self) -> bool:
        return self.status == OpStatus.UNSUPPORTED

    @property
    def failed(self) -> bool:
        return self.status == OpStatus.FAILED

    @property
    def status_name(self) -> str:
        return OpStatus.Name(self.status)

    @property
    def storage_delta(self) -> int:
        return self.storage_after - self.storage_before

    def raise_for_status(self) -> "OpResult":
        if self.unsupported:
            raise UnsupportedOperation(self.op, reason=self.error)
        if self.failed:
            raise RuntimeError(f"{self.op} on {self.ref or '?'} failed: {self.error}")
        return self


@dataclass
class ExecResult(OpResult):
    """Outcome of exec() on one ref (or one multi-ref session)."""

    refs: list = field(default_factory=list)
    connect: Optional[OpResult] = None
    statements: list = field(default_factory=list)

    @property
    def rows(self):
        """Rows returned by the last statement, if any."""
        for s in reversed(self.statements):
            if s.ok:
                return s.value
        return None


# ----------------------------------------------------------------------------
# Sessions handed to scripts
# ----------------------------------------------------------------------------


class _SessionBase:
    def __init__(self, suite, conn, ref: Ref, refs: list, exec_id: int,
                 timed: bool, label: str, keys_touched: int):
        self.suite = suite
        self.conn = conn
        self.ref = ref
        self.refs = refs
        self.exec_id = exec_id
        self.timed = timed
        self.label = label
        self.statements: list = []
        self._keys_touched = keys_touched

    def table(self, ref: RefLike, table: str) -> str:
        """Backend-qualified name of ``table`` on ``ref`` for a multi-ref
        script (e.g. "db/branch".public.t on Dolt)."""
        return self.suite._qualified_table(Ref.parse(ref), table)

    def record_keys_touched(self, n: int) -> None:
        """Keys the next statement touches (overrides the driver's count)."""
        self._keys_touched = n

    def _take_keys(self, rows_touched: int) -> int:
        """Explicit count if the script gave one, else the rows the driver
        reported (affected rows for writes, rows returned for reads)."""
        n, self._keys_touched = self._keys_touched, 0
        return n if n else max(int(rows_touched or 0), 0)

    def _record(self, query, vars, status, latency, start, end, value, error,
                rows_touched: int = 0):
        result = OpResult(
            op="sql", status=status, ref=str(self.ref), latency=latency,
            value=value, error=error,
        )
        self.statements.append(result)
        if self.timed:
            text = f"{query} -- args: {vars}" if vars else query
            self.suite.result_collector.emit(
                rc.GetOpTypeFromSQL(query), status=status, latency=latency,
                start_time=start, end_time=end, ref=str(self.ref),
                exec_id=self.exec_id, label=self.label, sql_query=text,
                error_message=error, num_keys_touched=self._take_keys(rows_touched),
            )
        return result


class Session(_SessionBase):
    """What a sync script sees as ``db``."""

    def resolve(self, table: str) -> None:
        """Inside an ``on_conflict`` callable: mark ``table``'s conflicts
        resolved as the working set now stands (Dolt clears its
        dolt_conflicts_<table>; backends without a conflict table do
        nothing)."""
        self.suite._mark_resolved(self, table)

    @contextlib.contextmanager
    def transaction(self):
        """Run the block inside BEGIN ... COMMIT; ROLLBACK if it raises.
        The control statements are recorded untimed. Backends that reject
        BEGIN (none of the current ones) fall back to autocommit."""
        try:
            self.sql("BEGIN", timed=False)
        except Exception:
            yield
            return
        try:
            yield
        except BaseException:
            try:
                self.sql("ROLLBACK", timed=False)
            except Exception:
                pass
            raise
        self.sql("COMMIT", timed=False)

    def sql(self, query: str, vars=None, timed: bool = None):
        """Run one statement on this session's connection and return its
        rows (None for statements without a result set). Records a row and
        re-raises on error."""
        if timed is not None:
            saved, self.timed = self.timed, timed
        self.suite._check_cancel()   # a stall watchdog cancelled this exec
        rows, error, status, touched = None, "", OpStatus.OK, 0
        start_wall = time.time()
        start = time.perf_counter()
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, vars)
                if cur.description is not None:
                    rows = cur.fetchall()
                    touched = len(rows)
                else:
                    touched = getattr(cur, "rowcount", 0)
        except Exception as e:
            status, error = OpStatus.FAILED, f"{type(e).__name__}: {e}"
            raise
        finally:
            latency = time.perf_counter() - start
            self._record(query, vars, status, latency, start_wall, time.time(),
                         rows, error, touched)
            if timed is not None:
                self.timed = saved
        return rows


class AsyncSession(_SessionBase):
    """What an async script sees as ``db``."""

    @contextlib.asynccontextmanager
    async def transaction(self):
        """Async twin of :meth:`Session.transaction`."""
        try:
            await self.sql("BEGIN", timed=False)
        except Exception:
            yield
            return
        try:
            yield
        except BaseException:
            try:
                await self.sql("ROLLBACK", timed=False)
            except Exception:
                pass
            raise
        await self.sql("COMMIT", timed=False)

    async def sql(self, query: str, vars=None, timed: bool = None):
        if timed is not None:
            saved, self.timed = self.timed, timed
        rows, error, status, touched = None, "", OpStatus.OK, 0
        start_wall = time.time()
        start = time.perf_counter()
        try:
            async with self.conn.cursor() as cur:
                await cur.execute(query, vars)
                if cur.description is not None:
                    rows = await cur.fetchall()
                    touched = len(rows)
                else:
                    touched = getattr(cur, "rowcount", 0)
        except Exception as e:
            status, error = OpStatus.FAILED, f"{type(e).__name__}: {e}"
            raise
        finally:
            latency = time.perf_counter() - start
            self._record(query, vars, status, latency, start_wall, time.time(),
                         rows, error, touched)
            if timed is not None:
                self.timed = saved
        return rows


def _script_text(script) -> str:
    if isinstance(script, str):
        return script
    if isinstance(script, (list, tuple)):
        parts = []
        for item in script:
            if isinstance(item, (list, tuple)):
                parts.append(f"{item[0]} -- args: {item[1]}")
            else:
                parts.append(str(item))
        return "\n".join(parts)
    return getattr(script, "__name__", repr(script))


# ----------------------------------------------------------------------------
# The suite
# ----------------------------------------------------------------------------


class DBToolSuite(ABC):
    """Git-like interface to one branchable database.

    One instance per worker thread. ``self.conn`` is the DB-API connection
    the sync verbs and exec() use; ``_connect_impl`` points it at a ref.
    """

    BACKEND_NAME = "base"
    # What _storage_bytes() covers: "database" (this database and its
    # branches only) or "server" (the whole server's data directory, for
    # backends without a per-database directory).
    STORAGE_SCOPE = "database"
    # Can a ref name a commit ("branch@hash")?
    SUPPORTS_COMMIT_REFS = False
    # Can one SQL statement address several branches (exec mode="multi")?
    SUPPORTS_MULTI_REF_EXEC = False

    def __init__(
        self,
        connection=None,
        result_collector: Optional[rc.ResultCollector] = None,
        measure_storage: bool = False,
    ):
        self.conn = connection
        self.result_collector = result_collector or rc.ResultCollector()
        # Default for the ``storage`` argument of every verb and exec().
        self.measure_storage = measure_storage
        self.async_pool = None
        # Ref the sync connection is on, or None if unknown.
        self._current_ref: Optional[Ref] = None
        self._fallback_warned: set = set()

    # ------------------------------------------------------------------
    # Capabilities
    # ------------------------------------------------------------------

    @classmethod
    def supports(cls, verb: str) -> bool:
        """Whether this backend overrides the hook for ``verb``."""
        if verb == "commit_refs":
            return cls.SUPPORTS_COMMIT_REFS
        if verb == "multi_ref_exec":
            return cls.SUPPORTS_MULTI_REF_EXEC
        if verb == "exec_async":
            return cls.open_async_pool is not DBToolSuite.open_async_pool
        hook = f"_{verb}_impl"
        return getattr(cls, hook, None) is not getattr(DBToolSuite, hook, None)

    @classmethod
    def capabilities(cls) -> dict:
        caps = {verb: cls.supports(verb) for verb in VERBS}
        caps["commit_refs"] = cls.SUPPORTS_COMMIT_REFS
        caps["multi_ref_exec"] = cls.SUPPORTS_MULTI_REF_EXEC
        caps["exec_async"] = cls.supports("exec_async")
        return caps

    # How each supported verb is realised, for the report: "native" (the
    # engine holds the versions and does the work; the adapter only records
    # a handle) or "simulated" (the adapter materialises or computes the
    # result with SQL, copies or foreign tables). Backends list only the
    # verbs that are not "native". In native-only mode (DBFORK_NATIVE_ONLY=1)
    # every simulated verb is reported and executed as unsupported.
    IMPLEMENTATION: dict = {}
    # Free-text detail per verb (optional).
    IMPLEMENTATION_NOTES: dict = {}

    @classmethod
    def _is_native(cls, verb: str) -> bool:
        return cls.IMPLEMENTATION.get(verb, "native") == "native"

    @classmethod
    def _available(cls, verb: str) -> bool:
        """Supported by the adapter and, in native-only mode, native."""
        return cls.supports(verb) and (not native_only() or cls._is_native(verb))

    @classmethod
    def implementation(cls) -> dict:
        out = {}
        for verb in VERBS + ("commit_refs", "multi_ref_exec"):
            if not cls._available(verb):
                out[verb] = "unsupported"
            else:
                out[verb] = cls.IMPLEMENTATION.get(verb, "native")
        return out

    def _unsupported(self, op: str, reason: str = "") -> UnsupportedOperation:
        return UnsupportedOperation(op, self.BACKEND_NAME, reason)

    # ------------------------------------------------------------------
    # Protected hooks: backends override the ones they support.
    # None of them should time themselves; the public wrappers do that.
    # ------------------------------------------------------------------

    @abstractmethod
    def _storage_bytes(self) -> int:
        """Storage used by the database (all branches), in bytes."""

    @abstractmethod
    def _connect_impl(self, ref: Ref) -> None:
        """Point ``self.conn`` at ``ref`` (reconnect, checkout, USE, ...)."""

    def _branch_impl(self, name: str, from_ref: Ref) -> None:
        raise self._unsupported("branch")

    def _commit_impl(self, ref: Ref, message: str) -> str:
        """Snapshot ``ref``'s working state; return the commit id."""
        raise self._unsupported("commit")

    def _diff_impl(self, ref_a: Ref, ref_b: Ref) -> Any:
        """Summary of the differences between two refs (backend-specific)."""
        raise self._unsupported("diff")


    # The schema half of diff(): "native" when the engine compares the
    # schemas itself (the adapter overrides _schema_diff_impl with an engine
    # call), else "simulated" from the catalog. SCHEMA_DIALECT ("mysql" |
    # "postgres") selects the catalog query the base implementation runs on
    # each ref through exec(). In native-only mode a simulated schema half
    # is left out of the result (value["schema"] is None).
    SCHEMA_DIFF_HALF: str = "simulated"
    SCHEMA_DIALECT: Optional[str] = None
    # Adapter bookkeeping tables and hidden columns to leave out.
    SCHEMA_SKIP_TABLE_PREFIXES: tuple = ("_bb_",)
    SCHEMA_SKIP_COLUMN_PREFIXES: tuple = ("__mo_",)

    _CATALOG_QUERIES = {
        "mysql": ("SELECT table_name, column_name, column_type FROM information_schema.columns "
                  "WHERE table_schema = DATABASE() ORDER BY table_name, ordinal_position"),
        "postgres": ("SELECT c.table_name, c.column_name, "
                     "CASE WHEN c.character_maximum_length IS NOT NULL THEN c.data_type || '(' || "
                     "c.character_maximum_length || ')' WHEN c.numeric_precision IS NOT NULL AND "
                     "c.data_type IN ('numeric', 'decimal') THEN c.data_type || '(' || c.numeric_precision "
                     "|| ',' || COALESCE(c.numeric_scale, 0) || ')' ELSE c.data_type END "
                     "FROM information_schema.columns c JOIN information_schema.tables t "
                     "ON t.table_schema = c.table_schema AND t.table_name = c.table_name "
                     "WHERE c.table_schema = current_schema() AND t.table_type = 'BASE TABLE' "
                     "ORDER BY c.table_name, c.ordinal_position"),
    }

    def _schema_at(self, ref: Ref) -> dict:
        """{table: {column: type}} of ``ref``, read from the catalog with
        the dialect's query through an untimed exec()."""
        query = self._CATALOG_QUERIES.get(self.SCHEMA_DIALECT or "")
        if query is None:
            raise self._unsupported("diff", "no catalog query for this backend")
        res = self.exec(lambda db: db.sql(query), refs=[ref], timed=False, raise_on_error=True)[0]
        out: dict = {}
        for table, column, ctype in res.value or []:
            if any(str(table).startswith(pf) for pf in self.SCHEMA_SKIP_TABLE_PREFIXES):
                continue
            if any(str(column).startswith(pf) for pf in self.SCHEMA_SKIP_COLUMN_PREFIXES):
                continue
            out.setdefault(str(table), {})[str(column)] = str(ctype).lower()
        return out

    @staticmethod
    def _compare_schemas(a: dict, b: dict) -> dict:
        """Schema changes from ``a`` to ``b``: tables added/dropped, and for
        tables on both sides columns added/dropped/changed (type)."""
        out = {"tables_added": sorted(set(b) - set(a)), "tables_dropped": sorted(set(a) - set(b)),
               "columns_added": {}, "columns_dropped": {}, "columns_changed": {}}
        for t in sorted(set(a) & set(b)):
            ca, cb = a[t], b[t]
            added = sorted(set(cb) - set(ca))
            dropped = sorted(set(ca) - set(cb))
            changed = [[c, ca[c], cb[c]] for c in sorted(set(ca) & set(cb)) if ca[c] != cb[c]]
            if added:
                out["columns_added"][t] = added
            if dropped:
                out["columns_dropped"][t] = dropped
            if changed:
                out["columns_changed"][t] = changed
        out["changed"] = bool(out["tables_added"] or out["tables_dropped"] or out["columns_added"]
                              or out["columns_dropped"] or out["columns_changed"])
        return out

    def _schema_diff_impl(self, ref_a: Ref, ref_b: Ref) -> dict:
        """The schema half of diff(): changes from ``ref_a`` to ``ref_b``
        (see _compare_schemas)."""
        return self._compare_schemas(self._schema_at(ref_a), self._schema_at(ref_b))

    def _diff_with_schema(self, ref_a: Ref, ref_b: Ref) -> dict:
        """diff() = the row half (_diff_impl) plus the schema half
        (_schema_diff_impl) under ``value["schema"]``. Native-only mode
        keeps a simulated schema half out, so an engine whose diff covers
        rows only answers rows only, and the checks that need the schema
        fail rather than the verb."""
        value = self._diff_impl(ref_a, ref_b)
        if not isinstance(value, dict):
            value = {"value": value}
        if native_only() and self.SCHEMA_DIFF_HALF != "native":
            value["schema"] = None
        else:
            value["schema"] = self._schema_diff_impl(ref_a, ref_b)
        return value
    def _log_impl(self, ref: Ref, limit: int) -> list:
        """Most recent ``limit`` commits reachable from ``ref``."""
        raise self._unsupported("log")

    def _merge_impl(self, into: Ref, source: Ref, message: str,
                    on_conflict: ConflictPolicy = "ours") -> Any:
        """Merge ``source`` into ``into``; resolve conflicts per
        ``on_conflict`` (see the module docstring)."""
        raise self._unsupported("merge")

    def _rebase_impl(self, ref: Ref, onto: Ref,
                     on_conflict: ConflictPolicy = "ours") -> Any:
        raise self._unsupported("rebase")

    def _conflict_session(self, ref: Ref, label: str = "resolve") -> "Session":
        """Session a conflict-resolution callable gets. Its statements are
        recorded with the given label inside the verb's latency."""
        return Session(self, self.conn, ref, [ref],
                       self.result_collector.next_exec_id(), True, label, 0)

    def _mark_resolved(self, session: "Session", table: str) -> None:
        """Hook behind Session.resolve(); no-op unless the backend keeps a
        per-table conflict list that must be cleared."""

    def _revert_impl(self, ref: Ref, commit: str) -> None:
        raise self._unsupported("revert")

    def _reset_impl(self, ref: Ref, to: str) -> None:
        raise self._unsupported("reset")

    def _delete_impl(self, ref: Ref) -> None:
        raise self._unsupported("delete")

    def _gc_impl(self) -> None:
        """Reclaim or flush storage (Dolt's dolt_gc, a checkpoint, ...)
        so that a storage measurement reflects what the backend keeps."""
        raise self._unsupported("gc")

    def _qualified_table(self, ref: Ref, table: str) -> str:
        raise self._unsupported("multi_ref_exec")

    def list_branches(self) -> list:
        raise self._unsupported("list_branches")

    # Async hooks (exec_async only). A backend that supports exec_async
    # overrides open_async_pool and _connect_impl_async.

    async def open_async_pool(self, size: int) -> None:
        raise self._unsupported("exec_async")

    def _pool_connection(self):
        """Async context manager yielding a pool connection."""
        return self.async_pool.connection()

    async def _connect_impl_async(self, conn, ref: Ref):
        """Return a connection on ``ref``: either ``conn`` after switching
        it, or a new connection (released by _release_async_conn)."""
        raise self._unsupported("exec_async")

    async def _release_async_conn(self, conn, pooled) -> None:
        if conn is not pooled:
            await conn.close()

    async def close_async_pool(self) -> None:
        if self.async_pool:
            await self.async_pool.close()
            self.async_pool = None

    # ------------------------------------------------------------------
    # Helpers for backends
    # ------------------------------------------------------------------

    def _execute(self, query: str, vars=None):
        """Run a statement on ``self.conn`` without recording it."""
        with self.conn.cursor() as cur:
            cur.execute(query, vars)
            if cur.description is not None:
                return cur.fetchall()
            return None

    # ------------------------------------------------------------------
    # Cancelling the operation in flight (stall watchdog)
    # ------------------------------------------------------------------

    def cancel_current_op(self) -> None:
        """Abort the operation this suite is running: cancel the statement
        on its connection (where the driver supports it) and make the
        backend's wait loops raise OperationCancelled at their next check.
        The verb or exec then fails and the workflow goes on."""
        self._cancel_requested = True
        conn = getattr(self, "conn", None)
        cancel = getattr(conn, "cancel", None)
        if callable(cancel):
            try:
                cancel()
            except Exception:
                pass

    def _check_cancel(self) -> None:
        """Backends call this inside waits and between statements."""
        if getattr(self, "_cancel_requested", False):
            raise OperationCancelled("operation cancelled")

    def _begin_op(self, op_name: str, ref: str, label: str) -> int:
        self._cancel_requested = False
        return self.result_collector.begin_op(op_name, ref, label, cancel=self.cancel_current_op)

    def _end_op(self, token: int, error: str) -> str:
        """Unregister the operation; a watchdog note is prefixed to the
        error of a failed operation."""
        note = self.result_collector.end_op(token, failed=bool(error))
        self._cancel_requested = False
        if note and error:
            return f"{note}; {error}"
        return error

    def _safe_storage(self) -> int:
        try:
            return int(self._storage_bytes() or 0)
        except Exception as e:
            print(f"Warning: storage measurement failed: {e}")
            return 0

    def _resolve(self, ref: Optional[RefLike]) -> tuple:
        """(Ref, commit_ref_fallback). A commit ref on a backend without
        commits becomes the branch head, with a warning."""
        if ref is None:
            if self._current_ref is None:
                raise ValueError("No ref given and no current ref")
            return self._current_ref, False
        r = Ref.parse(ref)
        if r.commit and not self._available("commit_refs"):
            if str(r) not in self._fallback_warned:
                self._fallback_warned.add(str(r))
                print(
                    f"WARNING: {self.BACKEND_NAME} has no commits; "
                    f"using head of '{r.branch}' for ref '{r}'"
                )
            return r.head(), True
        return r, False

    # ------------------------------------------------------------------
    # Public verbs
    # ------------------------------------------------------------------

    @property
    def current_ref(self) -> Optional[Ref]:
        """Ref the sync connection is on."""
        return self._current_ref

    def _run_verb(self, verb: str, fn: Callable, ref: Optional[Ref], *,
                  timed: bool, storage: Optional[bool], label: str,
                  raise_on_error: bool, fallback: bool = False) -> OpResult:
        op_type = _VERB_OP_TYPES[verb]
        storage = self.measure_storage if storage is None else storage
        before = self._safe_storage() if storage else 0
        status, value, error, exc = OpStatus.OK, None, "", None
        start_wall = time.time()
        start = time.perf_counter()
        token = self._begin_op(verb.upper(), str(ref) if ref else "", label)
        try:
            if native_only() and not self._is_native(verb):
                raise self._unsupported(verb, "simulated by the adapter; native-only mode")
            value = fn()
        except UnsupportedOperation as e:
            status, error, exc = OpStatus.UNSUPPORTED, e.reason or str(e), e
        except Exception as e:
            status, error, exc = OpStatus.FAILED, f"{type(e).__name__}: {e}", e
        finally:
            error = self._end_op(token, error)
        latency = time.perf_counter() - start if status != OpStatus.UNSUPPORTED else 0.0
        end_wall = time.time()
        after = self._safe_storage() if storage and status != OpStatus.UNSUPPORTED else 0
        if timed:
            self.result_collector.emit(
                op_type, status=status, latency=latency, start_time=start_wall,
                end_time=end_wall, ref=str(ref) if ref else "", label=label,
                error_message=error, disk_size_before=before,
                disk_size_after=after, commit_ref_fallback=fallback,
                num_keys_touched=0,
            )
        result = OpResult(
            op=verb, status=status, ref=str(ref) if ref else "",
            latency=latency, storage_before=before, storage_after=after,
            value=value, error=error,
        )
        if raise_on_error and exc is not None:
            raise exc
        return result

    def branch(self, name: str, from_ref: RefLike = None, *, timed: bool = True,
               storage: bool = None, label: str = "",
               raise_on_error: bool = False) -> OpResult:
        """Create branch ``name`` from ``from_ref`` (default: current ref)."""
        src, fallback = self._resolve(from_ref)
        return self._run_verb(
            "branch", lambda: self._branch_impl(name, src), src, timed=timed,
            storage=storage, label=label, raise_on_error=raise_on_error,
            fallback=fallback,
        )

    def commit(self, ref: RefLike = None, message: str = "", *, timed: bool = True,
               storage: bool = None, label: str = "",
               raise_on_error: bool = False) -> OpResult:
        """Snapshot ``ref``. The result's value is the commit id."""
        r, fallback = self._resolve(ref)
        return self._run_verb(
            "commit", lambda: self._commit_impl(r, message), r, timed=timed,
            storage=storage, label=label, raise_on_error=raise_on_error,
            fallback=fallback,
        )

    def diff(self, ref_a: RefLike, ref_b: RefLike, *, timed: bool = True,
             storage: bool = None, label: str = "",
             raise_on_error: bool = False) -> OpResult:
        """Differences from ``ref_a`` to ``ref_b``: per-table row counts
        (rows_added / rows_deleted / rows_modified, tables) and, under
        value["schema"], the schema changes (tables added and dropped;
        columns added, dropped and changed in type on tables present on
        both sides; see _compare_schemas). value["schema"] is None when a
        native-only run uses an engine diff that covers rows only."""
        a, fa = self._resolve(ref_a)
        b, fb = self._resolve(ref_b)
        return self._run_verb(
            "diff", lambda: self._diff_with_schema(a, b), b, timed=timed,
            storage=storage, label=label, raise_on_error=raise_on_error,
            fallback=fa or fb,
        )

    def log(self, ref: RefLike = None, limit: int = 10, *, timed: bool = True,
            storage: bool = None, label: str = "",
            raise_on_error: bool = False) -> OpResult:
        """Recent commits on ``ref`` (value is a list of dicts)."""
        r, fallback = self._resolve(ref)
        return self._run_verb(
            "log", lambda: self._log_impl(r, limit), r, timed=timed,
            storage=storage, label=label, raise_on_error=raise_on_error,
            fallback=fallback,
        )

    def merge(self, into: RefLike, source: RefLike, message: str = "", *,
              on_conflict: ConflictPolicy = "ours", timed: bool = True,
              storage: bool = None, label: str = "",
              raise_on_error: bool = False) -> OpResult:
        """Merge ``source`` into ``into``. The value is a backend-specific
        dict; Dolt reports ``fast_forward``, ``conflicts`` and
        ``conflict_tables``. See the module docstring for ``on_conflict``."""
        check_conflict_policy(on_conflict)
        dst, fd = self._resolve(into)
        src, fs = self._resolve(source)
        return self._run_verb(
            "merge", lambda: self._merge_impl(dst, src, message, on_conflict),
            dst, timed=timed, storage=storage, label=label,
            raise_on_error=raise_on_error, fallback=fd or fs,
        )

    def rebase(self, ref: RefLike, onto: RefLike, *,
               on_conflict: ConflictPolicy = "ours", timed: bool = True,
               storage: bool = None, label: str = "",
               raise_on_error: bool = False) -> OpResult:
        """Replay ``ref``'s commits on top of ``onto``, resolving conflicts
        per ``on_conflict`` (see the module docstring)."""
        check_conflict_policy(on_conflict)
        r, fr = self._resolve(ref)
        o, fo = self._resolve(onto)
        return self._run_verb(
            "rebase", lambda: self._rebase_impl(r, o, on_conflict), r,
            timed=timed, storage=storage, label=label,
            raise_on_error=raise_on_error, fallback=fr or fo,
        )

    def revert(self, ref: RefLike, commit: str, *, timed: bool = True,
               storage: bool = None, label: str = "",
               raise_on_error: bool = False) -> OpResult:
        """Add a commit to ``ref`` that undoes ``commit``."""
        r, fallback = self._resolve(ref)
        return self._run_verb(
            "revert", lambda: self._revert_impl(r, commit), r, timed=timed,
            storage=storage, label=label, raise_on_error=raise_on_error,
            fallback=fallback,
        )

    def reset(self, ref: RefLike, to: str, *, timed: bool = True,
              storage: bool = None, label: str = "",
              raise_on_error: bool = False) -> OpResult:
        """Move ``ref`` to ``to`` (a commit id, or whatever the backend
        accepts as a point to restore), discarding later changes."""
        r, fallback = self._resolve(ref)
        return self._run_verb(
            "reset", lambda: self._reset_impl(r, to), r, timed=timed,
            storage=storage, label=label, raise_on_error=raise_on_error,
            fallback=fallback,
        )

    def delete(self, ref: RefLike, *, timed: bool = True, storage: bool = None,
               label: str = "", raise_on_error: bool = False) -> OpResult:
        """Delete branch ``ref``."""
        r, fallback = self._resolve(ref)
        return self._run_verb(
            "delete", lambda: self._delete_impl(r.head()), r, timed=timed,
            storage=storage, label=label, raise_on_error=raise_on_error,
            fallback=fallback,
        )

    def gc(self) -> OpResult:
        """Run the backend's storage reclamation/flush (see _gc_impl). Not
        recorded as an operation row; the result carries status and
        latency for the run's storage summary."""
        status, error = OpStatus.OK, ""
        start = time.perf_counter()
        try:
            self._gc_impl()
        except UnsupportedOperation as e:
            status, error = OpStatus.UNSUPPORTED, e.reason or str(e)
        except Exception as e:
            status, error = OpStatus.FAILED, f"{type(e).__name__}: {e}"
        return OpResult(op="gc", status=status, latency=time.perf_counter() - start,
                        error=error)

    def flush(self, ref: RefLike = None) -> None:
        """Make durable storage visible before a storage measurement: a
        CHECKPOINT on ``ref`` (the current ref by default) where the engine
        speaks Postgres (FAILED and ignored elsewhere), the adapter's own
        hook, then sync."""
        self.exec(["CHECKPOINT"], refs=None if ref is None else [ref], timed=False)
        self._flush_impl()
        os.sync()

    def _flush_impl(self) -> None:
        """Hook for adapters whose engine needs its own checkpoint or gc
        before storage is measured."""

    # ------------------------------------------------------------------
    # exec
    # ------------------------------------------------------------------

    def _prepare_exec(self, refs, mode, storage):
        if mode not in ("per_ref", "multi"):
            raise ValueError(f"exec mode must be 'per_ref' or 'multi', got {mode!r}")
        if refs is None:
            refs = [None]
        elif isinstance(refs, (str, Ref)):
            refs = [refs]
        if not refs:
            raise ValueError("exec needs at least one ref")
        resolved = [self._resolve(r) for r in refs]
        storage = self.measure_storage if storage is None else storage
        exec_id = self.result_collector.next_exec_id()
        state = self.result_collector._get_thread_state()
        keys, state.num_keys_touched = state.num_keys_touched, 0
        return resolved, storage, exec_id, keys

    def _connect(self, ref: Ref, exec_id: int, timed: bool, label: str,
                 fallback: bool) -> Optional[OpResult]:
        """Switch the sync connection to ``ref`` if it is not on it already.
        Returns the CONNECT result, or None when no switch was needed."""
        if self._current_ref == ref:
            return None
        status, error, exc = OpStatus.OK, "", None
        start_wall = time.time()
        start = time.perf_counter()
        try:
            self._connect_impl(ref)
            self._current_ref = ref
        except UnsupportedOperation as e:
            status, error, exc = OpStatus.UNSUPPORTED, e.reason or str(e), e
            self._current_ref = None
        except Exception as e:
            status, error, exc = OpStatus.FAILED, f"{type(e).__name__}: {e}", e
            self._current_ref = None
        latency = time.perf_counter() - start if status != OpStatus.UNSUPPORTED else 0.0
        if timed:
            self.result_collector.emit(
                OpType.CONNECT, status=status, latency=latency,
                start_time=start_wall, end_time=time.time(), ref=str(ref),
                exec_id=exec_id, label=label, error_message=error,
                commit_ref_fallback=fallback, num_keys_touched=0,
            )
        result = OpResult(op="connect", status=status, ref=str(ref),
                          latency=latency, error=error)
        result._exc = exc
        return result

    @staticmethod
    def _run_script(script, session: Session, params, suite):
        if callable(script):
            return script(session)
        if isinstance(script, (list, tuple)):
            for item in script:
                if isinstance(item, (list, tuple)):
                    session.sql(item[0], item[1] if len(item) > 1 else None)
                else:
                    session.sql(item)
            return None
        if isinstance(script, str):
            code = compile(script, "<exec-script>", "exec")
            scope = {"db": session, "params": params or {}, "suite": suite,
                     "__name__": "__dbscript__"}
            exec(code, scope)
            run = scope.get("run")
            if callable(run):
                return run(session)
            return None
        raise TypeError(f"Unsupported script type: {type(script).__name__}")

    def exec(self, script, refs=None, *, mode: str = "per_ref", params=None,
             timed: bool = True, storage: bool = None, label: str = "",
             raise_on_error: bool = False) -> list:
        """Run ``script`` on ``refs``; see the module docstring.

        Returns one ExecResult per ref in per_ref mode, or a single-element
        list in multi mode.
        """
        resolved, storage, exec_id, keys = self._prepare_exec(refs, mode, storage)
        text = _script_text(script)
        if mode == "multi":
            all_refs = [r for r, _ in resolved]
            fallback = any(f for _, f in resolved)
            if not self._available("multi_ref_exec"):
                return [self._exec_unsupported(all_refs, exec_id, timed, label,
                                               text, raise_on_error)]
            return [self._exec_one(script, text, resolved[0][0], all_refs,
                                   fallback, exec_id, timed, storage, label,
                                   params, keys, raise_on_error)]
        return [
            self._exec_one(script, text, r, [r], f, exec_id, timed, storage,
                           label, params, keys, raise_on_error)
            for r, f in resolved
        ]

    def _exec_unsupported(self, refs, exec_id, timed, label, text,
                          raise_on_error) -> ExecResult:
        reason = "multi-branch query semantics"
        if timed:
            self.result_collector.emit(
                OpType.EXEC, status=OpStatus.UNSUPPORTED, ref=str(refs[0]),
                refs=refs, exec_id=exec_id, label=label, sql_query=text,
                error_message=reason, num_keys_touched=0,
            )
        result = ExecResult(op="exec", status=OpStatus.UNSUPPORTED,
                            ref=str(refs[0]), refs=[str(r) for r in refs],
                            error=reason)
        if raise_on_error:
            raise self._unsupported("multi_ref_exec", reason)
        return result

    def _exec_one(self, script, text, ref, all_refs, fallback, exec_id, timed,
                  storage, label, params, keys, raise_on_error) -> ExecResult:
        before = self._safe_storage() if storage else 0
        connect = self._connect(ref, exec_id, timed, label, fallback)
        result = ExecResult(op="exec", ref=str(ref),
                            refs=[str(r) for r in all_refs], connect=connect,
                            storage_before=before)
        if connect is not None and not connect.ok:
            result.status, result.error = connect.status, connect.error
            self._emit_exec(result, timed, exec_id, label, text, fallback, 0, 0)
            if raise_on_error:
                raise connect._exc
            return result

        exc = None
        start_wall = time.time()
        start = time.perf_counter()
        token = self._begin_op("EXEC", result.ref, label)
        try:
            attempt = 0
            while True:
                session = Session(self, self.conn, ref, all_refs, exec_id, timed,
                                  label, keys)
                try:
                    result.value = self._run_script(script, session, params, self)
                    self._exec_finish(session)
                except Exception as e:
                    self._exec_abort(session)
                    # A script the watchdog cancelled is not run again.
                    if not self._cancel_requested and self._exec_retry(e, attempt):
                        attempt += 1
                        continue
                    if isinstance(e, UnsupportedOperation):
                        result.status, result.error, exc = OpStatus.UNSUPPORTED, e.reason or str(e), e
                    else:
                        result.status, result.error, exc = OpStatus.FAILED, f"{type(e).__name__}: {e}", e
                break
        finally:
            result.error = self._end_op(token, result.error)
        result.latency = time.perf_counter() - start
        result.statements = session.statements
        result.storage_after = self._safe_storage() if storage else 0
        self._emit_exec(result, timed, exec_id, label, text, fallback,
                        start_wall, time.time())
        if raise_on_error and exc is not None:
            raise exc
        return result

    # Hooks around one run of an exec() script. A backend that buffers a
    # script's writes (instead of autocommitting each statement) publishes
    # them in _exec_finish, which is part of the EXEC row's latency, and
    # discards them in _exec_abort. When _exec_retry returns True for the
    # exception a run raised, the script is run again from the start (e.g.
    # a publish lost a race with another writer).

    def _exec_finish(self, session: "Session") -> None:
        """The script ran without raising."""

    def _exec_abort(self, session: "Session") -> None:
        """The script, or _exec_finish, raised."""

    def _exec_retry(self, exc: Exception, attempt: int) -> bool:
        return False

    def _emit_exec(self, result: ExecResult, timed, exec_id, label, text,
                   fallback, start_wall, end_wall, pool_wait: float = 0.0):
        if not timed:
            return
        self.result_collector.emit(
            OpType.EXEC, status=result.status, latency=result.latency,
            start_time=start_wall, end_time=end_wall, ref=result.ref,
            refs=result.refs, exec_id=exec_id, label=label, sql_query=text,
            error_message=result.error, disk_size_before=result.storage_before,
            disk_size_after=result.storage_after, commit_ref_fallback=fallback,
            num_keys_touched=0, pool_wait_time=pool_wait,
        )

    # ------------------------------------------------------------------
    # exec_async
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def _acquire_async_conn(self):
        """Borrow a pool connection; yields (conn, seconds waited)."""
        if not self.async_pool:
            raise ValueError("Async pool not open; call open_async_pool() first")
        start = time.perf_counter()
        async with self._pool_connection() as conn:
            yield conn, time.perf_counter() - start

    @staticmethod
    async def _run_script_async(script, session: AsyncSession, params, suite):
        if isinstance(script, (list, tuple)):
            for item in script:
                if isinstance(item, (list, tuple)):
                    await session.sql(item[0], item[1] if len(item) > 1 else None)
                else:
                    await session.sql(item)
            return None
        if isinstance(script, str):
            code = compile(script, "<exec-script>", "exec")
            scope = {"db": session, "params": params or {}, "suite": suite,
                     "__name__": "__dbscript__"}
            exec(code, scope)
            script = scope.get("run")
            if script is None:
                raise TypeError("exec_async needs the script to define "
                                "'async def run(db)'")
        if callable(script):
            value = script(session)
            if inspect.isawaitable(value):
                value = await value
            return value
        raise TypeError(f"Unsupported script type: {type(script).__name__}")

    async def exec_async(self, script, refs=None, *, mode: str = "per_ref",
                         params=None, timed: bool = True, storage: bool = None,
                         label: str = "", raise_on_error: bool = False) -> list:
        """Async exec() on a pool connection; the script must be a list of
        SQL, a coroutine function, or Python source defining
        ``async def run(db)``. Pool wait is recorded on the EXEC row, not
        in its latency."""
        resolved, storage, exec_id, keys = self._prepare_exec(refs, mode, storage)
        text = _script_text(script)
        if mode == "multi":
            all_refs = [r for r, _ in resolved]
            fallback = any(f for _, f in resolved)
            if not self._available("multi_ref_exec"):
                return [self._exec_unsupported(all_refs, exec_id, timed, label,
                                               text, raise_on_error)]
            return [await self._exec_one_async(
                script, text, resolved[0][0], all_refs, fallback, exec_id,
                timed, storage, label, params, keys, raise_on_error)]
        results = []
        for r, f in resolved:
            results.append(await self._exec_one_async(
                script, text, r, [r], f, exec_id, timed, storage, label,
                params, keys, raise_on_error))
        return results

    async def _exec_one_async(self, script, text, ref, all_refs, fallback,
                              exec_id, timed, storage, label, params, keys,
                              raise_on_error) -> ExecResult:
        before = self._safe_storage() if storage else 0
        result = ExecResult(op="exec", ref=str(ref),
                            refs=[str(r) for r in all_refs],
                            storage_before=before)
        async with self._acquire_async_conn() as (pooled, waited):
            # Connect (switch the pooled connection, or open one for ref)
            status, error, exc, conn = OpStatus.OK, "", None, None
            start_wall = time.time()
            start = time.perf_counter()
            try:
                conn = await self._connect_impl_async(pooled, ref)
            except UnsupportedOperation as e:
                status, error, exc = OpStatus.UNSUPPORTED, e.reason or str(e), e
            except Exception as e:
                status, error, exc = OpStatus.FAILED, f"{type(e).__name__}: {e}", e
            latency = time.perf_counter() - start if status != OpStatus.UNSUPPORTED else 0.0
            if timed:
                self.result_collector.emit(
                    OpType.CONNECT, status=status, latency=latency,
                    start_time=start_wall, end_time=time.time(), ref=str(ref),
                    exec_id=exec_id, label=label, error_message=error,
                    commit_ref_fallback=fallback, num_keys_touched=0,
                )
            result.connect = OpResult(op="connect", status=status, ref=str(ref),
                                      latency=latency, error=error)
            if exc is not None:
                result.status, result.error = status, error
                self._emit_exec(result, timed, exec_id, label, text, fallback,
                                0, 0, waited)
                if raise_on_error:
                    raise exc
                return result

            session = AsyncSession(self, conn, ref, all_refs, exec_id, timed,
                                   label, keys)
            start_wall = time.time()
            start = time.perf_counter()
            try:
                result.value = await self._run_script_async(script, session,
                                                            params, self)
            except UnsupportedOperation as e:
                result.status, result.error, exc = OpStatus.UNSUPPORTED, e.reason or str(e), e
            except Exception as e:
                result.status, result.error, exc = OpStatus.FAILED, f"{type(e).__name__}: {e}", e
            finally:
                result.latency = time.perf_counter() - start
                await self._release_async_conn(conn, pooled)
        result.statements = session.statements
        result.storage_after = self._safe_storage() if storage else 0
        self._emit_exec(result, timed, exec_id, label, text, fallback,
                        start_wall, time.time(), waited)
        if raise_on_error and exc is not None:
            raise exc
        return result

    # ------------------------------------------------------------------
    # Connection utilities
    # ------------------------------------------------------------------

    def get_current_connection(self):
        return self.conn

    def close_connection(self) -> None:
        if self.conn:
            try:
                self.conn.close()
            finally:
                self.conn = None
                self._current_ref = None

    def _get_table_columns(self, table_name: str) -> list:
        """(column_name, type_name, is_nullable, char_max_length,
        numeric_precision, numeric_scale) per column, in column order.
        Backends whose information_schema differs from Postgres override."""
        query = """
        SELECT
            column_name,
            udt_name,
            is_nullable,
            character_maximum_length,
            numeric_precision,
            numeric_scale
        FROM
            information_schema.columns
        WHERE
            table_name = %s
        ORDER BY
            ordinal_position;
        """
        return self._execute(query, (table_name,))

    def get_table_schema(self, table_name: str) -> str:
        """Schema of ``table_name`` in a CREATE TABLE format."""
        columns = self._get_table_columns(table_name)
        if not columns:
            raise Exception(f"Error: Table '{table_name}' not found.")
        column_definitions = []
        for (col_name, udt_name, is_nullable, char_len, num_prec,
             num_scale) in columns:
            data_type = udt_name
            if char_len is not None:
                data_type += f"({char_len})"
            elif udt_name in ("numeric", "decimal") and num_prec is not None:
                data_type += f"({num_prec}, {num_scale})"
            definition = f"  {col_name} {data_type}"
            if is_nullable == "NO":
                definition += " NOT NULL"
            column_definitions.append(definition)
        return "CREATE TABLE {} (\n{}\n);".format(
            table_name, ",\n".join(column_definitions)
        )
