"""Collects one Result row per measured operation and writes them to parquet.

Rows are produced by DBToolSuite (see dblib/db_api.py) through emit(). The
collector also keeps per-thread (or per-asyncio-task) driver context such as
the table under test, the current macrobench step and the number of keys the
next operation touches, which is copied into every row.
"""

import os
import uuid
import time
import threading
import asyncio
import weakref
from contextlib import contextmanager
import pyarrow as pa
import pyarrow.parquet as pq
from dblib import result_pb2 as rslt
from util.sql_parse import get_sql_operation_keyword


# Thread-local storage for thread_id
_thread_local = threading.local()

# Branch operations whose rows count as "operations" for throughput (as opposed to the
# per-statement rows inside an exec, which are a breakdown of one EXEC row).
VERB_OP_TYPES = frozenset(
    {
        rslt.OpType.BRANCH,
        rslt.OpType.COMMIT,
        rslt.OpType.DIFF,
        rslt.OpType.LOG,
        rslt.OpType.MERGE,
        rslt.OpType.REBASE,
        rslt.OpType.REVERT,
        rslt.OpType.RESET,
        rslt.OpType.DELETE,
        rslt.OpType.SCHEMA_DIFF,
        rslt.OpType.EXEC,
    }
)

# Per-statement rows emitted from inside an exec() script.
STATEMENT_OP_TYPES = frozenset(
    {rslt.OpType.READ, rslt.OpType.INSERT, rslt.OpType.UPDATE, rslt.OpType.DELETE_ROWS,
     rslt.OpType.DDL}
)


class _OperationState:
    """Driver context for the current thread or asyncio task."""

    def __init__(self):
        self.current_table_name = ""
        self.current_table_schema = ""
        self.initial_db_size = 0
        self.seed = 0
        self.num_keys_touched = 0
        self.branch_count = 0
        self.step_id = -1
        self.pool_wait_time = 0.0


def set_current_thread_id(thread_id: int) -> None:
    """Set the thread ID for the current thread (once per worker thread)."""
    _thread_local.thread_id = thread_id


def get_current_thread_id() -> int:
    """Thread ID for the current thread, 0 if not set."""
    return getattr(_thread_local, "thread_id", 0)


def GetOpTypeFromSQL(sql: str) -> rslt.OpType:
    """Operation type of a SQL statement (first statement, CTEs unwrapped)."""
    keyword = get_sql_operation_keyword(sql)
    if not keyword:
        return rslt.OpType.UNSPECIFIED
    keyword_map = {
        "SELECT": rslt.OpType.READ,
        "INSERT": rslt.OpType.INSERT,
        "UPDATE": rslt.OpType.UPDATE,
        "DELETE": rslt.OpType.DELETE_ROWS,
        "WITH": rslt.OpType.READ,
        "CREATE": rslt.OpType.DDL,
        "ALTER": rslt.OpType.DDL,
        "DROP": rslt.OpType.DDL,
        "VACUUM": rslt.OpType.DDL,
    }
    return keyword_map.get(keyword, rslt.OpType.UNSPECIFIED)


def str_to_op_type(op_str: str) -> rslt.OpType:
    """OpType for its enum name (case-insensitive), UNSPECIFIED if unknown."""
    try:
        return rslt.OpType[op_str.upper().strip()]
    except KeyError:
        return rslt.OpType.UNSPECIFIED


# Parquet schema of a Result row (fixed, so spilled row groups match).
RESULT_SCHEMA = pa.schema(
    [
        ("run_id", pa.string()),
        ("thread_id", pa.int64()),
        ("random_seed", pa.int64()),
        ("iteration_number", pa.int64()),
        ("op_type", pa.int64()),
        ("op_name", pa.string()),
        ("status", pa.string()),
        ("ref", pa.string()),
        ("refs", pa.list_(pa.string())),
        ("label", pa.string()),
        ("exec_id", pa.int64()),
        ("initial_db_size", pa.int64()),
        ("table_name", pa.string()),
        ("table_schema", pa.string()),
        ("num_keys_touched", pa.int64()),
        ("latency", pa.float64()),
        ("disk_size_before", pa.int64()),
        ("disk_size_after", pa.int64()),
        ("sql_query", pa.string()),
        ("error_message", pa.string()),
        ("commit_ref_fallback", pa.bool_()),
        ("branch_count", pa.int64()),
        ("step_id", pa.int64()),
        ("start_time", pa.float64()),
        ("end_time", pa.float64()),
        ("pool_wait_time", pa.float64()),
    ]
)


def _row_dict(result) -> dict:
    return {
        "run_id": result.run_id,
        "thread_id": result.thread_id,
        "random_seed": result.random_seed,
        "iteration_number": result.iteration_number,
        "op_type": result.op_type,
        "op_name": rslt.OpType.Name(result.op_type),
        "status": rslt.OpStatus.Name(result.status),
        "ref": result.ref,
        "refs": list(result.refs),
        "label": result.label,
        "exec_id": result.exec_id,
        "initial_db_size": result.initial_db_size,
        "table_name": result.table_name,
        "table_schema": result.table_schema,
        "num_keys_touched": result.num_keys_touched,
        "latency": result.latency,
        "disk_size_before": result.disk_size_before,
        "disk_size_after": result.disk_size_after,
        "sql_query": result.sql_query,
        "error_message": result.error_message,
        "commit_ref_fallback": result.commit_ref_fallback,
        "branch_count": result.branch_count,
        "step_id": result.step_id,
        "start_time": result.start_time,
        "end_time": result.end_time,
        "pool_wait_time": result.pool_wait_time,
    }


def _status_bucket(status) -> str:
    if status == rslt.OpStatus.OK:
        return "ok"
    if status == rslt.OpStatus.UNSUPPORTED:
        return "unsupported"
    return "failed"


class ResultCollector:
    def __init__(
        self,
        run_id: str = None,
        output_dir: str = "/tmp/run_stats",
        spill_rows: int = 0,
    ):
        """``spill_rows`` > 0 bounds memory: once that many rows are held,
        they are written to a partial parquet file next to the final one
        and dropped from ``results`` (which then only holds the tail).
        A long run with a spine load emits millions of rows; held as
        protobufs they cost a few KB each."""
        self.run_id = run_id or str(uuid.uuid4())
        self.output_dir = output_dir
        self.spill_rows = max(0, int(spill_rows or 0))
        # Spilled batches are written outside self._lock (a write takes a
        # second or two), serialized by this lock.
        self._spill_lock = threading.Lock()
        self._writer = None
        self._spill_path = None
        self._spilled_rows = 0
        # Per-op-type status counts over every row emitted (spilled or not)
        self._status_counts: dict = {}
        # Operations in flight: token -> {op, ref, label, thread_id, start,
        # cancel (callable or None), note (set when a watchdog cancelled it)}
        self._in_flight: dict = {}
        self._next_token = 1
        # Operations a watchdog cancelled, with their outcome
        self.stalls: list = []

        self._lock = threading.Lock()

        # Per-thread driver context
        self._thread_local = threading.local()

        # Per-asyncio-task driver context, shared by every worker thread's
        # event loop. Weakly keyed so a task's entry is dropped with the task.
        self._task_local = weakref.WeakKeyDictionary()

        # Shared results (protected by _lock)
        self.results = []
        self.iteration_counter = 0
        self._next_exec_id = 1

        # Driver-level failures (an operation raised in the runner)
        self.failed_operations = []

        # Branch operations some backend reported as unsupported during this run, as
        # "OPTYPE" or "OPTYPE:reason". Any entry marks the workflow as not
        # fully supported on this backend.
        self.unsupported_ops: set[str] = set()
        # Called as on_unsupported(op_name, reason) right after an
        # UNSUPPORTED row is recorded. The macrobench installs a hook that
        # ends the run there; None leaves the caller to inspect the result.
        self.on_unsupported = None

        os.makedirs(output_dir, exist_ok=True)

    # ------------------------------------------------------------------
    # Driver context
    # ------------------------------------------------------------------

    def _get_thread_state(self) -> _OperationState:
        """Context for the current asyncio task if there is one, else for
        the current thread."""
        try:
            task = asyncio.current_task()
            if task is not None:
                with self._lock:
                    state = self._task_local.get(task)
                    if state is None:
                        state = self._task_local[task] = _OperationState()
                    return state
        except RuntimeError:
            pass
        if not hasattr(self._thread_local, "state"):
            self._thread_local.state = _OperationState()
        return self._thread_local.state

    def set_recording(self, enabled: bool) -> None:
        """Turn recording on or off for the calling thread only.

        While off, emit() and record_failure() drop their data. Used for
        warm-up ops. Async tasks run on their worker's thread, so this
        covers them too.
        """
        self._thread_local.recording_disabled = not enabled

    def _recording_enabled(self) -> bool:
        return not getattr(self._thread_local, "recording_disabled", False)

    def reset(self):
        """Drop all collected rows (shared state only), spilled ones included."""
        with self._lock:
            self.results = []
            self.iteration_counter = 0
            self.failed_operations = []
            self.unsupported_ops = set()
            self._status_counts = {}
        with self._spill_lock:
            path = self._close_spill()
            self._spilled_rows = 0
            if path and os.path.exists(path):
                os.remove(path)

    @property
    def total_rows(self) -> int:
        """Rows emitted so far, spilled ones included."""
        with self._lock:
            held = len(self.results)
        with self._spill_lock:
            return self._spilled_rows + held

    # -- spilling (under self._spill_lock, never under self._lock) --------

    def _write_batch(self, batch: list) -> None:
        with self._spill_lock:
            self._write_batch_locked(batch)

    def _write_batch_locked(self, batch: list) -> None:
        if not batch:
            return
        if self._writer is None:
            os.makedirs(self.output_dir, exist_ok=True)
            self._spill_path = os.path.join(self.output_dir, f"{self.run_id}.parquet.partial")
            self._writer = pq.ParquetWriter(self._spill_path, RESULT_SCHEMA)
        self._writer.write_table(
            pa.Table.from_pylist([_row_dict(r) for r in batch], schema=RESULT_SCHEMA))
        self._spilled_rows += len(batch)

    def _close_spill(self):
        """Finish the partial file (caller holds _spill_lock); returns its
        path, None if nothing was spilled."""
        if self._writer is None:
            return None
        self._writer.close()
        self._writer = None
        path, self._spill_path = self._spill_path, None
        return path

    def set_context(
        self,
        table_name: str,
        table_schema: str,
        initial_db_size: int,
        seed: int,
    ):
        """Driver context copied into every row emitted by this thread."""
        state = self._get_thread_state()
        state.current_table_name = table_name
        state.current_table_schema = table_schema
        state.initial_db_size = initial_db_size
        state.seed = seed

    def record_num_keys_touched(self, num_keys: int) -> None:
        """Keys the next emitted row touches (consumed by that row)."""
        self._get_thread_state().num_keys_touched = num_keys

    def record_pool_wait(self, seconds: float) -> None:
        """Pool wait attributed to the next emitted row (consumed by it)."""
        self._get_thread_state().pool_wait_time += seconds

    def record_branch_count(self, branch_count: int) -> None:
        self._get_thread_state().branch_count = branch_count

    def record_step_id(self, step_id: int) -> None:
        self._get_thread_state().step_id = step_id

    # ------------------------------------------------------------------
    # Operations in flight (for a stall watchdog)
    # ------------------------------------------------------------------

    def begin_op(self, op_name: str, ref: str = "", label: str = "", cancel=None) -> int:
        """Register an operation that just started; returns its token.
        ``cancel()`` is what a watchdog may call to abort it."""
        with self._lock:
            token = self._next_token
            self._next_token += 1
            self._in_flight[token] = {"op": op_name, "ref": ref or "", "label": label or "",
                                      "thread_id": get_current_thread_id(), "start": time.time(),
                                      "cancel": cancel, "note": None}
        return token

    def end_op(self, token: int, failed: bool = False):
        """Unregister an operation; returns the watchdog's note if it was
        cancelled while running, else None. ``failed`` records the
        outcome of a cancelled operation (one that finished anyway stays
        OK and is listed as "completed")."""
        with self._lock:
            entry = self._in_flight.pop(token, None)
            if entry and entry["note"] is not None and entry.get("stall") is not None:
                entry["stall"]["outcome"] = "failed" if failed else "completed"
        return entry["note"] if entry else None

    def cancel_op(self, token: int, note: str) -> bool:
        """Abort an operation in flight (its cancel callback, outside the
        lock), attach ``note`` to it and list it in ``stalls``. False if
        it already finished."""
        with self._lock:
            entry = self._in_flight.get(token)
            if entry is None:
                return False
            entry["note"] = note
            entry["stall"] = {"op": entry["op"], "ref": entry["ref"], "label": entry["label"],
                              "thread_id": entry["thread_id"],
                              "elapsed_sec": round(time.time() - entry["start"], 1),
                              "time": round(time.time(), 3), "note": note, "outcome": "pending"}
            self.stalls.append(entry["stall"])
            cancel = entry["cancel"]
        if cancel is not None:
            try:
                cancel()
            except Exception:
                pass
        return True

    def in_flight_ops(self) -> list:
        """Operations running now, with their elapsed seconds and token."""
        now = time.time()
        with self._lock:
            items = [(t, dict(e)) for t, e in self._in_flight.items()]
        return [{"token": t, "op": e["op"], "ref": e["ref"], "label": e["label"],
                 "thread_id": e["thread_id"], "elapsed_sec": now - e["start"],
                 "cancelled": e["note"] is not None} for t, e in items]

    def stalled_ops(self, threshold_sec: float) -> list:
        """Operations in flight for longer than ``threshold_sec``."""
        return [o for o in self.in_flight_ops() if o["elapsed_sec"] > threshold_sec]

    def next_exec_id(self) -> int:
        with self._lock:
            exec_id = self._next_exec_id
            self._next_exec_id += 1
            return exec_id

    # ------------------------------------------------------------------
    # Rows
    # ------------------------------------------------------------------

    def emit(self, op_type: rslt.OpType, status: rslt.OpStatus = rslt.OpStatus.OK, *args, **kw) -> None:
        """Append one Result row (see _emit). An UNSUPPORTED row then
        reaches ``on_unsupported`` when a hook is set; the hook may raise,
        which ends the caller's operation with the row already recorded."""
        self._emit(op_type, status, *args, **kw)
        if status == rslt.OpStatus.UNSUPPORTED and self.on_unsupported is not None:
            self.on_unsupported(rslt.OpType.Name(op_type), kw.get("error_message") or "")

    def _emit(
        self,
        op_type: rslt.OpType,
        status: rslt.OpStatus = rslt.OpStatus.OK,
        latency: float = 0.0,
        start_time: float = 0.0,
        end_time: float = 0.0,
        ref: str = "",
        refs=None,
        label: str = "",
        exec_id: int = 0,
        sql_query: str = "",
        error_message: str = "",
        disk_size_before: int = 0,
        disk_size_after: int = 0,
        commit_ref_fallback: bool = False,
        num_keys_touched: int = None,
        pool_wait_time: float = None,
    ) -> None:
        """Append one Result row, filled from the arguments plus the calling
        thread's driver context. Consumes the pending num_keys_touched and
        pool_wait_time of that context."""
        if status == rslt.OpStatus.UNSUPPORTED:
            name = rslt.OpType.Name(op_type)
            with self._lock:
                self.unsupported_ops.add(
                    f"{name}:{error_message}" if error_message else name
                )

        state = self._get_thread_state()
        if not self._recording_enabled():
            state.num_keys_touched = 0
            state.pool_wait_time = 0.0
            return

        try:
            result = rslt.Result()
            result.run_id = self.run_id
            result.table_name = state.current_table_name
            result.table_schema = state.current_table_schema
            result.initial_db_size = state.initial_db_size
            result.random_seed = state.seed
            result.branch_count = state.branch_count
            result.step_id = state.step_id
            result.thread_id = get_current_thread_id()

            result.op_type = op_type
            result.status = status
            result.latency = latency
            result.start_time = start_time
            result.end_time = end_time
            result.ref = ref or ""
            if refs:
                result.refs.extend(str(r) for r in refs)
            result.label = label or ""
            result.exec_id = exec_id
            result.sql_query = sql_query or ""
            result.error_message = error_message or ""
            result.disk_size_before = disk_size_before
            result.disk_size_after = disk_size_after
            result.commit_ref_fallback = commit_ref_fallback
            result.num_keys_touched = (
                state.num_keys_touched
                if num_keys_touched is None
                else num_keys_touched
            )
            result.pool_wait_time = (
                state.pool_wait_time if pool_wait_time is None else pool_wait_time
            )

            with self._lock:
                result.iteration_number = self.iteration_counter
                self.results.append(result)
                self.iteration_counter += 1
                c = self._status_counts.setdefault(
                    rslt.OpType.Name(op_type), {"ok": 0, "unsupported": 0, "failed": 0})
                c[_status_bucket(status)] += 1
                batch = None
                if self.spill_rows and len(self.results) >= self.spill_rows:
                    batch, self.results = self.results, []
            if batch:
                self._write_batch(batch)
        except Exception as e:
            import sys
            import traceback

            print(f"ERROR in emit: {type(e).__name__}: {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
        finally:
            state.num_keys_touched = 0
            state.pool_wait_time = 0.0

    @contextmanager
    def timed(self, op_type: rslt.OpType, **fields):
        """Time a block of driver code and emit one row for it, e.g. the
        back-off wait before retrying a rate-limited API call."""
        start_perf = time.perf_counter()
        start_wall = time.time()
        try:
            yield
        finally:
            end_perf = time.perf_counter()
            self.emit(
                op_type,
                latency=end_perf - start_perf,
                start_time=start_wall,
                end_time=time.time(),
                **fields,
            )

    def record_failure(self, error: Exception, operation_number: int = None) -> None:
        """Record a failure raised by the driver (not a FAILED row)."""
        if not self._recording_enabled():
            return
        failure_info = {
            "thread_id": get_current_thread_id(),
            "error_type": type(error).__name__,
            "error_message": str(error),
            "operation_number": operation_number,
            "timestamp": time.time(),
        }
        with self._lock:
            self.failed_operations.append(failure_info)

    # ------------------------------------------------------------------
    # Summaries
    # ------------------------------------------------------------------

    @property
    def workflow_supported(self) -> bool:
        """False once any operation of this run was UNSUPPORTED."""
        return not self.unsupported_ops

    def support_summary(self) -> dict:
        """Support information for the run's summary JSON."""
        with self._lock:
            counts = {k: dict(v) for k, v in self._status_counts.items()}
            unsupported = sorted(self.unsupported_ops)
        return {
            "workflow_supported": not unsupported,
            "unsupported_ops": unsupported,
            "op_status_counts": counts,
        }

    def write_to_parquet(self, filename: str = None, append: bool = True):
        """Write all collected rows to a parquet file, appending if it
        exists (``append=False`` replaces it: one file per run). Rows
        spilled during the run are moved into the file row group by row
        group, never all in memory at once."""
        with self._lock:
            held, self.results = self.results, []
        with self._spill_lock:
            if self._writer is not None:
                self._write_batch_locked(held)   # the tail joins the partial file
                held = []
            spilled = self._close_spill()
            n_spilled, self._spilled_rows = self._spilled_rows, 0
        if not held and not spilled:
            print("No results to write.")
            return

        filename = filename or f"{self.run_id}.parquet"
        filepath = os.path.join(self.output_dir, filename)
        os.makedirs(self.output_dir, exist_ok=True)

        if spilled:
            if append and os.path.exists(filepath):
                tmp = filepath + ".tmp"
                total = 0
                with pq.ParquetWriter(tmp, RESULT_SCHEMA) as w:
                    for src in (filepath, spilled):
                        pf = pq.ParquetFile(src)
                        for i in range(pf.num_row_groups):
                            t = pf.read_row_group(i).cast(RESULT_SCHEMA)
                            w.write_table(t)
                            total += len(t)
                os.replace(tmp, filepath)
                os.remove(spilled)
                print(f"Appended {n_spilled} results to {filepath} (total: {total} rows)")
            else:
                os.replace(spilled, filepath)
                print(f"Wrote {n_spilled} benchmark results to {filepath}")
            return

        new_table = pa.Table.from_pylist([_row_dict(r) for r in held], schema=RESULT_SCHEMA)
        if append and os.path.exists(filepath):
            try:
                existing_table = pq.read_table(filepath)
                combined_table = pa.concat_tables(
                    [existing_table, new_table], promote_options="default"
                )
                pq.write_table(combined_table, filepath)
                print(
                    f"Appended {len(new_table)} results to {filepath} "
                    f"(total: {len(combined_table)} rows)"
                )
            except Exception as e:
                print(f"Error reading existing file, overwriting: {e}")
                pq.write_table(new_table, filepath)
                print(f"Wrote {len(new_table)} benchmark results to {filepath}")
        else:
            pq.write_table(new_table, filepath)
            print(f"Wrote {len(new_table)} benchmark results to {filepath}")
