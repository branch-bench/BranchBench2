"""Scenario framework for the macrobenchmark.

A Scenario is one of the six workflows (S1-S6). It gets a ScenarioContext
with the parsed config, a DBToolSuite factory, the shared ResultCollector,
the invariant recorder and the stop event, and drives the backend through
the git-like verbs and exec(). A FAILED result never stops the workload:
scenarios read the OpResult, record what they can and go on, as an agent
that catches exceptions would. An UNSUPPORTED result ends the run: the
context installs a hook on the result collector that records the verb,
cancels every worker and raises UnsupportedOperationAbort, and the runner
reports the run as unsupported naming the verb. A scenario therefore never
sees an unsupported result, and a check is not applicable only when its
own precondition did not arise (no commit to revert, a target after the
fault, and so on).
"""

import random
import threading
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Callable, Optional

from dblib import result_collector as rc
from dblib import result_pb2 as rslt
from dblib.db_api import DBToolSuite, OpResult
from macrobench import task_pb2 as tp
from macrobench.datagen.ch import CHScale


class ScenarioStopped(Exception):
    """Raised inside workers when the runtime cap hits."""


class UnsupportedOperationAbort(ScenarioStopped):
    """The backend reported a verb unsupported; the run ends there."""


class InvariantFailed(Exception):
    """Raised by the recorder in fail_fast mode."""


@dataclass
class InvariantResult:
    name: str
    passed: Optional[bool]   # None = not applicable (its precondition did not arise)
    detail: str = ""


class InvariantRecorder:
    """Collects sentinel checks. Thread-safe."""

    def __init__(self, enabled: bool = True, fail_fast: bool = False):
        self.enabled = enabled
        self.fail_fast = fail_fast
        self.results: list = []
        self._lock = threading.Lock()

    def check(self, name: str, passed: Optional[bool], detail: str = "") -> bool:
        if not self.enabled:
            return True
        with self._lock:
            self.results.append(InvariantResult(name, passed, detail))
        if passed is False and self.fail_fast:
            raise InvariantFailed(f"{name}: {detail}")
        return bool(passed)

    def expect(self, name: str, actual, expected, detail: str = "") -> bool:
        ok = actual == expected
        return self.check(name, ok, f"expected {expected!r}, got {actual!r}" + (f" ({detail})" if detail else ""))

    def not_applicable(self, name: str, reason: str) -> None:
        self.check(name, None, reason)

    def summary(self) -> dict:
        rs = self.results
        return {
            "enabled": self.enabled,
            "checked": sum(1 for r in rs if r.passed is not None),
            "passed": sum(1 for r in rs if r.passed is True),
            "failed": sum(1 for r in rs if r.passed is False),
            "not_applicable": sum(1 for r in rs if r.passed is None),
            "all_passed": all(r.passed is not False for r in rs),
            "results": [
                {"name": r.name, "passed": r.passed, "detail": r.detail} for r in rs
            ],
        }


@dataclass
class WorkSummary:
    """Thread-safe logical work outcomes for one scenario run."""

    expected: int = 0
    succeeded: int = 0
    failed: int = 0
    skipped: int = 0
    worker_errors: list = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def record(self, outcome: str, count: int = 1) -> None:
        if outcome not in ("succeeded", "failed", "skipped"):
            raise ValueError(f"unknown work outcome: {outcome}")
        with self._lock:
            setattr(self, outcome, getattr(self, outcome) + count)

    def record_worker_error(self, worker: int, item, error: Exception) -> None:
        with self._lock:
            self.worker_errors.append({
                "worker": int(worker),
                "item": str(item)[:120],
                "error": f"{type(error).__name__}: {str(error)[:240]}",
            })

    def summary(self) -> dict:
        with self._lock:
            return {
                "expected": int(self.expected),
                "succeeded": int(self.succeeded),
                "failed": int(self.failed),
                "skipped": int(self.skipped),
            }


def is_retryable_error(msg: str) -> bool:
    """Errors worth retrying after a wait: Neon/Xata rate- and
    resource-limit errors, a connection the server dropped (a compute
    restarted or suspended under it), and Bauplan's branch moving while an
    operation ran. A verb that failed this way left no partial state (the
    composed merges run in one transaction, or on a temporary branch), so
    the retry repeats the whole operation; the failed attempt stays recorded."""
    msg = (msg or "").lower()
    return any(p in msg for p in (
        "429", "too many", "running operations", "branches limit",
        "endpoints limit", "limit reached",
        "ssl connection has been closed", "connection already closed",
        "server closed the connection", "ssl syscall error",
        "terminating connection", "could not connect to server",
        # Bauplan: the branch moved while the operation ran.
        "branch_head_changed",
    ))


@dataclass
class ScenarioContext:
    config: tp.MacroBenchConfig
    suite_factory: Callable[[], DBToolSuite]
    collector: rc.ResultCollector
    spine: str                      # the root/default branch name
    invariants: InvariantRecorder
    progress: object = None         # SharedProgress or None
    stop_event: threading.Event = field(default_factory=threading.Event)
    stop_reason: str = ""
    unsupported_abort: str = ""     # "VERB: reason" of the unsupported verb that ended the run
    log: Callable[[str], None] = print
    metrics: dict = field(default_factory=dict)
    worker_conns: dict = field(default_factory=dict)
    work: WorkSummary = field(default_factory=WorkSummary)
    _slots: Optional[threading.BoundedSemaphore] = None
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def __post_init__(self):
        self.seed = self.config.seed or 42
        self.rng = random.Random(self.seed)
        self.scale = CHScale.from_config(self.config.schema)
        self.params = getattr(self.config.workload, self.scenario_key)
        self.branch_ops = self.config.workload.branch_ops
        self.data_ops = self.config.workload.data_ops
        cap = self.branch_ops.max_live_branches
        self._slots = threading.BoundedSemaphore(cap) if cap > 0 else None
        self._branch_seq = 0
        self.collector.on_unsupported = self._abort_on_unsupported

    def _abort_on_unsupported(self, op: str, reason: str) -> None:
        """Collector hook: the first unsupported verb ends the run. Runs on
        the thread that issued the verb, after its row was recorded."""
        what = f"{op}: {reason}" if reason else op
        with self._lock:
            first = not self.unsupported_abort
            if first:
                self.unsupported_abort = what
        if first:
            self.cancel_all(f"unsupported operation {what}")
        raise UnsupportedOperationAbort(what)

    # -- config helpers ----------------------------------------------------

    @property
    def scenario_key(self) -> str:
        return self.config.workload.WhichOneof("scenario")

    @property
    def commit_interval(self) -> int:
        return max(1, self.branch_ops.commit_interval)

    @property
    def statements_per_step(self) -> int:
        return max(1, self.data_ops.statements_per_step)

    @property
    def rows_per_write(self) -> int:
        return max(1, self.data_ops.rows_per_write)

    @property
    def worker_threads(self) -> int:
        return max(1, self.data_ops.worker_threads)

    def worker_rng(self, index: int) -> random.Random:
        return random.Random(self.seed * 7919 + index)

    def should_commit(self, step: int, last_step: bool = False) -> bool:
        """Commit after this (0-based) step per commit_interval."""
        return last_step or (step + 1) % self.commit_interval == 0

    # -- stop / progress ---------------------------------------------------

    def check_stop(self) -> None:
        if self.stop_event.is_set():
            if self.unsupported_abort:
                raise UnsupportedOperationAbort(self.unsupported_abort)
            raise ScenarioStopped()

    def tick(self, n: int = 1, outcome: str = "succeeded") -> None:
        """Account ``n`` units of logical work as ``outcome`` (succeeded,
        failed or skipped) and advance the progress bar."""
        self.work.record(outcome, n)
        if self.progress is not None:
            self.progress.update(n)

    def note(self, msg: str) -> None:
        text = str(msg or "")
        if self.progress is not None:
            self.progress.write(text)
        else:
            self.log(text)

    def add_metric(self, key: str, value) -> None:
        with self._lock:
            self.metrics[key] = value

    def bump_metric(self, key: str, by: int = 1) -> None:
        with self._lock:
            self.metrics[key] = self.metrics.get(key, 0) + by

    # -- suites and workers ------------------------------------------------

    def new_suite(self) -> DBToolSuite:
        suite = self.suite_factory()
        with self._lock:
            self.worker_conns[id(suite)] = suite
        return suite

    def close_suite(self, suite: DBToolSuite) -> None:
        with self._lock:
            self.worker_conns.pop(id(suite), None)
        try:
            suite.close_connection()
        except Exception:
            pass

    def cancel_all(self, reason: str = "") -> None:
        """Stop the scenario (deadline, stall watchdog, signal): set the
        stop event and cancel in-flight statements on every suite."""
        if reason and not self.stop_reason:
            self.stop_reason = reason
        self.stop_event.set()
        with self._lock:
            suites = list(self.worker_conns.values())
        for s in suites:
            try:
                s.conn.cancel()
            except Exception:
                pass

    def run_workers(self, items: list, fn: Callable, threads: int = None,
                    thread_base: int = 1) -> list:
        """Run ``fn(worker, item)`` over ``items`` on ``threads`` workers, each
        with its own suite (``worker.suite``), rng (``worker.rng``) and id.
        Returns the list of return values (or exceptions) in item order.
        A worker opens its suite on its first item and closes it on its own
        thread once the items run out, whatever the items did, so every
        suite this call opened is closed exactly once before it returns.
        The scenario accounts its own units through tick(); a worker that
        raises unexpectedly is recorded as a worker error only. A failed
        check under fail_fast stops the other workers and is raised here
        once every worker suite is closed."""
        threads = threads or self.worker_threads
        threads = max(1, min(threads, len(items) or 1))
        results = [None] * len(items)
        pending = list(enumerate(items))

        def next_item():
            with self._lock:
                return pending.pop(0) if pending else None

        def run_item(w, i, item):
            try:
                results[i] = fn(w, item)
            except ScenarioStopped as e:
                results[i] = e
            except InvariantFailed as e:
                results[i] = e
                self.cancel_all(f"invariant failed: {e}")
            except Exception as e:
                results[i] = e
                self.work.record_worker_error(w.id, item, e)
                self.note(f"[W{w.id}] {type(e).__name__}: {e}")

        def work():
            w = None
            try:
                while (pair := next_item()) is not None:
                    if w is None:
                        with self._lock:
                            idx = self._worker_counter = getattr(self, "_worker_counter", 0) + 1
                        rc.set_current_thread_id(thread_base + idx)
                        w = Worker(self, thread_base + idx, self.new_suite(), self.worker_rng(idx))
                    run_item(w, *pair)
            finally:
                if w is not None:
                    self.close_suite(w.suite)

        if threads == 1:
            # Same thread as the caller keeps suites/thread ids simple.
            work()
        else:
            with ThreadPoolExecutor(max_workers=threads) as pool:
                for f in [pool.submit(work) for _ in range(threads)]:
                    f.result()
        return self._pool_done(results)

    def _pool_done(self, results: list) -> list:
        """Every worker suite is closed. An unsupported verb or a failed
        check seen by a worker ends the run on the caller's thread too, so
        the scenario does not go on without it."""
        if self.unsupported_abort:
            raise UnsupportedOperationAbort(self.unsupported_abort)
        for r in results:
            if isinstance(r, InvariantFailed):
                raise r
        return results

    # -- branch helpers ----------------------------------------------------

    def next_branch_name(self, prefix: str) -> str:
        with self._lock:
            self._branch_seq += 1
            return f"{prefix}_{self._branch_seq}"

    def retry(self, fn: Callable[[], OpResult], max_retries: int = 8,
              base_delay: float = 1.0) -> OpResult:
        """Call ``fn`` again with backoff while it FAILS with a retryable
        error (see is_retryable_error); each wait is an API_RETRY_WAIT
        row labelled "retry", and every attempt's own row stays."""
        for attempt in range(max_retries):
            result = fn()
            if not result.failed or attempt == max_retries - 1:
                return result
            if not is_retryable_error(result.error):
                return result
            delay = base_delay * (2 ** attempt) * (0.5 + random.random())
            with self.collector.timed(rslt.OpType.API_RETRY_WAIT, label="retry"):
                if self.stop_event.wait(delay):
                    raise ScenarioStopped()
        return result

    def branch(self, suite: DBToolSuite, name: str, from_ref, label: str = "branch") -> OpResult:
        """branch() with slot accounting and rate-limit retry."""
        if self._slots is not None:
            while not self._slots.acquire(timeout=1.0):
                self.check_stop()
        res = self.retry(lambda: suite.branch(name, from_ref=from_ref, label=label))
        if not res.ok and self._slots is not None:
            self._slots.release()
        return res

    def delete(self, suite: DBToolSuite, name: str, label: str = "delete") -> OpResult:
        """delete() honouring keep_branches; frees the branch slot."""
        if self.branch_ops.keep_branches:
            return OpResult(op="delete", ref=name, note="kept")
        res = self.retry(lambda: suite.delete(name, label=label))
        if self._slots is not None:
            try:
                self._slots.release()
            except ValueError:
                pass
        return res

    def cross_branch_exec(self, suite: DBToolSuite, script_for, refs: list,
                          label: str) -> list:
        """Run a cross-branch read: one multi-ref exec() when the backend
        has multi-branch query semantics, else the script once per ref.
        ``script_for(multi: bool)`` returns the script to run."""
        if suite.SUPPORTS_MULTI_REF_EXEC and len(refs) > 1:
            self.add_metric("cross_branch_mode", "multi")
            return suite.exec(script_for(True), refs=refs, mode="multi", label=label)
        self.add_metric("cross_branch_mode", "per_ref")
        return suite.exec(script_for(False), refs=refs, mode="per_ref", label=label)


@dataclass
class Worker:
    ctx: ScenarioContext
    id: int
    suite: DBToolSuite
    rng: random.Random

    def exec(self, script, ref, label: str = "", **kw):
        """exec() on one ref; returns the ExecResult."""
        return self.suite.exec(script, refs=[ref], label=label, **kw)[0]

    def commit(self, ref, message: str, label: str = "commit") -> OpResult:
        return self.suite.commit(ref, message, label=label)


class Scenario(ABC):
    """One benchmark workflow. Subclasses set ``key`` (the Workload oneof
    field), ``name`` and ``extension`` and implement run()."""

    key: str = ""
    name: str = ""
    # Does the scenario run the TPC-C spine load when spine_clients > 0?
    uses_spine_load: bool = False
    # Payment transactions rewrite customer.c_data (S4 rebase conflicts).
    spine_touches_c_data: bool = False

    def __init__(self, ctx: ScenarioContext):
        self.ctx = ctx
        self.params = ctx.params
        self.spine_load = None  # set by the runner when started

    def seed(self, suite: DBToolSuite) -> dict:
        """Extension-table seed data on the spine (untimed). Returns
        {table: rows} for the e2e stats."""
        return {}

    def total_units(self) -> int:
        """Progress-bar units run() will tick."""
        return 0

    @abstractmethod
    def run(self) -> None:
        """The scenario lifecycle."""

    def validate(self) -> None:
        """Reject configs the scenario cannot run (raise ValueError)."""

    # -- helpers shared by scenarios --------------------------------------

    def quiesced(self):
        """Context manager pausing the spine load (no-op without one) so a
        spine commit is transaction-consistent."""
        if self.spine_load is not None:
            return self.spine_load.pause()
        return _NullContext()


class _NullContext:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


SCENARIOS: dict = {}


def register(cls):
    SCENARIOS[cls.key] = cls
    return cls


def create_scenario(ctx: ScenarioContext) -> Scenario:
    key = ctx.scenario_key
    if key not in SCENARIOS:
        raise ValueError(f"No scenario for workload.{key}; known: {sorted(SCENARIOS)}")
    return SCENARIOS[key](ctx)
