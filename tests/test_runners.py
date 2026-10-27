"""The three runners on the fake backend through the prepared-target seam:
the embedding application provisions a database and passes a
PreparedTarget; the runners never choose or build a database system."""

import json
import signal
import threading
from unittest import mock

import pytest
from google.protobuf import text_format

from dblib import result_collector as rc
from dblib.db_api import Ref
from dblib.target import PreparedTarget, SuiteRequest
from macrobench import faults
from macrobench import runner as macro_runner
from macrobench import task_pb2 as mtp
from macrobench.scenarios import base as scenario_base
from macrobench.scenarios.s1_rl_env import RlEnvScenario
from macrobench.datagen.ch import CHScale
from microbench import runner2, task2_pb2 as tp
from microbench.verbs import driver
from tests.fake_backend import FakeSuite
from tests.test_scenarios import CONFIGS


class SchemaFakeSuite(FakeSuite):
    """The microbench runner reads the table's DDL for its row generator."""

    def get_table_schema(self, table_name):
        return "CREATE TABLE t (\n  id INT NOT NULL,\n  v VARCHAR(8)\n);"


def shared_target(suite):
    """Every worker gets the one in-memory database, as test_scenarios does."""
    seen = []

    def open_suite(request: SuiteRequest):
        seen.append(request)
        suite.result_collector = request.result_collector
        return suite

    return PreparedTarget("main", type(suite), open_suite), seen


def test_microbench_run_measures_branch_creates_on_the_target(tmp_path):
    suite = SchemaFakeSuite(rc.ResultCollector())
    suite.exec(["CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"], timed=False)
    target, seen = shared_target(suite)
    task = tp.TaskConfig(run_id="smoke", table_name="t", num_threads=1)
    task.operation_benchmark.operation = tp.OperationType.BRANCH_CREATE
    task.operation_benchmark.num_ops = 3

    metrics = runner2.run(task, target, "fake", str(tmp_path))

    assert metrics["total_ops"] == 3
    assert [r.initial_ref for r in seen] == ["main"]
    assert sum(1 for b in suite.list_branches() if b.startswith("branch_tid0_")) == 3
    [summary_path] = tmp_path.glob("*_summary.json")
    summary = json.loads(summary_path.read_text())
    assert summary["backend"] == "fake" and summary["operation"] == "BRANCH_CREATE"
    assert summary["support"]["unsupported_ops"] == []


def test_macrobench_run_runs_a_scenario_and_reports_work(tmp_path):
    config = tmp_path / "rl_env.textproto"
    config.write_text('run_id: "bb_smoke" seed: 7 ' + CONFIGS["rl_env"])
    parsed = mtp.MacroBenchConfig()
    text_format.Parse(config.read_text(), parsed)
    suite = FakeSuite(rc.ResultCollector())
    target, seen = shared_target(suite)

    e2e = macro_runner.run(["--config", str(config), "--outdir", str(tmp_path), "--no-progress"],
                            target=target, system_name="fake")

    assert e2e["status"] == "completed"
    assert e2e["backend"] == "fake"
    assert e2e["setup"]["seed_commit"] == "OK"
    work = e2e["work"]
    assert work["expected"] > 0 and work == {"expected": work["expected"], "succeeded": work["expected"],
                                             "failed": 0, "skipped": 0}
    assert e2e["worker_errors"] == []
    assert set(e2e["storage"]["points"]) == {"after_setup", "after_workflow", "after_gc"}
    assert all(r.initial_ref == "main" for r in seen) and len(seen) >= 1
    written = json.loads((tmp_path / "bb_smoke_e2e_stats.json").read_text())
    assert written["work"] == work and written["capabilities"]["commit"] is True
    assert (tmp_path / "bb_smoke.parquet").is_file()


@pytest.mark.parametrize("source", ["existing_db", "generated", "sql_dump"])
def test_macrobench_run_only_skips_seed_for_existing_data(tmp_path, source):
    config = tmp_path / "rl_env.textproto"
    config.write_text(f'run_id: "bb_seed" database_setup {{ {source} {{}} }} ' + CONFIGS["rl_env"])
    suite = FakeSuite(rc.ResultCollector())
    target, _ = shared_target(suite)
    with mock.patch.object(macro_runner, "setup_data", wraps=macro_runner.setup_data) as seed, \
            mock.patch.object(RlEnvScenario, "run"):
        e2e = macro_runner.run(["--config", str(config), "--outdir", str(tmp_path),
                               "--no-progress", "--storage-sample-interval", "0"],
                              target=target, system_name="fake")

    assert seed.call_count == (0 if source == "existing_db" else 1)
    assert bool(e2e["setup"]) == (source != "existing_db")


def test_macrobench_run_propagates_base_exception_after_cleanup(tmp_path):
    config = tmp_path / "rl_env.textproto"
    config.write_text('run_id: "bb_exit" ' + CONFIGS["rl_env"])
    suite = FakeSuite(rc.ResultCollector())
    target, _ = shared_target(suite)
    handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    with mock.patch.object(RlEnvScenario, "run", side_effect=SystemExit(73)), \
            mock.patch.object(suite, "close_connection", wraps=suite.close_connection) as close:
        with pytest.raises(SystemExit) as caught:
            macro_runner.run(["--config", str(config), "--outdir", str(tmp_path),
                              "--no-progress", "--storage-sample-interval", "0"],
                             target=target, system_name="fake")

    assert caught.value.code == 73
    close.assert_called_once()
    assert suite.conn is None
    assert {sig: signal.getsignal(sig) for sig in handlers} == handlers
    # No timed operations ran, so only the final stats file is expected.
    stats = json.loads((tmp_path / "bb_exit_e2e_stats.json").read_text())
    assert stats["status"] == "aborted: SystemExit: 73"


def test_macrobench_run_reports_a_crashed_task_in_work_and_worker_errors(tmp_path):
    """rl_env runs its tasks on the caller's thread (worker_threads unset): a task that
    raises is a worker error in the emitted stats, and the units it never reached stay
    unfinished rather than being counted as failed."""
    config = tmp_path / "rl_env.textproto"
    config.write_text('run_id: "bb_crash" seed: 7 ' + CONFIGS["rl_env"])
    suite = FakeSuite(rc.ResultCollector())
    target, _ = shared_target(suite)

    with mock.patch.object(faults, "pick_fault", side_effect=ValueError("boom")):
        e2e = macro_runner.run(["--config", str(config), "--outdir", str(tmp_path), "--no-progress"],
                                target=target, system_name="fake")

    assert e2e["status"] == "completed"
    assert e2e["work"] == {"expected": 6, "succeeded": 0, "failed": 0, "skipped": 0}
    assert [e["error"] for e in e2e["worker_errors"]] == ["ValueError: boom"] * 2
    written = json.loads((tmp_path / "bb_crash_e2e_stats.json").read_text())
    assert (written["work"], written["worker_errors"]) == (e2e["work"], e2e["worker_errors"])


def test_macrobench_run_replaces_a_failed_rollout_without_counting_it(tmp_path):
    """A rollout whose branch fails is replaced by the next round: the group still
    fills, the work counts only the accepted leaves, and nothing is a worker error."""
    class FlakyBranchSuite(FakeSuite):
        failed_once = False

        def _branch_impl(self, name, from_ref):
            if name == "task_1_r1" and not FlakyBranchSuite.failed_once:
                FlakyBranchSuite.failed_once = True
                raise RuntimeError("simulated branch failure")
            super()._branch_impl(name, from_ref)

    config = tmp_path / "rl_env.textproto"
    config.write_text('run_id: "bb_flaky" seed: 7 ' + CONFIGS["rl_env"])
    suite = FlakyBranchSuite(rc.ResultCollector())
    target, _ = shared_target(suite)

    e2e = macro_runner.run(["--config", str(config), "--outdir", str(tmp_path), "--no-progress"],
                            target=target, system_name="fake")

    assert e2e["status"] == "completed"
    assert e2e["work"] == {"expected": 6, "succeeded": 6, "failed": 0, "skipped": 0}
    assert e2e["metrics"]["rollouts_replaced"] == 1 and e2e["metrics"]["leaves"] == 4
    assert e2e["worker_errors"] == []


def test_macrobench_run_ends_the_run_on_a_failed_check_in_a_worker(tmp_path):
    """With fail_fast on, a check that fails inside a worker ends the run as
    invariant_failed after the worker suites are closed, without inventing failed
    work units or worker errors."""
    config = tmp_path / "rl_env.textproto"
    config.write_text('run_id: "bb_check" seed: 7 invariants { fail_fast: true } ' + CONFIGS["rl_env"])
    suite = FakeSuite(rc.ResultCollector())
    target, _ = shared_target(suite)
    original = scenario_base.InvariantRecorder.expect

    def mismatched(self, name, actual, expected, detail=""):
        return original(self, name, actual, "forced mismatch" if name.startswith("S1.1") else expected, detail)

    with mock.patch.object(scenario_base.InvariantRecorder, "expect", mismatched):
        e2e = macro_runner.run(["--config", str(config), "--outdir", str(tmp_path), "--no-progress"],
                                target=target, system_name="fake")

    assert e2e["status"].startswith("invariant_failed: S1.1")
    assert e2e["invariants"]["failed"] >= 1 and e2e["invariants"]["all_passed"] is False
    work = e2e["work"]
    assert work["failed"] == 0 and work["succeeded"] + work["skipped"] <= work["expected"]
    assert e2e["worker_errors"] == []
    assert suite.conn is None, "the worker suites were closed before the failure surfaced"
    assert (tmp_path / "bb_check_e2e_stats.json").is_file() and (tmp_path / "bb_check.parquet").is_file()


def test_macrobench_run_closes_every_worker_suite_exactly_once(tmp_path):
    """Workers run concurrently on distinct suites of one database. Every suite the run
    opened is closed exactly once, on the thread that opened it, by the time run
    returns: after a complete run, and after worker 2's suite factory raised once
    worker 1 had constructed and recorded its suite."""
    class Connection(FakeSuite):
        """One shared in-memory database; each instance is its own connection to it and
        records the thread that opened it and every thread that closed it."""
        shared, live, peak, lock = None, 0, 0, threading.Lock()

        def __init__(self, result_collector=None, measure_storage=False):
            super().__init__(result_collector, measure_storage)
            if Connection.shared is None:
                Connection.shared = (self.branches, self.commits, self.history)
            self.branches, self.commits, self.history = Connection.shared
            self._connect_impl(Ref("main"))
            self.opened_on, self.closed_on = threading.get_ident(), []
            with Connection.lock:
                Connection.live += 1
                Connection.peak = max(Connection.peak, Connection.live)

        def close_connection(self):
            super().close_connection()
            self.closed_on.append(threading.get_ident())
            with Connection.lock:
                Connection.live -= 1

    def run(name, fail_second_worker=False):
        """rl_env on two concurrent workers. With fail_second_worker the requests with
        worker_index 1 and 2 meet at a barrier, so they come from the two workers. Worker 1
        then constructs and records its suite and sets an event, and worker 2 waits for
        that event before its factory raises."""
        Connection.shared, Connection.live, Connection.peak = None, 0, 0
        opened, both_requested = [], threading.Barrier(2, timeout=10)
        worker_1_recorded = threading.Event()

        def open_suite(request: SuiteRequest):
            if fail_second_worker and request.worker_index in (1, 2):
                both_requested.wait()
                if request.worker_index == 2:
                    assert worker_1_recorded.wait(10), "worker 1 recorded its suite"
                    raise RuntimeError("simulated connection failure")
            suite = Connection(request.result_collector)
            opened.append(suite)
            if request.worker_index == 1:
                worker_1_recorded.set()
            return suite

        config = tmp_path / f"{name}.textproto"
        config.write_text(f'run_id: "{name}" seed: 7 ' + CONFIGS["rl_env"].replace("data_ops {", "data_ops { worker_threads: 2", 1))
        e2e = macro_runner.run(["--config", str(config), "--outdir", str(tmp_path), "--no-progress"],
                                target=PreparedTarget("main", Connection, open_suite), system_name="fake")
        assert [s.closed_on for s in opened] == [[s.opened_on] for s in opened], "each suite closed exactly once, on the thread that opened it"
        assert all(s.conn is None for s in opened) and Connection.live == 0, "no connection remains"
        return e2e, opened

    e2e, opened = run("bb_pool")
    assert e2e["status"] == "completed" and e2e["worker_errors"] == []
    assert e2e["work"] == {"expected": 6, "succeeded": 6, "failed": 0, "skipped": 0}
    assert len({s.opened_on for s in opened}) >= 3 and Connection.peak >= 3, "the setup suite and two worker suites were open together on their own threads"

    e2e, opened = run("bb_pool_fail", fail_second_worker=True)
    assert e2e["status"].startswith("crashed: RuntimeError: simulated connection failure")
    assert len(opened) >= 2, "the setup suite and worker 1's suite were recorded"


def test_macrobench_run_counts_each_data_agent_step_once_after_a_reset(tmp_path):
    """data_agent resets a batch to an earlier commit and redoes the steps after it:
    the emitted work counts every step of the batch once. The fake backend has no
    rebase, so the run ends unsupported once the first batch's steps are done."""
    config = tmp_path / "data_agent.textproto"
    config.write_text('run_id: "bb_reset" seed: 7 ' + CONFIGS["data_agent"])
    suite = FakeSuite(rc.ResultCollector())
    target, _ = shared_target(suite)

    e2e = macro_runner.run(["--config", str(config), "--outdir", str(tmp_path), "--no-progress"],
                            target=target, system_name="fake")

    assert e2e["status"].startswith("unsupported: REBASE")
    assert any(r["name"].startswith("S6.2 batch 1 reset to step 0") for r in e2e["invariants"]["results"])
    assert e2e["work"] == {"expected": 4, "succeeded": 2, "failed": 0, "skipped": 0}


def test_verbs_run_measures_one_point_on_the_target(tmp_path):
    suite = FakeSuite(rc.ResultCollector())
    target, _ = shared_target(suite)
    small = CHScale.from_config(mtp.SchemaConfig(scale_factor=1, items=50, customers_per_district=5,
                                                 orders_per_district=5, suppliers=5))

    with mock.patch.object(driver.CHScale, "from_config", return_value=small):
        stats = driver.run(["--scale", "1", "--outdir", str(tmp_path), "--verbs", "branch", "--axes", "scale",
                             "--reps", "1", "--warmup", "0", "--no-control", "--no-gc", "--stock-rows", "50"],
                            target=target, system_name="fake")

    assert stats["status"] == "completed" and stats["backend"] == "fake"
    assert stats["setup"]["base_commit"] == "OK"
    assert stats["points"] and all(p["status"] == "ok" and p["failed"] == 0 for p in stats["points"])
    assert (tmp_path / "micro_verbs_fake_w1_stats.json").is_file()
    assert suite.list_branches() == ["main"], "every point's branch was dropped again"
