"""The three runners on the fake backend through the prepared-target seam:
the embedding application provisions a database and passes a
PreparedTarget; the runners never choose or build a database system."""

import json
from unittest import mock

from google.protobuf import text_format

from dblib import result_collector as rc
from dblib.target import PreparedTarget, SuiteRequest
from macrobench import faults
from macrobench import runner as macro_runner
from macrobench import task_pb2 as mtp
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


def test_macrobench_main_runs_a_scenario_and_reports_work(tmp_path):
    config = tmp_path / "rl_env.textproto"
    config.write_text('run_id: "bb_smoke" seed: 7 ' + CONFIGS["rl_env"])
    parsed = mtp.MacroBenchConfig()
    text_format.Parse(config.read_text(), parsed)
    suite = FakeSuite(rc.ResultCollector())
    target, seen = shared_target(suite)

    e2e = macro_runner.main(["--config", str(config), "--outdir", str(tmp_path), "--no-progress"],
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


def test_macrobench_main_reports_a_crashed_task_in_work_and_worker_errors(tmp_path):
    """rl_env runs its tasks on the caller's thread (worker_threads unset): a task that
    raises is still one failed unit of work and one worker error in the emitted stats."""
    config = tmp_path / "rl_env.textproto"
    config.write_text('run_id: "bb_crash" seed: 7 ' + CONFIGS["rl_env"])
    suite = FakeSuite(rc.ResultCollector())
    target, _ = shared_target(suite)

    with mock.patch.object(faults, "pick_fault", side_effect=ValueError("boom")):
        e2e = macro_runner.main(["--config", str(config), "--outdir", str(tmp_path), "--no-progress"],
                                target=target, system_name="fake")

    assert e2e["status"] == "completed"
    assert e2e["work"] == {"expected": 6, "succeeded": 0, "failed": 2, "skipped": 0}
    assert [e["error"] for e in e2e["worker_errors"]] == ["ValueError: boom"] * 2
    written = json.loads((tmp_path / "bb_crash_e2e_stats.json").read_text())
    assert (written["work"], written["worker_errors"]) == (e2e["work"], e2e["worker_errors"])


def test_macrobench_main_counts_each_data_agent_step_once_after_a_reset(tmp_path):
    """data_agent resets a batch to an earlier commit and redoes the steps after it:
    the emitted work counts every step of the batch once. The fake backend has no
    rebase, so the run ends unsupported once the first batch's steps are done."""
    config = tmp_path / "data_agent.textproto"
    config.write_text('run_id: "bb_reset" seed: 7 ' + CONFIGS["data_agent"])
    suite = FakeSuite(rc.ResultCollector())
    target, _ = shared_target(suite)

    e2e = macro_runner.main(["--config", str(config), "--outdir", str(tmp_path), "--no-progress"],
                            target=target, system_name="fake")

    assert e2e["status"].startswith("unsupported: REBASE")
    assert any(r["name"].startswith("S6.2 batch 1 reset to step 0") for r in e2e["invariants"]["results"])
    assert e2e["work"] == {"expected": 4, "succeeded": 2, "failed": 0, "skipped": 0}


def test_verbs_main_measures_one_point_on_the_target(tmp_path):
    suite = FakeSuite(rc.ResultCollector())
    target, _ = shared_target(suite)
    small = CHScale.from_config(mtp.SchemaConfig(scale_factor=1, items=50, customers_per_district=5,
                                                 orders_per_district=5, suppliers=5))

    with mock.patch.object(driver.CHScale, "from_config", return_value=small):
        stats = driver.main(["--scale", "1", "--outdir", str(tmp_path), "--verbs", "branch", "--axes", "scale",
                             "--reps", "1", "--warmup", "0", "--no-control", "--no-gc", "--stock-rows", "50"],
                            target=target, system_name="fake")

    assert stats["status"] == "completed" and stats["backend"] == "fake"
    assert stats["setup"]["base_commit"] == "OK"
    assert stats["points"] and all(p["status"] == "ok" and p["failed"] == 0 for p in stats["points"])
    assert (tmp_path / "micro_verbs_fake_w1_stats.json").is_file()
    assert suite.list_branches() == ["main"], "every point's branch was dropped again"
