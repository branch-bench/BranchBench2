"""Standalone commands prepare engine integrations without leaderboard imports."""

import json
from pathlib import Path
import subprocess
import sys
from unittest import mock

import pytest
from google.protobuf import text_format

from dblib import connector, result_collector as rc
from macrobench import runner as macro_runner, task_pb2 as mtp
from macrobench.datagen.ch import CHScale
from microbench import runner2, task2_pb2 as tp
from microbench.verbs import driver
from microbench.verbs.grid import points_for_run
from tests.fake_backend import FakeSuite
from tests.test_runners import SchemaFakeSuite, shared_target
from tests.test_scenarios import CONFIGS


@pytest.mark.parametrize("module, flags", [
    ("microbench.runner2", ["--config", "--output-dir", "--backend", "--db-name"]),
    ("macrobench.runner", ["--config", "--outdir", "--max-runtime-sec", "--measure-storage"]),
    ("microbench.verbs", ["--backend", "--db-name", "--scale", "--skip-done"]),
])
def test_cli_help(module, flags):
    result = subprocess.run([sys.executable, "-m", module, "--help"],
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert all(flag in result.stdout for flag in flags)


@pytest.mark.parametrize("message", [tp.TaskConfig, mtp.MacroBenchConfig])
def test_backend_presence_distinguishes_explicit_neon(message):
    config = message()
    assert not config.HasField("backend")
    text_format.Parse("backend: NEON", config)
    assert config.HasField("backend") and config.backend == 0


@pytest.mark.parametrize(
    "config_path",
    sorted((Path(macro_runner.__file__).parent / "configs").glob("*.textproto")),
    ids=lambda path: path.stem,
)
def test_bundled_macro_configs_select_a_cli_database(config_path):
    config = macro_runner.load_config(str(config_path))

    assert config.HasField("backend") and config.backend == mtp.Backend.DOLT
    assert config.database_setup.db_name
    assert config.database_setup.cleanup is True
    assert config.database_setup.WhichOneof("source") == "generated"


def test_bundled_macro_config_runs_with_its_provider_and_setup():
    provider, database = fake_connector(FakeSuite(rc.ResultCollector()))
    config = Path(macro_runner.__file__).parent / "configs" / "rl_env_mini.textproto"
    with mock.patch.object(connector, "load_connector", return_value=provider) as load, \
            mock.patch.object(macro_runner, "_run", return_value={}) as execute:
        macro_runner.main(["--config", str(config)])

    load.assert_called_once_with("dolt", state_dir=None)
    provider.provision.assert_called_once_with("macro_rl_env_mini")
    provider.delete_database.assert_called_once_with(database)
    assert execute.call_args.kwargs["system_name"] == "DOLT"


def test_macro_run_uses_supplied_target_instead_of_bundled_setup():
    target = mock.sentinel.target
    config = Path(macro_runner.__file__).parent / "configs" / "rl_env_mini.textproto"
    with mock.patch.object(macro_runner, "prepare_database") as prepare, \
            mock.patch.object(macro_runner, "_run", return_value={}) as execute:
        macro_runner.run(["--config", str(config)], target=target, system_name="leaderboard")

    prepare.assert_not_called()
    assert execute.call_args.kwargs == {
        "target": target,
        "system_name": "leaderboard",
    }


def test_bundled_micro_config_selects_a_cli_database(tmp_path, monkeypatch):
    config_path = Path(runner2.__file__).parent / "test_config2.textproto"
    target = mock.sentinel.target
    prepared = mock.MagicMock()
    prepared.return_value.__enter__.return_value = target
    monkeypatch.chdir(tmp_path)
    with mock.patch.object(runner2, "prepared_database", prepared), \
            mock.patch.object(runner2, "run", return_value={}) as execute:
        runner2.main(["--config", str(config_path)])

    backend, setup = prepared.call_args.args
    assert backend == "dolt"
    assert setup.db_name == "tpcc_test" and setup.cleanup is True
    assert setup.WhichOneof("source") == "sql_dump"
    assert Path(setup.sql_dump.sql_dump_path).is_file()
    execute.assert_called_once_with(mock.ANY, target, "DOLT", "/tmp/run_stats")


def test_micro_cli_defaults_to_neon_and_resolves_config_relative_dump(tmp_path, monkeypatch):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    dump = config_dir / "data.sql"
    dump.write_text("SELECT 1;")
    config = config_dir / "task.textproto"
    config.write_text('run_id: "cli" database_setup { db_name: "cli_db" '
                      'sql_dump { sql_dump_path: "data.sql" } } '
                      'operation_benchmark { num_ops: 1 }')
    target = mock.sentinel.target
    prepared = mock.MagicMock()
    prepared.return_value.__enter__.return_value = target
    monkeypatch.chdir(tmp_path)
    with mock.patch.object(runner2, "prepared_database", prepared), \
            mock.patch.object(runner2, "run", return_value={}) as execute:
        runner2.main(["--config", str(config)])

    backend, setup = prepared.call_args.args
    assert backend == "neon"
    assert Path(setup.sql_dump.sql_dump_path) == dump
    execute.assert_called_once_with(mock.ANY, target, "NEON", "/tmp/run_stats")


def test_macro_cli_defaults_to_neon_and_finalizes_before_writing(tmp_path):
    suite = FakeSuite(rc.ResultCollector())
    provider, database = fake_connector(suite)
    events = []

    def delete(handle):
        assert handle is database
        events.append("delete")
        raise RuntimeError("retry cleanup")

    def metrics(handle):
        assert handle is database
        events.append("metrics")
        return {
            "count": 1,
            "all_metrics": [{"metric": "compute_seconds", "value": 3}],
            "summary": {"compute_seconds": 3},
        }

    original_storage = suite._safe_storage

    def storage():
        events.append("storage")
        return original_storage()

    provider.delete_database.side_effect = delete
    provider.consumption_metrics.side_effect = metrics
    config = tmp_path / "config.textproto"
    config.write_text('run_id: "cli" measure_storage: true '
                      'database_setup { db_name: "cli_db" cleanup: true } '
                      + CONFIGS["rl_env"])
    with mock.patch.object(connector, "load_connector", return_value=provider) as load, \
            mock.patch.object(suite, "_safe_storage", side_effect=storage), \
            mock.patch.object(macro_runner.time, "sleep",
                              side_effect=lambda _: events.append("sleep")) as sleep, \
            mock.patch.object(macro_runner.os, "sync", side_effect=lambda: events.append("sync")):
        result = macro_runner.main([
            "--config", str(config), "--outdir", str(tmp_path), "--no-progress",
            "--storage-sample-interval", "0",
        ])

    load.assert_called_once_with("neon", state_dir=None)
    assert events[events.index("metrics"):] == [
        "metrics", "delete", "sleep", "delete", "sync", "storage",
    ]
    sleep.assert_called_once_with(2)
    assert result["neon_metrics_count"] == 1
    assert result["neon_all_metrics"] == [{"metric": "compute_seconds", "value": 3}]
    assert result["neon_compute_seconds"] == 3
    assert "after_cleanup" in result["storage"]["points"]
    written = json.loads((tmp_path / "cli_e2e_stats.json").read_text())
    assert (written["storage"]["points"]["after_cleanup"]
            == result["storage"]["points"]["after_cleanup"])
    assert written["neon_compute_seconds"] == 3


def fake_connector(suite):
    target, _ = shared_target(suite)
    provider = mock.Mock(spec=connector.Connector)
    provider.exists.return_value = False
    database = connector.DatabaseHandle("cli_db", "main")
    provider.provision.return_value = database
    provider.attach.return_value = database
    provider.prepare_target.return_value = target
    return provider, database


def test_micro_cli_loads_dump_runs_target_and_cleans_up(tmp_path):
    suite = SchemaFakeSuite(rc.ResultCollector())
    suite.exec(["CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"], timed=False)
    provider, database = fake_connector(suite)
    dump = tmp_path / "data.sql"
    dump.write_text("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);")
    config = tmp_path / "config.textproto"
    config.write_text(f'''run_id: "cli_micro" table_name: "t"
        database_setup {{ cleanup: true sql_dump {{ sql_dump_path: "{dump}" }} }}
        operation_benchmark {{ operation: BRANCH_CREATE num_ops: 2 }}''')
    with mock.patch.object(connector, "load_connector", return_value=provider) as load:
        result = runner2.main(["--config", str(config), "--backend", "custom_db",
                               "--db-name", "cli_db", "--output-dir", str(tmp_path)])

    load.assert_called_once_with("custom_db", state_dir=None)
    provider.provision.assert_called_once_with("cli_db")
    provider.load.assert_called_once_with(database, dump)
    provider.delete_database.assert_called_once_with(database)
    assert result["total_ops"] == 2
    assert list(tmp_path.glob("*_summary.json"))


def test_macro_cli_runs_configured_provider_and_cleans_up(tmp_path):
    provider, database = fake_connector(FakeSuite(rc.ResultCollector()))
    config = tmp_path / "config.textproto"
    config.write_text('run_id: "cli_macro" backend: DOLT '
                      'database_setup { db_name: "cli_db" cleanup: true generated {} } '
                      + CONFIGS["rl_env"])
    with mock.patch.object(connector, "load_connector", return_value=provider) as load:
        result = macro_runner.main(["--config", str(config), "--outdir", str(tmp_path),
                                    "--no-progress", "--storage-sample-interval", "0"])

    load.assert_called_once_with("dolt", state_dir=None)
    provider.provision.assert_called_once_with("cli_db")
    provider.delete_database.assert_called_once_with(database)
    assert result["status"] == "completed"
    assert result["work"]["expected"] == result["work"]["succeeded"]


def test_macro_cli_preserves_existing_database_when_cleanup_is_false(tmp_path):
    provider, database = fake_connector(FakeSuite(rc.ResultCollector()))
    config = tmp_path / "config.textproto"
    config.write_text('run_id: "cli_existing" backend: NEON '
                      'database_setup { db_name: "cli_db" existing_db { branch_id: "main" } } '
                      + CONFIGS["rl_env"])
    with mock.patch.object(connector, "load_connector", return_value=provider) as load, \
            mock.patch.object(macro_runner, "_run", return_value={}) as execute:
        macro_runner.main(["--config", str(config), "--backend", "dolt", "--outdir", str(tmp_path)])

    load.assert_called_once_with("dolt", state_dir=None)
    provider.attach.assert_called_once()
    provider.provision.assert_not_called()
    provider.load.assert_not_called()
    provider.delete_database.assert_not_called()
    assert execute.call_args.args[1].database_setup.WhichOneof("source") == "existing_db"


def test_verbs_cli_runs_generated_target_and_cleans_up(tmp_path):
    provider, database = fake_connector(FakeSuite(rc.ResultCollector()))
    small = CHScale.from_config(mtp.SchemaConfig(scale_factor=1, items=50, customers_per_district=5,
                                                 orders_per_district=5, suppliers=5))
    with mock.patch.object(connector, "load_connector", return_value=provider) as load, \
            mock.patch.object(driver.CHScale, "from_config", return_value=small):
        result = driver.main(["--backend", "dolt", "--db-name", "cli_db", "--scale", "1",
                              "--outdir", str(tmp_path), "--verbs", "branch", "--axes", "scale",
                              "--reps", "1", "--warmup", "0", "--no-control", "--no-gc",
                              "--stock-rows", "50"])

    load.assert_called_once_with("dolt", state_dir=None)
    provider.provision.assert_called_once_with("cli_db")
    provider.delete_database.assert_called_once_with(database)
    assert result["status"] == "completed"
    assert result["points"] and all(point["status"] == "ok" for point in result["points"])


def test_verbs_cli_skips_finished_grid_without_provisioning(tmp_path):
    done = tmp_path / "finished.json"
    done.write_text(json.dumps({"points": [{"id": point.id, "status": "ok"}
                                           for point in points_for_run(1, ["branch"], ["scale"])]}))
    with mock.patch.object(connector, "load_connector") as load:
        result = driver.main(["--backend", "dolt", "--outdir", str(tmp_path),
                              "--verbs", "branch", "--axes", "scale", "--skip-done", str(done)])

    assert result is None
    load.assert_not_called()


def test_verbs_cli_logs_cleanup_failure_after_completed_run(tmp_path, capsys):
    provider, _ = fake_connector(FakeSuite(rc.ResultCollector()))
    provider.delete_database.side_effect = RuntimeError("delete failed")
    stats = {"status": "completed"}
    with mock.patch.object(connector, "load_connector", return_value=provider), \
            mock.patch.object(driver, "_run", return_value=stats):
        result = driver.main(["--backend", "dolt", "--db-name", "cli_db",
                              "--outdir", str(tmp_path), "--verbs", "branch",
                              "--axes", "scale"])

    assert result is stats
    assert "Cleanup failed: delete failed" in capsys.readouterr().out


def test_verbs_cli_cleanup_failure_does_not_mask_run_failure(tmp_path, capsys):
    provider, _ = fake_connector(FakeSuite(rc.ResultCollector()))
    provider.delete_database.side_effect = RuntimeError("delete failed")
    with mock.patch.object(connector, "load_connector", return_value=provider), \
            mock.patch.object(driver, "_run", side_effect=SystemExit(73)):
        with pytest.raises(SystemExit) as caught:
            driver.main(["--backend", "dolt", "--db-name", "cli_db",
                         "--outdir", str(tmp_path), "--verbs", "branch",
                         "--axes", "scale"])

    assert caught.value.code == 73
    assert "Cleanup failed: delete failed" in capsys.readouterr().out


@pytest.mark.parametrize("stage", ["seed_ch", "fork", "run_point"])
def test_verbs_cli_closes_connections_before_deletion_on_failure(tmp_path, stage):
    suite = FakeSuite(rc.ResultCollector())
    provider, database = fake_connector(suite)
    events = []
    original_close = suite.close_connection

    def close():
        events.append("close")
        original_close()

    provider.delete_database.side_effect = lambda database: events.append("delete")
    argv = ["--backend", "dolt", "--db-name", "cli_db", "--outdir", str(tmp_path),
            "--verbs", "branch", "--axes", "scale", "--reps", "1", "--warmup", "0",
            "--no-control"]
    with mock.patch.object(connector, "load_connector", return_value=provider), \
            mock.patch.object(driver, "seed_ch", return_value={}), \
            mock.patch.object(driver, stage, side_effect=SystemExit(73)), \
            mock.patch.object(suite, "close_connection", side_effect=close):
        with pytest.raises(SystemExit) as caught:
            driver.main(argv)

    assert caught.value.code == 73
    assert events == (["close", "delete"] if stage == "seed_ch" else ["close", "close", "delete"])
    provider.delete_database.assert_called_once_with(database)


@pytest.mark.parametrize("runner, command", [
    (runner2, "micro"), (macro_runner, "macro"), (driver, "verbs"),
])
def test_cli_cleans_up_when_execution_raises(tmp_path, runner, command):
    provider, database = fake_connector(FakeSuite(rc.ResultCollector()))
    if command == "verbs":
        argv = ["--backend", "dolt", "--db-name", "cli_db", "--outdir", str(tmp_path)]
    else:
        config = tmp_path / "config.textproto"
        workload = 'operation_benchmark { num_ops: 1 }' if command == "micro" else CONFIGS["rl_env"]
        config.write_text('run_id: "cli_failure" backend: DOLT '
                          'database_setup { db_name: "cli_db" cleanup: true } ' + workload)
        argv = ["--config", str(config)]
    execute_name = "run" if command == "micro" else "_run"
    with mock.patch.object(connector, "load_connector", return_value=provider), \
            mock.patch.object(runner, execute_name, side_effect=SystemExit(73)):
        with pytest.raises(SystemExit) as caught:
            runner.main(argv)

    assert caught.value.code == 73
    provider.delete_database.assert_called_once_with(database)
