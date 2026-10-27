"""Standalone commands prepare engine integrations without leaderboard imports."""

import json
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


@pytest.mark.parametrize("runner, text", [
    (runner2, 'run_id: "cli" operation_benchmark { num_ops: 1 }'),
    (macro_runner, 'run_id: "cli" ' + CONFIGS["rl_env"]),
])
def test_cli_rejects_implicit_provider_before_loading_an_integration(tmp_path, runner, text):
    config = tmp_path / "config.textproto"
    config.write_text(text)
    with mock.patch.object(connector, "load_connector") as load:
        with pytest.raises(SystemExit) as caught:
            runner.main(["--config", str(config)])
    assert caught.value.code == 2
    load.assert_not_called()


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
