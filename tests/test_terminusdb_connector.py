from unittest import mock

from dblib import result_collector as rc
from dblib.systems.terminusdb import adapter as terminusdb
from dblib.connector import DatabaseHandle, load_connector
from dblib.target import SuiteRequest
from microbench import runner2


def test_terminusdb_connector_provisions_and_opens_the_legacy_suite(tmp_path) -> None:
    server = load_connector("terminusdb", state_dir=tmp_path)
    client = mock.Mock()
    database = DatabaseHandle("bb_test", "main")
    request = SuiteRequest(rc.ResultCollector(), "main", 0, measure_storage=True)

    with mock.patch.object(terminusdb, "REUSE_LOAD", False), \
            mock.patch.object(terminusdb, "_Client", return_value=client), \
            mock.patch.object(terminusdb, "_forget") as forget, \
            mock.patch.object(terminusdb.TerminusDBToolSuite, "init_for_bench",
                              return_value=mock.sentinel.suite) as init:
        provisioned = server.provision("bb_test")
        suite = server.prepare_target(database).open_suite(request)

    assert provisioned == database
    forget.assert_called_once_with("bb_test")
    client.call.assert_called_once_with(
        "POST",
        f"db/{terminusdb.TERMINUSDB_TEAM}/bb_test",
        body={"label": "bb_test", "comment": "db-fork benchmark"},
    )
    client.close.assert_called_once_with()
    init.assert_called_once_with(request.result_collector, "bb_test", "main", True)
    assert suite is mock.sentinel.suite


def test_terminusdb_connector_load_uses_reusable_database_setup(tmp_path) -> None:
    server = load_connector("terminusdb", state_dir=tmp_path)
    dump = tmp_path / "dump.sql"
    dump.write_text("SELECT 1;")
    with mock.patch.object(terminusdb, "setup_database") as setup:
        server.load(DatabaseHandle("bb_test", "main"), dump)
    setup.assert_called_once_with("bb_test", str(dump))


def test_terminusdb_connector_reuses_loaded_database_when_enabled(tmp_path) -> None:
    server = load_connector("terminusdb", state_dir=tmp_path)
    with mock.patch.object(terminusdb, "REUSE_LOAD", True), \
            mock.patch.object(server, "exists", return_value=True), \
            mock.patch.object(terminusdb, "_Client") as client:
        database = server.provision("bb_test")
    assert database == DatabaseHandle("bb_test", "main")
    client.assert_not_called()

    with mock.patch.object(terminusdb, "setup_database") as setup:
        server.prepare_target(database)
    setup.assert_called_once_with("bb_test")

    with mock.patch.object(terminusdb, "REUSE_LOAD", True), \
            mock.patch.object(terminusdb, "drop_database") as drop:
        server.delete("bb_test")
    drop.assert_called_once_with("bb_test")


def test_terminusdb_connector_maps_missing_database_to_false(tmp_path) -> None:
    server = load_connector("terminusdb", state_dir=tmp_path)
    client = mock.Mock()
    client.call.side_effect = terminusdb.TerminusDBError(404, "not_found", "missing")
    with mock.patch.object(terminusdb, "_Client", return_value=client):
        assert server.exists("bb_test") is False
    client.close.assert_called_once_with()


def test_terminusdb_connector_reads_server_version(tmp_path) -> None:
    server = load_connector("terminusdb", state_dir=tmp_path)
    client = mock.Mock()
    client.call.return_value = {"api:version": "12.0.7"}
    with mock.patch.object(terminusdb, "_Client", return_value=client):
        assert server.version() == "12.0.7"


def test_terminusdb_scope_does_not_expose_url_credentials(tmp_path) -> None:
    server = load_connector("terminusdb", state_dir=tmp_path)
    with mock.patch.object(
        terminusdb, "TERMINUSDB_URL", "https://secret:password@example.test:6363"
    ):
        assert server.scope() == {
            "scheme": "https",
            "host": "example.test",
            "port": 6363,
            "team": terminusdb.TERMINUSDB_TEAM,
            "user": terminusdb.TERMINUSDB_USER,
        }


def test_terminusdb_statement_shapes_remain_available() -> None:
    plan = terminusdb._parse("UPDATE t SET v = %(v)s WHERE id = %(id)s")
    assert plan.kind == "update"
    assert plan.sets == [("v", ("param", "v"))]
    assert plan.where == [(["id"], "=", [("param", "id")])]


def test_micro_cli_maps_terminusdb_enum_to_its_connector(tmp_path) -> None:
    config = tmp_path / "micro.textproto"
    config.write_text(
        'run_id: "terminusdb_cli" backend: TERMINUSDB table_name: "t" '
        'database_setup { db_name: "bb_test" cleanup: true } '
        'operation_benchmark { operation: BRANCH_CREATE num_ops: 1 }'
    )
    prepared = mock.MagicMock()
    prepared.return_value.__enter__.return_value = mock.sentinel.target
    with mock.patch.object(runner2, "prepared_database", prepared), \
            mock.patch.object(runner2, "run", return_value={}) as run:
        runner2.main(["--config", str(config)])
    prepared.assert_called_once_with("terminusdb", mock.ANY)
    run.assert_called_once_with(
        mock.ANY, mock.sentinel.target, "TERMINUSDB", "/tmp/run_stats"
    )
