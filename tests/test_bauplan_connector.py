from types import SimpleNamespace
from unittest import mock

import pytest

from dblib import bauplan, result_collector as rc
from dblib.connector import DatabaseHandle, load_connector
from dblib.target import SuiteRequest
from macrobench import runner as macro_runner
from tests.test_scenarios import CONFIGS


def test_bauplan_connector_provisions_and_opens_the_legacy_suite(tmp_path) -> None:
    server = load_connector("bauplan", state_dir=tmp_path)
    client = mock.Mock()
    database = DatabaseHandle("bb_test", "main")
    request = SuiteRequest(rc.ResultCollector(), "main", 0, measure_storage=True)

    with mock.patch.object(bauplan, "new_client", return_value=client), \
            mock.patch.object(bauplan, "root_branch", return_value="alice.bb_test"), \
            mock.patch.object(bauplan.BauplanToolSuite, "init_for_bench",
                              return_value=mock.sentinel.suite) as init:
        provisioned = server.provision("bb_test")
        suite = server.prepare_target(database).open_suite(request)

    assert provisioned == database
    client.create_branch.assert_called_once_with(
        "alice.bb_test", from_ref=bauplan.BAUPLAN_SOURCE_REF
    )
    client.create_namespace.assert_called_once_with("bb_bb_test", "alice.bb_test")
    init.assert_called_once_with(request.result_collector, "bb_test", "main", False)
    assert suite is mock.sentinel.suite


def test_bauplan_connector_removes_its_branch_when_namespace_creation_fails(tmp_path) -> None:
    server = load_connector("bauplan", state_dir=tmp_path)
    client = mock.Mock()
    client.create_namespace.side_effect = RuntimeError("namespace failed")
    with mock.patch.object(bauplan, "new_client", return_value=client), \
            mock.patch.object(bauplan, "root_branch", return_value="alice.bb_test"):
        with pytest.raises(RuntimeError, match="namespace failed"):
            server.provision("bb_test")
    client.delete_branch.assert_called_once_with("alice.bb_test", if_exists=True)


@pytest.mark.parametrize("resource", ["root", "child", "tag"])
def test_bauplan_connector_detects_all_owned_resources(tmp_path, resource) -> None:
    server = load_connector("bauplan", state_dir=tmp_path)
    client = mock.Mock()
    client.get_branches.return_value = (
        [SimpleNamespace(name=f"alice.bb_test{'__dev' if resource == 'child' else ''}")]
        if resource != "tag" else []
    )
    client.get_tags.return_value = (
        [SimpleNamespace(name="alice.bb_test.c123")] if resource == "tag" else []
    )
    with mock.patch.object(bauplan, "new_client", return_value=client), \
            mock.patch.object(bauplan, "root_branch", return_value="alice.bb_test"):
        assert server.exists("bb_test") is True


def test_bauplan_cleanup_ignores_only_a_missing_s3_prefix() -> None:
    client = mock.Mock()
    client.get_branches.return_value = []
    client.get_tags.return_value = []
    fs = mock.Mock()
    path = "bucket/prefix/bb_test"

    with mock.patch.object(bauplan, "new_client", return_value=client), \
            mock.patch.object(bauplan, "root_branch", return_value="alice.bb_test"), \
            mock.patch.object(bauplan, "_s3_fs", return_value=(fs, "bucket/prefix")):
        fs.get_file_info.return_value.type = bauplan.pafs.FileType.NotFound
        bauplan.drop_database("bb_test")
        fs.delete_dir.assert_not_called()

        fs.get_file_info.side_effect = OSError("S3 access denied")
        with pytest.raises(OSError, match="S3 access denied"):
            bauplan.drop_database("bb_test")

    assert fs.get_file_info.call_args_list == [mock.call(path), mock.call(path)]


def test_bauplan_connector_rejects_sql_dumps(tmp_path) -> None:
    server = load_connector("bauplan", state_dir=tmp_path)
    with pytest.raises(NotImplementedError, match="does not load SQL dumps"):
        server.load(DatabaseHandle("bb_test", "main"), tmp_path / "dump.sql")


def test_bauplan_preflight_requires_s3_before_account_access(tmp_path) -> None:
    server = load_connector("bauplan", state_dir=tmp_path)
    with mock.patch.object(bauplan, "BAUPLAN_S3_URI", ""), \
            mock.patch.object(bauplan, "new_client") as client:
        with pytest.raises(RuntimeError, match="BAUPLAN_S3_URI"):
            server.preflight()
    client.assert_not_called()


def test_macro_cli_maps_bauplan_enum_to_its_connector(tmp_path) -> None:
    config = tmp_path / "macro.textproto"
    config.write_text(
        'run_id: "bauplan_cli" backend: BAUPLAN '
        'database_setup { db_name: "bb_test" cleanup: true generated {} } '
        + CONFIGS["rl_env"]
    )
    connector = mock.Mock()
    prepared = (connector, mock.sentinel.database, mock.sentinel.target)
    with mock.patch.object(macro_runner, "prepare_database", return_value=prepared) as prepare, \
            mock.patch.object(macro_runner, "_run", return_value={}) as run:
        macro_runner.main(["--config", str(config)])
    prepare.assert_called_once_with("bauplan", mock.ANY)
    connector.delete_database.assert_called_once_with(mock.sentinel.database)
    assert run.call_args.kwargs["system_name"] == "BAUPLAN"
