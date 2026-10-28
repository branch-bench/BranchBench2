"""Provider selection, standalone lifecycle safety, and persisted recovery."""

from contextlib import ExitStack
from types import SimpleNamespace
from unittest import mock

import pytest
from psycopg2.errors import DuplicateDatabase

from dblib.connector import Connector, DatabaseHandle, connector_names, load_connector, prepared_database
from microbench import task2_pb2 as tp


def setup_config(*, source: str | None = None, cleanup: bool = True) -> tp.DatabaseSetup:
    setup = tp.DatabaseSetup(db_name="bb_test", cleanup=cleanup)
    if source:
        getattr(setup, source).SetInParent()
    return setup


def server_mock() -> mock.Mock:
    server = mock.Mock(spec=Connector)
    server.exists.return_value = False
    server.provision.return_value = DatabaseHandle("bb_test", "main")
    server.attach.return_value = DatabaseHandle("bb_test", "existing")
    return server


def test_loader_is_lazy_and_returns_fresh_connectors(tmp_path) -> None:
    with mock.patch("dblib.connector.import_module") as imported:
        names = connector_names()
        imported.assert_not_called()
    assert names == ["bauplan", "chronos", "dolt", "dolt_mysql", "file_copy",
                     "matrixone", "neon", "seekdb", "terminusdb", "xata"]
    assert connector_names() == names
    with pytest.raises(ValueError, match="no built-in"):
        load_connector("../neon")
    for name in names:
        first = load_connector(name, state_dir=tmp_path)
        second = load_connector(name, state_dir=tmp_path)
        assert isinstance(first, Connector)
        assert first is not second
        assert first.state_dir == tmp_path


@pytest.mark.parametrize("name", ["", "db;DROP TABLE x", "../db", "MixedCase", "x" * 64])
def test_invalid_database_name_fails_before_loading_a_provider(name) -> None:
    setup = setup_config()
    setup.db_name = name
    with mock.patch("dblib.connector.load_connector") as load:
        with pytest.raises(ValueError, match="db_name"):
            with prepared_database("dolt", setup):
                pytest.fail("invalid target reached execution")
        load.assert_not_called()


@pytest.mark.parametrize("source", [pytest.param(None, id="generated"), "sql_dump"])
def test_generated_setup_replaces_existing_resources_before_provision(source, tmp_path) -> None:
    server = server_mock()
    server.exists.return_value = True
    setup = setup_config(source=source, cleanup=False)
    if source == "sql_dump":
        dump = tmp_path / "data.sql"
        dump.write_text("SELECT 1;")
        setup.sql_dump.sql_dump_path = str(dump)
    with mock.patch("dblib.connector.load_connector", return_value=server):
        with prepared_database("dolt", setup) as target:
            assert target is server.prepare_target.return_value
    assert server.method_calls[:4] == [
        mock.call.preflight(),
        mock.call.exists("bb_test"),
        mock.call.delete("bb_test"),
        mock.call.provision("bb_test"),
    ]
    server.delete_database.assert_not_called()


def test_stale_resource_delete_failure_stops_provision() -> None:
    server = server_mock()
    server.exists.return_value = True
    server.delete.side_effect = RuntimeError("delete failed")
    with mock.patch("dblib.connector.load_connector", return_value=server):
        with pytest.raises(RuntimeError, match="delete failed"):
            with prepared_database("dolt", setup_config()):
                pytest.fail("failed deletion reached execution")
    server.provision.assert_not_called()
    server.delete_database.assert_not_called()


@pytest.mark.parametrize("failure", ["load", "prepare_target", "run"])
def test_cleanup_covers_load_and_execution_failure(failure, tmp_path) -> None:
    server = server_mock()
    setup = setup_config(source="sql_dump")
    dump = tmp_path / "data.sql"
    dump.write_text("SELECT 1;")
    setup.sql_dump.sql_dump_path = str(dump)
    if failure != "run":
        getattr(server, failure).side_effect = RuntimeError(failure)
    with mock.patch("dblib.connector.load_connector", return_value=server):
        with pytest.raises(RuntimeError, match=failure):
            with prepared_database("dolt", setup):
                raise RuntimeError("run")
    server.delete_database.assert_called_once_with(server.provision.return_value)
    server.delete.assert_not_called()


@pytest.mark.parametrize("name", ["dolt", "dolt_mysql", "seekdb", "matrixone", "file_copy", "chronos"])
def test_duplicate_create_after_collision_check_never_deletes_the_winner(name, tmp_path) -> None:
    server = load_connector(name, state_dir=tmp_path)
    module = __import__(f"dblib.systems.{name}.connector", fromlist=[name])
    conn = mock.MagicMock()
    cursor = conn.cursor.return_value.__enter__.return_value
    cursor.execute.side_effect = DuplicateDatabase("another run created bb_test")
    winner = {"present": True}
    with ExitStack() as stack:
        stack.enter_context(mock.patch("dblib.connector.load_connector", return_value=server))
        stack.enter_context(mock.patch.object(server, "exists", return_value=False))
        delete = stack.enter_context(mock.patch.object(
            server, "delete", side_effect=lambda db: winner.update(present=False)))
        if name in ("dolt", "file_copy", "chronos"):
            stack.enter_context(mock.patch.object(server, "_admin", return_value=conn))
        else:
            stack.enter_context(mock.patch.object(getattr(module, name), "connect", return_value=conn))
        if name == "file_copy":
            destructive = stack.enter_context(mock.patch.object(module.FileCopyToolSuite, "FileCopyInfo"))
        elif name == "chronos":
            stack.enter_context(mock.patch.object(module, "_ensure_chronos_loaded", return_value=module.chronos.ChronosToolSuite))
            destructive = stack.enter_context(mock.patch.object(module.chronos.ChronosToolSuite, "ChronosInfo"))
        with pytest.raises(DuplicateDatabase):
            with prepared_database(name, setup_config()):
                pytest.fail("duplicate database reached execution")
        delete.assert_not_called()
        assert winner["present"]
        if name in ("file_copy", "chronos"):
            destructive.assert_not_called()


@pytest.mark.parametrize("name", ["neon", "xata"])
def test_hosted_partial_provision_deletes_only_the_owned_project(name) -> None:
    module = __import__(f"dblib.systems.{name}.connector", fromlist=[name])
    server = load_connector(name)
    if name == "neon":
        create = mock.patch.object(module.NeonToolSuite, "create_neon_project", return_value={
            "project": {"id": "owned-project"},
            "branch": {"id": "br-owned", "name": "main"},
            "connection_uris": [{"connection_uri": "unused"}],
        })
        delete = mock.patch.object(module.neon, "api_request")
    else:
        create = mock.patch.object(module.XataToolSuite, "create_xata_project", return_value=(
            "owned-project", "br-owned", "main", "unused"))
        delete = mock.patch.object(module.XataToolSuite, "delete_project")
    with create, delete as cleanup, mock.patch.object(module.psycopg2, "connect", side_effect=RuntimeError("connect failed")):
        with pytest.raises(RuntimeError, match="connect failed"):
            server.provision("bb_test")
    if name == "neon":
        cleanup.assert_called_once_with("DELETE", "projects/owned-project")
    else:
        cleanup.assert_called_once_with("owned-project")


def test_neon_failed_project_initialization_removes_the_returned_project() -> None:
    from dblib.systems.neon import adapter as neon

    project = {"project": {"id": "owned-project"}, "operations": [{"id": "op"}]}
    with mock.patch.object(neon, "api_request", return_value=project) as request, \
            mock.patch.object(neon, "wait_operations", side_effect=RuntimeError("initialization failed")):
        with pytest.raises(RuntimeError, match="initialization failed"):
            neon.create_project("project_bb_test")
    assert request.call_args_list[-1] == mock.call("DELETE", "projects/owned-project")


def test_xata_failed_branch_initialization_removes_the_returned_project() -> None:
    from dblib.systems.xata.adapter import XataToolSuite

    with mock.patch.object(XataToolSuite, "_request", side_effect=[
            {"id": "owned-project"}, RuntimeError("branch creation failed")]), \
            mock.patch.object(XataToolSuite, "delete_project") as delete:
        with pytest.raises(RuntimeError, match="branch creation failed"):
            XataToolSuite.create_xata_project("project_bb_test")
    delete.assert_called_once_with("owned-project")


def test_owned_partial_provision_cleanup_failure_remains_visible() -> None:
    from dblib.systems.neon import connector as neon

    project = {"project": {"id": "owned-project"}, "branch": {"id": "br", "name": "main"}}
    with mock.patch.object(neon.NeonToolSuite, "create_neon_project", return_value=project), \
            mock.patch.object(neon.psycopg2, "connect", side_effect=RuntimeError("connect failed")), \
            mock.patch.object(neon.neon, "api_request", side_effect=RuntimeError("cleanup failed")):
        with pytest.raises(RuntimeError, match="cleanup failed") as caught:
            load_connector("neon").provision("bb_test")
    assert str(caught.value.__context__) == "connect failed"


@pytest.mark.parametrize("name", ["dolt_mysql", "seekdb", "matrixone"])
def test_database_existence_treats_underscore_as_literal(name, tmp_path) -> None:
    module = __import__(f"dblib.systems.{name}.connector", fromlist=[name])
    server = load_connector(name)
    server.data_dir = tmp_path
    conn = mock.MagicMock()
    cursor = conn.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = ("bbXtest",)
    cursor.fetchall.return_value = [("bbXtest",)]
    with mock.patch.object(getattr(module, name), "connect", return_value=conn):
        assert not server.exists("bb_test")
        with pytest.raises(ValueError, match="does not exist"):
            server.attach("bb_test", SimpleNamespace(branch_id=""))
        cursor.fetchall.return_value = [("bb_test",)]
        assert server.exists("bb_test")


def test_file_copy_rejects_non_root_attach_instead_of_targeting_base() -> None:
    server = load_connector("file_copy")
    with mock.patch.object(server, "exists", return_value=True) as exists:
        with pytest.raises(ValueError, match="root"):
            server.attach("bb_test", SimpleNamespace(branch_id="child"))
        exists.assert_not_called()


@pytest.mark.parametrize("action", ["provision", "attach"])
def test_file_copy_rejects_names_that_lose_the_owned_branch_prefix(action) -> None:
    server = load_connector("file_copy")
    with mock.patch.object(server, "_admin") as admin:
        with pytest.raises(ValueError, match="30"):
            if action == "provision":
                server.provision("b" * 31)
            else:
                server.attach("b" * 31, SimpleNamespace(branch_id=""))
        admin.assert_not_called()


def test_cleanup_false_keeps_database_after_execution_failure() -> None:
    server = server_mock()
    with mock.patch("dblib.connector.load_connector", return_value=server):
        with pytest.raises(RuntimeError, match="run"):
            with prepared_database("dolt", setup_config(cleanup=False)):
                raise RuntimeError("run")
    server.delete.assert_not_called()
    server.delete_database.assert_not_called()


def test_existing_database_skips_provision_and_load_but_honors_cleanup() -> None:
    server = server_mock()
    setup = setup_config(source="existing_db")
    with mock.patch("dblib.connector.load_connector", return_value=server):
        with prepared_database("dolt", setup) as target:
            assert target is server.prepare_target.return_value
    server.attach.assert_called_once_with("bb_test", setup.existing_db)
    server.exists.assert_not_called()
    server.provision.assert_not_called()
    server.load.assert_not_called()
    server.delete_database.assert_called_once_with(server.attach.return_value)


def test_failed_attach_does_not_delete_existing_resources() -> None:
    server = server_mock()
    server.attach.side_effect = ValueError("missing branch")
    with mock.patch("dblib.connector.load_connector", return_value=server):
        with pytest.raises(ValueError, match="missing branch"):
            with prepared_database("dolt", setup_config(source="existing_db")):
                pytest.fail("failed attachment reached execution")
    server.delete.assert_not_called()
    server.delete_database.assert_not_called()


def test_missing_dump_fails_before_provider_access(tmp_path) -> None:
    setup = setup_config(source="sql_dump")
    setup.sql_dump.sql_dump_path = str(tmp_path / "missing.sql")
    with mock.patch("dblib.connector.load_connector") as load:
        with pytest.raises(ValueError, match="SQL dump does not exist"):
            with prepared_database("dolt", setup):
                pytest.fail("missing dump reached execution")
        load.assert_not_called()


@pytest.mark.parametrize("name,root", [("file_copy", "bb_test"), ("chronos", "main")])
def test_attach_does_not_initialize_or_delete_existing_data(name, root) -> None:
    server = load_connector(name)
    module = __import__(f"dblib.systems.{name}.connector", fromlist=[name])
    suite = module.FileCopyToolSuite if name == "file_copy" else module.chronos.ChronosToolSuite
    info = "FileCopyInfo" if name == "file_copy" else "ChronosInfo"
    with mock.patch.object(server, "exists", return_value=True), mock.patch.object(suite, info) as initialize:
        database = server.attach("bb_test", SimpleNamespace(branch_id=""))
        assert database == DatabaseHandle("bb_test", root)
        initialize.assert_not_called()


@pytest.mark.parametrize("branch_id,expected", [("", "br-default"), ("br-selected", "br-selected")])
def test_neon_attach_and_cleanup_use_supplied_project_identity(branch_id, expected) -> None:
    from dblib.systems.neon import connector as neon

    branches = {"branches": [
        {"id": "br-default", "name": "main", "default": True},
        {"id": "br-selected", "name": "selected", "default": False},
    ]}
    server = load_connector("neon")
    existing = SimpleNamespace(neon_project_id="actual-project", branch_id=branch_id)
    with mock.patch.object(neon.NeonToolSuite, "get_project_branches", return_value=branches) as lookup:
        database = server.attach("bb_test", existing)
    lookup.assert_called_once_with("actual-project")
    assert database.state == neon.NeonState("actual-project", expected)
    with mock.patch.object(neon.neon, "api_request") as request, \
            mock.patch.object(neon.neon, "list_projects") as discover:
        server.delete_database(database)
    request.assert_called_once_with("DELETE", "projects/actual-project")
    discover.assert_not_called()


def test_neon_consumption_metrics_waits_for_nonempty_metrics() -> None:
    from dblib.systems.neon import connector as neon

    server = load_connector("neon")
    database = DatabaseHandle(
        "bb_test", "main", neon.NeonState("actual-project", "br-default"))
    metrics = {"count": 1, "all_metrics": [{"metric": "compute_seconds"}]}
    with mock.patch.object(neon.time, "sleep") as sleep, \
            mock.patch.object(neon.NeonToolSuite, "get_consumption_metrics",
                              side_effect=[None, {"all_metrics": []}, metrics]) as fetch:
        assert server.consumption_metrics(database) is metrics

    assert sleep.call_args_list == [mock.call(60)] * 17
    assert fetch.call_args_list == [mock.call("actual-project")] * 3


def test_file_copy_recovery_state_survives_fresh_connector_instance(tmp_path) -> None:
    from dblib.systems.file_copy import connector as file_copy

    server = load_connector("file_copy", state_dir=tmp_path)
    events = []
    current_method = ["copy"]
    conn = mock.MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value
    def execute(sql: str, params=None) -> None:
        events.append((sql, params))
        if sql.startswith("ALTER SYSTEM SET"):
            current_method[0] = params[0]

    cur.execute.side_effect = execute
    cur.fetchone.side_effect = lambda: (current_method[0],)

    with mock.patch.object(server, "_admin", return_value=conn), \
            mock.patch.object(file_copy.FileCopyToolSuite, "FileCopyInfo") as destructive:
        server.provision("bb_test")
    destructive.assert_not_called()
    assert events[:3] == [
        ('CREATE DATABASE "bb_test"', None),
        ("SHOW file_copy_method;", None),
        ("ALTER SYSTEM SET file_copy_method = %s;", (file_copy.file_copy.FILE_COPY_METHOD,)),
    ]
    recovery = tmp_path / "file_copy.prev_method"
    assert recovery.read_text().strip() == "copy"
    assert recovery.stat().st_mode & 0o777 == 0o600
    other = load_connector("file_copy", state_dir=tmp_path)
    with mock.patch.object(other, "_admin", return_value=conn), \
            mock.patch.object(other, "exists", return_value=False), \
            mock.patch.object(file_copy.file_copy, "drop_database"):
        other.version()
        other.capture_prev_method()
        assert recovery.read_text().strip() == "copy"
        other.delete("bb_test")
    assert not recovery.exists()
    assert ("ALTER SYSTEM SET file_copy_method = %s;", ("copy",)) in events
    assert current_method[0] == "copy"
    after_recovery = list(events)
    repeat = load_connector("file_copy", state_dir=tmp_path)
    with mock.patch.object(repeat, "_admin", return_value=conn), \
            mock.patch.object(repeat, "exists", return_value=False), \
            mock.patch.object(file_copy.file_copy, "drop_database") as drop:
        repeat.delete("bb_test")
    drop.assert_called_once_with("bb_test")
    assert events == after_recovery
    assert not recovery.exists()
    assert all(not sql.startswith("ALTER SYSTEM RESET") for sql, _ in events)


def test_file_copy_failed_method_setup_removes_owned_database_and_restores_method(tmp_path) -> None:
    from dblib.systems.file_copy import connector as file_copy

    server = load_connector("file_copy", state_dir=tmp_path)
    conn = mock.MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value
    cur.fetchone.return_value = ("copy",)
    calls = []

    def execute(sql: str, params=None) -> None:
        calls.append((sql, params))
        if sql.startswith("ALTER SYSTEM SET") and params == (file_copy.file_copy.FILE_COPY_METHOD,):
            raise RuntimeError("method setup failed")

    cur.execute.side_effect = execute
    with mock.patch.object(server, "_admin", return_value=conn), \
            mock.patch.object(server, "exists", return_value=False), \
            mock.patch.object(file_copy.file_copy, "drop_database") as drop:
        with pytest.raises(RuntimeError, match="method setup failed"):
            server.provision("bb_test")
    drop.assert_called_once_with("bb_test")
    assert ("ALTER SYSTEM SET file_copy_method = %s;", ("copy",)) in calls
    assert not (tmp_path / "file_copy.prev_method").exists()


@pytest.mark.parametrize("name", ["dolt", "dolt_mysql"])
def test_dolt_cleanup_removes_dropped_database_trash(name, tmp_path) -> None:
    server = load_connector(name)
    trash = tmp_path / ".dolt_dropped_databases" / "bb_test"
    trash.mkdir(parents=True)
    conn = mock.MagicMock()
    conn.cursor.return_value.__enter__.return_value.fetchone.return_value = None
    admin = (mock.patch.object(server, "_admin", return_value=conn) if name == "dolt"
             else mock.patch("dblib.systems.dolt_mysql.connector.dolt_mysql.connect", return_value=conn))
    with mock.patch.object(server, "data_dir", tmp_path), admin, \
            mock.patch("dblib.systems.dolt_mysql.connector.dolt_mysql.drop_database"):
        assert server.exists("bb_test")
        server.delete("bb_test")
        assert not trash.exists()
        assert not server.exists("bb_test")
