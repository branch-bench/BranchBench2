"""dblib.open_backend / open_suite over a mocked connector, and lazy imports."""

import subprocess
import sys
from unittest import mock

import pytest

import dblib
from dblib import connector as dconnector
from dblib import result_collector as rc
from dblib.connector import DatabaseHandle
from dblib.target import PreparedTarget
from tests.fake_backend import FakeSuite


def fake_connector():
    """A connector mock whose target opens FakeSuites and records requests."""
    provider = mock.Mock(spec=dconnector.Connector)
    provider.exists.return_value = True
    database = DatabaseHandle("scratch", "main")
    provider.provision.return_value = database
    provider.attach.return_value = database
    requests = []

    def open_suite(request):
        requests.append(request)
        return FakeSuite(request.result_collector)
    provider.prepare_target.return_value = PreparedTarget("main", FakeSuite, open_suite)
    return provider, database, requests


def test_create_opens_suites_and_drops_on_close():
    provider, database, requests = fake_connector()
    with mock.patch.object(dconnector, "load_connector", return_value=provider) as load, \
            mock.patch("dblib.backends.load_connector", load):
        with dblib.open_backend("dolt", "scratch", create=True, drop_on_close=True) as backend:
            assert backend.default_branch == "main" and backend.db_name == "scratch"
            collector = rc.ResultCollector()
            s1, s2 = backend.suite(collector), backend.suite()
            assert s1.backend_handle is backend and s1.result_collector is collector
            assert [r.worker_index for r in requests] == [0, 1]
            assert all(r.initial_ref == "main" for r in requests)
            provider.delete_database.assert_not_called()
    load.assert_called_once_with("dolt", state_dir=None)
    provider.preflight.assert_called_once()
    provider.delete.assert_called_once_with("scratch")         # it existed: deleted first
    provider.provision.assert_called_once_with("scratch")
    provider.delete_database.assert_called_once_with(database)


def test_attach_passes_the_existing_identity_and_keeps_the_database():
    provider, database, _ = fake_connector()
    with mock.patch("dblib.backends.load_connector", return_value=provider):
        suite = dblib.open_suite("neon", "mydb", neon_project_id="proj-1", branch_id="br-1")
        suite.backend_handle.close()
    name, existing = provider.attach.call_args.args
    assert name == "mydb" and existing.neon_project_id == "proj-1" and existing.branch_id == "br-1"
    provider.provision.assert_not_called()
    provider.delete_database.assert_not_called()


def test_a_failed_load_deletes_what_was_created(tmp_path):
    provider, database, _ = fake_connector()
    provider.load.side_effect = RuntimeError("bad dump")
    dump = tmp_path / "d.sql"
    dump.write_text("SELECT 1;")
    with mock.patch("dblib.backends.load_connector", return_value=provider):
        with pytest.raises(RuntimeError, match="bad dump"):
            dblib.open_backend("dolt", "scratch", create=True, sql_dump=str(dump))
    provider.delete_database.assert_called_once_with(database)


def test_close_can_be_retried_after_a_failed_delete():
    provider, database, _ = fake_connector()
    provider.delete_database.side_effect = [RuntimeError("busy"), None]
    with mock.patch("dblib.backends.load_connector", return_value=provider):
        backend = dblib.open_backend("dolt", "scratch", create=True, drop_on_close=True)
    with pytest.raises(RuntimeError):
        backend.close()
    backend.close()
    backend.close()
    assert provider.delete_database.call_count == 2


@pytest.mark.parametrize("kwargs,match", [
    (dict(db_name="Bad-Name"), "db_name"),
    (dict(db_name="ok", sql_dump="/nonexistent.sql", create=True), "does not exist"),
    (dict(db_name="ok", sql_dump="/etc/hostname"), "needs create"),
])
def test_argument_errors(kwargs, match):
    with pytest.raises(ValueError, match=match):
        dblib.open_backend("dolt", **kwargs)


def test_unknown_backend():
    with pytest.raises(ValueError, match="no built-in database connector"):
        dblib.open_backend("postgres", "x")


def test_import_dblib_loads_no_system():
    code = "import sys, dblib; print(sorted(m for m in sys.modules if m.startswith('dblib.systems.')))"
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True).stdout
    assert out.strip() == "[]"
    assert "dolt" in dblib.connector_names()
