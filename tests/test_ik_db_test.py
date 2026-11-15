"""IK-DB-test is discovered as its own system and records results under its own name."""

from unittest import mock

from dblib import result_collector as rc
from dblib.connector import DatabaseHandle, connector_names, load_connector
from dblib.systems import load_system
from dblib.systems.ik_db_test.adapter import IKDBTestToolSuite
from dblib.target import SuiteRequest


def test_ik_db_test_is_a_runnable_self_hosted_system_with_its_own_identity() -> None:
    assert "ik_db_test" in connector_names()
    meta = load_system("ik_db_test")
    assert meta["system"] == "IK-DB-test"
    assert meta["hosted"] == "no" and meta["credentials"] == []
    server = load_connector("ik_db_test")
    target = server.prepare_target(DatabaseHandle("bb_test", "main"))
    request = SuiteRequest(rc.ResultCollector(), "main", 0, measure_storage=True)
    with mock.patch.object(IKDBTestToolSuite, "init_for_bench", return_value=mock.sentinel.suite) as init:
        assert target.open_suite(request) is mock.sentinel.suite
    assert target.suite_type.BACKEND_NAME == "ik_db_test"
    init.assert_called_once_with(request.result_collector, "bb_test", "main", True)
