import asyncio
import os
import tempfile

import pyarrow.parquet as pq
import pytest

from dblib import result_pb2 as rslt
from dblib.db_api import Ref, UnsupportedOperation, OpStatus
from dblib.result_collector import ResultCollector
from tests.fake_backend import FakeSuite


@pytest.fixture
def suite(tmp_path):
    rc = ResultCollector(run_id="t", output_dir=str(tmp_path))
    s = FakeSuite(rc)
    s.exec(["CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
            ("INSERT INTO t VALUES (?, ?)", (1, "a"))], timed=False)
    return s


def rows(suite, op=None):
    out = suite.result_collector.results
    return [r for r in out if op is None or r.op_type == op]


def test_ref_parse():
    assert Ref.parse("main") == Ref("main")
    assert Ref.parse("main@abc") == Ref("main", "abc")
    assert str(Ref("b", "h")) == "b@h"
    assert Ref.parse(Ref("x")) == Ref("x")


def test_capabilities():
    caps = FakeSuite.capabilities()
    assert caps["branch"] and caps["commit"] and caps["log"] and caps["diff"]
    assert not caps["merge"] and not caps["rebase"] and not caps["revert"]
    assert caps["exec_async"]


def test_unsupported_verb_records_and_continues(suite):
    r = suite.merge("main", "other")
    assert r.unsupported and r.latency == 0
    row = rows(suite, rslt.OpType.MERGE)[0]
    assert row.status == rslt.OpStatus.UNSUPPORTED
    assert not suite.result_collector.workflow_supported
    assert "MERGE" in suite.result_collector.support_summary()["unsupported_ops"][0]
    with pytest.raises(UnsupportedOperation):
        r.raise_for_status()
    with pytest.raises(UnsupportedOperation):
        suite.rebase("main", "x", raise_on_error=True)


def test_failed_verb_records(suite):
    r = suite.branch("main")  # already exists
    assert r.failed and "exists" in r.error
    row = rows(suite, rslt.OpType.BRANCH)[0]
    assert row.status == rslt.OpStatus.FAILED and row.error_message
    assert suite.result_collector.workflow_supported  # failed != unsupported


def test_branch_commit_log_diff_reset_delete(suite):
    assert suite.commit("main", "init").ok
    b = suite.branch("feat", "main", storage=True)
    assert b.ok and b.storage_before > 0 and b.storage_after > b.storage_before
    res = suite.exec([("INSERT INTO t VALUES (?, ?)", (2, "b"))], refs=["feat"])
    assert res[0].ok and res[0].connect is not None and res[0].connect.ok
    c = suite.commit("feat", "add")
    assert c.ok and isinstance(c.value, str)
    log = suite.log("feat", limit=5)
    assert [e["message"] for e in log.value] == ["add", "init"]
    d = suite.diff("main", "feat")
    assert d.value["rows_a"] == 1 and d.value["rows_b"] == 2 and "schema" in d.value
    # commit ref (supported here)
    at = suite.exec(["SELECT count(*) FROM t"], refs=[f"feat@{log.value[1]['hash']}"])
    assert at[0].ok and at[0].rows == [(1,)]
    assert not rows(suite, rslt.OpType.EXEC)[-1].commit_ref_fallback
    # reset feat back to the init snapshot
    assert suite.reset("feat", log.value[1]["hash"]).ok
    assert suite.exec(["SELECT count(*) FROM t"], refs=["feat"])[0].rows == [(1,)]
    # delete: the backend moves the connection off the branch first
    assert suite.current_ref == Ref("feat")
    assert suite.delete("feat").ok
    assert suite.current_ref == Ref("main")
    assert "feat" not in suite.list_branches()
    assert suite.delete("main").failed


def test_exec_rows_and_statement_breakdown(suite):
    suite.result_collector.record_num_keys_touched(3)
    res = suite.exec(
        ["SELECT * FROM t", ("INSERT INTO t VALUES (?, ?)", (5, "e")),
         "CREATE INDEX i ON t(v)"],
        refs=["main"], label="phase1",
    )[0]
    assert res.ok and len(res.statements) == 3
    ops = [r.op_type for r in rows(suite)]
    # on main already: no CONNECT row; then READ, INSERT, DDL, EXEC
    assert ops == [rslt.OpType.READ, rslt.OpType.INSERT, rslt.OpType.DDL,
                   rslt.OpType.EXEC]
    out = rows(suite)
    assert out[0].num_keys_touched == 3 and out[1].num_keys_touched == 0
    assert all(r.exec_id == out[0].exec_id for r in out)
    assert all(r.label == "phase1" for r in out)
    assert "CREATE INDEX" in out[-1].sql_query
    assert out[-1].latency >= sum(r.latency for r in out[:-1]) * 0.5


def test_exec_python_script_and_params(suite):
    script = """
n = params["n"]
def run(db):
    db.sql("INSERT INTO t VALUES (?, ?)", (n, "p"))
    return db.sql("SELECT count(*) FROM t")[0][0]
"""
    res = suite.exec(script, refs=["main"], params={"n": 9})[0]
    assert res.ok and res.value == 2
    # top-level only script
    res = suite.exec("db.sql('SELECT 1')", refs=["main"])[0]
    assert res.ok and res.rows == [(1,)]
    # callable
    res = suite.exec(lambda db: db.sql("SELECT 2")[0][0], refs=["main"])[0]
    assert res.value == 2


def test_exec_failure_in_script(suite):
    res = suite.exec(["SELECT * FROM nope"], refs=["main"])[0]
    assert res.failed and "nope" in res.error
    stmt_row, exec_row = rows(suite)
    assert stmt_row.status == rslt.OpStatus.FAILED
    assert exec_row.status == rslt.OpStatus.FAILED
    with pytest.raises(Exception):
        suite.exec(["SELECT * FROM nope"], refs=["main"], raise_on_error=True)


def test_exec_per_ref_and_connect_rows(suite):
    suite.branch("b1", "main", timed=False)
    suite.branch("b2", "main", timed=False)
    res = suite.exec(["SELECT count(*) FROM t"], refs=["b1", "b2", "b2"])
    assert [r.ref for r in res] == ["b1", "b2", "b2"]
    assert res[2].connect is None  # already on b2
    connects = rows(suite, rslt.OpType.CONNECT)
    assert [c.ref for c in connects] == ["b1", "b2"]
    assert all(c.exec_id == connects[0].exec_id for c in connects)


def test_exec_multi_unsupported(suite):
    res = suite.exec(["SELECT 1"], refs=["main", "main"], mode="multi")
    assert len(res) == 1 and res[0].unsupported
    row = rows(suite, rslt.OpType.EXEC)[0]
    assert list(row.refs) == ["main", "main"]
    assert not suite.result_collector.workflow_supported


def test_native_only_mode_marks_simulated_verbs_unsupported(suite, monkeypatch):
    import dblib.db_api as api
    FakeSuite.IMPLEMENTATION = {"diff": "simulated", "commit_refs": "simulated"}
    try:
        assert FakeSuite.implementation()["diff"] == "simulated"
        monkeypatch.setattr(api, "NATIVE_ONLY", True)
        impl = FakeSuite.implementation()
        assert impl["diff"] == "unsupported" and impl["commit_refs"] == "unsupported"
        assert impl["commit"] == "native"
        suite.commit("main", "c1")
        r = suite.diff("main", "main")
        assert r.unsupported and "native-only" in r.error
        assert rows(suite, rslt.OpType.DIFF)[0].status == rslt.OpStatus.UNSUPPORTED
        assert suite.commit("main", "c2").ok          # native verbs still run
        # A simulated commit ref falls back to the branch head, flagged.
        res = suite.exec(["SELECT 1"], refs=["main@deadbeef"])[0]
        assert res.ok and rows(suite, rslt.OpType.EXEC)[0].commit_ref_fallback
    finally:
        FakeSuite.IMPLEMENTATION = {}


def test_schema_diff_compare_and_fake_catalog(suite):
    a = {"t": {"id": "int", "v": "text"}, "gone": {"id": "int"}}
    b = {"t": {"id": "int", "v": "varchar(8)", "w": "int"}, "new": {"id": "int"}}
    d = FakeSuite._compare_schemas(a, b)
    assert d["tables_added"] == ["new"] and d["tables_dropped"] == ["gone"]
    assert d["columns_added"] == {"t": ["w"]} and d["columns_dropped"] == {}
    assert d["columns_changed"] == {"t": [["v", "text", "varchar(8)"]]} and d["changed"]
    assert not FakeSuite._compare_schemas(a, a)["changed"]
    assert "schema_diff" not in FakeSuite.implementation()   # one verb: diff covers rows and schema
    suite.branch("other", "main")
    suite.exec(["CREATE TABLE extra (id INT PRIMARY KEY, v TEXT)", "ALTER TABLE t ADD COLUMN w INT"], refs=["other"])
    r = suite.diff("main", "other")
    assert r.ok and r.value["schema"]["tables_added"] == ["extra"] and r.value["schema"]["columns_added"] == {"t": ["w"]}
    assert rows(suite, rslt.OpType.DIFF)[-1].status == rslt.OpStatus.OK
    assert not suite.diff("other", "other").value["schema"]["changed"]


def test_commit_ref_fallback(suite, capsys):
    FakeSuite.SUPPORTS_COMMIT_REFS = False
    try:
        res = suite.exec(["SELECT 1"], refs=["main@deadbeef"])[0]
        assert res.ok and res.ref == "main"
        assert rows(suite, rslt.OpType.EXEC)[0].commit_ref_fallback
        assert "WARNING" in capsys.readouterr().out
    finally:
        FakeSuite.SUPPORTS_COMMIT_REFS = True


def test_untimed_exec_records_nothing(suite):
    suite.exec(["SELECT 1"], refs=["main"], timed=False)
    assert rows(suite) == []


def test_exec_async(suite):
    suite.branch("b1", "main", timed=False)

    async def go():
        await suite.open_async_pool(2)
        r1 = await suite.exec_async(["SELECT count(*) FROM t"], refs=["main"])
        script = "async def run(db):\n    return (await db.sql('SELECT 1'))[0][0]"
        r2 = await suite.exec_async(script, refs=["b1"])
        r3 = await suite.exec_async("db.sql('SELECT 1')", refs=["main"])
        await suite.close_async_pool()
        return r1, r2, r3

    r1, r2, r3 = asyncio.run(go())
    assert r1[0].ok and r1[0].rows == [(1,)]
    assert r2[0].ok and r2[0].value == 1
    assert r3[0].failed and "async def run" in r3[0].error
    ops = [(r.op_type, r.status) for r in rows(suite)]
    assert ops[0] == (rslt.OpType.CONNECT, rslt.OpStatus.OK)
    assert ops[1] == (rslt.OpType.READ, rslt.OpStatus.OK)
    assert ops[2] == (rslt.OpType.EXEC, rslt.OpStatus.OK)
    assert suite.async_pool is None


def test_parquet_output(suite, tmp_path):
    suite.commit("main", "x")
    suite.merge("main", "y")
    suite.exec(["SELECT 1"], refs=["main"])
    suite.result_collector.write_to_parquet()
    table = pq.read_table(os.path.join(tmp_path, "t.parquet"))
    assert set(table["op_name"].to_pylist()) == {"COMMIT", "MERGE", "READ", "EXEC"}
    assert set(table["status"].to_pylist()) == {"OK", "UNSUPPORTED"}
    assert "ref" in table.column_names and "exec_id" in table.column_names


def test_in_flight_ops_and_stall_detection(tmp_path):
    import threading
    import time as _time
    from dblib.result_collector import ResultCollector

    c = ResultCollector(run_id="t", output_dir=str(tmp_path))
    started = threading.Event()
    release = threading.Event()

    def op():
        t = c.begin_op("REBASE", "dev_1", "dev_rebase")
        started.set()
        release.wait()
        c.end_op(t)

    th = threading.Thread(target=op)
    th.start()
    assert started.wait(2)
    _time.sleep(0.05)
    live = c.in_flight_ops()
    assert [(o["op"], o["ref"], o["label"]) for o in live] == [("REBASE", "dev_1", "dev_rebase")]
    assert c.stalled_ops(0.01) and not c.stalled_ops(60)
    token = live[0]["token"]
    assert c.cancel_op(token, "stalled: cancelled after 1s") is True
    assert c.in_flight_ops()[0]["cancelled"] is True
    release.set()
    th.join(2)
    assert c.in_flight_ops() == []
    assert c.cancel_op(token, "x") is False


def test_cancelled_op_is_failed_and_noted(tmp_path):
    """A watchdog cancel makes the running verb fail with the stall note
    and leaves the suite usable for the next operation."""
    import threading
    from dblib.result_collector import ResultCollector
    from dblib.db_api import DBToolSuite, OperationCancelled

    class Slow(DBToolSuite):
        BACKEND_NAME = "slow"

        def __init__(self, collector):
            super().__init__(None, collector)
            self.gate = threading.Event()

        def _connect_impl(self, ref):
            pass

        def _storage_bytes(self):
            return 0

        def _branch_impl(self, name, from_ref):
            self.gate.wait(2)
            self._check_cancel()

        def _commit_impl(self, ref, message):
            return "c1"

    c = ResultCollector(run_id="t", output_dir=str(tmp_path))
    s = Slow(c)
    out = {}

    def run():
        out["res"] = s.branch("b", "main", label="lbl")

    th = threading.Thread(target=run)
    th.start()
    for _ in range(50):
        live = c.in_flight_ops()
        if live:
            break
        threading.Event().wait(0.02)
    assert live and live[0]["op"] == "BRANCH"
    assert c.cancel_op(live[0]["token"], "stalled: cancelled after 9s")
    s.gate.set()
    th.join(3)
    res = out["res"]
    assert not res.ok
    assert res.error.startswith("stalled: cancelled after 9s; OperationCancelled")
    row = [r for r in c.results if r.op_type == rslt.OpType.BRANCH][-1]
    assert row.status == rslt.OpStatus.FAILED and "stalled" in row.error_message
    assert s.commit("main", "after").ok
    assert [(st["op"], st["outcome"]) for st in c.stalls] == [("BRANCH", "failed")]
