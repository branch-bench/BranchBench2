"""Macrobench building blocks against the in-memory fake backend: schema
loading, the CH generator (and the TPC-C consistency conditions on its
output), the fault catalog, the invariant recorder, the spine-load pause
gate and the on_conflict plumbing."""

import threading
import time

import pytest

from dblib import result_collector as rc
from dblib.db_api import DBToolSuite
from macrobench import faults, schema, task_pb2 as tp
from macrobench.datagen.ch import CHScale, CHGenerator, seed_ch
from macrobench.intensity import apply_intensity
from macrobench.scenarios import SCENARIOS, load_params
from macrobench.scenarios.base import InvariantFailed, InvariantRecorder
from macrobench.scenarios.params import parse_params
from macrobench.tpcc import Quiescer, SpineLoad
from tests.fake_backend import FakeSuite


def mini_schema() -> tp.SchemaConfig:
    return tp.SchemaConfig(scale_factor=1, items=50, customers_per_district=10,
                           orders_per_district=10, suppliers=5, skip_foreign_keys=True)


@pytest.fixture
def suite():
    s = FakeSuite(rc.ResultCollector())
    yield s
    s.close_connection()


def _apply(suite, cfg, key):
    # sqlite has no DATE type keyword issues, but CHAR(n)/DECIMAL are fine.
    stats = schema.apply_schema(suite, "main", cfg, key, log=lambda m: None)
    assert stats["failed"] == [], stats["failed"]
    return stats


def test_schema_statements_follow_config():
    cfg = mini_schema()
    stmts = schema.ddl_statements(cfg, "rl_env")
    assert not any(schema.is_foreign_key(s) for s in stmts)
    assert "rl_task" in schema.table_names(cfg, "rl_env")
    cfg.base = tp.BaseSchema.NO_BASE
    assert schema.table_names(cfg, "multi_agent") == ["task", "dependency"]
    cfg.extensions.append(tp.SchemaExtension.DATA_AGENT_EXT)
    assert "batch_manifest" in schema.table_names(cfg, "multi_agent")


def test_generator_counts_match_scale():
    scale = CHScale.from_config(mini_schema())
    gen = CHGenerator(scale, seed=1)
    counts = {t: sum(1 for _ in rows) for t, _cols, rows in gen.tables()}
    expected = scale.expected_counts()
    for table, n in expected.items():
        assert counts[table] == n, table
    assert counts["order_line"] >= 5 * expected["orders"]


def test_seeded_data_satisfies_consistency_conditions(suite):
    cfg = mini_schema()
    _apply(suite, cfg, "rl_env")
    counts = seed_ch(suite, "main", CHScale.from_config(cfg), log=lambda m: None)
    assert counts["stock"] == 50
    res = suite.exec(lambda db: faults.check_consistency(db), refs=["main"])[0]
    assert res.ok, res.error
    assert faults.all_hold(res.value), res.value


@pytest.mark.parametrize("fault_id", sorted(faults.FAULTS))
def test_each_fault_breaks_and_repair_restores(suite, fault_id):
    cfg = mini_schema()
    _apply(suite, cfg, "rl_env")
    seed_ch(suite, "main", CHScale.from_config(cfg), log=lambda m: None)
    fault = faults.FAULTS[fault_id]
    state = suite.exec(lambda db: fault.inject(db, 1, 2), refs=["main"])[0]
    assert state.ok, state.error
    broken = suite.exec(lambda db: faults.check_consistency(db), refs=["main"])[0].value
    assert any(broken[c] > 0 for c in fault.conditions), (fault.name, broken)
    suite.exec(lambda db: fault.repair(db, 1, 2, state.value), refs=["main"])
    fixed = suite.exec(lambda db: faults.check_consistency(db), refs=["main"])[0].value
    assert faults.all_hold(fixed), fixed


def test_invariant_recorder_summary_and_fail_fast():
    inv = InvariantRecorder()
    inv.check("a", True)
    inv.expect("b", 1, 2)
    inv.not_applicable("c", "unsupported")
    s = inv.summary()
    assert (s["passed"], s["failed"], s["not_applicable"]) == (1, 1, 1)
    assert s["all_passed"] is False
    strict = InvariantRecorder(fail_fast=True)
    with pytest.raises(InvariantFailed):
        strict.check("x", False, "boom")
    off = InvariantRecorder(enabled=False)
    off.check("ignored", False)
    assert off.summary()["results"] == []


def test_quiescer_pauser_has_priority():
    q = Quiescer()
    stop = threading.Event()
    in_flight = []

    def client():
        while not stop.is_set():
            with q.transaction():
                in_flight.append(1)
                time.sleep(0.005)
                in_flight.pop()

    threads = [threading.Thread(target=client, daemon=True) for _ in range(4)]
    for t in threads:
        t.start()
    time.sleep(0.02)
    t0 = time.time()
    with q.pause():
        waited = time.time() - t0
        assert in_flight == []
        with q.pause():  # nested on the same thread
            assert in_flight == []
        time.sleep(0.02)
        assert in_flight == []
    assert waited < 1.0
    stop.set()
    for t in threads:
        t.join(1)


class _MergeSpy(FakeSuite):
    def _merge_impl(self, into, source, message, on_conflict="ours"):
        self.seen = on_conflict
        return {"fast_forward": False, "conflicts": 0}

    def _rebase_impl(self, ref, onto, on_conflict="ours"):
        self.seen = on_conflict
        return {"conflicts": 0}


def test_on_conflict_is_validated_and_passed_through():
    s = _MergeSpy(rc.ResultCollector())
    s.branch("b", "main")
    assert s.merge("main", "b", on_conflict="theirs").ok and s.seen == "theirs"
    policy = lambda db, conflicts: None  # noqa: E731
    assert s.rebase("b", "main", on_conflict=policy).ok and s.seen is policy
    with pytest.raises(ValueError):
        s.merge("main", "b", on_conflict="mine")
    assert DBToolSuite.supports("merge") is False


def test_spine_load_wait_for_total_counts_transactions_and_stops():
    load = SpineLoad(lambda: None, "main", scale=None, clients=0)
    stop = threading.Event()

    def produce():
        for _ in range(7):
            time.sleep(0.002)
            with load._lock:
                load.total += 1
                load._total_cond.notify_all()

    load._threads = [threading.Thread(target=produce, daemon=True)]
    load._threads[0].start()
    assert load.wait_for_total(5, stop=stop, poll=0.01)
    assert load.total >= 5
    load._threads[0].join(1)
    # Target beyond what the (now finished) load produced: returns False
    # once no client is running, instead of blocking.
    assert not load.wait_for_total(100, stop=stop, poll=0.01)
    stop.set()
    assert not load.wait_for_total(100, stop=stop, poll=0.01)


def _scaled(key, branch=1.0, data=1.0, workload=None, **values):
    """(workload, params, changed) after apply_intensity on ``values``."""
    cls = SCENARIOS[key]
    w = workload or tp.Workload()
    w.branch_intensity, w.data_intensity = branch, data
    params = parse_params(key, cls.Params, values)
    return w, params, apply_intensity(w, cls, params)


def test_intensity_scales_branch_and_data_knobs():
    w = tp.Workload()
    w.branch_ops.commit_interval = 4
    w.data_ops.statements_per_step = 8
    w.data_ops.rows_per_write = 100
    w.data_ops.spine_clients = 2
    w.data_ops.worker_threads = 8
    w, p, changed = _scaled("dev_agent", 2.0, 0.5, workload=w, dev_branches=10, concurrent_branches=3,
                            dev_phase_steps=5, rebases_per_branch=1, spine_commit_every_txns=20)
    assert w.branch_ops.commit_interval == 2
    assert p.dev_branches == 20
    assert p.rebases_per_branch == 2                    # capped at 2
    assert p.concurrent_branches == 3                   # threads untouched
    assert w.data_ops.statements_per_step == 4
    assert w.data_ops.rows_per_write == 50
    assert w.data_ops.spine_clients == 1
    assert w.data_ops.worker_threads == 8               # threads untouched
    assert p.dev_phase_steps == 2                       # round(2.5) -> 2
    assert p.spine_commit_every_txns == 10              # data knob
    assert changed["dev_agent.dev_branches"] == (10, 20)
    assert "data_ops.worker_threads" not in changed


def test_intensity_keeps_ops_agent_offset_inside_spine_and_caps_fanout():
    _, p, _ = _scaled("ops_agent", 0.5, 2.0, spine_commits=6, fault_commit_offset=5,
                      investigation_branches=2, commit_every_txns=20)
    assert p.spine_commits == 3
    assert 0 < p.fault_commit_offset < p.spine_commits
    assert p.commit_every_txns == 40                    # data knob, not branch

    _, p, _ = _scaled("context_mgmt", 3.0, fanout=6, compaction_cycles=1)
    assert p.fanout == 10
    assert p.compaction_cycles == 3

    with pytest.raises(ValueError):
        _scaled("rl_env", -1.0, tasks=2)
    _, p, changed = _scaled("rl_env", 0.0, tasks=2)    # 0 means unchanged
    assert changed == {}
    assert p.tasks == 2


def test_session_transaction_rolls_back_on_error(suite):
    suite.exec([("CREATE TABLE txn_probe (id INT PRIMARY KEY, v INT)", None),
                ("INSERT INTO txn_probe VALUES (1, 10)", None)], refs=["main"], label="setup")

    def script(db):
        with db.transaction():
            db.sql("UPDATE txn_probe SET v = 20 WHERE id = 1")
        try:
            with db.transaction():
                db.sql("UPDATE txn_probe SET v = 30 WHERE id = 1")
                db.sql("INSERT INTO txn_probe VALUES (1, 99)")  # duplicate key
        except Exception:
            pass
        return db.sql("SELECT v FROM txn_probe WHERE id = 1")[0][0]

    res = suite.exec(script, refs=["main"], label="probe")[0]
    assert res.ok and res.value == 20


def test_run_transaction_retries_then_raises(suite):
    from macrobench import tpcc
    calls = {"n": 0}

    def flaky(db, rng, scale):
        calls["n"] += 1
        raise RuntimeError("serialization failure")

    tpcc.TRANSACTIONS["flaky"] = flaky
    try:
        res = suite.exec(lambda db: tpcc.run_transaction("flaky", db, None, None, retries=3),
                         refs=["main"], label="spine")[0]
    finally:
        del tpcc.TRANSACTIONS["flaky"]
    assert not res.ok and calls["n"] == 3


def test_intensity_caps_rl_env_group_size():
    from importlib.resources import files
    from google.protobuf import text_format
    cfg = tp.MacroBenchConfig()
    text_format.Parse((files("macrobench") / "configs" / "rl_env.textproto").read_text(), cfg)
    cfg.workload.branch_intensity = 5.0
    key, p = load_params(cfg)
    apply_intensity(cfg.workload, SCENARIOS[key], p)
    assert p.tasks == 80                 # tasks keep scaling
    assert p.group_size == 32            # a GRPO group is capped
    assert p.rollouts_per_round == 4     # concurrency is not an intensity knob