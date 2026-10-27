"""The branch-verb microbenchmark's grid and point loop, on the fake
backend (branch, commit, diff, reset and delete; merge, rebase and revert
are unsupported there, which exercises that path)."""

import pytest

from dblib import result_collector as rc
from macrobench import schema, task_pb2 as tp
from macrobench.datagen.ch import CHScale, seed_ch
from microbench.verbs import grid
from microbench.verbs.driver import run_point
from microbench.verbs.ops import CONTROL, VERB_IMPL, drop, fork
from tests.fake_backend import FakeSuite

STOCK_ROWS = 50


def test_every_axis_names_a_base_parameter():
    for a in grid.AXES:
        assert a.name in grid.BASE
        assert a.verb in grid.VERBS
        for k in a.fixed:
            assert k in grid.BASE


def test_points_at_base_scale_cover_every_axis_once():
    pts = grid.points_for_run(grid.BASE["scale"])
    ids = [p.id for p in pts]
    assert len(ids) == len(set(ids))
    for a in grid.AXES:
        expected = 1 if a.name == "scale" else len(a.values)
        assert sum(1 for p in pts if p.axis is a) == expected


def test_other_scales_run_only_their_scale_points():
    pts = grid.points_for_run(4)
    assert pts and all(p.axis.name == "scale" and p.value == 4 for p in pts)
    assert grid.points_for_run(3) == []


def test_point_params_hold_the_rest_at_base():
    pt = next(p for p in grid.points_for_run(1) if p.id == "merge:conflict=0.5")
    assert pt.params["conflict"] == 0.5
    assert pt.params["rows"] == 1000          # the axis's fixed override
    assert pt.params["scale"] == grid.BASE["scale"]


@pytest.mark.parametrize("overlap,shared", [(0.0, 0), (0.1, 10), (0.5, 50), (1.0, 100)])
def test_key_ranges_share_the_requested_rows(overlap, shared):
    a, b = grid.key_ranges(0, 100, overlap, stock_rows=100_000)
    ra, rb = set(range(a, a + 100)), set(range(b, b + 100))
    assert len(ra & rb) == shared
    assert min(ra | rb) >= 1 and max(ra | rb) <= 100_000


def test_key_ranges_rotate_between_reps_and_clamp():
    a0, _ = grid.key_ranges(0, 100)
    a1, _ = grid.key_ranges(1, 100)
    assert a0 != a1
    a, b = grid.key_ranges(7, 100_000)           # the whole warehouse
    assert a == 1 and b == 1


@pytest.fixture
def seeded():
    suite = FakeSuite(rc.ResultCollector())
    cfg = tp.SchemaConfig(scale_factor=1, items=STOCK_ROWS, customers_per_district=5,
                          orders_per_district=5, suppliers=5, skip_foreign_keys=True)
    stats = schema.apply_schema(suite, "main", cfg, "micro_verbs", log=lambda m: None)
    assert stats["failed"] == []
    seed_ch(suite, "main", CHScale.from_config(cfg), seed=1, batch_rows=50, log=lambda m: None)
    suite.commit("main", "base", timed=False, raise_on_error=True)
    yield suite
    suite.close_connection()


def _run(suite, verb, params, idx=0, reps=2):
    """One point as the driver runs it: on a branch forked from the base
    and dropped afterwards."""
    fork(suite, f"p{idx}", "main")
    try:
        return run_point(f"{verb.name}:test", verb, params, suite, "main", f"p{idx}",
                         reps=reps, warmup=1, storage_reps=0, stock_rows=STOCK_ROWS, log=lambda m: None)
    finally:
        drop(suite, f"p{idx}")


def test_supported_verbs_measure_every_timed_rep(seeded):
    for i, name in enumerate(("branch", "commit", "delete", "diff", "reset")):
        params = dict(grid.BASE, rows=5, written=5, distance=5)
        out = _run(seeded, VERB_IMPL[name], params, idx=i)
        assert out["status"] == "ok", out
        assert len(out["reps"]) == 2 and out["failed"] == 0
        assert out["latency"]["n"] == 2 and out["storage"]["n"] == 2
        if VERB_IMPL[name].deferred:
            assert out["deferred"]["n"] == 2
        else:
            assert out["deferred"] == {}


def test_branch_from_an_older_commit_and_with_live_branches(seeded):
    out = _run(seeded, VERB_IMPL["branch"], dict(grid.BASE, age=2, live=3), idx=10)
    assert out["status"] == "ok" and out["failed"] == 0
    # The point's extra branches and the reps' branches are gone.
    assert not any(b.startswith("p10") for b in seeded.branches)


def test_unsupported_verb_stops_after_the_first_rep(seeded):
    out = _run(seeded, VERB_IMPL["merge"], dict(grid.BASE, rows=5), idx=20)
    assert out["status"] == "unsupported"
    assert out["reps"] == []
    assert not any(b.startswith("p20") for b in seeded.branches)


def test_control_point_records_only_deferred_storage(seeded):
    out = _run(seeded, CONTROL, dict(grid.BASE), idx=30)
    assert out["status"] == "ok"
    assert out["latency"] == {} and out["storage"] == {}
    assert out["deferred"]["n"] == 2
