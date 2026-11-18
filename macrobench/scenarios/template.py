"""Template: copy this file to start a new scenario.

A minimal feature-branch workflow. Each worker forks one branch from main,
writes a few rows on it and commits; every second branch is merged into
main and the others are thrown away; every branch is deleted. The checks:
main holds exactly the merged features' rows, and none of the discarded
ones.

Registered as an *example*: it can be run by name
(``./scripts/run_macrobench.sh --mini example dolt``) and the fake-backend
tests run it, but it is left out of ``--list`` and of sweeps.

To make a new scenario from it (guides/adding_a_scenario.md):

1. Copy to ``macrobench/scenarios/<key>.py``; set ``key``, ``name``,
   ``order``, the labels and ``group``; drop ``example = True``.
2. Declare the parameters in ``Params`` (``param(default, intensity=...,
   doc=...)``), write ``run()`` and the checks.
3. Put the scenario's tables in ``macrobench/schema/<key>.sql``.
4. Add ``macrobench/configs/<key>.textproto`` and ``<key>_mini.textproto``.
5. Run ``uv run pytest tests/test_scenarios.py tests/test_scenario_params.py``.
"""

import threading
from dataclasses import dataclass

from macrobench.scenarios.base import Scenario, Worker, register
from macrobench.scenarios.params import param


@register
class ExampleScenario(Scenario):
    # Identity: the config's workload.scenario value, a display name, and
    # where it sorts and how it is labelled in lists and figures.
    key = "example"
    name = "Example: feature branches merged into main"
    order = 99
    label, plot_label, title = "Example", "Example", "Example: feature branches"
    group = "branch"          # "branch" or "data": which work dominates
    example = True            # not part of --list or sweeps
    # The tables live in macrobench/schema/example.sql (found by the key).

    @dataclass
    class Params:
        # Defaults apply when the config leaves a parameter out. Tag sizes
        # with intensity="branch" (how many branch operations) or "data"
        # (how much SQL per branch); --branch-intensity / --data-intensity
        # multiply them.
        features: int = param(2, intensity="branch", doc="feature branches, one worker each")
        rows_per_feature: int = param(5, intensity="data", doc="rows each feature writes")
        merge_every: int = param(2, doc="merge every n-th feature into main; the rest are discarded")

    def validate(self) -> None:
        p = self.params
        if p.features <= 0 or p.rows_per_feature <= 0 or p.merge_every <= 0:
            raise ValueError("example needs features, rows_per_feature and merge_every > 0")

    def total_units(self) -> int:
        return self.params.features               # one progress tick per feature

    def run(self) -> None:
        ctx, p = self.ctx, self.params
        self._merge_lock = threading.Lock()       # merges into main one at a time
        ctx.run_workers(list(range(1, p.features + 1)), self._feature)

        # Checks run on main after every worker is done. expect() compares
        # an observed value with the expected one; check() takes a bool.
        suite = ctx.new_suite()
        try:
            merged = [f for f in range(1, p.features + 1) if self._merges(f)]
            r = suite.exec([("SELECT COUNT(*) FROM feature_row", None)],
                           refs=[ctx.spine], label="invariant")[0]
            ctx.invariants.expect("EX.1 main holds every merged feature's rows",
                                  int(r.rows[0][0]) if r.ok else None,
                                  len(merged) * p.rows_per_feature)
            r = suite.exec([("SELECT COUNT(DISTINCT feature) FROM feature_row", None)],
                           refs=[ctx.spine], label="invariant")[0]
            ctx.invariants.expect("EX.2 no discarded feature reached main",
                                  int(r.rows[0][0]) if r.ok else None, len(merged))
        finally:
            ctx.close_suite(suite)

    def _merges(self, feature: int) -> bool:
        return feature % self.params.merge_every == 0

    def _feature(self, w: Worker, feature: int) -> None:
        """One worker's whole branch lifecycle. Every database call goes
        through the API (ctx.branch, w.exec, w.commit, suite.merge,
        ctx.delete), which times and records it; give each a label so its
        rows can be told apart in the results."""
        ctx, p = self.ctx, self.params
        branch = f"feature_{feature}"
        res = ctx.branch(w.suite, branch, ctx.spine, label="feature_branch")
        if not res.ok:                            # failures are recorded, not raised
            ctx.note(f"feature {feature}: branch {res.status_name} {res.error}")
            ctx.tick()
            return

        # Data operations: a list of (sql, params) run on the branch; %s
        # placeholders work on every backend.
        w.exec([("INSERT INTO feature_row (feature, n, note) VALUES (%s, %s, %s)",
                 (feature, n, f"feature {feature} row {n}")) for n in range(1, p.rows_per_feature + 1)],
               branch, label="feature_write")
        w.commit(branch, f"feature {feature}", label="feature_commit")

        if self._merges(feature):
            with self._merge_lock:
                m = ctx.retry(lambda: w.suite.merge(ctx.spine, branch, message=f"merge feature {feature}",
                                                   label="feature_merge"))
            if not m.ok:
                ctx.note(f"feature {feature}: merge {m.status_name} {m.error[:120]}")
        ctx.delete(w.suite, branch, label="feature_delete")
        ctx.tick()
