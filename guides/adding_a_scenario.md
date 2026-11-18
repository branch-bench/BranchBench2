# Adding a scenario

A scenario is a seeded driver: it replays an agent workflow (which branches
to create, what SQL to run on them, when to commit, merge, rebase or roll
back) against one backend through the branch-operations API, and checks
invariants that define a correct outcome. Everything about a scenario
lives in three places:

| File | What |
|---|---|
| `macrobench/scenarios/<key>.py` | the scenario class: parameters, workflow, checks, labels |
| `macrobench/schema/<key>.sql` | its tables (optional) |
| `macrobench/configs/<key>.textproto`, `<key>_mini.textproto` | full-size and smoke-size configs |

Nothing else needs editing:
- modules in `macrobench/scenarios/` are imported automatically;
- the run scripts, the progress monitor and the plots read the scenario
  list and labels from the registry;
- the fake-backend tests pick the scenario up by its key.

`macrobench/scenarios/template.py` is a complete, working example (key
`example`). Start by copying it.

## 1. The scenario class

```python
from dataclasses import dataclass
from macrobench.scenarios.base import Scenario, Worker, register
from macrobench.scenarios.params import param

@register
class ReviewScenario(Scenario):
    key = "review"                        # workload.scenario in configs
    name = "S7 review agent"
    order = 7                             # position in lists, sweeps and figures
    label, plot_label, title = "S7 review", "S7\nreview", "S7 review agent"
    group = "branch"                      # "branch" or "data": which work dominates
    # uses_spine_load = True              # run the TPC-C load on main (data_ops.spine_clients)
    # expected_failures = (("probe", "no such column"),)   # operations that fail by design

    @dataclass
    class Params:
        reviewers: int = param(4, intensity="branch", doc="review branches")
        edits: int = param(10, intensity="data", doc="edits per review")
        approve_share: float = param(0.5, doc="share of reviews merged")

    def validate(self): ...               # reject impossible parameter combinations
    def total_units(self): ...            # progress-bar ticks run() will make
    def seed(self, suite): ...            # optional: rows on main before the run (untimed)
    def run(self): ...                    # the workflow and its checks
```

**Parameters.**
- **Types:** each field is an `int`, `float`, `bool` or `str`.
- **Configs** give values as text; they are parsed by the declared type. A
  parameter left out keeps its default. Unknown names, unparsable values and
  values outside `choices=` are errors naming the scenario and the field.
- **`intensity=`:** tag a size with `"branch"` (how many branch operations:
  branches, rounds, rebases) or `"data"` (how much SQL per branch: steps,
  rows). `--branch-intensity` / `--data-intensity` then multiply it.
- **What not to tag:** thread counts, probabilities and fractions; they
  bound the run rather than size it.
- **`cap=`:** bounds a scaled value.
- **Dependent values:** keep them consistent in `after_intensity()` (see S1
  and S5).
- **Reading them:** `self.params.<name>`, or `python -m macrobench.scenarios
  --describe <key>` from the command line.

**Writing `run()`.** Use the context (`self.ctx`) helpers, so every
operation is timed, recorded and retried consistently:
- `ctx.run_workers(items, fn)` runs `fn(worker, item)` on a thread pool.
  Each worker has its own connection (`w.suite`) and seeded RNG (`w.rng`).
- `ctx.branch(suite, name, from_ref)` and `ctx.delete(suite, name)` respect
  the live-branch cap and retry rate limits.
- `w.exec([(sql, params), ...], ref, label=...)` runs data operations on
  one branch.
- `w.commit(ref, message, label=...)`; `suite.merge(into, source,
  on_conflict=...)`, `suite.rebase(...)`, `suite.reset(...)`,
  `suite.revert(...)`, `suite.diff(...)`.
- `ctx.retry(lambda: ...)` retries an operation that failed with a
  retryable error (rate limits, dropped connections).
- `ctx.should_commit(step)` follows `branch_ops.commit_interval`;
  `ctx.tick()` advances the progress bar; `ctx.check_stop()` honours the
  runtime cap; `ctx.note(...)` logs; `ctx.add_metric(...)` adds to the
  run's metrics.

**Rules of the contract:**
- **Label every operation.** The label names the step (`"review_merge"`,
  `"review_edit"`). Results and figures split branch work from data work
  and group by label.
- **Failures don't stop the run.** A failed operation returns an
  `OpResult` with `.failed`. Record what you can and continue, as an agent
  that catches exceptions would.
- **Unsupported operations do stop it.** If a backend cannot perform a
  branch operation, the run ends there and your code never sees the result.
  Don't write fallbacks for missing operations; that is the adapters' job.
- **Use the same SQL on every backend.** Write it in the subset PostgreSQL
  and MySQL share, with `%s` placeholders.

## 2. Invariant checks

Checks define what a correct outcome is.
- `ctx.invariants.expect(name, actual, expected)` compares values;
  `check(name, passed, detail)` takes a bool; `not_applicable(name,
  reason)` records a check whose precondition did not arise.
- **Check reads** that only exist to verify should use
  `label="invariant"`, which keeps them out of the figures.
- **Name checks** `<ID>.<n> <what must hold>` (e.g. `"S7.1 approved
  reviews reach main"`), so a failure reads as a sentence.
- **Prefer sentinels:** rows you write on purpose and look for later.
  Checking that a value landed exactly where it should, and nowhere else,
  catches lost writes and leaks across branches.

## 3. Tables

Put the `CREATE TABLE` statements in `macrobench/schema/<key>.sql`. It is
loaded after the CH-benCHmark base (`schema { base: CH_BENCH }`), or alone
(`schema { base: NO_BASE }`). Give every table a primary key: merges,
diffs and rebases match rows by key.

## 4. Configs

```
# macrobench/configs/review_mini.textproto
run_id: "macro_review_mini_dolt"          # run_macrobench.sh rewrites run_id and backend
backend: DOLT
database_setup { db_name: "macro_review_mini" cleanup: true generated {} }
schema { base: NO_BASE }
workload {
  branch_ops { commit_interval: 1 }
  data_ops { statements_per_step: 4 rows_per_write: 5 worker_threads: 2 }
  scenario: "review"
  params { key: "reviewers" value: "4" }
  params { key: "edits"     value: "5" }
}
```

The mini config should finish in seconds; the fake-backend test runs it. The
full config is the size you would report.

## 5. Test it

```bash
uv run pytest tests/test_scenarios.py tests/test_scenario_params.py   # no server needed
./scripts/run_macrobench.sh --mini review dolt                         # against a real backend
```

The fake-backend test runs every registered scenario from its mini config,
with tiny CH-benCHmark data and one worker. It requires:
- every check passes;
- no operation fails, apart from your `expected_failures`;
- every branch is deleted.

The fake backend has no merge, rebase or revert. A scenario that uses
them must end at that operation as unsupported: add its key to
`NEEDS_MERGE` in `tests/test_scenarios.py`. Then run the mini config on a
real backend (e.g. the Docker Compose Doltgres) and check the log for
`Invariants: N passed, 0 failed`.
