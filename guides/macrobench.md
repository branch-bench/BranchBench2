# Macrobenchmarks

The macrobenchmark runs one of six agent-workflow scenarios (BranchBench
S1-S6) against a backend through the git-like API, and records every branch
op and every statement, which branch operations the backend could not perform, and
whether the sentinel-based invariance checks held.

| Scenario | Key | Shape | Branch ops |
|----------|-----|-------|------------|
| S1 Agentic RL environment | `rl_env` | T task branches, G rollout leaves each, forks from recorded steps | branch (from commit), commit, diff, delete |
| S2 Agent context management | `context_mgmt` | long spine, candidate branches per compaction | branch, commit, merge (ff), revert, rebase, delete |
| S3 Multi-agent collaboration | `multi_agent` | N agents merging into a spine that keeps changing | branch, commit, merge (with conflicts), delete |
| S4 Development agent | `dev_agent` | dev branches off a busy production spine, never merged; one spine migration adds a column the branches already added with another type (a schema conflict at their next rebase, which the branch resolves by moving its column and retrying; a backend that replays history merges the spine in instead) | branch, commit, rebase (with data and schema conflicts), merge (conflict recovery), delete |
| S5 Operations agent | `ops_agent` | dense commits, bad deploy, investigation branches, PITR | branch (from commit), commit, diff, reset, delete |
| S6 Data agent | `data_agent` | ingestion batches rebased and merged into a warehouse | branch, commit, reset, rebase, merge, delete |

Each scenario lives in `macrobench/scenarios/s<N>_<key>.py` with its
parameters, parametrized SQL and exec() scripts and its invariant checks
(`uv run python -m macrobench.scenarios --list` prints the registered
scenarios; [adding_a_scenario.md](adding_a_scenario.md) explains how to
write one); `macrobench/faults.py` holds the fault
catalog and the TPC-C consistency conditions S1 and S5 use;
`macrobench/tpcc.py` holds the TPC-C transactions and CH queries that the
optional spine load runs.

## Configuration

A run is one `MacroBenchConfig` textproto (`macrobench/task.proto`):

```
# macrobench/configs/data_agent.textproto
run_id: "macro_data_agent_dolt"
backend: DOLT
database_setup { db_name: "macro_data_agent" cleanup: true generated {} }
schema { base: CH_BENCH scale_factor: 1 }                 # generated CH-benCHmark at W=1
workload {
  branch_ops { commit_interval: 2 }                        # commit every 2 steps
  data_ops { statements_per_step: 8 rows_per_write: 5     # exec() intensity
             write_fraction: 0.8 spine_clients: 4 analytical_fraction: 0.8 }
  data_agent { batches: 32 concurrent_batches: 8 batch_rows: 2000   # the scenario
               days_back: 3 steps_per_batch: 10 resets_per_batch: 1 }  # and its parameters
}
```

- `schema` picks the base (CH-benCHmark or none), the scale factor W and
  row-density knobs, and the extension tables (`macrobench/schema/*.sql`;
  by default the scenario's own). For a fresh database, the runner creates
  the schema and seeds it with the generators in `macrobench/datagen/`.
  The CLI uses `backend` and `database_setup` to select and prepare the
  database. `--backend` and `--db-name` override those selections for
  provider-neutral presets. The callable runner uses its supplied target.
- `workload.branch_ops` and `workload.data_ops` set the shared intensity
  (commit interval, retention, live-branch cap; statements per step, rows per
  write, read/write/DDL mix, worker threads, background spine clients).
- The scenario block (`data_agent { ... }`) names the scenario and sets its
  parameters (the paper's structural parameters: T/G/S, N, fan-out,
  cycles, D, ...); a parameter left out keeps its default. A scenario
  without a block of its own in `macrobench/task.proto` (a new one, such
  as the template) is named with `workload.scenario` and configured with
  `workload.params`, values as text parsed by the type the scenario
  declares:

  ```
  scenario: "example"
  params { key: "branches" value: "4" }
  ```

  An unknown name or a bad value is an error either way. `uv run python -m
  macrobench.scenarios --describe <key>` lists a scenario's parameters
  with their types, defaults and meaning.
- `workload.branch_intensity` and `workload.data_intensity` are multipliers
  applied on top of the explicit knobs before the run (0 or 1 = unchanged).
  Branch intensity scales the number of branch operations (branches, forks,
  rounds, rebases, investigation points) and divides `commit_interval`;
  data intensity scales the SQL per branch (statements per step, rows per
  write, spine clients, per-branch steps and rows). Thread counts, caps and
  probabilities are not scaled. Each scenario tags which of its parameters
  scale (`--describe` shows them); the effective values are printed and
  written to the e2e stats under `params`.
- `invariants { disabled: true }` turns the sentinel checks off;
  `fail_fast: true` stops at the first failure.

`macrobench/configs/<key>.textproto` holds the paper-scale parameters and
`<key>_mini.textproto` a smoke-test size.

## Running

```bash
./scripts/run_macrobench.sh [--mini] [--outdir DIR] [--max-runtime-sec N] [--op-stall-sec N] \
    [--measure-storage] [--storage-sample-interval SEC] [--branch-intensity X] [--data-intensity Y] \
    <scenario> <backend> [scale_factor]
```

| Argument | Description |
|----------|-------------|
| `scenario` | `rl_env`, `context_mgmt`, `multi_agent`, `dev_agent`, `ops_agent`, `data_agent` |
| `backend` | `dolt`, `dolt_mysql`, `seekdb`, `matrixone`, `neon`, `file_copy`, `chronos`, `bauplan` (`xata` is experimental; see [backends.md](backends.md)) |
| `scale_factor` | W warehouses for the generated data (default: the config's) |
| `--max-runtime-sec N` | runtime cap; default one hour times the largest of the scale factor and the intensity multipliers (3600 s at scale 1, intensity 1), 0 = no cap. A run that reaches it (or gets Ctrl-C / SIGTERM) is recorded as `interrupted` with its rows and stats written |
| `--op-stall-sec N` | stall limit for one operation: a branch operation or an exec that has been running longer than this (default 600 s, 0 = off) is cancelled and recorded `FAILED` with a `stalled: cancelled after Ns` note; the run goes on, so later failures and invariant results are recorded too; the e2e stats list the cancelled operations under `stalls` with their outcome (`failed`, or `completed` when it finished before the cancel landed), and a run with a cancelled operation that failed ends with status `incomplete` instead of `completed` |
| `--storage-sample-interval SEC` | seconds between background storage samples (default 5, 0 = off) |
| `--measure-storage` | also measure storage around every operation |

```bash
./scripts/run_macrobench.sh --mini rl_env dolt
./scripts/run_macrobench.sh --outdir run_stats --max-runtime-sec 7200 multi_agent dolt
# twice the branch churn, half the SQL per branch
./scripts/run_macrobench.sh --mini --branch-intensity 2 --data-intensity 0.5 rl_env dolt
./scripts/run_macrobench.sh --measure-storage data_agent dolt 5
# or the runner itself; --backend and --db-name override the config's
uv run python -m macrobench.runner --config macrobench/configs/ops_agent_mini.textproto \
    --backend dolt --db-name macro_ops --outdir run_stats
```

**An unsupported operation stops the run.** A backend that lacks a branch operation
does not run the scenario past it: the branch operation is recorded as UNSUPPORTED, every
worker is cancelled, `workflow_supported` is false and `status` is
`unsupported: <OPERATION>: <reason>` in the e2e stats. Failed operations, by
contrast, never stop a run; the invariant checks report their effect. Add
`DBFORK_NATIVE_ONLY=1` (or `--native-only` below) to treat every simulated
operation as unsupported and see how far each system gets on its own
primitives.

## Running every scenario on every backend

```bash
./scripts/run_macro_all.sh [--mini] [--outroot DIR] [--scenarios a,b,...] [--native-only] \
    [--parallel] [--no-manage-servers] [--max-runtime-sec N] [--seed N] [backend ...]
```

runs the six scenarios (or `--scenarios`) on each backend (default: `dolt
dolt_mysql seekdb matrixone neon file_copy chronos`) into
`run_stats/macro_<UTC date>/<backend>/`, with one log per run under `logs/`
and a driver log. A server that is already listening is used as it is, so
with the Docker Compose services up the script needs no further setup;
`--no-manage-servers` makes it only check that each server is reachable.
`--parallel` runs the backends at the same time (one server per machine, or
Neon next to local servers). Watch a running set with
`scripts/remote/progress.sh <outroot>`. The script's header documents the
remaining options (remote servers, co-located drivers, storage).

## From Python

The embedding application provisions a database, wraps it in a
`dblib.target.PreparedTarget` (see [embedding.md](embedding.md))
and calls `run` with the runner's own options:

```python
from macrobench import runner

e2e = runner.run(["--config", "macrobench/configs/rl_env_mini.textproto", "--outdir", "run_stats",
                   "--no-progress", "--max-runtime-sec", "1800"],
                  target=target, system_name="Doltgres")
```

| Option | Description |
|----------|-------------|
| `--config` | the `MacroBenchConfig` textproto; the packaged presets are `importlib.resources.files("macrobench") / "configs"` |
| `--outdir` | where the parquet and e2e stats files go (default `/tmp/run_stats`) |
| `--max-runtime-sec N` | runtime cap, 0 = no cap. A run that reaches it (or gets Ctrl-C / SIGTERM) is recorded as `interrupted` with its rows and stats written |
| `--op-stall-sec N` | stall limit for one operation: a branch operation or an exec that has been running longer than this (default 600 s, 0 = off) is cancelled and recorded `FAILED` with a `stalled: cancelled after Ns` note; the run goes on, so later failures and invariant results are recorded too; the e2e stats list the cancelled operations under `stalls` with their outcome (`failed`, or `completed` when it finished before the cancel landed), and a run with a cancelled operation that failed ends with status `incomplete` instead of `completed` |
| `--storage-sample-interval SEC` | seconds between background storage samples (default 5, 0 = off) |
| `--measure-storage` | also measure storage around every operation |
| `--no-progress` | no progress bar |

`run` returns the e2e stats it wrote. `system_name` is recorded as
`backend` in them. The callable runner does not delete the database.
Its caller applies cleanup after execution, so the last workflow storage
point is `after_gc`. The standalone `python -m macrobench.runner` command
selects a connector and applies the database cleanup policy itself.

## Storage

Every run records the adapter's storage size (`DBToolSuite._storage_bytes()`:
the database's own directory, or the whole server data directory on adapters
that declare `STORAGE_SCOPE = "server"`) at three workflow-level points,
written to the e2e stats under `storage.points`:

| Point | When |
|---|---|
| `after_setup` | schema and seed data loaded, before the scenario |
| `after_workflow` | the scenario is done (its branches deleted) |
| `after_gc` | after the adapter's GC/flush hook (`gc()`, UNSUPPORTED when the adapter has none) |

A background sampler also records the size every 5 s during the scenario
(`storage.samples`, wall-clock seconds and bytes); `--storage-sample-interval
SEC` changes the interval (`0` turns it off), as does
`storage_sample_interval_sec` in the config. `--measure-storage` additionally
measures before and after every branch operation and every exec() call (two directory
walks per operation; off by default, and not attributable under concurrent
workers). The report's `storage.png` draws the sampled series with the workflow-level
points as markers; `summary.md` carries them as `storage_after_*_mb`.

## Output

```
run_stats/
├── <run_id>.parquet            # one row per branch op, statement, exec() and connect
└── <run_id>_e2e_stats.json     # status, support summary, invariants, scenario metrics
```

The e2e stats carry the workload parameters, the seed statistics,
`capabilities` and `implementation` (how the backend realises each branch operation:
native, composed or simulated), `workflow_supported` with `unsupported_ops`
and `op_status_counts`, the `invariants` results, the scenario's own
`metrics` (e.g. S3 merge order and conflicts, S5 time to recovery, S6
fast-forward vs three-way merges), the spine load's transaction counts and
the `storage` block (the workflow-level points, the GC status, the
sampled series; see [Storage](#storage)), the logical `work` counters
(expected, succeeded, failed, skipped units) and `worker_errors`.
