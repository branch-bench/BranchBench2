# BranchBench benchmark engine

The shared core of BranchBench: the git-like database API
(`dblib/db_api.py`), the microbenchmark runners (single operations and the
branch-verb scaling grid) and the six agent-workflow macrobenchmark scenarios
S1-S6, with their result collection, configs, schemas and the CH-benCHmark
dataset. Every operation is timed and storage-measured; an operation that
errors is recorded and the workload goes on, while one the adapter cannot
perform ends the run as unsupported, naming the verb.

This repository also owns the concrete database adapters, reusable database
connectors, and standalone benchmark commands. A connector creates or opens
a database, loads data, and supplies a `PreparedTarget` to the abstract
runners. The runners do not select database vendors. The CLI selects a
connector and applies the configured cleanup policy.

The engine works without a leaderboard checkout. The separate
[leaderboard application](https://github.com/branch-bench/BranchBench2-Leaderboard)
imports this package and owns server scripts, run recovery, AWS workers,
maintainer-merge automation, validation, publication, and the website.
The engine contains no leaderboard, GitHub Actions, AWS worker, or S3 code.

See [ownership requirements](requirements/README.md) and the
[database contribution guide](CONTRIBUTING.md).

## Quick Start

```bash
# 1. Install Python 3.13 and the dependencies into .venv (needs uv:
#    https://docs.astral.sh/uv/). The build compiles the .proto files.
uv sync --frozen

# 2. Check the standalone commands without connecting to a database
uv run --frozen python -m microbench.runner2 --help
uv run --frozen python -m macrobench.runner --help
uv run --frozen python -m microbench.verbs --help

# 3. Run the prepared-target tests (no database server needed)
uv run --frozen python -m pytest tests/test_runners.py

# 4. Build the distributable wheel and sdist
uv build
```

For a real run, install and start the selected database, set its connection
variables, and select the backend explicitly. Supply `--db-name` unless
the config sets `database_setup.db_name`. For example:

```bash
uv run --frozen python -m macrobench.runner \
  --config macrobench/configs/rl_env_mini.textproto \
  --backend dolt --db-name branchbench_local --outdir run_stats/local
```

Use a disposable benchmark database. The microbench and macrobench CLIs honor `database_setup.cleanup`
and the configured database source. Keep `cleanup: false` when you need to
retain the database. `cleanup: true` can delete an existing database too.
The workload can change its contents. Hosted providers also need credentials in the process
environment. Fetch secrets from Bitwarden at point of use; do not save them
in configuration files.

### Repository Layout

```
dblib/              # DB API, concrete adapters, prepared targets, result collection
dblib/connectors/   # Provider database creation, loading, target factories and deletion
microbench/         # Microbenchmark runner, its operations, and the branch-verb grid
macrobench/         # Macrobenchmark scenarios, schema generators, configs and runner
util/               # Shared helpers (SQL loading, DB utilities)
db_setup/           # The CH-benCHmark schema and seed dataset, plus loader scripts
scripts/plotting/   # Plotting and analysis scripts (optional extra: plotting)
tests/              # Runner, connector and adapter tests; live tests require servers
requirements/       # Ownership and standalone execution requirements
run_stats_final/    # Result artifacts of the paper's runs
```

## Embedding the engine

The seam between an application and the engine is three frozen dataclasses in
`dblib/target.py`:

| Type | Who creates it | What it holds |
|------|----------------|---------------|
| `PreparedTarget` | a connector or embedding application | `root_ref` (the branch a run starts on), `suite_type` (the adapter class, for its capability report) and `open_suite`, a callable that opens one adapter instance for one worker |
| `SuiteRequest` | the engine, once per worker | the shared `ResultCollector`, the `initial_ref` to connect to, the `worker_index`, the refs created during untimed setup and whether to measure storage per operation |
| `ExecutionState` | the microbench runner | the working ref and the setup refs after the untimed setup phase |

The CLI and embedding applications use the same three execution entry points:

```python
from dblib.connector import load_connector
from macrobench import runner as macro_runner
connector = load_connector("dolt")
database = connector.provision("branchbench_example")
try:
    target = connector.prepare_target(database)
    e2e = macro_runner.run(
        ["--config", "macrobench/configs/rl_env_mini.textproto", "--outdir", "run_stats/example"],
        target=target, system_name="Doltgres",
    )
finally:
    connector.delete_database(database)
```

The other entry points are `runner2.run(task, target, system_name, output_dir)`
and `driver.run(argv, target=target, system_name=system_name)`. The caller owns
the database lifetime when it uses these functions. Built-in connectors
provide the reusable provider operations. A custom caller can still supply
its own `PreparedTarget` without changing the runners.

Adapters read provider connection settings from the process environment.
`DBFORK_NATIVE_ONLY` reports simulated verbs as unsupported.
`MACRO_SPILL_ROWS` controls when the macrobench collector spills rows to parquet.

Packaged resources: the macrobench presets
(`importlib.resources.files("macrobench") / "configs"`), the scenario schemas
(`macrobench/schema/*.sql`), the CH-benCHmark schema and seed dump
(`importlib.resources.files("db_setup")`), and the generated `*_pb2.py`
protobuf modules. `build_protos.py` compiles the `.proto` files with
`grpc_tools.protoc` during every build and fails the build when it cannot.

## Table of Contents

- [Database API](#database-api)
- [Macrobenchmarks](#macrobenchmarks)
- [Microbenchmarks](#microbenchmarks)
- [Plotting Results](#plotting-results)
- [Output Files](#output-files)
- [Development](#development)

---

## Database API

`dblib/db_api.py` defines `DBToolSuite`, the adapter base class every
database system implements. Built-in adapters live in `dblib/<system>.py`. An
adapter overrides the protected hooks it supports (`_branch_impl`,
`_commit_impl`, ..., `_storage_bytes`, `_connect_impl`, `_flush_impl`); the
public verbs wrap them with timing, storage measurement and result recording.
The runners reach adapters through the `PreparedTarget` factory.

### Verbs

| Verb | Meaning | Returns |
|------|---------|---------|
| `branch(name, from_ref)` | create a branch | |
| `commit(ref, message)` | snapshot the branch's working state | commit id |
| `diff(ref_a, ref_b)` | differences between two refs | backend-specific summary |
| `log(ref, limit)` | recent commits | list of dicts |
| `merge(into, source, message, on_conflict)` | merge `source` into `into` | `fast_forward`, `conflicts`, `conflict_tables` (Dolt) |
| `rebase(ref, onto, on_conflict)` | replay `ref`'s commits on `onto` | `conflicts`, `conflict_tables` (Dolt) |
| `revert(ref, commit)` | add a commit undoing `commit` | |
| `reset(ref, to)` | move `ref` to a commit (or restore point) | |
| `delete(ref)` | delete a branch | |

A ref is a branch name or `branch@commit` on backends with commits. On a
backend without commits, a commit ref falls back to the branch head with a
warning and the row is flagged `commit_ref_fallback`.

`merge()` and `rebase()` take `on_conflict`: `"ours"` (default), `"theirs"`,
or a callable `resolve(db, conflicts)` that the backend calls on the
half-merged working set with a session and a list of
`{"table": name, "rows": [...]}` entries (Dolt's base/our/their columns).
The callable resolves conflicts with SQL through `db.sql()`; anything it
leaves is resolved as "ours". Schema conflicts (a dropped table modified on
the other side, two indexes on the same columns) cannot be resolved in place:
the backend aborts and the verb is recorded as FAILED with the reason.

Every verb returns an `OpResult` with `status` OK, UNSUPPORTED (the backend
has no such operation: zero latency, workload continues) or FAILED (the
backend tried and errored). Nothing raises unless `raise_on_error=True`;
`result.raise_for_status()` raises on demand. `DBToolSuite.capabilities()`
reports which verbs a backend supports, and `DBToolSuite.implementation()`
how each one is realised: `native` (one backend primitive), `composed`
(several native primitives driven by the backend class) or `simulated` (SQL
emulation of something the backend lacks). The macrobench e2e stats carry
both under `capabilities` and `implementation`.

### exec()

```python
results = db.exec(script, refs=["feature"], mode="per_ref", label="eval")
```

runs `script` on each ref in turn (`mode="per_ref"`), or once with every ref
addressable from one session (`mode="multi"`, on adapters that declare
`SUPPORTS_MULTI_REF_EXEC`; others record it as UNSUPPORTED). A script is a list of SQL statements
(strings or `(sql, params)`), Python source, or a callable. Python source
runs with `db` (the session), `params` and `suite` in scope and may define
`run(db)`, whose return value becomes the result's `value`:

```python
script = """
def run(db):
    db.sql("UPDATE stock SET s_quantity = s_quantity - %s WHERE s_i_id = %s", (params["qty"], params["item"]))
    return db.sql("SELECT count(*) FROM stock WHERE s_quantity = 0")[0][0]
"""
```

Every statement issued through `db.sql()` autocommits and is recorded as its
own row (READ/INSERT/UPDATE/DELETE_ROWS/DDL) whose `num_keys_touched` is the rows affected (writes) or returned (reads), unless the script set it with `db.record_keys_touched(n)`. Per ref, `exec()` also records a CONNECT
row when it had to switch the connection to that branch, and one EXEC row
with the script's total latency and storage delta. `exec_async()` runs the
same thing on a connection pool (`open_async_pool(size)`) for throughput
measurement; its scripts must be a SQL list, a coroutine function, or source
defining `async def run(db)`.

### Result rows

Each parquet row carries `op_name`, `status`, `ref`, `label`, `exec_id`
(grouping the rows of one `exec()`), `latency`, `disk_size_before/after`,
`sql_query` (the statement, or the script text on EXEC rows),
`error_message`, `commit_ref_fallback` and the driver context (table, step,
thread). Run summaries include `workflow_supported`, the list of
`unsupported_ops`, per-op status counts and the backend's capabilities.

---


---

## Macrobenchmarks

The macrobenchmark runs one of six agent-workflow scenarios (BranchBench
S1-S6) against a backend through the git-like API, and records every branch
op and every statement, which verbs the backend could not perform, and
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
parametrized SQL and exec() scripts; `macrobench/faults.py` holds the fault
catalog and the TPC-C consistency conditions S1 and S5 use;
`macrobench/tpcc.py` holds the TPC-C transactions and CH queries that the
optional spine load runs.

### Configuration

A run is one `MacroBenchConfig` textproto (`macrobench/task.proto`):

```
run_id: "macro_data_agent_dolt"
schema { base: CH_BENCH scale_factor: 1 }                 # generated CH-benCHmark at W=1
workload {
  branch_ops { commit_interval: 2 }                        # commit every 2 steps
  data_ops { statements_per_step: 8 rows_per_write: 5     # exec() intensity
             write_fraction: 0.8 spine_clients: 4 analytical_fraction: 0.8 }
  data_agent { batches: 32 concurrent_batches: 4 batch_rows: 2000
               days_back: 3 steps_per_batch: 10 reset_prob: 0.1 }
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
- The scenario message holds the paper's structural parameters (T/G/S, N,
  fan-out, cycles, D, p, ...).
- `workload.branch_intensity` and `workload.data_intensity` are multipliers
  applied on top of the explicit knobs before the run (0 or 1 = unchanged).
  Branch intensity scales the number of branch verbs (branches, forks,
  rounds, rebases, investigation points) and divides `commit_interval`;
  data intensity scales the SQL per branch (statements per step, rows per
  write, spine clients, per-branch steps and rows). Thread counts, caps and
  probabilities are not scaled. `macrobench/intensity.py` lists the exact
  fields; the effective values are printed and written to the e2e stats.
- `invariants { disabled: true }` turns the sentinel checks off;
  `fail_fast: true` stops at the first failure.

`macrobench/configs/<key>.textproto` holds the paper-scale parameters and
`<key>_mini.textproto` a smoke-test size.

### Running

The embedding application provisions a database, wraps it in a
`dblib.target.PreparedTarget` (see [Embedding the engine](#embedding-the-engine))
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
| `--op-stall-sec N` | stall limit for one operation: a verb or an exec that has been running longer than this (default 600 s, 0 = off) is cancelled and recorded `FAILED` with a `stalled: cancelled after Ns` note; the run goes on, so later failures and invariant results are recorded too; the e2e stats list the cancelled operations under `stalls` with their outcome (`failed`, or `completed` when it finished before the cancel landed), and a run with a cancelled operation that failed ends with status `incomplete` instead of `completed` |
| `--storage-sample-interval SEC` | seconds between background storage samples (default 5, 0 = off) |
| `--measure-storage` | also measure storage around every operation |
| `--no-progress` | no progress bar |

`run` returns the e2e stats it wrote. `system_name` is recorded as
`backend` in them. The callable runner does not delete the database.
Its caller applies cleanup after execution, so the last workflow storage
point is `after_gc`. The standalone `python -m macrobench.runner` command
selects a connector and applies the database cleanup policy itself.

A backend that lacks a verb does not run the scenario past it: the verb is
recorded as UNSUPPORTED, the run stops there with every worker cancelled,
`workflow_supported` is false and `status` is `unsupported: <VERB>: <reason>`
in the e2e stats. Failed operations, by contrast, never stop a run.

#### Storage

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
measures before and after every verb and every exec() call (two directory
walks per operation; off by default, and not attributable under concurrent
workers). The report's `storage.png` draws the sampled series with the workflow-level
points as markers; `summary.md` carries them as `storage_after_*_mb`.

### Output

```
run_stats/
├── <run_id>.parquet            # one row per branch op, statement, exec() and connect
└── <run_id>_e2e_stats.json     # status, support summary, invariants, scenario metrics
```

The e2e stats carry the workload parameters, the seed statistics,
`capabilities` and `implementation` (how the backend realises each verb:
native, composed or simulated), `workflow_supported` with `unsupported_ops`
and `op_status_counts`, the `invariants` results, the scenario's own
`metrics` (e.g. S3 merge order and conflicts, S5 time to recovery, S6
fast-forward vs three-way merges), the spine load's transaction counts and
the `storage` block (the workflow-level points, the GC status, the
sampled series; see [Storage](#storage)), the logical `work` counters
(expected, succeeded, failed, skipped units) and `worker_errors`.

---

## Microbenchmarks

### Single operations

`microbench.runner2.run(task, target, system_name, output_dir)` runs one
operation type `num_ops` times on one worker thread (or `num_threads` threads)
against a prepared target. `task` is a `microbench.task2_pb2.TaskConfig`
(`microbench/task2.proto`; `microbench/test_config2.textproto` is an example):
`run_id`, `table_name`, `num_threads`, `measure_storage`, the async settings
(`concurrent_requests`, `use_async`) and one `operation_benchmark` with the
operation, `num_ops`, `warmup_ops`, an optional untimed `setup` phase
(`num_branches` in a `SPINE`, `FAN_OUT` or `BUSHY` shape, with inserts,
updates and deletes per branch) and the operation-specific `range_config`,
`ddl_config` and `branch_op_config`. The standalone command uses `backend`
and `database_setup`; the callable runner uses its supplied target.

The sample selects Dolt and loads the bundled CH-benCHmark seed into a fresh
database. Its `customer` table supplies the benchmark data.

```bash
uv run --frozen python -m microbench.runner2 \
  --config microbench/test_config2.textproto --output-dir run_stats/micro
```

The setup phase runs untimed and leaves an `ExecutionState`: the last setup
branch becomes every worker's starting ref, and the setup branches are
partitioned over the workers. The timed phase then runs the operation; every
worker opens its own adapter through the target. The application's own
driver can also use the building blocks directly (`WorkerContext`,
`SharedBranchManager` and the operations in `microbench/operations/`), which is
what the BranchBench leaderboard does for its suite.

#### Operations

| Group | `operation` values |
|-------|----------------------|
| Data | `READ`, `INSERT`, `UPDATE`, `DELETE`, `RANGE_READ`, `RANGE_UPDATE` |
| Branch | `BRANCH_CREATE`, `BRANCH_CONNECT`, `BRANCH_DELETE`, `BRANCH_COMMIT`, `BRANCH_DIFF`, `BRANCH_LOG`, `BRANCH_MERGE`, `BRANCH_REBASE`, `BRANCH_REVERT`, `BRANCH_RESET` |
| Schema | the `DDL_*` operations |

The branch group covers the nine verbs of the shared API (`dblib/db_api.py`)
plus the connection switch. `BRANCH_COMMIT` to `BRANCH_RESET` each build the
state they need untimed and record only the verb:

| Operation | Untimed preparation | Timed |
|-----------|---------------------|-------|
| `BRANCH_COMMIT` | insert rows on the branch | `commit` |
| `BRANCH_DIFF` | once per branch: a child with committed inserts | `diff(branch, child)` |
| `BRANCH_LOG` | once per branch: `history_commits` commits | `log(branch, log_limit)` |
| `BRANCH_MERGE` | a child with committed inserts; the branch commits other rows | `merge(branch, child)` |
| `BRANCH_REBASE` | same as merge | `rebase(child, branch)` |
| `BRANCH_REVERT` | insert rows and commit them | `revert` of that commit |
| `BRANCH_RESET` | two commits of inserted rows | `reset` to the first |

`branch_op_config` in the task config sets `changes_per_op` (rows per
prepared side, default 1), `history_commits` and `log_limit` (default 10
each). Merges and rebases never conflict: the two sides insert different
keys. A verb the backend lacks is recorded as `UNSUPPORTED`.


#### Output Files

```
<output_dir>/
├── <run_id>.parquet                                         # one row per timed operation
├── <run_id>_setup.parquet                                   # the untimed setup phase, when there was one
├── <run_id>_<OPERATION>_threads<N>_summary.json             # throughput, latency and support summary
└── <run_id>_<OPERATION>_threads<N>_failures.json            # failed operations, when there were any
```

### Branch-verb grid

`microbench.verbs.driver.run(argv, target=..., system_name=...)` measures the
cost of each verb against the one quantity it depends on
(`microbench/verbs/grid.py`: one axis per verb and parameter, such as rows
changed, commits on the branch, live branches, or the data size in
warehouses). The CH-benCHmark data is generated at `--scale` W warehouses and
committed as the base; every grid point runs on a branch forked from the base
and deleted afterwards. Options: `--verbs`, `--axes`, `--reps`, `--warmup`,
`--storage-reps`, `--no-gc`, `--no-control`, `--seed`, `--suffix`,
`--skip-done` and `--outdir` (required). The driver writes
`micro_verbs_<system>_w<W>.parquet` and `micro_verbs_<system>_w<W>_stats.json`
(per point: parameters, status, every rep's latency and storage delta, and
their quantiles) and returns the stats.

```bash
uv run --frozen python -m microbench.verbs --backend dolt \
  --db-name branchbench_verbs --scale 1 --verbs branch \
  --outdir run_stats/verbs
```

The standalone verb-grid command creates a fresh database and deletes it
after execution. Use a database name that does not already exist.

---

## Plotting Results

After running benchmarks, use the plotting scripts in the `scripts/plotting/`
directory to generate visualizations. They need the `plotting` extra
(`uv sync --extra plotting`).

### Macrobenchmark Report

`plot_macrobench.py` reads every `<run_id>_e2e_stats.json` / `<run_id>.parquet`
pair under one or more directories, groups the runs by scenario and backend,
and writes a summary plus comparison figures. Backends are read from the
stats files, so a directory holding Dolt and Neon runs compares them directly:

```bash
uv run python scripts/plotting/plot_macrobench.py --data-dir run_stats --outdir figures/macro

# several directories, filtered to two backends and two scenarios
uv run python scripts/plotting/plot_macrobench.py \
    --data-dir run_stats/dolt --data-dir run_stats/neon \
    --backends DOLT NEON --scenarios ops_agent data_agent \
    --outdir figures/macro
```

| Output | Content |
|---|---|
| `data/summary.md`, `data/summary.csv` | per run: status, elapsed/setup time, support, invariants, agent op counts and median latency per verb (spine load excluded), spine throughput and statement latency, scenario metrics |
| `time_breakdown.png` | summed latency split into branch ops (verbs plus the connection switch `exec()` does to reach a ref) and data ops (statements); spine load excluded |
| `latency_by_op_branch.png`, `latency_by_op_data.png`, `data/latency_by_op.md/.csv` | one panel per operation (branch verbs + CONNECT in one figure; statements inside `exec()` + EXEC in the other, with READ/UPDATE/DELETE split into point vs range by rows touched, READ:scan for aggregate/join reads, INSERT:bulk for multi-row inserts): median latency with a 95% CI, grouped by scenario (shaded bands), colour = backend; ops some backends lack are grouped last; the .md adds a backend x op support matrix |
| `latency_cross_branch.png`, `data/cross_branch.md/.csv` | one scenario-level read over several branches (rl_env's `score` over a task's 16 leaves, context_mgmt's `policy_check` over the candidates) as a single logical step: median wall-clock latency per backend, whisker to p90, annotated `multi` (one multi-ref `exec()`) or `per_ref x N` (the same read once per branch on a backend without multi-branch queries, N connection switches included); the table adds the summed exec latency and the per-branch cost |
| `latency_exec_by_label.png`, `data/exec_by_label.md/.csv` | `exec()` latency per script label (rollout_step, ingest, spine, ...) with statements per exec, and the mean exec time split into READ/INSERT/UPDATE/DELETE_ROWS/DDL/CONNECT/other |
| `progress.png` | completed agent steps (branch verbs and `exec()` runs, background load excluded) against elapsed seconds, one curve per backend |
| `exec_time_by_op.png` | per scenario, one horizontal 100% bar per script label and backend (adjacent rows) showing where `exec()` time goes: statement types, CONNECT, and `other` (untimed BEGIN/COMMIT/ROLLBACK round trips plus the Python between statements); mean ms and count at the bar end |
| `data/data_ops.md/.csv` | data statements (READ/INSERT/UPDATE/DELETE_ROWS/DDL) split by workload role: spine traffic vs. the agent's own statements (ingest, backfill, rollout_step, ...) |
| `cdf/latency_cdf_<scenario>.png` | latency CDF of each branch verb and data statement type (agent rows only), one line per backend |
| `storage.png` | storage over the run: the 5 s sampled series (or the per-operation sizes of runs made with `--measure-storage`) with the four workflow-level points as markers; log axis when database-scope and server-scope backends share a panel |

Only the newest run per (scenario, backend) is used unless `--all-runs` is
given; `--run-glob` narrows by run id (e.g. `'macro_*_mini_*'`).

### Microbenchmark Latency Plots

Plot microbenchmark latency results:

```bash
uv run python scripts/plotting/plot_branch_latency_micro.py \
    --data-dir <data_directory> \
    --outdir <output_figures_dir> \
    --operation <operation_type>
```

#### Arguments

| Argument | Description |
|----------|-------------|
| `--data-dir` | Directory with microbenchmark parquet files |
| `--branch-dir` | Directory with branch operation data (for combined plots) |
| `--connect-dir` | Directory with connect operation data (for combined plots) |
| `--outdir` | Directory to save figures |
| `--operation` | Operation type: `branch`, `connect`, `both`, or `combined` |

#### Examples

```bash
# Plot branch creation latency only
uv run python scripts/plotting/plot_branch_latency_micro.py \
    --data-dir run_stats_final/micro/single_thread/branch \
    --outdir figures/ \
    --operation branch

# Plot branch connection latency only
uv run python scripts/plotting/plot_branch_latency_micro.py \
    --data-dir run_stats_final/micro/single_thread/connect \
    --outdir figures/ \
    --operation connect

# Plot combined (branch and connect on single plot)
uv run python scripts/plotting/plot_branch_latency_micro.py \
    --branch-dir run_stats_final/micro/single_thread/branch \
    --connect-dir run_stats_final/micro/single_thread/connect \
    --outdir figures/ \
    --operation combined
```

### Microbenchmark Per-Operation Plot

One figure with every operation of the microbenchmark, one bar per backend:

```bash
uv run python scripts/plotting/plot_micro_ops.py \
    --data-dir <data_directory> [<data_directory> ...] \
    --outdir <output_figures_dir> [--branches 16]
```

It reads every result parquet under the directories (with the summary JSON
that names its backend) and writes `micro_ops_latency.png` and
`micro_ops_latency.csv`: median latency with a whisker to the 95th
percentile, on a log axis, for the six data operations and for the nine
verbs plus the connection switch. A verb a backend lacks is marked `n/a`.

### Microbenchmark Throughput Plots

Plot throughput vs threads/branches:

```bash
uv run python scripts/plotting/plot_throughput_experiments.py \
    --data-dir <data_directory> \
    --output <output_figures_path> 
```

---

## Output Files

Benchmark results are saved as Parquet files and JSON summaries.

### Macrobenchmark Output Structure

```
run_stats/
├── macro_rl_env_mini_dolt.parquet
├── macro_rl_env_mini_dolt_e2e_stats.json
├── macro_data_agent_dolt_w5.parquet
└── macro_data_agent_dolt_w5_e2e_stats.json
```

### Microbenchmark Output Structure

```
run_stats_final/micro/
├── single_thread/      # Single-threaded latency benchmarks
│   ├── branch/
│   └── connect/
├── multithread/        # Multi-threaded latency benchmarks
├── tp_fix_branch/      # Throughput: fixed branches, varying threads
├── tp_fix_thread/      # Throughput: fixed threads, varying branches
└── tp_proportional/    # Throughput: proportional threads and branches
```

### Parquet Schema

See [Result rows](#result-rows); the authoritative definition is
`dblib/result.proto`.

---

## Development

- **[uv](https://docs.astral.sh/uv/)** manages Python (3.13, pinned in
  `.python-version`) and the dependencies (locked in `uv.lock`). `uv sync
  --frozen` installs the project in editable mode and compiles the protos;
  `uv run` recompiles them whenever a `.proto` file changes.
- `uv run --frozen python -m pytest tests/test_runners.py` runs tests against the
  in-memory sqlite adapter in `tests/fake_backend.py`, including the three
  runners through the prepared-target seam (`tests/test_runners.py`).
- Adapter live tests are separate from server-free tests. Read each test's
  prerequisites before running it against a database or hosted account.
- `uv build` produces the sdist and the wheel. The wheel carries the
  generated protobuf modules, the presets, the schemas and the dataset, so an
  application pins this repository by Git revision and never builds protos
  itself. The build dependencies are pinned in `pyproject.toml`, so the
  generated code matches the protobuf runtime the lock installs.
- To add a dependency, use `uv add <package>` and commit the updated
  `uv.lock`. The optional `plotting` extra holds the analysis-only
  dependencies.

---

## Additional Resources

- **Scenario configurations**: See `macrobench/configs/` and `macrobench/task.proto`
- **Microbenchmark config**: See `microbench/test_config2.textproto` and `microbench/task2.proto`
- **Database schemas and dataset**: See `db_setup/` and `macrobench/schema/`
- **Database adapters and connectors**: `dblib/<system>.py` and `dblib/connectors/<system>.py`
- **Adding a database**: [CONTRIBUTING.md](CONTRIBUTING.md)
- **Leaderboard server scripts and registration**: the leaderboard repository, `leaderboard/systems/<system>/`
