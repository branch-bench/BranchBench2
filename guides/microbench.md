# Microbenchmarks

Microbenchmarks measure single operations under controlled conditions.

## Per-operation microbenchmark (the paper's)

`microbench/verbs` measures each of the eight branch operations (branch,
commit, delete, diff, merge, rebase, reset, revert) along the axes its cost
can depend on: data size, rows changed, how old the branch point is, live
branches, commits on the branch, rows written, deletions in parallel, rows
the upstream moved, share of conflicting rows, and rows written after a
reset target. Every point varies one parameter from a common base, is
repeated, and records latency and storage per repetition;
`microbench/verbs/grid.py` defines the grid.

```bash
./scripts/run_micro_verbs.sh [--outroot DIR] [--scales 1,2,4,8] [--reps N] [--warmup N] \
    [--verbs a,b,...] [--axes a,b,...] [--native-only] [--parallel] [backend ...]
```

The servers must already be up (Docker Compose, see
[backends.md](backends.md)); the script starts nothing. Results go to
`<outroot>/<backend>/micro_verbs_<backend>_w<W>.parquet` with a
`_stats.json`; the `plot_micro_*` scripts in [results.md](results.md) draw
them. The script's header documents every option.

### Running the driver directly

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

## Single operations

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

### Operations

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


### Output files

```
<output_dir>/
├── <run_id>.parquet                                         # one row per timed operation
├── <run_id>_setup.parquet                                   # the untimed setup phase, when there was one
├── <run_id>_<OPERATION>_threads<N>_summary.json             # throughput, latency and support summary
└── <run_id>_<OPERATION>_threads<N>_failures.json            # failed operations, when there were any
```

## Latency and throughput scripts

The latency and throughput scripts below wrap the single-operation runner
above (`microbench/runner2.py`). They load a SQL dump
instead of generating data: `db_setup/tpcc_schema.sql` is in the repository,
while `db_setup/ch-w1.sql`, a `pg_dump` of a CH-benCHmark database at W=1,
is not; build it by loading the CH-benCHmark tables into PostgreSQL
(`db_setup/load_ch_data.py`) and dumping them with `pg_dump`.

### Latency Benchmarks

Measure single-threaded operation latency with varying numbers of branches. For
multi-threaded load, use the throughput benchmarks.

#### Single-Threaded Latency

Use `scripts/run_single_thread_bench.sh` to measure single-threaded operation latency:

```bash
./scripts/run_single_thread_bench.sh <backend> <sql_dump_path> <num_branches> [OPTIONS]
```

##### Arguments

| Argument | Description |
|----------|-------------|
| `backend` | Database backend: `dolt`, `dolt_mysql`, `seekdb`, `matrixone`, `neon`, `file_copy`, `chronos` (experimental: `xata`, `terminusdb`) |
| `sql_dump_path` | Path to SQL dump file (e.g., `db_setup/tpcc_schema.sql`) |
| `num_branches` | Number of branches to create for testing |

##### Options

| Option | Description |
|--------|-------------|
| `--seed <seed>` | Random seed for reproducibility |
| `--shape <shape>` | Branch tree shape: `spine`, `bushy`, or `fan_out` (default: `spine`) |
| `--operations <ops>` | Comma-separated list (e.g., `UPDATE,RANGE_UPDATE`) |
| `--range-size <n>` | Range size for RANGE_UPDATE operation (default: 200) |
| `--num-ops <n>` | Number of operations to perform |
| `--output-dir <dir>` | Output directory (default: `/tmp/run_stats`) |

##### Operations

| Group | `--operations` names |
|-------|----------------------|
| Data | `READ`, `INSERT`, `UPDATE`, `DELETE`, `RANGE_READ`, `RANGE_UPDATE` |
| Branch | `BRANCH_CREATE`, `BRANCH_CONNECT`, `BRANCH_DELETE`, `BRANCH_COMMIT`, `BRANCH_DIFF`, `BRANCH_LOG`, `BRANCH_MERGE`, `BRANCH_REBASE`, `BRANCH_REVERT`, `BRANCH_RESET` |
| Schema | the `DDL_*` operations |

The branch group covers the nine branch operations of the shared API (`dblib/db_api.py`)
plus the connection switch. `BRANCH_COMMIT` to `BRANCH_RESET` each build the
state they need untimed and record only the operation:

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
keys. A branch operation the backend lacks is recorded as `UNSUPPORTED`.

##### Examples

```bash
# Run with 16 branches
./scripts/run_single_thread_bench.sh dolt db_setup/tpcc_schema.sql 16

# Run with custom seed and bushy branch shape
./scripts/run_single_thread_bench.sh neon db_setup/tpcc_schema.sql 32 --seed 12345 --shape bushy

# Run with custom range size
./scripts/run_single_thread_bench.sh dolt db_setup/tpcc_schema.sql 16 --operations RANGE_UPDATE --range-size 500
```

#### Output Files

Latency benchmark results are saved to the output directory:

```
<output_dir>/
├── single_thread/
│   ├── branch/
│   │   ├── <backend>_<dataset>_<N>_<shape>_branch.parquet
│   │   ├── <backend>_<dataset>_<N>_<shape>_branch_setup.parquet
│   │   └── <backend>_<dataset>_<N>_<shape>_branch_summary.json
│   └── connect/
│       └── (similar structure)
```

---

### Throughput Benchmarks

Measure throughput (operations per second) with independent control over threads, branches and concurrent requests.

#### Running Throughput Benchmarks

Use `scripts/run_throughput_bench.sh` with one of three sweep modes:

```bash
# Sweep concurrency (fix threads and branches, vary concurrent requests per thread)
./scripts/run_throughput_bench.sh <backend> <sql_dump_path> --sweep-concurrency --threads <N> --branches <N> [OPTIONS]

# Sweep branches (fix threads, vary branches)
./scripts/run_throughput_bench.sh <backend> <sql_dump_path> --sweep-branches --threads <N> [OPTIONS]

# Sweep proportionally (vary both threads and branches together)
./scripts/run_throughput_bench.sh <backend> <sql_dump_path> --sweep-proportional [OPTIONS]
```

The script runs in **async mode** by default: each thread keeps
`--concurrent-requests` ops in flight, each on its own pooled connection
checked out on the thread's branch. Async mode supports `dolt`, `dolt_mysql`,
`seekdb` and `neon`; use `--mode sync` for the other backends (one op at a time per
thread). Every point of a sweep uses the same mode, including concurrency 1.

Each thread works on one branch, assigned round-robin. With fewer threads
than branches, the extra branches exist but get no load; with more threads
than branches, threads share branches.

#### Arguments

| Argument | Description |
|----------|-------------|
| `backend` | Database backend: `dolt`, `dolt_mysql`, `seekdb`, `matrixone`, `neon`, `xata`, `file_copy`, `chronos` |
| `sql_dump_path` | Path to SQL dump file |
| `--sweep-concurrency` | Fix threads and branches, vary concurrent requests (requires `--threads` and `--branches`; async only) |
| `--sweep-branches` | Fix threads, vary branches (requires `--threads`) |
| `--sweep-proportional` | Vary both threads and branches proportionally |

#### Options

| Option | Description |
|--------|-------------|
| `--mode <async\|sync>` | Runner to use (default: `async`; `sync` for backends without async support) |
| `--threads <N>` | Fixed thread count (for `--sweep-concurrency` and `--sweep-branches`) |
| `--branches <N>` | Fixed branch count (for `--sweep-concurrency`) |
| `--threads-per-branch <N>` | Threads per branch ratio for `--sweep-proportional` (default: 4) |
| `--branch-list <list>` | Comma-separated branch counts (e.g., `1,2,4,8,16`) |
| `--concurrency-list <list>` | Comma-separated concurrency levels for `--sweep-concurrency` (default: `1,2,4,...,1024`) |
| `--concurrent-requests <n>` | Ops in flight per thread for the other sweeps (default: 1; > 1 needs async) |
| `--num-ops <n>` | Number of operations per thread (overrides the per-operation defaults) |
| `--point-ops <n>` / `--range-ops <n>` | Operations per thread for point / range operations |
| `--warmup-ops <n>` / `--warmup-fraction <f>` | Warm-up operations per thread, not counted in throughput |
| `--operations <ops>` | Comma-separated list (e.g., `READ,RANGE_READ`) |
| `--output-dir <dir>` | Output directory (default: `/tmp/run_stats`) |

Total timed operations per run are `num_ops x threads`; concurrent requests
overlap them but do not add to the count. Keep `threads x (concurrent requests + 1)`
below the server's connection limit.

#### Examples

```bash
# Fix 8 threads on 1 branch, vary concurrency: 1,2,4,8,16,32
./scripts/run_throughput_bench.sh dolt db_setup/ch-w1.sql --sweep-concurrency --threads 8 --branches 1 --concurrency-list "1,2,4,8,16,32"

# Fix 4 threads x 16 concurrent requests, vary branches (4 active, the rest idle)
./scripts/run_throughput_bench.sh dolt db_setup/ch-w1.sql --sweep-branches --threads 4 --concurrent-requests 16 --branch-list "4,16,64,256,1024"

# One thread per branch with 4 requests in flight each, 1-128 branches
./scripts/run_throughput_bench.sh neon db_setup/ch-w1.sql --sweep-proportional --threads-per-branch 1 --concurrent-requests 4

# Backend without async support
./scripts/run_throughput_bench.sh xata db_setup/ch-w1.sql --sweep-branches --threads 16 --mode sync

# Run only specific operations
./scripts/run_throughput_bench.sh dolt db_setup/ch-w1.sql --sweep-proportional --operations READ,RANGE_READ
```

#### Output Files

Throughput benchmark results are saved to the output directory:

```
<output_dir>/
├── <backend>_<dataset>_tp_t<threads>_b<branches>[_cr<concurrency>].parquet
├── <backend>_<dataset>_tp_t<threads>_b<branches>[_cr<concurrency>]_<operation>_threads<threads>_summary.json
└── <backend>_<dataset>_tp_t<threads>_b<branches>[_cr<concurrency>]_setup.parquet
```

Async runs include `_cr<concurrency>` in the name (also at concurrency 1); sync
runs do not. The summary JSON records `execution_mode` and `concurrent_requests`.
