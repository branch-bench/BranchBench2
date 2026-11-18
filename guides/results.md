# Results and plots

Every run writes a parquet file with one row per operation and a JSON
summary; the scripts in `scripts/plotting/` turn them into tables and
figures.

## Paper figures

| Script | Input | Output |
|---|---|---|
| `plot_macro_cost.py --run-dir <sweep> --outdir <dir>` | a macrobench sweep (`run_macro_all.sh` output) | cost bars (branch vs data worker-seconds and elapsed time per scenario and system), cost panels, rank inversion |
| `plot_rq1_outcomes.py --run-dir <sweep> ...` | a simulated and optionally a native-only sweep | the correctness grid: per scenario and system, completed / failed check / unsupported |
| `plot_micro_verbs.py`, `plot_micro_tradeoff.py`, `plot_micro_stories.py`, `plot_micro_all.py` (`--run-dir <micro sweep>`) | a `run_micro_verbs.sh` sweep | per-operation latency and storage figures |

Each script's docstring lists its options.


After running benchmarks, use the plotting scripts in the `scripts/plotting/`
directory to generate visualizations. They need the `plotting` extra
(`uv sync --extra plotting`).

## Macrobenchmark Report

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
| `data/summary.md`, `data/summary.csv` | per run: status, elapsed/setup time, support, invariants, agent op counts and median latency per branch operation (spine load excluded), spine throughput and statement latency, scenario metrics |
| `time_breakdown.png` | summed latency split into branch ops (branch operations plus the connection switch `exec()` does to reach a ref) and data ops (statements); spine load excluded |
| `latency_by_op_branch.png`, `latency_by_op_data.png`, `data/latency_by_op.md/.csv` | one panel per operation (branch operations + CONNECT in one figure; statements inside `exec()` + EXEC in the other, with READ/UPDATE/DELETE split into point vs range by rows touched, READ:scan for aggregate/join reads, INSERT:bulk for multi-row inserts): median latency with a 95% CI, grouped by scenario (shaded bands), colour = backend; ops some backends lack are grouped last; the .md adds a backend x op support matrix |
| `latency_cross_branch.png`, `data/cross_branch.md/.csv` | one scenario-level read over several branches (rl_env's `score` over a task's 16 leaves, context_mgmt's `policy_check` over the candidates) as a single logical step: median wall-clock latency per backend, whisker to p90, annotated `multi` (one multi-ref `exec()`) or `per_ref x N` (the same read once per branch on a backend without multi-branch queries, N connection switches included); the table adds the summed exec latency and the per-branch cost |
| `latency_exec_by_label.png`, `data/exec_by_label.md/.csv` | `exec()` latency per script label (rollout_step, ingest, spine, ...) with statements per exec, and the mean exec time split into READ/INSERT/UPDATE/DELETE_ROWS/DDL/CONNECT/other |
| `progress.png` | completed agent steps (branch operations and `exec()` runs, background load excluded) against elapsed seconds, one curve per backend |
| `exec_time_by_op.png` | per scenario, one horizontal 100% bar per script label and backend (adjacent rows) showing where `exec()` time goes: statement types, CONNECT, and `other` (untimed BEGIN/COMMIT/ROLLBACK round trips plus the Python between statements); mean ms and count at the bar end |
| `data/data_ops.md/.csv` | data statements (READ/INSERT/UPDATE/DELETE_ROWS/DDL) split by workload role: spine traffic vs. the agent's own statements (ingest, backfill, rollout_step, ...) |
| `cdf/latency_cdf_<scenario>.png` | latency CDF of each branch operation and data statement type (agent rows only), one line per backend |
| `storage.png` | storage over the run: the 5 s sampled series (or the per-operation sizes of runs made with `--measure-storage`) with the four workflow-level points as markers; log axis when database-scope and server-scope backends share a panel |

Only the newest run per (scenario, backend) is used unless `--all-runs` is
given; `--run-glob` narrows by run id (e.g. `'macro_*_mini_*'`).

## Microbenchmark Latency Plots

Plot microbenchmark latency results:

```bash
uv run python scripts/plotting/plot_branch_latency_micro.py \
    --data-dir <data_directory> \
    --outdir <output_figures_dir> \
    --operation <operation_type>
```

### Arguments

| Argument | Description |
|----------|-------------|
| `--data-dir` | Directory with microbenchmark parquet files |
| `--branch-dir` | Directory with branch operation data (for combined plots) |
| `--connect-dir` | Directory with connect operation data (for combined plots) |
| `--outdir` | Directory to save figures |
| `--operation` | Operation type: `branch`, `connect`, `both`, or `combined` |

### Examples

```bash
# Plot branch creation latency only
uv run python scripts/plotting/plot_branch_latency_micro.py \
    --data-dir run_stats/micro/single_thread/branch \
    --outdir figures/ \
    --operation branch

# Plot branch connection latency only
uv run python scripts/plotting/plot_branch_latency_micro.py \
    --data-dir run_stats/micro/single_thread/connect \
    --outdir figures/ \
    --operation connect

# Plot combined (branch and connect on single plot)
uv run python scripts/plotting/plot_branch_latency_micro.py \
    --branch-dir run_stats/micro/single_thread/branch \
    --connect-dir run_stats/micro/single_thread/connect \
    --outdir figures/ \
    --operation combined
```

## Microbenchmark Per-Operation Plot

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
branch operations plus the connection switch. A branch operation a backend lacks is marked `n/a`.

## Microbenchmark Throughput Plots

Plot throughput vs threads/branches:

```bash
uv run python scripts/plotting/plot_throughput_experiments.py \
    --data-dir <data_directory> \
    --output <output_figures_path> 
```

# Output Files

Benchmark results are saved as Parquet files and JSON summaries.

## Macrobenchmark Output Structure

```
run_stats/
├── macro_rl_env_mini_dolt.parquet
├── macro_rl_env_mini_dolt_e2e_stats.json
├── macro_data_agent_dolt_w5.parquet
└── macro_data_agent_dolt_w5_e2e_stats.json
```

## Microbenchmark Output Structure

```
run_stats/micro/
├── single_thread/      # Single-threaded latency benchmarks
│   ├── branch/
│   └── connect/
├── multithread/        # Multi-threaded latency benchmarks
├── tp_fix_branch/      # Throughput: fixed branches, varying threads
├── tp_fix_thread/      # Throughput: fixed threads, varying branches
└── tp_proportional/    # Throughput: proportional threads and branches
```

## Parquet Schema

See [Result rows](api.md#result-rows); the authoritative definition is
`dblib/result.proto`.
