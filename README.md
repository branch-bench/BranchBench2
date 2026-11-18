# BranchBench (BB2): a benchmark for database branching in agentic workloads

Agents increasingly explore, test and undo changes on real databases, and
for that they need what developers get from git: branches they can create,
compare, merge and throw away. Many systems now offer some form of
database branching, implemented at very different layers, with different
semantics and very different costs. BB2 measures them on the same footing.

- **One git-like API** (`dblib/db_api.py`) over every system: `branch`,
  `commit`, `diff`, `log`, `merge`, `rebase`, `revert`, `reset`, `delete`,
  plus `exec()` to run SQL on one or more branches.
- **Adapters** that map the API onto each system's own primitives and
  simulate what a system lacks, reporting every operation as native,
  simulated or unsupported, so a run separates what the engine does from
  what the adapter adds.
- **Six agent scenarios** (macrobenchmark): RL rollouts, context
  management, multi-agent collaboration, a development agent, an operations
  agent recovering from an incident, and a data agent. Each records every
  operation's latency, splits branch work from data work, tracks storage,
  and checks invariants that define a correct outcome.
- **A per-operation microbenchmark** that varies one condition at a time
  (data size, history depth, live branches, conflicts, ...).

Evaluated systems: Dolt and Doltgres, MatrixOne, SeekDB, Chronos, Neon, and
PostgreSQL 18's database clone (`file_copy`) as a baseline. See
[guides/backends.md](guides/backends.md) for the full list, including the
experimental adapters (Xata, TerminusDB), which are not officially
supported.

This repository is the benchmark engine: the API, the adapters and
connectors in `dblib/systems/`, the runners and the standalone commands. The
separate [leaderboard application](https://github.com/branch-bench/BranchBench2-Leaderboard)
imports it and owns server scripts, run recovery, AWS workers, validation,
publication and the website. See the [ownership requirements](requirements/README.md)
and, to add a database, [CONTRIBUTING.md](CONTRIBUTING.md).

## Quick start

Requirements: [uv](https://docs.astral.sh/uv/) and Docker (with Compose).

```bash
# 1. Python 3.13 and the dependencies, into .venv (compiles the .proto files)
uv sync                                 # add --extra chronos for Chronos, --extra plotting for the plots

# 2. A server to run against (Doltgres here; see docker-compose.yml for the others)
docker compose up -d --wait doltgres

# 3. A small run of one scenario: S2 (context management) on Doltgres
./scripts/run_macrobench.sh --mini --outdir run_stats context_mgmt dolt

# 4. The offline tests (an in-memory fake backend, no server needed)
uv run pytest tests/test_db_api.py tests/test_scenarios.py tests/test_scenario_params.py tests/test_macrobench.py
```

Step 3 prints the checks and writes two files to `run_stats/`: a parquet
file with one row per operation, and `<run_id>_e2e_stats.json` with the
run's status, which operations the backend supports, the invariant results
and the scenario's metrics. A full-size run drops `--mini`;
`./scripts/run_macro_all.sh` runs every scenario on every backend. The
runner can also be called directly, naming the backend and a disposable
database:

```bash
uv run python -m macrobench.runner --config macrobench/configs/rl_env_mini.textproto \
    --backend dolt --db-name branchbench_local --outdir run_stats/local
```

`database_setup.cleanup` in the config decides whether the database is
dropped afterwards; `cleanup: true` drops an existing database of that name
too.

Note: `uv sync` removes optional extras that it is not asked for, so keep
passing `--extra chronos` (and `--extra plotting`) once installed.

## How a run works

A scenario is a seeded driver: it replays an agent workflow (which branches
to create, what SQL to run on them, when to commit, merge, rebase or roll
back) with concurrent workers, against one backend through the API.
Everything the driver does goes through the API, which times each call and
records it.

- **An unsupported operation stops the run.** When a backend cannot perform
  a step of the workflow, the run ends there and is reported as
  `unsupported: <OPERATION>: <reason>`. Set `DBFORK_NATIVE_ONLY=1` (or
  `--native-only`) to treat every simulated operation as unsupported and see
  how far a system gets on its own primitives.
- **A failed operation does not stop the run.** It is recorded, and the
  scenario's invariant checks report its effect on the outcome.
- **A run is correct** when it completes, reports nothing unsupported, and
  passes every invariant check.

## Guides

| Guide | What it covers |
|---|---|
| [guides/api.md](guides/api.md) | The Python API: branch operations, refs, `exec()`, conflict resolution, result rows |
| [guides/macrobench.md](guides/macrobench.md) | The six scenarios, their configuration and intensity knobs, running them, storage measurement |
| [guides/adding_a_scenario.md](guides/adding_a_scenario.md) | Writing a new scenario: the class, its parameters and checks, tables, configs, tests |
| [guides/microbench.md](guides/microbench.md) | The per-operation microbenchmark, the single-operation runner and the latency/throughput scripts |
| [guides/backends.md](guides/backends.md) | Every backend, what it supports, Docker Compose; each system's own README has its setup |
| [guides/embedding.md](guides/embedding.md) | Calling the runners from another application through `PreparedTarget` |
| [guides/results.md](guides/results.md) | Output files and the plotting scripts, including the paper's figures |
| [guides/environment.md](guides/environment.md) | Environment variables |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Adding a database system |

## Repository layout

```
dblib/              # the git-like API (db_api.py), prepared targets, connectors, result collection
dblib/systems/      # one package per database: adapter, connector, metadata, README, server scripts
macrobench/         # the scenarios (scenarios/, incl. template.py), their configs, schema and data generators, the runner
microbench/         # the per-operation microbenchmark (verbs/), the single-operation runner
scripts/            # entry points (run_*.sh) and plotting/ (optional extra: plotting)
db_setup/           # the CH-benCHmark schema and seed dataset, plus loader scripts
compose/            # files the Docker Compose services mount
guides/             # the documentation above
tests/              # offline tests (fake backend) and live tests per backend
requirements/       # ownership and standalone execution requirements
util/               # shared helpers
```

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
- The live tests (`tests/test_<backend>_live.py`) run against a real
  server and skip when none is reachable.
