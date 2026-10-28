# Add a database to BranchBench

Submit the complete database integration in this repository. The engine CLI
must work without the leaderboard. After the merge, the leaderboard follows
engine `main` automatically and publishes the alpha result.

Create one self-contained directory:

```text
dblib/systems/<system>/
├── __init__.py
├── adapter.py
├── connector.py
├── system.json
├── README.md
├── install          # optional
├── start            # required for self-hosted systems
├── check            # required for self-hosted systems
└── stop             # required for self-hosted systems
```

## 1. Implement the adapter

Add `dblib/systems/<system>/adapter.py`, using a lowercase system key with digits or underscores.
Subclass `DBToolSuite` from `dblib.db_api`. Implement the supported protected
hooks, including connection management and storage measurement.

Declare which verbs are native or simulated. Report unsupported operations
through the existing API. Do not modify workload logic to hide a provider's
unsupported behavior. Reuse `dblib/mysql_common.py`, `dblib/pg_merge.py`, and
`dblib/util.py` where their existing behavior fits.

## 2. Implement the connector

Add `dblib/systems/<system>/connector.py` and use the contract in `dblib/connector.py`.
Follow a working connector for the same protocol.

The connector supplies these operations:

- Create or open the benchmark database.
- Load a dataset and construct a `PreparedTarget`.
- Open an independent adapter instance for each requested worker.
- Delete the run's database resources and verify their absence.
- Report the connection scope and provider version without secrets.
- Check hosted account access before provisioning, when applicable.

Export the connector class as `CONNECTOR`. The engine discovers complete
packages in `dblib/systems/` without a separate registry table. Its loader creates a
connector with a caller-selected recovery directory. Preserve cleanup errors
and recovery information. Validate database identifiers before using them in
provider calls. Read credentials from the process environment only.

Add `system.json` and a README to the same directory. For a self-hosted
database, add `start`, `check`, and `stop`. Add `install` only when the runner
must install the server. Lifecycle scripts receive `DBFORK_STATE_DIR` and must
store their PID and recovery files there.

## 3. Verify engine use

Add focused tests for the adapter's behavior and connector lifecycle.
Keep fake-database tests independent of live services. Clearly separate
live tests that need a server or can create billable resources.

Check all three standalone entry points without the leaderboard installed:

```bash
uv sync --frozen
uv run --frozen python -m microbench.runner2 --help
uv run --frozen python -m macrobench.runner --help
uv run --frozen python -m microbench.verbs --help
uv run --frozen python -m pytest tests/test_runners.py
uv build
```

Then run an appropriate workload against a disposable database using
`--backend <system>` and `--db-name <name>`. Check recorded failures,
unsupported operations, work counts, and cleanup. A `completed` status
alone does not prove that every unit succeeded.

Submit the complete system package, tests, and required dependency changes in
one engine PR. State which checks used a live database.

## 4. Publication

The database maker does not change the leaderboard repository. The leaderboard
checks engine `main` every five minutes. When it has moved, the leaderboard
commits the new engine pin and lockfile, benchmarks the changed self-hosted
systems on a fresh EC2 worker, and publishes their results to the alpha site.
Hosted systems need a separately configured run. Alpha timings are not
calibrated production rankings.
