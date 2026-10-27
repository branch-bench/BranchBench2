# Add a database to BranchBench

Implement database support in this repository first. The engine CLI must
work without the leaderboard. Register the database with the leaderboard
after maintainers publish the engine change.

## 1. Implement the adapter

Add `dblib/<system>.py`, using a lowercase system key with digits or underscores.
Subclass `DBToolSuite` from `dblib.db_api`. Implement the supported protected
hooks, including connection management and storage measurement.

Declare which verbs are native or simulated. Report unsupported operations
through the existing API. Do not modify workload logic to hide a provider's
unsupported behavior. Reuse `dblib/mysql_common.py`, `dblib/pg_merge.py`, and
`dblib/util.py` where their existing behavior fits.

## 2. Implement the connector

Add `dblib/connectors/<system>.py` and use the contract in `dblib/connector.py`.
Follow a working connector for the same protocol.

The connector supplies these operations:

- Create or open the benchmark database.
- Load a dataset and construct a `PreparedTarget`.
- Open an independent adapter instance for each requested worker.
- Delete the run's database resources and verify their absence.
- Report the connection scope and provider version without secrets.
- Check hosted account access before provisioning, when applicable.

Export the connector class as `CONNECTOR`. The engine discovers modules in
`dblib/connectors/` without a separate registry table. Its loader creates a
connector with a caller-selected recovery directory. Preserve cleanup errors
and recovery information. Validate database identifiers before using them in
provider calls. Read credentials from the process environment only.
Fetch secrets from Bitwarden at point of use.

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

Submit the adapter, discoverable connector, tests, and required dependency
changes in an engine PR. State which checks used a live database.

## 4. Register with the leaderboard

After the engine commit is published, open a separate PR in
[BranchBench2-Leaderboard](https://github.com/branch-bench/BranchBench2-Leaderboard).

1. Pin the published engine commit in `pyproject.toml` and update `uv.lock`.
2. Add `leaderboard/systems/<system>/system.json` and setup instructions.
3. For self-hosted systems, add server `start`, `check`, and `stop` scripts.
   Add `install` when the server needs installation.
4. Verify the leaderboard run and generated site. Do not copy adapters or
   connectors into the leaderboard repository.

Submit code and registration metadata, not workstation timing JSON.
A maintainer merge that changes exactly one self-hosted system can trigger
the configured alpha workflow. An engine merge alone does not trigger it.
For an engine-pin-only update, maintainers select the system through manual
workflow dispatch. Alpha timings are not calibrated production rankings.
