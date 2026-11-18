# Backends

Each backend is a package in `dblib/systems/<key>/`: an adapter
(`adapter.py`, a `DBToolSuite` subclass implementing the hooks of
[api.md](api.md) on one system's branching primitives), a connector
(`connector.py`, which creates, loads and deletes the benchmark database),
`system.json` metadata, a README with the system's setup and how each
operation is implemented, and for self-hosted systems `start`, `check` and
`stop` scripts. [CONTRIBUTING.md](../CONTRIBUTING.md) describes how to add
one.

The adapter reports, per operation, whether it is **native** (the system's
own primitive), **composed** (several native primitives driven by the
adapter) or **simulated** (the adapter builds it from SQL). A run with
`DBFORK_NATIVE_ONLY=1` treats every simulated operation as unsupported.

| Backend key | System | In the paper | Server | Setup and details |
|---|---|---|---|---|
| `dolt` | Doltgres (Dolt, PostgreSQL protocol) | yes | container (`doltgres`) or `start` | [README](../dblib/systems/dolt/README.md) |
| `dolt_mysql` | Dolt (MySQL protocol) | yes | container (`dolt`) or `start` | [README](../dblib/systems/dolt_mysql/README.md) |
| `matrixone` | MatrixOne | yes | container (`matrixone`) or `start` | [README](../dblib/systems/matrixone/README.md) |
| `seekdb` | SeekDB (OceanBase) | yes | container (`seekdb`) or `start` | [README](../dblib/systems/seekdb/README.md) |
| `chronos` | Chronos (Python library over PostgreSQL) | yes | container (`chronos-postgres`) or `start`, plus `uv sync --extra chronos` | [README](../dblib/systems/chronos/README.md) |
| `neon` | Neon | yes | hosted (API key) | [README](../dblib/systems/neon/README.md) |
| `file_copy` | PostgreSQL 18 `CREATE DATABASE ... STRATEGY FILE_COPY` | yes (baseline) | PostgreSQL 18 on a reflink filesystem | [README](../dblib/systems/file_copy/README.md) |
| `bauplan` | Bauplan | no | hosted (API key) | [README](../dblib/systems/bauplan/README.md) |
| `xata` | Xata | experimental, not officially supported | hosted | [README](../dblib/systems/xata/README.md) |
| `terminusdb` | TerminusDB | experimental, not officially supported (microbenchmark only) | container or binary | [README](../dblib/systems/terminusdb/README.md) |
| `ik_db`, `ik_db_test`, `tiger` | leaderboard demonstration and mock entries | no | | READMEs in `dblib/systems/` |

What each backend supports:

| Backend | branch | commit/diff/log | merge | rebase/revert | reset | delete | commit refs | multi-branch exec | exec_async |
|---------|--------|-----------------|-------|---------------|-------|--------|-------------|-------------------|------------|
| `dolt`, `dolt_mysql` | yes | yes | yes | yes | yes | yes | yes | yes | yes |
| `neon` | yes | yes (LSN + log table) | yes (SQL three-way over postgres_fdw) | yes | yes (restore to LSN) | yes | yes | per-ref fallback | yes |
| `xata` (experimental) | yes | | | | | yes | | | yes |
| `seekdb` | yes | yes (SCN snapshots) | yes (SQL three-way) | yes | yes | yes | yes | yes | yes |
| `matrixone` | yes | yes (snapshots) | yes (DATA BRANCH MERGE) | yes | yes (diff-driven) | yes | yes | yes | yes |
| `bauplan` | yes | yes (tags) | yes (native; row-level on conflict) | yes | yes (branch from tag) | yes | yes | | |
| `file_copy` | yes (CREATE DATABASE clone) | yes (clone per commit + log table) | yes (SQL three-way over postgres_fdw) | yes | yes (drop + clone from the snapshot) | yes | yes | yes (postgres_fdw) | yes |
| `chronos` | yes (interval fork, metadata only) | yes (checkpoint + log table; diff as SQL over the interval tables) | yes (three-way over Chronos diffs) | yes | yes (diff-driven) | yes (deferred while children live) | yes (checkpoints) | yes (interval views) | |
| `terminusdb` (experimental, microbench only) | yes | commit: reads the head (every write commits); diff, log: yes | yes (`apply` of the diff since the common ancestor) | rebase: yes; revert: `apply` of the inverse diff | yes | yes | yes | | |

## Servers with Docker Compose

`docker-compose.yml` runs the servers that work in containers, at the
versions the paper evaluated:

```bash
docker compose up -d --wait doltgres      # or: dolt, matrixone, seekdb, chronos-postgres
docker compose up -d --wait               # all five
docker compose down                       # stop; data stays in ./compose-data
```

Ports match the adapters' defaults, so nothing else needs configuring when
the benchmark runs on the same host, except `PGSQL_USER=postgres` for
Chronos (the adapter defaults to the user that `dblib/systems/chronos/start`
runs PostgreSQL as). Override a host port
with `DOLTGRES_HOST_PORT`, `DOLT_HOST_PORT`, `MATRIXONE_HOST_PORT`,
`SEEKDB_HOST_PORT` or `CHRONOS_HOST_PORT` (and point the adapter at it, e.g.
`DOLT_PORT`), and a version with `DOLTGRES_VERSION`, `DOLT_VERSION`,
`MATRIXONE_VERSION`, `SEEKDB_VERSION` or `CHRONOS_PG_VERSION`. To measure
storage, set the backend's data-directory variable to the bind mount (see
[environment.md](environment.md)). The data files are owned by root inside
the containers; remove them with
`docker run --rm -v "$PWD/compose-data:/d" postgres:18.6 sh -c 'rm -rf /d/*'`
or `sudo rm -rf compose-data`.

The SeekDB service sets `undo_retention = 86400` on first start
(`compose/seekdb-init/`), and the Chronos PostgreSQL runs with
`max_locks_per_transaction = 1024`; both are needed for full-size runs.

Each self-hosted system's `start`, `check` and `stop` scripts are the
other way to run its server: they install nothing (except `install`, where
present) and keep their PID and logs in `DBFORK_STATE_DIR`.
