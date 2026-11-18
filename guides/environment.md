# Environment variables

Every adapter reads its connection settings from the process environment;
nothing is read from files, so keep credentials out of configs. With the
servers from `docker-compose.yml` on the same host, the defaults below work
as they are (for Chronos, also set `PGSQL_USER=postgres`).

## Shared

| Variable | Default | Meaning |
|---|---|---|
| `DBFORK_SERVER_HOST` | `localhost` / `127.0.0.1` | Host of every local backend's server, unless the backend's own `*_HOST` is set |
| `DBFORK_SERVER_SSH` | (unset) | `user@host` for measuring storage with `du`/`df` over ssh when the servers run on another machine; per-backend `*_SSH` overrides it |
| `DBFORK_NATIVE_ONLY` | (unset) | `1`: every operation an adapter simulates is reported as unsupported (and ends a macrobenchmark run), so a run shows the engine's own primitives |
| `MACRO_SPILL_ROWS` | `200000` | Result rows held in memory before the collector spills them to the parquet file |
| `DBFORK_STATE_DIR` | | Where a system's `start`/`check`/`stop` scripts keep their PID and log files |

## Per backend

| Backend | Connection | Storage measurement | Other |
|---|---|---|---|
| `dolt` (Doltgres) | `DOLT_HOST`, `DOLT_PORT` (5432), `DOLT_USER` (postgres), `DOLT_PASSWORD` (password) | `DOLT_DATA_DIR` (the server's databases directory), `DOLT_SSH` | |
| `dolt_mysql` (Dolt) | `DOLT_MYSQL_HOST`, `DOLT_MYSQL_PORT` (3306), `DOLT_MYSQL_USER` (root), `DOLT_MYSQL_PASSWORD` (empty) | `DOLT_MYSQL_DATA_DIR`, `DOLT_MYSQL_SSH` | |
| `matrixone` | `MO_HOST`, `MO_PORT` (6001), `MO_USER` (root), `MO_PASSWORD` (111) | `MO_DATA_DIR` (the server's `mo-data`), `MO_SSH` | `MO_COUNT_CONFLICTS` (1) |
| `seekdb` | `SEEKDB_HOST`, `SEEKDB_PORT` (2881), `SEEKDB_USER` (root), `SEEKDB_PASSWORD` (empty) | `SEEKDB_DATA_DIR` (the server's `store`), `SEEKDB_SSH` | |
| `chronos` | `CHRONOS_HOST`, `CHRONOS_PORT`, else `PGSQL_HOST`, `PGSQL_PORT` (5433); `PGSQL_USER` (`postgres` for the compose container, the account that ran `start` otherwise), `PGSQL_PASSWORD` (empty) | from SQL, nothing to set | `CHRONOS_DEBUG_CONFLICTS` |
| `file_copy` | `FILE_COPY_HOST`, `FILE_COPY_PORT`, else `PGSQL_HOST`, `PGSQL_PORT` (5433); `PGSQL_USER`, `PGSQL_PASSWORD` as above | `PGSQL_DATA_DIR` (volume dedicated to the server), `FILE_COPY_SSH` | `FILE_COPY_STRATEGY`, `FILE_COPY_METHOD` (clone), `FILE_COPY_PARK_TIMEOUT_SEC` |
| `neon` | `NEON_API_KEY_ORG` (required), `NEON_ORG_ID`, `NEON_REGION`, `NEON_PG_VERSION` | from the Neon API and SQL | `NEON_COMPUTE_CU`, `NEON_ACTIVE_BUDGET`, `NEON_API_RATE_PER_MIN`, `NEON_API_BURST`, `NEON_BRANCH_SUSPEND_TIMEOUT_SEC`, `NEON_SUSPEND_ON_SWITCH`, `NEON_HISTORY_RETENTION_SEC` |
| `bauplan` | `BAUPLAN_API_KEY` (or `~/.bauplan/config.yaml`), `BAUPLAN_PROFILE`, `BAUPLAN_USERNAME`, `BAUPLAN_S3_URI`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` | | `BAUPLAN_SOURCE_REF`, `BAUPLAN_CLIENT_TIMEOUT`, `BAUPLAN_HISTORY_LIMIT`, `BAUPLAN_PUBLISH_RETRIES` |
| `xata` (experimental) | `XATA_API_KEY`, `XATA_ORGANIZATION_ID` | | |
| `terminusdb` (experimental) | `TERMINUSDB_URL`, `TERMINUSDB_USER`, `TERMINUSDB_PASSWORD`, `TERMINUSDB_TEAM` | `TERMINUSDB_STORAGE_DIR`, `TERMINUSDB_CONTAINER` | `TERMINUSDB_TIMEOUT_SEC`, `TERMINUSDB_LOAD_BATCH`, `TERMINUSDB_REUSE_LOAD` |

Defaults are in parentheses. Each system's README in `dblib/systems/<key>/`
explains its settings in context.

## Servers on other machines

To run the benchmark driver on a different machine from the servers, set
each backend's `*_HOST` to the server's address (or `DBFORK_SERVER_HOST`
for all of them), bind the servers to an address the driver can reach, and
give the driver passwordless ssh to the servers (`*_SSH` or
`DBFORK_SERVER_SSH`) if storage should be measured. The `*_DATA_DIR`
variables are then paths on the server machine.

## Example

```bash
# Neon (hosted): API key of the organization the benchmark creates projects in
export NEON_API_KEY_ORG=...

# PostgreSQL 18 server of the file_copy backend (reflink filesystem, see backends.md)
export PGSQL_DATA_DIR=/mnt/pgbench

# Storage measurement for the docker-compose servers on this host
# (absolute paths: the scripts run from wherever they are started)
export DOLT_DATA_DIR=/path/to/repo/compose-data/doltgres
export DOLT_MYSQL_DATA_DIR=/path/to/repo/compose-data/dolt
export MO_DATA_DIR=/path/to/repo/compose-data/matrixone
export SEEKDB_DATA_DIR=/path/to/repo/compose-data/seekdb/store
```
