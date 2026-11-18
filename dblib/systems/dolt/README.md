# Doltgres backend — `dolt`

This package contains the Doltgres adapter, connector, metadata, and server
lifecycle scripts.

Runs against Doltgres, Dolt's PostgreSQL-compatible server (port 5432,
user `postgres`, password `password` by default). Use the `doltgres` compose service,
or install a release from https://github.com/dolthub/doltgresql and start
`doltgres` with a `config.yaml` that sets the listener. Branches are Dolt
branches; every branch operation is native (`dolt_branch`, `dolt_commit`, `dolt_diff`,
`dolt_merge`, `dolt_rebase`, `dolt_revert`, `dolt_reset`), and another
branch's tables are read as `"<db>/<branch>".public.<table>`. Storage is the
database's directory under `DOLT_DATA_DIR`.
