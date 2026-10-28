# Dolt (MySQL) backend — `dolt_mysql`

The adapter lives in the engine at `dblib/systems/dolt_mysql/adapter.py`. Its database lifecycle is in `dblib/systems/dolt_mysql/connector.py`. This folder holds system metadata, server scripts, and deployment notes.

Runs against Dolt's MySQL-compatible server (port 3306). Start it with:

`dolt sql-server --host 127.0.0.1 --port 3306 --data-dir ~/dolt/databases`

and set a commit identity (`dolt config --global --add user.name` / `user.email`).

The loader reads Postgres `pg_dump` files (e.g. `ch-w1.sql`) directly and
converts them to MySQL, so no separate MySQL schema is needed.

Supports single-threaded, multi-threaded, and async (`concurrent_requests > 1`)
microbenchmark runs via `runner2.py`. In async mode each thread opens a pool
of `concurrent_requests` connections; `exec_async()` checks out the target
branch on a pooled connection the first time it is used there.
