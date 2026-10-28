# IK-DB

IK-DB is a MySQL-protocol database with Git-style branches. For the alpha
demonstration its server is the Dolt binary, which `install` fetches, so the
adapter and connector reuse `dblib/systems/dolt_mysql` under the `ik_db`
identity. Results are recorded as IK-DB, not as Dolt.

Settings come from the `DOLT_MYSQL_*` environment variables documented in
`dblib/systems/dolt_mysql/README.md`. The lifecycle scripts store the server
PID and log in `DBFORK_STATE_DIR`.
