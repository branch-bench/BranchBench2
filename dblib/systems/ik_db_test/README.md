# IK-DB-test

IK-DB-test is a mock database. It stands in for a database that a database
maker submits, so the submission path can be tested from the maker's side.
It is a MySQL-protocol database with Git-style branches.

Its server is the Dolt binary, which `install` fetches. The adapter and
connector reuse `dblib/systems/dolt_mysql` under the `ik_db_test` identity.
Results are recorded as IK-DB-test, not as Dolt.

Settings come from the `DOLT_MYSQL_*` environment variables documented in
`dblib/systems/dolt_mysql/README.md`. The lifecycle scripts store the server
PID and log in `DBFORK_STATE_DIR`.
