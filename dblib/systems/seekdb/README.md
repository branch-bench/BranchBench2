# SeekDB backend — `seekdb`

The adapter lives in the engine at `dblib/systems/seekdb/adapter.py`. Its database lifecycle is in `dblib/systems/seekdb/connector.py`. This folder holds system metadata, server scripts, and deployment notes.

Runs against SeekDB (OceanBase's MySQL-compatible server, port 2881). Install
and start it with `brew tap oceanbase/seekdb && brew install seekdb &&
seekdb-start`, or run a source build's `bin/seekdb` from its base directory.

Connection settings come from `SEEKDB_HOST` (default `127.0.0.1`),
`SEEKDB_PORT` (`2881`), `SEEKDB_USER` (`root`) and `SEEKDB_PASSWORD` (empty).
Storage is measured on the server's data directory, `SEEKDB_DATA_DIR`
(default: `~/seekdb/store` if it exists, else the brew install's
`/opt/homebrew/var/seekdb/data`); it covers the whole server, since SeekDB
has no per-database directory. It uses the same `pg_dump` loader as
`dolt_mysql`. Every connection raises `ob_query_timeout` to one hour: the
default 10 s is too short for seeding and for the joins behind merge/rebase.

SeekDB branches by forking whole databases (copy-on-write, milliseconds), so
each branch is its own database: `main` is `<db_name>`, and branch `X` is
`<db_name>__X`. `branch()` runs `FORK DATABASE`, connecting runs `USE`, and
`delete()` runs `DROP DATABASE` (forked databases can take several seconds to
drop). A multi-branch script addresses another branch as
`` `<db>__<branch>`.`<table>` ``.

SeekDB has no commits, so the history verbs are built from two primitives it
does have: `current_scn()` with flashback reads (`<table> AS OF SNAPSHOT
<scn>`), and database forks.

- `commit()` records the current SCN in the branch database's `_bb_commits`
  table (its id is the SCN in hex) together with the table and column list at
  that point. Forks copy that table, so a branch inherits its parent's log;
  `log()` reads it.
- `diff()`, `reset()`, `revert()` and `branch(name, "b@commit")` read the
  commit's snapshot with flashback queries and apply the differences with
  anti-joins (`DELETE ... NOT IN`, `REPLACE INTO ... SELECT`). A fork's
  tables have no history from before the fork, so each log row names the
  database whose flashback holds its snapshot; a snapshot older than the
  server's `undo_retention` (default 30 min; raise it with
  `ALTER SYSTEM SET undo_retention = 86400`) can no longer be read. Flashback
  cannot roll back DDL, so a restore drops the tables and columns the commit's
  recorded schema does not list (indexes stay). An index built after row
  updates makes that table's earlier snapshots unreadable for good (error
  1412): a restore then keeps the table's current rows, a merge or rebase
  uses the fallback fork point or, failing that, a two-way merge for it,
  and the verb's result lists the table (`not_restored`,
  `fallback_base_tables`, `two_way_tables`) with a warning.
- `merge()` and `rebase()` are a SQL three-way merge: base = the fork point
  of whichever side descends from the other (the fork's own content right
  after the fork; the parent at the SCN just before the fork is kept as a
  fallback for tables whose history later DDL made unreadable), ours = the
  target database, theirs = the other branch. Rows are matched by primary key; a row changed differently on
  both sides is a conflict, resolved per `on_conflict` (a callable gets rows
  in Dolt's `base_*` / `our_*` / `their_*` shape). Tables and columns added
  on one side are added to the other; a differing primary key is a schema
  conflict and the verb fails without touching data. Tables without a
  primary key (`history`) only receive the other side's new rows. `rebase()`
  reads the upstream at one SCN, which becomes the branch's new fork point,
  and the merge after it is then a fast-forward.
- `SEEKDB_NATIVE_MERGE=1` makes `merge()` use SeekDB's own `MERGE TABLE ...
  STRATEGY OURS|THEIRS` per table instead. It has no common ancestor (every
  differing row is a conflict; the source's deletes are never applied) but
  measures the native primitive; conflicts are counted with `STRATEGY FAIL`.

A commit ref used with `exec()` is materialised as a temporary fork
(`<db>__tmp_*`) restored to that snapshot and dropped when the connection
closes.

Async mode (`use_async` / `concurrent_requests > 1`) uses an aiomysql pool,
like `dolt_mysql`. Each pool connection is opened on the worker's branch
database.
