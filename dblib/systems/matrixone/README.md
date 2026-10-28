# MatrixOne backend — `matrixone`

The adapter lives in the engine at `dblib/systems/matrixone/adapter.py`. Its database lifecycle is in `dblib/systems/matrixone/connector.py`. This folder holds system metadata, server scripts, and deployment notes.

Runs against MatrixOne (MySQL-compatible, port 6001). Build it from source
(`make build` in a checkout of https://github.com/matrixorigin/matrixone, Go
1.26+ and cmake needed; v4.2.5 was used here) and start it with
`./mo-service -launch ./etc/launch/launch.toml`, or run the
`matrixorigin/matrixone` image. Connection settings come from `MO_HOST`
(default `127.0.0.1`), `MO_PORT` (`6001`), `MO_USER` (`root`) and
`MO_PASSWORD` (`111`, the standalone default). Storage is measured on the
server's data directory, `MO_DATA_DIR` (default `~/mo/matrixone/mo-data`),
which covers the whole server.

MatrixOne ships "git for data" primitives: database and table branches with
recorded lineage (`DATA BRANCH CREATE DATABASE ... FROM ... {snapshot}`), a
three-way `DATA BRANCH DIFF` / `DATA BRANCH MERGE` that finds the lowest
common ancestor itself, `DATA BRANCH PICK ... BETWEEN SNAPSHOT`, named
snapshots with time-travel reads (`t{snapshot = 'x'}`, readable even after
the database is dropped) and `RESTORE DATABASE ... {snapshot}`. As on
SeekDB, each branch is its own database (`main` is `<db_name>`, branch `X`
is `<db_name>__X`), and a multi-branch script addresses another branch as
`` `<db>__<branch>`.`<table>` `` or a commit as `` `<db>`.`<table>`{snapshot = '...'} ``.

A commit is a database snapshot; its message and order live in the branch
database's `_bb_commits` table, which branches inherit. Per verb
(`MatrixOneToolSuite.IMPLEMENTATION` / `IMPLEMENTATION_NOTES` carry the
same summary for the report):

- `branch()` (native): `CREATE SNAPSHOT` on the parent (the fork point)
  and `DATA BRANCH CREATE DATABASE ... FROM ... {snapshot}`; the new branch
  is snapshotted too so its own delta can be read later.
- `commit()` (composed): `CREATE SNAPSHOT FOR DATABASE` plus the log row.
  `log()` (simulated) reads the table.
- `diff()` (native): `DATA BRANCH DIFF ... OUTPUT SUMMARY` per table.
- `merge()` (composed): `DATA BRANCH MERGE <src>.<t> INTO <dst>.<t> WHEN
  CONFLICT SKIP|ACCEPT` per table ("ours" | "theirs"); the conflict count,
  and the rows a resolve callable gets (Dolt's `base_*`/`our_*`/`their_*`
  shape), come from the two sides' native diffs against the fork snapshot.
  A callable runs over the SKIP result. Tables and columns the source has
  and the target lacks are added first (`DATA BRANCH CREATE TABLE ... FROM`,
  `ALTER TABLE ADD COLUMN`); a differing primary key is a schema conflict
  and the verb fails without touching data. Set `MO_COUNT_CONFLICTS=0` to
  skip the conflict count when `on_conflict` is "ours"/"theirs".
- `rebase()` (composed): a re-fork. A temporary clone of the upstream's
  head snapshot receives the branch's delta since its fork point through
  `DATA BRANCH PICK ... BETWEEN SNAPSHOT` (the branch's own snapshots;
  key-less tables are replayed row by row), conflicts with the upstream's
  delta over the same period are resolved per `on_conflict` ("ours" = the
  upstream, "theirs" = the branch, as in git), and the clone replaces the
  branch. Its lineage now starts at the upstream head, so MatrixOne's LCA
  for a later merge is that head and the merge sees only newer changes.
- `reset()` (composed): `DATA BRANCH DIFF` of the head against the commit's
  snapshot, undone with SQL (rows added since are deleted, rows changed or
  deleted put back, tables and columns created since dropped). The native
  `RESTORE DATABASE <branch> {snapshot}` (`MO_NATIVE_RESET=1`) does it in
  one statement but gives the tables new identities, and later lineage
  diffs across that edge report updated rows as inserted on both sides,
  which broke the rebase of reset batches in S6. A commit inherited from
  the parent is restored row by row from the snapshot's time-travel view.
- `revert()` (simulated): `DATA BRANCH DIFF` between the commit's snapshot
  and its predecessor, inverse applied with SQL.
- `delete()` (native): `DATA BRANCH DELETE DATABASE` (`DROP DATABASE` after
  a restore, which leaves the database without branch metadata).

MatrixOne's diff is reliable for two snapshots of one table, for a clone
(or chain of clones) against its ancestor's head, and for a clone against
the exact snapshot it was cloned from; it drops the ancestor's updates and
deletes when a clone is compared with an *older* snapshot of the ancestor,
which is why `rebase()` replays the branch's delta with `PICK` instead of
merging the branch into the clone. `DATA BRANCH DIFF ... OUTPUT AS` fails
on a table altered since the base (v4.2.5), so diff rows are fetched to the
client. A native-merge conflict between an update and a delete keeps the
update (the row is resurrected). Snapshot names are silently truncated to
64 characters; the backend keeps its own below that. `CREATE INDEX IF NOT
EXISTS` is not accepted (S2 falls back to `CREATE INDEX`).

A commit ref used with `exec()` is materialised as a temporary branch
(`<db>__tmp_*`) created from the snapshot and dropped when the connection
closes. `drop_database()` drops every branch database and every snapshot of
the run (`bb_<db>_*`). Async mode uses an aiomysql pool, like `dolt_mysql`.
