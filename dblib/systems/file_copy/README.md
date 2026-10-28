# PostgreSQL file-copy backend — `file_copy`

The adapter lives in the engine at `dblib/systems/file_copy/adapter.py`. Its database lifecycle is in `dblib/systems/file_copy/connector.py`. This folder holds system metadata, server scripts, and deployment notes.

Plain PostgreSQL (18 or later) where a branch is a database. The one native
branching primitive is `CREATE DATABASE <child> TEMPLATE <parent> STRATEGY =
FILE_COPY`; with `file_copy_method = clone` (the run sets it server-wide,
`FILE_COPY_METHOD`) the data files are reflinked, so on a copy-on-write
file system (XFS with `reflink=1`, Btrfs, APFS) a clone shares its blocks
with the template until either side writes. Connection settings come from
`PGSQL_USER`, `PGSQL_PASSWORD`, `PGSQL_HOST`, `PGSQL_PORT`. This repository's server is PostgreSQL 18 on an XFS loop volume
mounted at `/mnt/pgbench` (`sudo -u postgres /usr/lib/postgresql/18/bin/pg_ctl
-D /mnt/pgbench/pgdata -l /mnt/pgbench/pg.log start`, port 5434).

Everything beyond branch and delete is built on that clone, on
`postgres_fdw` (one database reading another's tables) and on plain SQL;
`DBToolSuite.implementation()` records which is which:

- `branch()` (composed): the clone of the parent, plus, for a fork from a
  branch head, a second clone of the child kept as the merge base (the
  parent moves on); a fork from a commit uses the commit's snapshot as its
  base. A template may have no other connection while it is copied, so the
  backend keeps every connection it opens in a process-wide registry: a
  clone closes the idle connections to its template (a parked connection
  reopens on its next use, recorded as a `CONNECT`), waits for the busy
  ones (a verb or exec in flight) and holds new ones back until the copy is
  done. The wait is part of the verb's latency; the e2e stats'
  `backend_observations` count `parked_connections`, `quiesce_waits`,
  `quiesce_wait_sec` and `clone_retries`.
- `commit()` (composed): a row in the branch's `_bb_commits` table, then a
  clone of the branch database as the commit's snapshot (read-only; a
  commit ref connects to it). A clone costs two checkpoints plus one
  reflink per data file, with the branch's connections parked meanwhile.
  `log()` reads the table.
- `diff()`, `merge()`, `rebase()`, `revert()` (simulated): the same SQL
  three-way machinery as Neon's (`dblib/pg_merge.py`), on local databases
  instead of computes. The merge base is the fork or last-rebase snapshot
  of whichever side descends from the other; `rebase()` first clones the
  upstream (its head keeps moving under load) and that clone becomes the
  branch's new base, so commits are not replayed and lineage is not
  rewritten; `revert()` applies the inverse of (predecessor snapshot ->
  commit snapshot) and records the result as a commit with its own
  snapshot.
- `reset()` (composed): `DROP DATABASE` of the branch, then `CREATE DATABASE
  ... TEMPLATE <commit snapshot>`; the snapshot's log ends at that commit.
- `delete()` (native): `DROP DATABASE` of the branch, its bases and its
  snapshots; a snapshot another branch's log still references (a commit
  merged into the spine, a fork from a commit) is kept until the run's
  cleanup, so reverting a deleted candidate's commit still works.
- Multi-branch queries (simulated): `db.table(ref, t)` imports the other
  ref's table into the session's database through `postgres_fdw` (once per
  exec; the foreign-server connection is dropped when the exec ends, since
  it would otherwise count as a connection to that database).

Storage: with `PGSQL_DATA_DIR` set to the mount point of a volume dedicated
to the server, storage is the volume's usage minus WAL (`pg_ls_waldir()`),
which counts shared blocks once (`server` scope); without it, the sum of
`pg_database_size()` over the run's databases, which counts them once per
clone. `gc()` is a `CHECKPOINT`. All databases of a run share the prefix
`<db_name>__`; cleanup (and the start of a run) drops them with `FORCE`.
