# Chronos backend — `chronos`

The adapter lives in the engine at `dblib/systems/chronos/adapter.py`. Its database lifecycle is in `dblib/systems/chronos/connector.py`. This folder holds system metadata, server scripts, and deployment notes.

[Chronos](https://github.com/mitdbg/chronos) (arXiv:2609.14889) is a
bolt-on branching library, not a server: every registered table lives once
in a physical interval table (`_chronos_b_interval_<t>`) whose rows carry a
half-open visibility interval `[live_lo, live_hi)` and a tombstone; a
branch owns an interval and reads the versions that contain its branch
point. A fork is a metadata transaction (the parent's interval is split, no
data is copied), a read is a range predicate, a write splits the inherited
version at the writer's interval. The library speaks its own Python API
(`ChronosBranchContext`: `create_branch`, `checkout` sessions with
`query`/`execute`, `create_checkpoint`, `diff`, `merge_apply`,
`delete_branch`) and runs the application's SQL itself over a parsed
subset. `dblib/systems/chronos/adapter.py` in the engine is an adapter between that API and the suite.

Setup: the backend uses the same PostgreSQL server as `file_copy`
(`PGSQL_*`), one database per run, and needs the optional dependency
(`uv sync --extra chronos`, a source build of `chronos-core` pinned to the
commit the backend was written against; it needs `libpq-dev`, `libboost-dev`,
`libsqlite3-dev`, `python3-dev` and a C++17 compiler). The run's schema and
seed data are loaded as ordinary PostgreSQL tables with full SQL; the first
versioning verb *adopts* them: every table is registered with Chronos (its
rows copied into the interval table, a table without a primary key gets a
synthetic `_bb_rowid` key, the original is dropped) — the integration path
the Chronos documentation describes. From then on every statement goes
through a Chronos branch session. Chronos runs with
`enable_schema_branching=True` because the scenarios create tables and
columns on branches; in that mode Chronos refuses its native
`merge_apply`, so merge and everything derived from it are built here on
its native diff and bulk-write primitives. `DBToolSuite.implementation()`
records which is which:

- `branch()` (composed): `create_checkpoint` on the parent (the fork base,
  kept for later merges) and `create_branch_from_checkpoint`; a fork from a
  commit forks from that commit's checkpoint. Both are metadata
  transactions. Chronos has no checkpoint deletion, so fork bases and
  commits pin their versions for the run.
- `commit()` (composed): `create_checkpoint` (an immutable interval split;
  a commit ref is a read-only session on it) plus a row in the `_bb_log`
  bookkeeping table, which `log()` reads (simulated: Chronos keeps no
  commit history).
- `diff()` (composed): `ChronosBranchContext.diff` between two branches; a
  commit ref is read through a temporary branch forked from its checkpoint.
- `merge()`, `rebase()` (simulated): a three-way merge in the shim —
  native `diff(base, source)` and `diff(base, target)` against the fork or
  last sync checkpoint, conflicts compared by key, the changes applied with
  native `upsert_rows`/`delete_keys`, tables and columns added on the
  source replayed as DDL on the target (a differing key or type is a schema
  conflict and fails the verb). After a merge or rebase the source gets a
  sync checkpoint that becomes the next base. `rebase()` is the same merge
  with the upstream as the source (the branch's own changes are "theirs").
- `revert()` (simulated): the inverse of the diff between the commit's
  checkpoint and its predecessor's, applied with the same primitives, then
  a commit. `reset()` (simulated): the diff from the branch to the commit's
  checkpoint applied back, with tables and columns added since dropped;
  Chronos cannot move a branch's read point.
- `delete()` (native): `delete_branch` (metadata; versions are reclaimed by
  Chronos's background GC). Chronos deletes a branch's whole subtree, so a
  branch with live children is tombstoned (hidden) and deleted once they
  are gone (`deferred_deletes` in `backend_observations`).
- Multi-branch queries (simulated): `db.table(ref, t)` creates a temporary
  view over the interval table with the visibility predicate of the ref's
  branch point — what Chronos itself substitutes for a table — and the
  SELECT, rewritten by the session for its own branch, runs on a plain
  connection.

What the six-scenario sweep showed (PostgreSQL 18 on this box, CH scale 1,
chronos-core 607418d): a fork, checkpoint or delete costs 10 to 25 ms, in
the range of Dolt's and well below the clone-based backends, and the
per-statement overhead of Chronos's own DML executor is 2 to 10× plain
PostgreSQL. Three properties of the design dominate the longer scenarios:

- History accumulates in the live tables. Every write after a checkpoint
  splits the row into an old and a new version in the same physical table,
  checkpoints pin the old versions for the run, and SELECTs carry no index
  on the interval bounds, so analytical queries scan every version. S4
  (spine checkpoint every 2 s under eight TPC-C clients) had four
  order_line versions per visible row after twelve minutes, and the spine's
  CH queries dominated its 41 minutes.
- Branch-local DDL copies the table. `ALTER TABLE` on a branch creates a new
  physical table with the rows visible to that branch (S4: one copy of
  customer per dev branch, one of stock per spine migration).
- Branch management contends with the branch's writers. Every write holds a
  `FOR SHARE` lock on its branch's metadata row and `create_branch`,
  `create_checkpoint` and `delete_branch` need the row exclusively (they
  also raise `chronos_session_barrier_in_progress` instead of waiting when
  another one is in flight). The shim retries the barrier and holds a
  per-branch gate while the management call runs (`gate_waits`,
  `barrier_retries_*` in `backend_observations`); without it S5's branch
  deletes starved under the spine load until the stall watchdog cancelled
  them.

Two more things the shim guards against: the background interval-GC worker
dies on its first error (in S1 "out of shared memory", PostgreSQL's
`max_locks_per_transaction` exhausted by a GC transaction touching every
physical table) and would then block the context's close forever, so the
shim clears the flag and runs the reclamation synchronously in `gc()`
(`gc_worker_died`); the rows a merge applies are re-read from the
source branch rather than taken from the diff's reported values
(`diff_values_stale` counts disagreements); and a bulk `upsert_rows` or
`delete_keys` that raises a duplicate version key falls back to one
statement per row through Chronos's own executor
(`bulk_write_fallback_rows`). The two merge guards came from S6, where the
first run lost one roll-up increment (a merge wrote a row back to its
pre-rebase value) and the second run's merge failed on exactly that
duplicate key; the third run passed every invariant with neither guard
firing, so both failures are intermittent and are recorded, not explained
(`run_stats/macro_20261011/logs/data_agent_chronos_run{1,2}.log`). The
shim's merge is not atomic: it writes table by table into the live target,
so a failure midway leaves a partial merge, unlike Chronos's staged native
merge. Concurrent TPC-C deliveries on
the spine race on the tombstone of the same new_order row and fail with a
duplicate key, which Chronos retries three times before the driver's own
retry (`DELETE_ROWS` failures in the stats).

The SQL shim: SELECTs run as Chronos rewrites them (each registered table
replaced by its visible-version subquery; joins, CTEs, subqueries and
window functions pass through to PostgreSQL). DML is executed by Chronos's
own evaluator over its subset — `INSERT ... VALUES` with a column list,
`UPDATE` with simple assignments, `DELETE`; no `INSERT ... SELECT`,
`RETURNING`, `NOW()`, `BETWEEN`, scalar subqueries. A statement it rejects
is simulated: the shim evaluates it as a SELECT on the branch (PostgreSQL
computes the new values) and applies the rows with
`upsert_rows`/`delete_keys` (`simulated_update` etc. in
`backend_observations`). `CREATE TABLE` is normalised to what Chronos
accepts (table-level `PRIMARY KEY` made inline; `NOT NULL`, `UNIQUE`,
`CHECK`, `FOREIGN KEY` and non-constant defaults dropped; a keyless table
gets `_bb_rowid`), `CREATE VIEW` is simulated by inlining the view's query
into later SELECTs, a `UNIQUE` index becomes a plain one (Chronos enforces
no constraints beyond the logical key). Parameters are translated from
`%s` to Chronos's named form. Storage is `pg_database_size()` of the run's
database (`database` scope); `gc()` waits for Chronos's background
reclamation and runs `VACUUM`.
