# Neon backend — `neon` (Lakebase Postgres on Neon)

The adapter lives in the engine at `dblib/systems/neon/adapter.py`. Its database lifecycle is in `dblib/systems/neon/connector.py`. This folder holds system metadata and deployment notes.

Runs against Neon's API (`console.neon.tech/api/v2`) with `NEON_API_KEY_ORG`
from the process environment. Each run creates a project of its own (`project_<db_name>`,
region `NEON_REGION`, default `aws-us-east-1`, Postgres `NEON_PG_VERSION`
17, a 2-day history window `NEON_HISTORY_RETENTION_SEC`) and deletes it
afterwards; stale benchmark projects left by interrupted runs are deleted
before a run starts. Every compute is a fixed `NEON_COMPUTE_CU` (2 CU) and never
auto-suspends (`NEON_BRANCH_SUSPEND_TIMEOUT_SEC=-1`): Neon's scale-to-zero
counts only running statements as activity, so with the plan default (5
min on Launch) a compute that a worker holds but has not queried for 5
minutes, or one a merge reads over `postgres_fdw` while busy elsewhere, is
suspended under its connections ("SSL connection has been closed
unexpectedly"). Computes are suspended explicitly instead, by the
active-compute budget below and on delete.

Branches are Neon branches with one read-write compute each. Neon versions
storage but has no commits, diff or merge, so the adapter builds them from
point-in-time branching (`parent_lsn`), branch restore and `postgres_fdw`:

- `branch()` (native): `POST /branches` with `parent_id`, `parent_lsn` for a
  commit ref, and a compute; the verb waits for the operations to finish
  and writes a fork row on the new branch (its own LSN and the parent point
  it was cloned from, the base of later merges).
- `commit()` (composed): a row in the branch's `_bb_commits` table plus
  `pg_current_wal_flush_lsn()` as the commit's point in time; `log()` reads
  the table. A commit ref is read through a temporary branch at that LSN,
  cached between uses, deleted when the connection closes.
- `diff()`: per table a row hash on each compute; the differing tables are
  pulled over `postgres_fdw` into temp tables and compared locally.
- `merge()`: SQL three-way merge on the target's compute. The base is a
  temporary branch at the source's fork LSN (its creation is inside the
  verb's latency), theirs is the source's compute, both pulled over
  `postgres_fdw`; rows are matched by primary key, conflicts resolved per
  `on_conflict` with the same callable row shape as the other backends;
  deletes go in reverse foreign-key order and upserts in forward order
  (the role is not a superuser, so constraints stay on).
- `rebase()`: the same three-way merge of the upstream into the branch,
  then a rebase row moves the branch's base to the upstream's LSN so the
  merge back only sees newer changes.
- `revert()`: temporary branches at the commit's LSN and its predecessor,
  inverse delta applied locally.
- `reset()` (native): `POST /branches/{id}/restore` to the commit's LSN.
  Neon requires `preserve_under_name` and keeps the pre-restore state in a
  backup branch (`<branch>_bk_*`) that cannot be deleted before the project.
- `delete()` (native): `DELETE /branches/{id}`; a branch that still has
  children is deferred and retried after later deletes and at close.
- Multi-branch queries (`exec(mode="multi")`) are not provided: Neon has no
  multi-branch query semantics, and the scenarios' cross-branch reads run
  once per branch instead (recorded as the `multi_ref_exec: unsupported`
  implementation entry).

Quotas are capacity, not errors. Neon limits *active computes* per
project (20 on the Launch plan, the default branch exempt), not branches,
so the process keeps at most `NEON_ACTIVE_BUDGET` (18) computes active:
when a compute is needed above that, the least recently used one that no
connection holds is suspended first (`NEON_SUSPEND_ON_SWITCH=1` instead
suspends a branch's compute whenever the connection moves off it); a
suspended compute resumes on the next connect, and both land in `CONNECT`.
A connect refused for the compute limit rebuilds the registry from
Neon's own list of active computes, suspends the least recently used
ones no connection holds, and retries (`active_compute_limit_hits`,
`limit_suspends` and `active_compute_unregistered` in the e2e stats'
`backend_observations` count how often that happened). All API calls share one process-wide token bucket under the
documented 700 requests/minute (`NEON_API_RATE_PER_MIN`, `NEON_API_BURST`);
bucket waits are part of the verb's cost and are summed in the e2e stats'
`backend_observations`, while reactive waits (429/423/503 backoff, compute
limit) are recorded as `API_RETRY_WAIT` rows. Connections use the unpooled
endpoint: the pooler runs PgBouncer in transaction mode, where session
state (temp tables, the `search_path` postgres_fdw sets on its remote
sessions) leaks between clients. Storage is the sum of the branches'
`logical_size` from the API; there is no GC hook.
