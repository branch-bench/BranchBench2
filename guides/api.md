# Database API

`dblib/db_api.py` defines `DBToolSuite`, the adapter base class every
database system implements. Built-in adapters live in
`dblib/systems/<system>/adapter.py`. An
adapter overrides the protected hooks it supports (`_branch_impl`,
`_commit_impl`, ..., `_storage_bytes`, `_connect_impl`, `_flush_impl`); the
public verbs wrap them with timing, storage measurement and result recording.
The runners reach adapters through the `PreparedTarget` factory.

## Verbs

| Verb | Meaning | Returns |
|------|---------|---------|
| `branch(name, from_ref)` | create a branch | |
| `commit(ref, message)` | snapshot the branch's working state | commit id |
| `diff(ref_a, ref_b)` | differences between two refs | backend-specific summary |
| `log(ref, limit)` | recent commits | list of dicts |
| `merge(into, source, message, on_conflict)` | merge `source` into `into` | `fast_forward`, `conflicts`, `conflict_tables` (Dolt) |
| `rebase(ref, onto, on_conflict)` | replay `ref`'s commits on `onto` | `conflicts`, `conflict_tables` (Dolt) |
| `revert(ref, commit)` | add a commit undoing `commit` | |
| `reset(ref, to)` | move `ref` to a commit (or restore point) | |
| `delete(ref)` | delete a branch | |

A ref is a branch name or `branch@commit` on backends with commits. On a
backend without commits, a commit ref falls back to the branch head with a
warning and the row is flagged `commit_ref_fallback`.

`merge()` and `rebase()` take `on_conflict`: `"ours"` (default), `"theirs"`,
or a callable `resolve(db, conflicts)` that the backend calls on the
half-merged working set with a session and a list of
`{"table": name, "rows": [...]}` entries (Dolt's base/our/their columns).
The callable resolves conflicts with SQL through `db.sql()`; anything it
leaves is resolved as "ours". Schema conflicts (a dropped table modified on
the other side, two indexes on the same columns) cannot be resolved in place:
the backend aborts and the verb is recorded as FAILED with the reason.

Every verb returns an `OpResult` with `status` OK, UNSUPPORTED (the backend
cannot perform the operation; zero latency) or FAILED (the backend tried and
errored). Nothing raises unless `raise_on_error=True`;
`result.raise_for_status()` raises on demand.

**An unsupported operation ends the run.** The benchmark drivers treat the
first UNSUPPORTED result as the end of the workflow: the macrobenchmark
cancels every worker and records the run as `unsupported: <VERB>: <reason>`
(see [macrobench.md](macrobench.md#running)). A FAILED operation does not
stop a run; it is recorded and the invariant checks report its effect. Code
that calls the API directly gets the UNSUPPORTED result back and should stop
the same way (`if result.unsupported: ...`).

`DBToolSuite.capabilities()` reports which verbs a backend supports, and
`DBToolSuite.implementation()` how each one is realised: `native` (the
system's own primitive), `composed` (several native primitives driven by
the adapter) or `simulated` (the adapter builds it from SQL). With `DBFORK_NATIVE_ONLY=1` every simulated operation
is reported as unsupported. The macrobench e2e stats carry both under
`capabilities` and `implementation`.

## exec()

```python
results = db.exec(script, refs=["feature"], mode="per_ref", label="eval")
```

runs `script` on each ref in turn (`mode="per_ref"`), or once with every ref
addressable from one session (`mode="multi"`, on adapters that declare
`SUPPORTS_MULTI_REF_EXEC`; others record it as UNSUPPORTED). A script is a list of SQL statements
(strings or `(sql, params)`), Python source, or a callable. Python source
runs with `db` (the session), `params` and `suite` in scope and may define
`run(db)`, whose return value becomes the result's `value`:

```python
script = """
def run(db):
    db.sql("UPDATE stock SET s_quantity = s_quantity - %s WHERE s_i_id = %s", (params["qty"], params["item"]))
    return db.sql("SELECT count(*) FROM stock WHERE s_quantity = 0")[0][0]
"""
```

Every statement issued through `db.sql()` autocommits and is recorded as its
own row (READ/INSERT/UPDATE/DELETE_ROWS/DDL) whose `num_keys_touched` is the rows affected (writes) or returned (reads), unless the script set it with `db.record_keys_touched(n)`. Per ref, `exec()` also records a CONNECT
row when it had to switch the connection to that branch, and one EXEC row
with the script's total latency and storage delta. `exec_async()` runs the
same thing on a connection pool (`open_async_pool(size)`) for throughput
measurement; its scripts must be a SQL list, a coroutine function, or source
defining `async def run(db)`.

## Result rows

Each parquet row carries `op_name`, `status`, `ref`, `label`, `exec_id`
(grouping the rows of one `exec()`), `latency`, `disk_size_before/after`,
`sql_query` (the statement, or the script text on EXEC rows),
`error_message`, `commit_ref_fallback` and the driver context (table, step,
thread). Run summaries include `workflow_supported`, the list of
`unsupported_ops`, per-op status counts and the backend's capabilities.
