# Database API

## Opening a backend

`dblib.open_suite()` and `dblib.open_backend()` (`dblib/backends.py`) open
a backend's database in one call, for code that drives it through the API
directly: an agent, or another benchmark's harness. They use the same
connectors as the benchmark commands (`dblib/systems/<key>/connector.py`);
[embedding.md](embedding.md) covers handing a prepared database to the
benchmark runners instead.

```python
import dblib

# One suite (one connection) on an existing database:
suite = dblib.open_suite("dolt", "mydb")
suite.branch("experiment")
suite.exec(["UPDATE stock SET s_quantity = s_quantity - 1 WHERE s_i_id = 7"], refs=["experiment"])
print(suite.diff("main", "experiment").value)
suite.close_connection()

# Create a fresh database once and give each worker its own suite:
with dblib.open_backend("matrixone", "bench", create=True, drop_on_close=True) as backend:
    suites = [backend.suite() for _ in range(4)]     # one connection each
    ...                                              # the database is deleted on exit
```

- **Backends** are named by their package in `dblib/systems/`
  (`dblib.connector_names()` lists them): `dolt`, `dolt_mysql`, `seekdb`,
  `matrixone`, `neon`, `file_copy`, `chronos`, `bauplan`, plus the
  experimental `xata` and `terminusdb`.
- **Connection settings** come from the process environment
  ([environment.md](environment.md)).
- **`create=True` (re)creates the database,** deleting one of that name
  first, and can load a SQL dump (`sql_dump=`). On Neon it creates a
  project; attaching to an existing Neon database needs `neon_project_id`.
- **A suite holds one connection** that switches branches, so give every
  concurrent worker or agent its own.
- **Results:** pass a `ResultCollector` (`backend.suite(collector)`) to
  record every operation, as the benchmarks do.
- **Imports:** a system's package is imported only when its backend is opened.

## The interface

`dblib/db_api.py` defines `DBToolSuite`, the adapter base class every
database system implements. Built-in adapters live in
`dblib/systems/<system>/adapter.py`. An
adapter overrides the protected hooks it supports (`_branch_impl`,
`_commit_impl`, ..., `_storage_bytes`, `_connect_impl`, `_flush_impl`); the
public branch operations wrap them with timing, storage measurement and result recording.
The runners reach adapters through the `PreparedTarget` factory.

## Branch operations

| Operation | Meaning | Returns |
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
the backend aborts and the branch operation is recorded as FAILED with the reason.

Every branch operation returns an `OpResult` with `status` OK, UNSUPPORTED (the backend
cannot perform the operation; zero latency) or FAILED (the backend tried and
errored). Nothing raises unless `raise_on_error=True`;
`result.raise_for_status()` raises on demand.

**An unsupported operation ends the run.** The benchmark drivers treat the
first UNSUPPORTED result as the end of the workflow: the macrobenchmark
cancels every worker and records the run as `unsupported: <OPERATION>: <reason>`
(see [macrobench.md](macrobench.md#running)). A FAILED operation does not
stop a run; it is recorded and the invariant checks report its effect. Code
that calls the API directly gets the UNSUPPORTED result back and should stop
the same way (`if result.unsupported: ...`).

`DBToolSuite.capabilities()` reports which branch operations a backend supports, and
`DBToolSuite.implementation()` how each one is realised: `native` (the
system's own primitive), `composed` (several native primitives driven by
the adapter) or `simulated` (the adapter builds it from SQL). With `DBFORK_NATIVE_ONLY=1` every simulated operation
is reported as unsupported. The macrobench e2e stats carry both under
`capabilities` and `implementation`.

## exec()

```python
results = suite.exec(script, refs=["feature"], mode="per_ref", label="eval", max_rows=100)
```

`exec()` runs `script` on each ref in turn (`mode="per_ref"`), or once with
every ref addressable from one session (`mode="multi"`, on adapters that
declare `SUPPORTS_MULTI_REF_EXEC`: Dolt, SeekDB, MatrixOne, file_copy over
postgres_fdw and Chronos over views on its interval tables; others record
it as UNSUPPORTED).

A script is one of:

- **A list of SQL statements:** each a string, or `(sql, params)` with `%s`
  placeholders. This is what an agent or another benchmark's harness should
  pass.
- **A callable taking the session:** for driver code that needs logic
  between statements. It calls `db.sql(query, params)`, which returns the
  rows, and its return value becomes the result's `value`. Values reach it
  by closure:

  ```python
  def restock(db):
      db.sql("UPDATE stock SET s_quantity = s_quantity + %s WHERE s_i_id = %s", (qty, item))
      return db.sql("SELECT count(*) FROM stock WHERE s_quantity = 0")[0][0]
  suite.exec(restock, refs=["feature"])
  ```

Text is never run as Python: passing a string raises `TypeError` before
anything runs.

**Results.** `exec()` returns one `ExecResult` per ref.
- **`.statements`:** one `StatementResult` per statement, with:
  - `.columns`: column names, or `None` for statements without a result set;
  - `.rows`;
  - `.rowcount`: rows returned, or rows a write affected;
  - `.truncated`: true when `max_rows` cut the rows kept;
  - `.sql`, `.status` and `.error`.
- **`.rows` / `.columns`:** shortcuts for the last statement that returned
  rows.
- **`max_rows`** only bounds what the result keeps. A callable's `db.sql()`
  always gets every row.

**Recording.** Every statement autocommits and is recorded as its own row
(READ/INSERT/UPDATE/DELETE_ROWS/DDL). Its `num_keys_touched` is the rows
affected (writes) or returned (reads), unless the script set it with
`db.record_keys_touched(n)`. Per ref, `exec()` also records:
- a CONNECT row when it had to switch the connection to that branch;
- one EXEC row with the script's total latency and storage delta.

`exec_async()` does the same on a connection pool (`open_async_pool(size)`)
for throughput measurement. Its scripts are a SQL list or a coroutine
function.

## Result rows

Each parquet row carries `op_name`, `status`, `ref`, `label`, `exec_id`
(grouping the rows of one `exec()`), `latency`, `disk_size_before/after`,
`sql_query` (the statement, or the script text on EXEC rows),
`error_message`, `commit_ref_fallback` and the driver context (table, step,
thread). Run summaries include `workflow_supported`, the list of
`unsupported_ops`, per-op status counts and the backend's capabilities.
