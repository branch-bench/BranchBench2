# Embedding the engine

The seam between an application and the engine is three frozen dataclasses in
`dblib/target.py`:

| Type | Who creates it | What it holds |
|------|----------------|---------------|
| `PreparedTarget` | a connector or embedding application | `root_ref` (the branch a run starts on), `suite_type` (the adapter class, for its capability report) and `open_suite`, a callable that opens one adapter instance for one worker |
| `SuiteRequest` | the engine, once per worker | the shared `ResultCollector`, the `initial_ref` to connect to, the `worker_index`, the refs created during untimed setup and whether to measure storage per operation |
| `ExecutionState` | the microbench runner | the working ref and the setup refs after the untimed setup phase |

The CLI and embedding applications use the same three execution entry points:

```python
from dblib.connector import load_connector
from macrobench import runner as macro_runner
connector = load_connector("dolt")
database = connector.provision("branchbench_example")
try:
    target = connector.prepare_target(database)
    e2e = macro_runner.run(
        ["--config", "macrobench/configs/rl_env_mini.textproto", "--outdir", "run_stats/example"],
        target=target, system_name="Doltgres",
    )
finally:
    connector.delete_database(database)
```

The other entry points are `runner2.run(task, target, system_name, output_dir)`
and `driver.run(argv, target=target, system_name=system_name)`. The caller owns
the database lifetime when it uses these functions. Built-in connectors
provide the reusable provider operations. A custom caller can still supply
its own `PreparedTarget` without changing the runners.

Adapters read provider connection settings from the process environment.
`DBFORK_NATIVE_ONLY` reports simulated verbs as unsupported.
`MACRO_SPILL_ROWS` controls when the macrobench collector spills rows to parquet.

Packaged resources: the macrobench presets
(`importlib.resources.files("macrobench") / "configs"`), the scenario schemas
(`macrobench/schema/*.sql`), the CH-benCHmark schema and seed dump
(`importlib.resources.files("db_setup")`), and the generated `*_pb2.py`
protobuf modules. `build_protos.py` compiles the `.proto` files with
`grpc_tools.protoc` during every build and fails the build when it cannot.
