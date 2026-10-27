"""Schema for the macrobenchmark: the CH-benCHmark base plus per-scenario
extension tables (the .sql files next to this module).

All DDL is written in the dialect subset Postgres and MySQL share so one file
serves Doltgres, Neon, Xata, plain Postgres and Dolt/SeekDB over MySQL. The
statements are applied one at a time through exec() so a failing statement
is reported on its own and does not hide the rest.
"""

import re
from importlib.resources import files
from pathlib import Path

from macrobench import task_pb2 as tp

SCHEMA_DIR = Path(__file__).resolve().parent
CH_SCHEMA_PATH = files("db_setup") / "ch_benchmark_schema.sql"

EXTENSION_FILES = {
    tp.SchemaExtension.RL_ENV_EXT: "rl_env.sql",
    tp.SchemaExtension.CONTEXT_MGMT_EXT: "context_mgmt.sql",
    tp.SchemaExtension.MULTI_AGENT_EXT: "multi_agent.sql",
    tp.SchemaExtension.DEV_AGENT_EXT: "dev_agent.sql",
    tp.SchemaExtension.OPS_AGENT_EXT: "ops_agent.sql",
    tp.SchemaExtension.DATA_AGENT_EXT: "data_agent.sql",
}

# Workload.scenario oneof field name -> the extension that scenario needs.
SCENARIO_EXTENSIONS = {
    "rl_env": tp.SchemaExtension.RL_ENV_EXT,
    "context_mgmt": tp.SchemaExtension.CONTEXT_MGMT_EXT,
    "multi_agent": tp.SchemaExtension.MULTI_AGENT_EXT,
    "dev_agent": tp.SchemaExtension.DEV_AGENT_EXT,
    "ops_agent": tp.SchemaExtension.OPS_AGENT_EXT,
    "data_agent": tp.SchemaExtension.DATA_AGENT_EXT,
}

# CH tables in dependency order (seeding and FK creation follow it).
CH_TABLES = [
    "region", "nation", "supplier", "warehouse", "district", "item",
    "customer", "history", "stock", "orders", "new_order", "order_line",
]


def split_statements(sql: str) -> list:
    """Split a SQL file into statements, dropping comments and blanks."""
    lines = []
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        lines.append(line)
    text = "\n".join(lines)
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)
    return [s.strip() for s in text.split(";") if s.strip()]


def is_foreign_key(stmt: str) -> bool:
    return "FOREIGN KEY" in stmt.upper()


def is_drop(stmt: str) -> bool:
    return stmt.upper().startswith("DROP ")


def extensions_for(schema: tp.SchemaConfig, scenario_key: str) -> list:
    """Extensions to create: the configured ones, else the scenario's own."""
    if schema.extensions:
        return [e for e in schema.extensions if e != tp.SchemaExtension.EXT_UNSPECIFIED]
    ext = SCENARIO_EXTENSIONS.get(scenario_key)
    return [ext] if ext is not None else []


def ddl_statements(schema: tp.SchemaConfig, scenario_key: str) -> list:
    """Every DDL statement to run, in order, for this schema config."""
    stmts = []
    if schema.base == tp.BaseSchema.CH_BENCH:
        base = split_statements(CH_SCHEMA_PATH.read_text())
        # The database is fresh; DROPs are noise on backends that log them.
        base = [s for s in base if not is_drop(s)]
        if schema.skip_foreign_keys:
            base = [s for s in base if not is_foreign_key(s)]
        stmts.extend(base)
    for ext in extensions_for(schema, scenario_key):
        path = SCHEMA_DIR / EXTENSION_FILES[ext]
        stmts.extend(split_statements(path.read_text()))
    if schema.custom_ddl_path:
        stmts.extend(split_statements(Path(schema.custom_ddl_path).read_text()))
    return stmts


def apply_schema(suite, ref, schema: tp.SchemaConfig, scenario_key: str,
                 log=print) -> dict:
    """Create the schema on ``ref`` (untimed). Returns
    {"statements": n, "failed": [(stmt_head, error), ...]}."""
    stmts = ddl_statements(schema, scenario_key)
    failed = []
    for stmt in stmts:
        res = suite.exec([stmt], refs=[ref], timed=False)[0]
        if not res.ok:
            failed.append((stmt[:60], res.error))
            log(f"  schema statement failed: {stmt[:60]}... -> {res.error}")
    return {"statements": len(stmts), "failed": failed}


def table_names(schema: tp.SchemaConfig, scenario_key: str) -> list:
    """Names of every table the schema creates, in creation order."""
    names = []
    for stmt in ddl_statements(schema, scenario_key):
        m = re.match(r"CREATE\s+TABLE\s+(\w+)", stmt, re.IGNORECASE)
        if m:
            names.append(m.group(1))
    return names
