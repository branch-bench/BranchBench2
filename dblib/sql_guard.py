"""Classify one SQL statement from an agent as a read or a write, or refuse it.

The agent tool layer (dblib/agent_tools.py) runs agent SQL on the branch the
agent names. This check keeps that SQL inside the branch: it refuses
statements that would switch branches or databases, call an engine's own
branching SQL (``dolt_checkout()``, ``CALL DOLT_MERGE``, ``DATA BRANCH``,
``FORK DATABASE``, ``CREATE DATABASE ... TEMPLATE``), name a table in
another database or branch, change session state, or that the parser does
not recognise. It also tells reads from writes, for read-only mode and
protected branches.

It is a guardrail against mistakes, not a security boundary: a parser can
be fooled, and the database still runs whatever passes. For untrusted
agents, give the backend's connection a database role that can only do
what the agent should do.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Optional

import sqlglot
from sqlglot import exp

# sqlglot dialect per backend (the SQL each backend's connection speaks).
DIALECTS = {
    "dolt": "postgres",        # Doltgres
    "neon": "postgres",
    "file_copy": "postgres",
    "chronos": "postgres",
    "xata": "postgres",
    "terminusdb": "postgres",
    "dolt_mysql": "mysql",
    "seekdb": "mysql",
    "matrixone": "mysql",
    "bauplan": "duckdb",
}

READ, WRITE = "read", "write"

# sqlglot logs a warning for every statement it falls back to Command on.
logging.getLogger("sqlglot").setLevel(logging.ERROR)

_EXPLAIN = re.compile(r"^EXPLAIN\s+(?:(ANALYZE)\s+)?(?:VERBOSE\s+)?(?P<inner>.*)$", re.I | re.S)

# Object kinds an agent may create, alter or drop inside its branch.
# Databases, schemas, functions, procedures, triggers, extensions, roles and
# servers are out: they reach other branches or run code the guard can't see.
_DDL_KINDS = {"TABLE", "INDEX", "VIEW", "SEQUENCE"}

# Dolt functions that only read version data (the table functions behind
# dolt_diff etc.); every other dolt_* function changes branches or commits.
_DOLT_READ_FUNCTIONS = {
    "dolt_diff", "dolt_diff_stat", "dolt_diff_summary", "dolt_log",
    "dolt_patch", "dolt_schema_diff", "dolt_query_diff", "dolt_hashof",
    "dolt_hashof_table", "dolt_hashof_db", "dolt_version", "dolt_merge_base",
    "active_branch",
}
# Functions that act outside the statement's own data: session settings,
# other connections, the server's files, other databases.
_DENIED_FUNCTIONS = {
    "set_config", "pg_terminate_backend", "pg_cancel_backend",
    "pg_reload_conf", "pg_read_file", "pg_read_binary_file", "pg_ls_dir",
    "pg_stat_file", "lo_import", "lo_export", "pg_advisory_lock",
    "pg_switch_wal", "mo_ctl", "load_file", "sys_exec", "sys_eval",
}
_DENIED_PREFIXES = ("dblink",)
# Functions that change state although they appear in a SELECT.
_WRITE_FUNCTIONS = {"nextval", "setval"}

# Schemas a MySQL-dialect statement may name: everything else is another
# database (on Dolt, SeekDB and MatrixOne, another branch).
_MYSQL_SYSTEM_SCHEMAS = {"information_schema"}

_WRITE_NODES = (exp.Insert, exp.Update, exp.Delete, exp.Merge, exp.TruncateTable)
_SESSION_NODES = (exp.Use, exp.Set, exp.Transaction, exp.Commit, exp.Rollback,
                  exp.Grant, exp.Revoke, exp.Pragma, exp.Attach, exp.Detach,
                  exp.Copy, exp.LoadData, exp.Kill, exp.Lock)


@dataclass(frozen=True)
class Verdict:
    kind: Optional[str]          # READ or WRITE; None when refused
    reason: str = ""             # why it was refused

    @property
    def refused(self) -> bool:
        return self.kind is None


def _refuse(reason: str) -> Verdict:
    return Verdict(None, reason)


def _function_name(node: exp.Expression) -> str:
    if isinstance(node, exp.Anonymous):
        return str(node.this).lower()
    return (node.sql_name() or "").lower()


def classify(sql: str, dialect: str = "postgres") -> Verdict:
    """READ, WRITE, or refused (with the reason) for one statement."""
    text = (sql or "").strip().rstrip(";").strip()
    if not text:
        return _refuse("empty statement")
    try:
        trees = [t for t in sqlglot.parse(text, read=dialect) if t is not None]
    except Exception as e:
        return _refuse(f"not a recognised SQL statement ({type(e).__name__}); "
                       "use plain SELECT/INSERT/UPDATE/DELETE/DDL, and the branch tools for versioning")
    if len(trees) != 1:
        return _refuse("one statement per list item (found several separated by ';')")
    tree = trees[0]

    if isinstance(tree, exp.Command):
        head = str(tree.this or "").upper()
        m = _EXPLAIN.match(text) if head == "EXPLAIN" else None
        if m and m.group("inner"):
            inner = classify(m.group("inner"), dialect)
            if inner.refused:
                return inner
            # EXPLAIN ANALYZE runs the statement; a bare EXPLAIN only plans it.
            return Verdict(WRITE if m.group(1) and inner.kind == WRITE else READ)
        if dialect == "mysql" and head == "REPLACE":
            return Verdict(WRITE)
        return _refuse(f"{head or 'this'} statements are not allowed here: "
                       "engine branching, procedures, triggers and admin commands go around the branch tools")
    if isinstance(tree, _SESSION_NODES):
        what = type(tree).__name__.upper()
        hint = (" Pass atomic=true to run the list as one transaction."
                if isinstance(tree, (exp.Transaction, exp.Commit, exp.Rollback)) else "")
        return _refuse(f"{what} is not allowed: it changes session or server state.{hint}")

    for node in tree.walk():
        if isinstance(node, exp.Func):
            name = _function_name(node)
            if name.startswith("dolt_") and name not in _DOLT_READ_FUNCTIONS:
                return _refuse(f"{name}() changes branches or commits; use the branch tools")
            if name in _DENIED_FUNCTIONS or name.startswith(_DENIED_PREFIXES):
                return _refuse(f"{name}() acts outside this branch's data")
        if isinstance(node, exp.Table):
            if node.args.get("catalog"):
                return _refuse(f"table {node.sql(dialect=dialect)} names another database or branch; "
                               "query one branch per call (the ref argument)")
            if dialect == "mysql" and node.db and node.db.lower() not in _MYSQL_SYSTEM_SCHEMAS:
                return _refuse(f"table {node.sql(dialect=dialect)} names another database or branch; "
                               "query one branch per call (the ref argument)")

    if isinstance(tree, (exp.Create, exp.Drop, exp.Alter)):
        kind = str(tree.args.get("kind") or "").upper()
        if kind not in _DDL_KINDS:
            return _refuse(f"{type(tree).__name__.upper()} {kind or '?'} is not allowed; "
                           f"only {', '.join(sorted(_DDL_KINDS))}")
        return Verdict(WRITE)
    if isinstance(tree, exp.Analyze):
        return Verdict(WRITE)                              # refreshes statistics

    writes = any(isinstance(n, _WRITE_NODES) for n in tree.walk())
    if isinstance(tree, _WRITE_NODES) or writes:
        return Verdict(WRITE)
    if isinstance(tree, (exp.Query, exp.Show, exp.Describe, exp.Values)):
        if tree.find(exp.Into) is not None:
            return Verdict(WRITE)                          # SELECT ... INTO creates a table
        if any(isinstance(n, exp.Func) and _function_name(n) in _WRITE_FUNCTIONS for n in tree.walk()):
            return Verdict(WRITE)
        return Verdict(READ)
    return _refuse(f"{type(tree).__name__.upper()} statements are not allowed here")

