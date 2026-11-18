"""Branch operations and SQL as tools for an LLM agent.

    import dblib
    from dblib.agent_tools import AgentTools

    suite = dblib.open_suite("dolt", "mydb")
    tools = AgentTools(suite)
    tools.definitions("anthropic")      # -> the `tools` list for the Messages API
    tools.call("branch", {"name": "fix_prices"})
    tools.call("sql", {"ref": "fix_prices", "statements": ["UPDATE item SET i_price = i_price * 1.1"]})

Every tool takes and returns JSON-safe dicts, and ``call()`` never raises.
The result's ``status`` is the operation's (OK, FAILED, UNSUPPORTED) or
REFUSED (a guardrail said no) or INVALID (bad arguments, unknown tool); an
``error`` says why. dblib/mcp_server.py serves the same tools over MCP.

Agent SQL is a list of statements run on one ref (dblib.sql_guard checks
each one first); it is never Python. The guardrails (``Guardrails``):

- protected branches (default: the backend's default branch) take no
  direct writes and cannot be reset, reverted, rebased or deleted; they
  change only by merging into them (unless ``allow_merge_into_protected``
  is off);
- ``read_only`` leaves only reads: list_branches, capabilities, log, diff
  and SELECT-like SQL;
- ``max_live_branches`` caps branch creation;
- ``max_rows`` / ``max_cell_chars`` bound what a result sends back;
- ``timeout_s`` cancels a call that runs too long, where the backend can
  cancel a running statement (measured 2026-11-18: Chronos and other
  PostgreSQL servers can; Doltgres ignores the cancel and pymysql has none,
  so Dolt, MatrixOne and SeekDB finish the statement, and the result's
  ``note`` says it ran past the timeout).

These are guardrails against mistakes. For an untrusted agent, also give
the backend connection a database role limited to what the agent may do.
"""

from __future__ import annotations

import base64
import datetime as _dt
import decimal
import json
import re
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Optional

from dblib.db_api import BRANCH_OPS, OpStatus
from dblib.sql_guard import DIALECTS, WRITE, classify

BRANCH_NAME = re.compile(r"^[a-z][a-z0-9_]{0,47}$")
# Commit ids are engine hashes, sequence ids or LSNs (Neon: "0/16B3748").
COMMIT_ID = re.compile(r"^[A-Za-z0-9_.:/\-]{1,128}$")
MAX_STATEMENTS = 50

OK, FAILED, UNSUPPORTED = "OK", "FAILED", "UNSUPPORTED"
REFUSED, INVALID, ERROR = "REFUSED", "INVALID", "ERROR"


@dataclass
class Guardrails:
    protected: Optional[tuple] = None        # None: the default branch only; () protects nothing
    read_only: bool = False
    allow_merge_into_protected: bool = True
    max_live_branches: Optional[int] = None
    max_rows: int = 50
    max_cell_chars: int = 500
    timeout_s: Optional[float] = None


class ToolError(Exception):
    def __init__(self, status: str, message: str):
        super().__init__(message)
        self.status = status


def _invalid(msg: str) -> ToolError:
    return ToolError(INVALID, msg)


def _refused(msg: str) -> ToolError:
    return ToolError(REFUSED, msg)


# ---------------------------------------------------------------------------
# Tool definitions
# ---------------------------------------------------------------------------

_REF_DOC = ('A branch name, or "branch@commit" for that branch as of a commit from log '
            '(read-only).')

# name -> (description, properties, required, kind) where kind is "read",
# "write" (changes a branch) or the branch operation that must be supported.
_TOOLS = {
    "list_branches": (
        "List the database's branches, the default branch and which branches are protected.",
        {}, [], "read"),
    "capabilities": (
        "Which branch operations this backend supports and how (native or simulated by "
        "the adapter), the SQL dialect, and the guardrails in force.",
        {}, [], "read"),
    "sql": (
        "Run SQL statements, in order, on one branch. Each list item is one statement; "
        "each autocommits unless atomic is true. The list stops at the first failing "
        "statement. Returns per statement its columns, rows, rowcount (rows returned "
        "or changed) and whether the rows were truncated. Engine branching SQL, "
        "session settings and other databases are not allowed; use the branch tools.",
        {"ref": {"type": "string", "description": "Branch to run on. " + _REF_DOC},
         "statements": {"type": "array", "items": {"type": "string"}, "minItems": 1,
                        "maxItems": MAX_STATEMENTS,
                        "description": "SQL statements, one per item, no trailing ';' needed."},
         "atomic": {"type": "boolean", "default": False,
                    "description": "Run the list as one transaction: all or nothing."}},
        ["ref", "statements"], "sql"),
    "branch": (
        "Create a branch: an isolated copy of another branch (or of a commit) that "
        "can be changed without affecting the original.",
        {"name": {"type": "string", "pattern": BRANCH_NAME.pattern,
                  "description": "New branch name: lowercase letters, digits, underscores."},
         "from_ref": {"type": "string",
                      "description": "What to branch from (default: the default branch). " + _REF_DOC}},
        ["name"], "branch"),
    "commit": (
        "Record the branch's current state as a commit, to diff against, revert or "
        "reset to later. Returns the commit id.",
        {"ref": {"type": "string", "description": "Branch to commit."},
         "message": {"type": "string", "description": "Commit message."}},
        ["ref", "message"], "commit"),
    "log": (
        "Recent commits on a branch, newest first.",
        {"ref": {"type": "string", "description": "Branch."},
         "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 10}},
        ["ref"], "log"),
    "diff": (
        "What changed from one ref to another: rows added, deleted and modified per "
        "table, and schema changes.",
        {"from_ref": {"type": "string", "description": "Base. " + _REF_DOC},
         "to_ref": {"type": "string", "description": "Compared state. " + _REF_DOC}},
        ["from_ref", "to_ref"], "diff"),
    "merge": (
        "Merge source into a target branch. Conflicting rows are resolved by "
        "on_conflict: 'ours' keeps the target's version, 'theirs' takes the source's. "
        "Schema conflicts abort the merge.",
        {"into": {"type": "string", "description": "Target branch."},
         "source": {"type": "string", "description": "Branch to merge in."},
         "message": {"type": "string", "description": "Merge commit message."},
         "on_conflict": {"type": "string", "enum": ["ours", "theirs"], "default": "ours"}},
        ["into", "source"], "merge"),
    "rebase": (
        "Replay a branch's commits on top of another branch, so it includes that "
        "branch's latest changes. on_conflict as for merge.",
        {"ref": {"type": "string", "description": "Branch to rebase."},
         "onto": {"type": "string", "description": "Branch to rebase onto."},
         "on_conflict": {"type": "string", "enum": ["ours", "theirs"], "default": "ours"}},
        ["ref", "onto"], "rebase"),
    "revert": (
        "Add a commit to a branch that undoes one earlier commit, keeping later changes.",
        {"ref": {"type": "string", "description": "Branch."},
         "commit": {"type": "string", "description": "Commit id (from log) to undo."}},
        ["ref", "commit"], "revert"),
    "reset": (
        "Move a branch back to a commit, discarding every change made after it.",
        {"ref": {"type": "string", "description": "Branch."},
         "to": {"type": "string", "description": "Commit id (from log) to go back to."}},
        ["ref", "to"], "reset"),
    "delete": (
        "Delete a branch and every change on it that was not merged elsewhere.",
        {"ref": {"type": "string", "description": "Branch to delete."}},
        ["ref"], "delete"),
}

_READ_ONLY_TOOLS = {"list_branches", "capabilities", "log", "diff", "sql"}
_DESTRUCTIVE = {"reset", "delete", "revert", "rebase", "merge"}


def _schema(props: dict, required: list) -> dict:
    return {"type": "object", "properties": props, "required": required,
            "additionalProperties": False}


# ---------------------------------------------------------------------------
# JSON-safe values
# ---------------------------------------------------------------------------

def jsonable(value: Any, max_chars: int = 0) -> Any:
    """Plain JSON types for a database value: Decimal as a string (exact),
    dates and times in ISO format, bytes as base64, strings cut at
    ``max_chars`` (0 = no limit) with a marker."""
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        return value if value == value and abs(value) != float("inf") else str(value)
    if isinstance(value, str):
        if max_chars and len(value) > max_chars:
            return value[:max_chars] + f"...[{len(value) - max_chars} more chars]"
        return value
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, (_dt.datetime, _dt.date, _dt.time)):
        return value.isoformat()
    if isinstance(value, _dt.timedelta):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {"base64": base64.b64encode(bytes(value)).decode()}
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, dict):
        return {str(k): jsonable(v, max_chars) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [jsonable(v, max_chars) for v in value]
    return jsonable(str(value), max_chars)


# ---------------------------------------------------------------------------
# The tool layer
# ---------------------------------------------------------------------------

class AgentTools:
    """The branch tools over one DBToolSuite (one connection: calls are
    serialised). Give each concurrent agent its own suite and AgentTools."""

    def __init__(self, suite, *, default_branch: Optional[str] = None,
                 guardrails: Optional[Guardrails] = None,
                 dialect: Optional[str] = None, label: str = "agent",
                 hide_unsupported: bool = True):
        self.suite = suite
        handle = getattr(suite, "backend_handle", None)
        self.default_branch = (default_branch
                               or (handle.default_branch if handle is not None else None)
                               or "main")
        self.guardrails = guardrails or Guardrails()
        if self.guardrails.protected is None:
            self.protected = frozenset({self.default_branch})
        else:
            self.protected = frozenset(self.guardrails.protected)
        self.dialect = dialect or DIALECTS.get(suite.BACKEND_NAME, "postgres")
        self.label = label
        self.hide_unsupported = hide_unsupported
        self._lock = threading.Lock()
        self.calls = 0

    # -- definitions ------------------------------------------------------

    def tool_names(self) -> list:
        impl = self.suite.implementation()
        names = []
        for name, (_, _, _, kind) in _TOOLS.items():
            if self.guardrails.read_only and name not in _READ_ONLY_TOOLS:
                continue
            if (self.hide_unsupported and kind in BRANCH_OPS
                    and impl.get(kind) == "unsupported"):
                continue
            names.append(name)
        return names

    def definitions(self, style: str = "mcp") -> list:
        """Tool definitions: style "anthropic" (Messages API ``tools``),
        "openai" (Chat Completions ``tools``) or "mcp" (name, description,
        input_schema and annotations)."""
        out = []
        for name in self.tool_names():
            desc, props, required, _ = _TOOLS[name]
            props = dict(props)
            if name == "sql" and self.guardrails.read_only:
                desc += " Read-only session: only SELECT-like statements are allowed."
            schema = _schema(props, required)
            if style == "anthropic":
                out.append({"name": name, "description": desc, "input_schema": schema})
            elif style == "openai":
                out.append({"type": "function",
                            "function": {"name": name, "description": desc, "parameters": schema}})
            elif style == "mcp":
                out.append({"name": name, "description": desc, "input_schema": schema,
                            "annotations": {
                                "readOnlyHint": name in ("list_branches", "capabilities", "log", "diff")
                                                or (name == "sql" and self.guardrails.read_only),
                                "destructiveHint": name in _DESTRUCTIVE or name == "sql",
                                "idempotentHint": name in ("list_branches", "capabilities", "log", "diff"),
                                "openWorldHint": False}})
            else:
                raise ValueError(f"unknown definition style {style!r}")
        return out

    # -- dispatch ---------------------------------------------------------

    def call(self, name: str, arguments: Optional[dict] = None) -> dict:
        """Run one tool. Never raises; see the module docstring for the
        result's shape."""
        start = time.perf_counter()
        try:
            if name not in self.tool_names():
                hidden = name in _TOOLS
                raise _invalid(f"unknown tool {name!r}" if not hidden else
                               f"tool {name!r} is not available here "
                               f"({'read-only session' if self.guardrails.read_only else 'not supported by this backend'})")
            args = self._check_args(name, arguments)
            with self._lock:
                self.calls += 1
                out = getattr(self, f"_tool_{name}")(**args)
        except ToolError as e:
            out = {"status": e.status, "error": str(e)}
        except Exception as e:  # a bug or a backend error outside an OpResult
            out = {"status": ERROR, "error": f"{type(e).__name__}: {e}"}
        out = {"status": out.get("status", OK), **out,
               "latency_s": round(time.perf_counter() - start, 6)}
        return out

    def call_json(self, name: str, arguments: Optional[dict] = None) -> str:
        return json.dumps(self.call(name, arguments))

    def _check_args(self, name: str, arguments) -> dict:
        if arguments is None:
            arguments = {}
        if not isinstance(arguments, dict):
            raise _invalid("arguments must be an object")
        _, props, required, _ = _TOOLS[name]
        unknown = set(arguments) - set(props)
        if unknown:
            raise _invalid(f"unknown argument(s) {sorted(unknown)}; expected {sorted(props)}")
        missing = [r for r in required if arguments.get(r) in (None, "")]
        if missing:
            raise _invalid(f"missing argument(s) {missing}")
        for key, val in arguments.items():
            want = props[key].get("type")
            if want == "string" and not isinstance(val, str):
                raise _invalid(f"{key} must be a string")
            if want == "integer" and (not isinstance(val, int) or isinstance(val, bool)):
                raise _invalid(f"{key} must be an integer")
            if want == "boolean" and not isinstance(val, bool):
                raise _invalid(f"{key} must be true or false")
            if want == "array" and not (isinstance(val, list) and val and all(isinstance(v, str) for v in val)):
                raise _invalid(f"{key} must be a non-empty list of strings")
            if "enum" in props[key] and val not in props[key]["enum"]:
                raise _invalid(f"{key} must be one of {props[key]['enum']}")
        return dict(arguments)

    # -- validation helpers -----------------------------------------------

    def _branch_name(self, name: str, arg: str) -> str:
        if name == self.default_branch or BRANCH_NAME.match(name):
            return name
        raise _invalid(f"{arg}: {name!r} is not a valid branch name "
                       "(lowercase letters, digits and underscores, starting with a letter)")

    def _ref(self, value: str, arg: str, *, writable: bool = False) -> str:
        branch, sep, commit = value.partition("@")
        self._branch_name(branch, arg)
        if sep:
            if not COMMIT_ID.match(commit):
                raise _invalid(f"{arg}: {commit!r} is not a commit id")
            if writable:
                raise _refused(f"{arg}: {value!r} is a commit ref, which is read-only; "
                               "branch from it to make changes")
        return value

    def _commit_id(self, value: str, arg: str) -> str:
        if not COMMIT_ID.match(value):
            raise _invalid(f"{arg}: {value!r} is not a commit id (take one from log)")
        return value

    def _not_protected(self, branch: str, what: str) -> None:
        if branch in self.protected:
            raise _refused(f"{branch!r} is protected: {what} is not allowed on it. "
                           "Work on a branch and merge it in.")

    def _writes_allowed(self) -> None:
        if self.guardrails.read_only:
            raise _refused("read-only session")

    # -- running a suite call with a timeout ------------------------------

    def _run(self, fn):
        timeout = self.guardrails.timeout_s
        if not timeout:
            return fn(), False
        fired = threading.Event()

        def cancel():
            fired.set()
            self.suite.cancel_current_op()
        timer = threading.Timer(timeout, cancel)
        timer.daemon = True
        timer.start()
        try:
            return fn(), fired.is_set()
        finally:
            timer.cancel()

    def _op_result(self, res, timed_out: bool, value=None) -> dict:
        status = {OpStatus.OK: OK, OpStatus.FAILED: FAILED,
                  OpStatus.UNSUPPORTED: UNSUPPORTED}.get(res.status, FAILED)
        out = {"status": status}
        if status == OK:
            out["result"] = jsonable(res.value if value is None else value,
                                     self.guardrails.max_cell_chars)
        else:
            out["error"] = res.error or status.lower()
            if timed_out:
                out["error"] = f"timed out after {self.guardrails.timeout_s}s: {out['error']}"
            if status == UNSUPPORTED:
                out["error"] = (f"{res.op} is not supported by the {self.suite.BACKEND_NAME} backend"
                                + (f": {res.error}" if res.error else ""))
        notes = [res.note] if res.note else []
        if timed_out and status == OK:
            notes.append(f"ran past the {self.guardrails.timeout_s}s timeout: "
                         "this backend could not cancel it")
        if notes:
            out["note"] = "; ".join(notes)
        return out

    def _label(self, name: str) -> str:
        return f"{self.label}:{name}"

    # -- tools ------------------------------------------------------------

    def _tool_list_branches(self) -> dict:
        branches = sorted(self.suite.list_branches())
        return {"result": {"branches": branches, "default_branch": self.default_branch,
                           "protected": sorted(self.protected & set(branches))}}

    def _tool_capabilities(self) -> dict:
        g = self.guardrails
        return {"result": {
            "backend": self.suite.BACKEND_NAME,
            "sql_dialect": self.dialect,
            "branch_operations": {k: v for k, v in self.suite.implementation().items()
                                  if k in BRANCH_OPS},
            "commit_refs": self.suite.implementation().get("commit_refs"),
            "guardrails": {"protected": sorted(self.protected), "read_only": g.read_only,
                           "allow_merge_into_protected": g.allow_merge_into_protected,
                           "max_live_branches": g.max_live_branches,
                           "max_rows": g.max_rows, "timeout_s": g.timeout_s},
        }}

    def _tool_sql(self, ref: str, statements: list, atomic: bool = False) -> dict:
        if len(statements) > MAX_STATEMENTS:
            raise _invalid(f"at most {MAX_STATEMENTS} statements per call")
        kinds = []
        for i, sql in enumerate(statements):
            verdict = classify(sql, self.dialect)
            if verdict.refused:
                raise _refused(f"statement {i + 1}: {verdict.reason}. Nothing was run.")
            kinds.append(verdict.kind)
        writes = WRITE in kinds
        self._ref(ref, "ref", writable=writes)
        if writes:
            if self.guardrails.read_only:
                raise _refused("read-only session: only SELECT-like statements are allowed. Nothing was run.")
            self._not_protected(ref, "writing")

        state = {"rolled_back": False}
        _TX = ("BEGIN", "COMMIT", "ROLLBACK")

        def script(db):
            if atomic:
                db.sql("BEGIN", timed=False)
            try:
                for sql in statements:
                    db.sql(sql)
            except BaseException:
                if atomic:
                    try:
                        db.sql("ROLLBACK", timed=False)
                        state["rolled_back"] = True
                    except Exception:
                        pass
                raise
            if atomic:
                db.sql("COMMIT", timed=False)

        # The EXEC row records the script's text: the statements themselves.
        script.__name__ = "-- atomic\n" + "\n".join(statements)
        res, timed_out = self._run(lambda: self.suite.exec(
            script if atomic else list(statements), refs=[ref],
            max_rows=self.guardrails.max_rows, label=self._label("sql"))[0])
        mc = self.guardrails.max_cell_chars
        stmts = []
        for st in (s for s in res.statements if s.sql not in _TX):
            item = {"sql": st.sql, "status": OK if st.ok else FAILED}
            if st.ok:
                item["rowcount"] = st.rowcount
                if st.columns is not None:
                    item["columns"] = list(st.columns)
                    item["rows"] = jsonable(st.rows or [], mc)
                    item["truncated"] = st.truncated
            else:
                item["error"] = st.error
            stmts.append(item)
        out = self._op_result(res, timed_out, value={})
        out.pop("result", None)
        out["result"] = {"ref": ref, "statements": stmts}
        not_run = len(statements) - len(stmts)
        if not_run:
            out["result"]["not_run"] = not_run
        if atomic:
            out["result"]["rolled_back"] = state["rolled_back"]
        if res.connect is not None and not res.connect.ok and "error" in out:
            out["error"] = f"could not switch to {ref!r}: {out['error']}"
        return out

    def _tool_branch(self, name: str, from_ref: Optional[str] = None) -> dict:
        self._writes_allowed()
        if not BRANCH_NAME.match(name):
            raise _invalid(f"name: {name!r} is not a valid branch name "
                           "(lowercase letters, digits and underscores, starting with a letter)")
        src = self._ref(from_ref or self.default_branch, "from_ref")
        cap = self.guardrails.max_live_branches
        if cap is not None:
            live = len(self.suite.list_branches())
            if live >= cap:
                raise _refused(f"{live} branches exist and the limit is {cap}; delete one first")
        res, t = self._run(lambda: self.suite.branch(name, src, label=self._label("branch")))
        return self._op_result(res, t, value={"branch": name, "from": src})

    def _tool_commit(self, ref: str, message: str) -> dict:
        self._writes_allowed()
        self._ref(ref, "ref", writable=True)
        res, t = self._run(lambda: self.suite.commit(ref, message, label=self._label("commit")))
        return self._op_result(res, t, value={"commit": res.value} if res.ok else None)

    def _tool_log(self, ref: str, limit: int = 10) -> dict:
        self._ref(ref, "ref")
        if not 1 <= limit <= 100:
            raise _invalid("limit must be between 1 and 100")
        res, t = self._run(lambda: self.suite.log(ref, limit, label=self._label("log")))
        return self._op_result(res, t)

    def _tool_diff(self, from_ref: str, to_ref: str) -> dict:
        self._ref(from_ref, "from_ref")
        self._ref(to_ref, "to_ref")
        res, t = self._run(lambda: self.suite.diff(from_ref, to_ref, label=self._label("diff")))
        return self._op_result(res, t)

    def _tool_merge(self, into: str, source: str, message: str = "",
                    on_conflict: str = "ours") -> dict:
        self._writes_allowed()
        self._ref(into, "into", writable=True)
        self._ref(source, "source")
        if into in self.protected and not self.guardrails.allow_merge_into_protected:
            self._not_protected(into, "merging")
        res, t = self._run(lambda: self.suite.merge(
            into, source, message or f"merge {source} into {into}",
            on_conflict=on_conflict, label=self._label("merge")))
        return self._op_result(res, t)

    def _tool_rebase(self, ref: str, onto: str, on_conflict: str = "ours") -> dict:
        self._writes_allowed()
        self._ref(ref, "ref", writable=True)
        self._ref(onto, "onto")
        self._not_protected(ref, "rebasing")
        res, t = self._run(lambda: self.suite.rebase(ref, onto, on_conflict=on_conflict,
                                                     label=self._label("rebase")))
        return self._op_result(res, t)

    def _tool_revert(self, ref: str, commit: str) -> dict:
        self._writes_allowed()
        self._ref(ref, "ref", writable=True)
        self._commit_id(commit, "commit")
        self._not_protected(ref, "revert")
        res, t = self._run(lambda: self.suite.revert(ref, commit, label=self._label("revert")))
        return self._op_result(res, t)

    def _tool_reset(self, ref: str, to: str) -> dict:
        self._writes_allowed()
        self._ref(ref, "ref", writable=True)
        self._commit_id(to, "to")
        self._not_protected(ref, "reset")
        res, t = self._run(lambda: self.suite.reset(ref, to, label=self._label("reset")))
        return self._op_result(res, t)

    def _tool_delete(self, ref: str) -> dict:
        self._writes_allowed()
        self._ref(ref, "ref", writable=True)
        self._not_protected(ref, "deleting")
        res, t = self._run(lambda: self.suite.delete(ref, label=self._label("delete")))
        return self._op_result(res, t, value={"deleted": ref})
