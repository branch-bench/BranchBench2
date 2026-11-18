# Running an agent on a branchable database

The branch operations and SQL are available as tools an LLM agent can call:
- **from Python:** `dblib.agent_tools.AgentTools`, whose tool definitions
  plug into any function-calling API;
- **from any MCP client:** `dblib.mcp_server` (Claude Code, Claude Desktop
  and others).

Both work on every backend, and every call can be recorded in the
benchmarks' result format.

## Quick start

**1. Install, and start a database server** (Doltgres here; `dolt`,
`matrixone`, `seekdb` and `chronos-postgres` work the same way):

```bash
uv sync --extra mcp                     # add --extra chronos for Chronos
docker compose up -d --wait doltgres
```

Connection settings come from the process environment, not from files. The
compose servers work with the defaults; Chronos also needs
`export PGSQL_USER=postgres`, and Neon `export NEON_API_KEY_ORG=...`
([environment.md](environment.md)).

**2. Create and seed the database.** Seed it through the suite, not the
agent tools: the tools refuse writes to the default branch.

```python
import dblib

backend = dblib.open_backend("dolt", "agent_demo", create=True)   # or sql_dump="data.sql"
suite = backend.suite()
suite.exec(["CREATE TABLE item (id INT PRIMARY KEY, name VARCHAR(32), price DECIMAL(10,2))",
            "INSERT INTO item VALUES (1, 'apple', 1.50), (2, 'pear', 0.80)"], refs=["main"])
suite.commit("main", "seed")      # commit before branching: Dolt branches from the last commit
```

**3a. Give your own agent the tools:**

```python
from dblib.agent_tools import AgentTools, Guardrails

tools = AgentTools(suite, guardrails=Guardrails(max_live_branches=10))
tools.definitions("anthropic")    # or "openai": the `tools` list for the API
tools.call_json(name, arguments)  # run one tool call, JSON result for the model
```

[From Python](#from-python) has a complete agent loop. Delete the database
afterwards with `backend.close(drop=True)`.

**3b. Or connect an MCP client** to the database from step 2. For Claude
Code:

```bash
claude mcp add branchdb -- uv run --directory /path/to/repo \
    python -m dblib.mcp_server --backend dolt --db agent_demo
```

For a client configured with JSON (Claude Desktop and others):

```json
{"mcpServers": {"branchdb": {
  "command": "uv",
  "args": ["run", "--directory", "/path/to/repo", "python", "-m", "dblib.mcp_server",
           "--backend", "dolt", "--db", "agent_demo", "--record", "/path/to/runs"]}}}
```

Instead of step 2, the server can make a throwaway database for each
session: `--create --sql-dump data.sql --drop-on-exit`.

**Before the agent starts:**
- **It works on branches.** The default branch is protected: the agent
  creates a branch, changes it, checks it with `diff`, and `merge`s it
  back. Turn this off with `Guardrails(protected=())` or `--no-protect`.
- **Its SQL stays on one branch.** Engine branching SQL, `USE`, `SET`,
  `BEGIN` and other databases are refused; `atomic: true` runs a list as
  one transaction.
- **A timeout stops a running statement only on Chronos;** elsewhere the
  statement finishes and the result says it ran past the timeout.

## The tools

| Tool | What it does |
|---|---|
| `list_branches` | Branches, the default branch, which branches are protected |
| `capabilities` | Supported branch operations (native or simulated), SQL dialect, guardrails |
| `sql` | Run a list of SQL statements on one branch (or `branch@commit`, read-only) |
| `branch` | Create a branch from a branch or a commit |
| `commit` | Record a branch's state; returns the commit id |
| `log` | Recent commits on a branch |
| `diff` | Rows added, deleted and modified per table, and schema changes, between two refs |
| `merge` | Merge one branch into another (`on_conflict`: `ours` / `theirs`) |
| `rebase` | Replay a branch's commits onto another |
| `revert` | Undo one commit with a new commit |
| `reset` | Move a branch back to a commit, discarding later changes |
| `delete` | Delete a branch |

**Tools the agent doesn't see:** operations the backend doesn't support,
and in a read-only session everything but the reads.

**Results:** every result is JSON with a `status` and, unless the call
failed, a `result`.
- `status` is `OK`, `FAILED`, `UNSUPPORTED`, `REFUSED` (a guardrail) or
  `INVALID` (bad arguments).
- A failed call has an `error` saying why.

**SQL results:** for every statement, `sql` returns:
- `columns` and `rows` (at most `max_rows`, default 50; `truncated` says
  if there were more);
- `rowcount`: rows returned, or rows a write changed.

Values come back JSON-safe: decimals as exact strings, dates in ISO
format, bytes as base64.

**How `sql` runs statements:**
- Each statement autocommits. Pass `atomic: true` to run the list as one
  transaction.
- The list stops at the first failing statement.
- Statements are SQL only, one per list item. Text is never run as
  Python.

## Guardrails

- **Protected branches.** By default, the default branch is protected. It
  takes no direct writes and can't be reset, reverted, rebased or deleted;
  it changes only by merges into it. The agent works on its own branch and
  merges when it's done. You can protect other branches, none, or forbid
  merges too.
- **SQL that stays on its branch.** `dblib/sql_guard.py` refuses:
  - the engine's own branching SQL (`dolt_checkout()`, `CALL DOLT_*`,
    MatrixOne `DATA BRANCH`, SeekDB `FORK DATABASE`,
    `CREATE DATABASE ... TEMPLATE`);
  - tables in other databases or branches;
  - session and server state (`USE`, `SET`, `BEGIN`, `COPY`, ...);
  - functions, procedures and triggers;
  - anything it can't parse.

  If any statement in a list is refused, nothing in the list runs.
- **Read-only sessions** allow only reads.
- **Limits:** a cap on live branches, a row and cell limit on results, and
  a per-call timeout. A timeout can only cancel a running statement where
  the backend supports that: Chronos and other PostgreSQL servers do;
  Doltgres, Dolt (MySQL), MatrixOne and SeekDB finish the statement and the
  result says it ran past the timeout.

These guardrails catch mistakes; they are not a security boundary. For an
agent you don't trust, also connect with a database role that can only do
what the agent should.

## From Python

```python
import anthropic
import dblib
from dblib.agent_tools import AgentTools, Guardrails

suite = dblib.open_suite("dolt", "mydb")
tools = AgentTools(suite, guardrails=Guardrails(max_live_branches=10))

client = anthropic.Anthropic()
messages = [{"role": "user", "content": "Raise every fruit under 1.00 by 10% and merge it."}]
while True:
    reply = client.messages.create(model="claude-opus-5-5", max_tokens=4096,
                                   tools=tools.definitions("anthropic"), messages=messages)
    messages.append({"role": "assistant", "content": reply.content})
    calls = [b for b in reply.content if b.type == "tool_use"]
    if not calls:
        break
    messages.append({"role": "user", "content": [
        {"type": "tool_result", "tool_use_id": c.id, "content": tools.call_json(c.name, c.input)}
        for c in calls]})
```

- **OpenAI-style APIs:** `tools.definitions("openai")` gives the tool
  format they expect.
- **One suite per agent:** a suite is one connection, so give each
  concurrent agent its own suite and `AgentTools`.
- **Recording:** pass a `ResultCollector` to `open_suite` / `suite()` to
  record the session. Every call is labelled `agent:<tool>`.

## Over MCP

```bash
uv sync --extra mcp                     # plus --extra chronos for Chronos
uv run python -m dblib.mcp_server --backend dolt --db mydb
```

**Connecting a client.** In Claude Code:

```bash
claude mcp add branchdb -- uv run --directory /path/to/db-fork \
    python -m dblib.mcp_server --backend dolt_mysql --db mydb
```

Other clients take the same command in their MCP server configuration.

**Options:**

| Option | Effect |
|---|---|
| `--create` | Create the database first (drops one of that name); add `--sql-dump FILE` to load a dump |
| `--drop-on-exit` | Drop the database when the server stops |
| `--read-only` | Reads only |
| `--protect BRANCH`, `--no-protect`, `--no-merge-into-protected` | Choose protected branches |
| `--max-live-branches N`, `--max-rows N`, `--timeout S` | Limits |
| `--record DIR` | Write every operation to `DIR/<run id>.parquet` (as the benchmarks do; `--run-id` names it) |

Connection settings come from the process environment, as for the
benchmarks ([environment.md](environment.md)). The server logs to stderr.

## Tests

- `tests/test_sql_guard.py`, `tests/test_agent_tools.py` and
  `tests/test_mcp_server.py` run without a database server. The last one
  includes the CLI over stdio.
- `tests/test_agent_tools_live.py` runs every tool and the MCP server on
  real backends:

  ```bash
  DBFORK_AGENT_LIVE=dolt,dolt_mysql,matrixone,seekdb,chronos uv run pytest tests/test_agent_tools_live.py
  ```
