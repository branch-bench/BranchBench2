"""MCP server exposing a branchable database's branch operations and SQL.

    uv sync --extra mcp
    uv run python -m dblib.mcp_server --backend dolt --db mydb

serves the tools of dblib/agent_tools.py over stdio, so an MCP client
(Claude Code, Claude Desktop, ...) can branch, query, diff and merge. For
Claude Code:

    claude mcp add branchdb -- uv run --directory /path/to/db-fork \\
        python -m dblib.mcp_server --backend dolt --db mydb

Connection settings come from the process environment, as for every
backend (guides/environment.md). ``--record DIR`` writes every operation the
agent runs to a parquet file in the benchmarks' result format.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import subprocess
import sys
from typing import Optional

from dblib.agent_tools import OK, AgentTools, Guardrails

INSTRUCTIONS = """\
Tools for a branchable SQL database. Branches are isolated copies: make one
with `branch`, change it with `sql`, check the result with `diff` against
the branch you started from, and `merge` it back when it is right. Protected
branches (see `list_branches`) take no direct writes. `commit` records a
state you can `reset` or `revert` to. `capabilities` lists what this
backend supports and the SQL dialect it speaks."""


def build_server(tools: AgentTools, *, name: str = "branchable-db", after_call=None):
    """An mcp low-level Server serving ``tools``. ``after_call(name, result)``
    runs after every tool call (the CLI uses it to write recorded rows)."""
    import anyio
    import mcp.types as types
    from mcp.server.lowlevel import Server

    server = Server(name, instructions=INSTRUCTIONS)

    @server.list_tools()
    async def list_tools() -> list:
        return [types.Tool(name=d["name"], description=d["description"],
                           inputSchema=d["input_schema"],
                           annotations=types.ToolAnnotations(**d["annotations"]))
                for d in tools.definitions("mcp")]

    @server.call_tool(validate_input=False)   # AgentTools checks arguments and says what is wrong
    async def call_tool(tool: str, arguments: Optional[dict]):
        result = await anyio.to_thread.run_sync(tools.call, tool, arguments or {})
        if after_call is not None:
            await anyio.to_thread.run_sync(after_call, tool, result)
        return types.CallToolResult(
            content=[types.TextContent(type="text", text=json.dumps(result))],
            structuredContent=result,
            isError=result.get("status") != OK,
        )

    return server


# Runs a connector's delete by database name (argv: system key, db name).
_DELETE = ("import sys\n"
           "from dblib.connector import load_connector\n"
           "load_connector(sys.argv[1]).delete(sys.argv[2])\n"
           "print(f'{sys.argv[2]}: deleted', file=sys.stderr)\n")


def _delete_detached(key: str, db: str) -> int:
    """Delete ``db`` in a process of its own session. An MCP client closes
    the server's stdin when it disconnects and kills the server's process
    group 2 s later (SIGKILL 2 s after that), which is sooner than some
    connectors' delete-and-verify (SeekDB: about 4 s). Returns the pid."""
    proc = subprocess.Popen([sys.executable, "-c", _DELETE, key, db], start_new_session=True,
                            stdin=subprocess.DEVNULL, stdout=sys.stderr, stderr=sys.stderr)
    return proc.pid


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m dblib.mcp_server", description=__doc__.split("\n\n")[0])
    p.add_argument("--backend", required=True,
                   help="a package in dblib/systems/ (dblib.connector_names()), e.g. dolt, "
                        "dolt_mysql, seekdb, matrixone, neon, file_copy, chronos, bauplan")
    p.add_argument("--db", required=True, help="database name")
    p.add_argument("--create", action="store_true",
                   help="(re)create the database first (drops one of that name)")
    p.add_argument("--sql-dump", help="with --create: SQL file to load")
    p.add_argument("--drop-on-exit", action="store_true", help="drop the database when the server stops")
    p.add_argument("--neon-project-id", help="Neon: project of an existing database")
    p.add_argument("--read-only", action="store_true", help="reads only: no SQL writes, no branch changes")
    p.add_argument("--protect", action="append", metavar="BRANCH",
                   help="protected branch (repeatable; default: the default branch)")
    p.add_argument("--no-protect", action="store_true", help="protect no branch")
    p.add_argument("--no-merge-into-protected", action="store_true",
                   help="protected branches take no merges either")
    p.add_argument("--max-live-branches", type=int)
    p.add_argument("--max-rows", type=int, default=50, help="rows returned per statement (default 50)")
    p.add_argument("--timeout", type=float, help="seconds before a call is cancelled")
    p.add_argument("--record", metavar="DIR", help="write every operation to DIR/<run id>.parquet")
    p.add_argument("--run-id", help="name of the recorded run (default mcp_<backend>_<db>)")
    p.add_argument("--name", default="branchable-db", help="server name shown to the client")
    return p


def main(argv=None) -> int:
    args = _parser().parse_args(argv)
    if args.sql_dump and not args.create:
        _parser().error("--sql-dump needs --create")

    # stdout carries the MCP protocol. Backends and the result collector
    # print progress there, so keep a private copy of the real stdout for
    # the protocol and point fd 1 (and sys.stdout) at stderr.
    proto_fd = os.dup(sys.stdout.fileno())
    os.dup2(sys.stderr.fileno(), sys.stdout.fileno())
    sys.stdout = sys.stderr

    import anyio
    import dblib
    from dblib.result_collector import ResultCollector
    from mcp.server.stdio import stdio_server

    log = lambda msg: print(msg, file=sys.stderr, flush=True)  # noqa: E731
    run_id = args.run_id or f"mcp_{args.backend}_{args.db}"
    collector = ResultCollector(run_id=run_id, output_dir=args.record or "/tmp/run_stats")
    backend = dblib.open_backend(args.backend, args.db, create=args.create,
                                 sql_dump=args.sql_dump, neon_project_id=args.neon_project_id or "",
                                 drop_on_close=args.drop_on_exit)
    suite = None
    try:
        suite = backend.suite(collector)
        guard = Guardrails(
            protected=() if args.no_protect else (tuple(args.protect) if args.protect else None),
            read_only=args.read_only,
            allow_merge_into_protected=not args.no_merge_into_protected,
            max_live_branches=args.max_live_branches, max_rows=args.max_rows,
            timeout_s=args.timeout)
        tools = AgentTools(suite, default_branch=backend.default_branch, guardrails=guard,
                           label="mcp")

        def after_call(_tool, _result):
            if args.record:
                if collector.total_rows:
                    collector.write_to_parquet(f"{run_id}.parquet", append=True)
            else:
                collector.reset()       # nothing keeps the rows: don't let them pile up

        server = build_server(tools, name=args.name, after_call=after_call)
        log(f"{args.name}: {args.backend}/{args.db}, tools: {', '.join(tools.tool_names())}")

        async def serve():
            stdin = anyio.wrap_file(io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8"))
            stdout = anyio.wrap_file(io.TextIOWrapper(os.fdopen(proto_fd, "wb", buffering=0),
                                                      encoding="utf-8", write_through=True))
            async with stdio_server(stdin=stdin, stdout=stdout) as (read, write):
                await server.run(read, write, server.create_initialization_options())

        anyio.run(serve)
    finally:
        if suite is not None:
            try:
                suite.close_connection()
            except Exception:
                pass
        if args.drop_on_exit and hasattr(backend, "connector"):
            pid = _delete_detached(backend.key, backend.db_name)
            log(f"{args.name}: deleting {backend.db_name} in the background (pid {pid})")
        else:
            backend.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
