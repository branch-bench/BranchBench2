"""dblib.mcp_server: the tools over MCP, in memory and as a stdio subprocess
(with the sqlite fake behind it; tests/test_agent_tools_live.py runs it on
real backends)."""

import asyncio
import json
import os
import sys

import pyarrow.parquet as pq
import pytest

pytest.importorskip("mcp")

from mcp import ClientSession  # noqa: E402
from mcp.client.stdio import StdioServerParameters, stdio_client  # noqa: E402
from mcp.shared.memory import create_connected_server_and_client_session  # noqa: E402

from dblib.agent_tools import AgentTools, Guardrails  # noqa: E402
from dblib.mcp_server import build_server  # noqa: E402
from tests.fake_backend import FakeSuite  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def seeded():
    s = FakeSuite()
    s.exec(["CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
            "INSERT INTO t VALUES (1, 'a'), (2, 'b')"], timed=False)
    return s


def data(result):
    """The tool result as the client sees it: structured content, which
    must match the JSON text content."""
    assert json.loads(result.content[0].text) == result.structuredContent
    return result.structuredContent


def run(coro):
    return asyncio.run(coro)


def test_in_memory_session():
    tools = AgentTools(seeded())
    calls = []

    async def go():
        server = build_server(tools, after_call=lambda name, res: calls.append(name))
        async with create_connected_server_and_client_session(server) as client:
            init = client  # initialised by the helper
            listed = await init.list_tools()
            names = [t.name for t in listed.tools]
            assert names == tools.tool_names()
            by_name = {t.name: t for t in listed.tools}
            assert by_name["sql"].inputSchema["required"] == ["ref", "statements"]
            assert by_name["delete"].annotations.destructiveHint is True
            assert by_name["diff"].annotations.readOnlyHint is True

            r = await client.call_tool("branch", {"name": "feat"})
            assert not r.isError and data(r)["status"] == "OK"
            r = await client.call_tool("sql", {"ref": "feat", "statements": [
                "INSERT INTO t VALUES (3, 'c')", "SELECT v FROM t ORDER BY id"]})
            out = data(r)
            assert not r.isError
            assert out["result"]["statements"][1]["rows"] == [["a"], ["b"], ["c"]]

            # guardrail and argument errors come back as tool errors, not protocol errors
            r = await client.call_tool("sql", {"ref": "main", "statements": ["DELETE FROM t"]})
            assert r.isError and data(r)["status"] == "REFUSED"
            r = await client.call_tool("branch", {"name": "Bad Name"})
            assert r.isError and data(r)["status"] == "INVALID"
            r = await client.call_tool("no_such_tool", {})
            assert r.isError and data(r)["status"] == "INVALID"
            r = await client.call_tool("diff", {"from_ref": "main", "to_ref": "feat"})
            assert data(r)["result"]["rows_added"] == 1
    run(go())
    assert calls == ["branch", "sql", "sql", "branch", "no_such_tool", "diff"]


def test_in_memory_read_only_lists_only_reads():
    tools = AgentTools(seeded(), guardrails=Guardrails(read_only=True))

    async def go():
        async with create_connected_server_and_client_session(build_server(tools)) as client:
            names = {t.name for t in (await client.list_tools()).tools}
            assert names == {"list_branches", "capabilities", "log", "diff", "sql"}
            r = await client.call_tool("sql", {"ref": "main", "statements": ["SELECT count(*) FROM t"]})
            assert data(r)["result"]["statements"][0]["rows"] == [[2]]
    run(go())


def _params(*extra):
    return StdioServerParameters(
        command=sys.executable,
        args=[os.path.join(ROOT, "tests", "mcp_fake_server.py"), "--backend", "dolt", "--db", "x", *extra],
        cwd=ROOT, env={**os.environ, "PYTHONPATH": ROOT})


def test_stdio_subprocess_survives_stdout_noise_and_records(tmp_path):
    async def go():
        async with stdio_client(_params("--record", str(tmp_path), "--run-id", "agent_run")) as (r, w):
            async with ClientSession(r, w) as client:
                init = await client.initialize()
                assert init.serverInfo.name == "branchable-db"
                assert "branch" in (init.instructions or "")
                assert "sql" in {t.name for t in (await client.list_tools()).tools}
                assert data(await client.call_tool("branch", {"name": "feat"}))["status"] == "OK"
                out = data(await client.call_tool("sql", {"ref": "feat", "statements": [
                    "UPDATE t SET v = 'z'", "SELECT * FROM t"]}))
                assert out["result"]["statements"][0]["rowcount"] == 2
                bad = await client.call_tool("sql", {"ref": "feat", "statements": ["CALL dolt_merge('main')"]})
                assert bad.isError
    run(go())
    table = pq.read_table(tmp_path / "agent_run.parquet")
    assert set(table.column("label").to_pylist()) >= {"mcp:branch", "mcp:sql"}
    assert table.num_rows >= 4      # BRANCH, UPDATE, READ, EXEC (+ CONNECT)


def test_stdio_cli_flags():
    async def go():
        params = _params("--read-only", "--max-rows", "1")
        async with stdio_client(params) as (r, w):
            async with ClientSession(r, w) as client:
                await client.initialize()
                names = {t.name for t in (await client.list_tools()).tools}
                assert "branch" not in names and "sql" in names
                out = data(await client.call_tool("sql", {"ref": "main", "statements": ["SELECT * FROM t"]}))
                st = out["result"]["statements"][0]
                assert st["rows"] == [[1, "a"]] and st["truncated"] is True
                caps = data(await client.call_tool("capabilities", {}))["result"]["guardrails"]
                assert caps["read_only"] is True and caps["max_rows"] == 1
    run(go())
