"""The agent tools and the MCP server against live backends.

    DBFORK_AGENT_LIVE=dolt,dolt_mysql,matrixone,seekdb,chronos uv run pytest tests/test_agent_tools_live.py

Each backend gets a scratch database (agentlive_<backend>), dropped at the
end. Connection settings come from the environment as usual.
"""

import asyncio
import json
import os
import sys
import time

import pytest

BACKENDS = [b for b in os.environ.get("DBFORK_AGENT_LIVE", "").split(",") if b]
pytestmark = pytest.mark.skipif(not BACKENDS, reason="set DBFORK_AGENT_LIVE=<backend,...>")

import dblib  # noqa: E402
from dblib.agent_tools import AgentTools, Guardrails  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Engine branching SQL each backend would accept if the guard let it through.
BYPASS = {
    "dolt": "SELECT dolt_checkout('main')",
    "dolt_mysql": "CALL DOLT_CHECKOUT('main')",
    "matrixone": "DATA BRANCH CREATE DATABASE x FROM y",
    "seekdb": "FORK DATABASE x TO y",
    "chronos": "CREATE DATABASE x TEMPLATE y",
    "file_copy": "CREATE DATABASE x TEMPLATE y",
    "neon": "SELECT * FROM dblink('x', 'select 1') AS t(a int)",
}


@pytest.fixture(params=BACKENDS)
def backend(request):
    key = request.param
    handle = dblib.open_backend(key, f"agentlive_{key}", create=True, drop_on_close=True)
    suite = handle.suite()
    main = handle.default_branch
    suite.exec(["CREATE TABLE item (id INT PRIMARY KEY, name VARCHAR(32), price DECIMAL(10,2), added DATE)",
                "INSERT INTO item VALUES (1, 'apple', 1.50, '2026-01-01'), (2, 'pear', 2.25, '2026-01-02'), "
                "(3, 'plum', 0.75, '2026-01-03')"], refs=[main], timed=False)
    assert suite.commit(main, "seed", timed=False).ok
    yield key, handle, suite
    suite.close_connection()
    handle.close()


def ok(r):
    assert r["status"] == "OK", r
    json.dumps(r)
    return r.get("result")


def test_tools_end_to_end(backend):
    key, handle, suite = backend
    main = handle.default_branch
    tools = AgentTools(suite, guardrails=Guardrails(max_rows=2))
    impl = suite.implementation()

    lb = ok(tools.call("list_branches"))
    assert main in lb["branches"] and lb["protected"] == [main]
    caps = ok(tools.call("capabilities"))
    assert caps["backend"] == key

    # real driver types come back JSON-safe
    sel = ok(tools.call("sql", {"ref": main, "statements": [
        "SELECT id, name, price, added FROM item ORDER BY id"]}))["statements"][0]
    assert sel["columns"] == ["id", "name", "price", "added"]
    assert sel["rows"][0][:3] == [1, "apple", "1.50"]
    assert str(sel["rows"][0][3]).startswith("2026-01-01")
    assert sel["rowcount"] == 3 and sel["truncated"] is True and len(sel["rows"]) == 2

    # the guard stops engine branching SQL and writes to the protected branch
    r = tools.call("sql", {"ref": main, "statements": ["SELECT 1", BYPASS.get(key, "USE x")]})
    assert r["status"] == "REFUSED", r
    r = tools.call("sql", {"ref": main, "statements": ["DELETE FROM item"]})
    assert r["status"] == "REFUSED" and "protected" in r["error"]

    # work on a branch
    ok(tools.call("branch", {"name": "fix_prices"}))
    st = ok(tools.call("sql", {"ref": "fix_prices", "statements": [
        "UPDATE item SET price = price * 2 WHERE id <= 2",
        "INSERT INTO item VALUES (4, 'kiwi', 3.00, '2026-01-04')"]}))["statements"]
    assert [s["rowcount"] for s in st] == [2, 1]
    c1 = ok(tools.call("commit", {"ref": "fix_prices", "message": "double prices, add kiwi"}))["commit"]
    assert c1

    # an atomic list that fails leaves nothing behind
    r = tools.call("sql", {"ref": "fix_prices", "atomic": True, "statements": [
        "INSERT INTO item VALUES (5, 'fig', 1.00, '2026-01-05')",
        "INSERT INTO item VALUES (1, 'duplicate', 0, '2026-01-01')"]})
    assert r["status"] == "FAILED" and r["result"]["rolled_back"] is True, r
    n = ok(tools.call("sql", {"ref": "fix_prices", "statements": [
        "SELECT count(*) FROM item WHERE id = 5"]}))["statements"][0]["rows"][0][0]
    assert n == 0

    d = ok(tools.call("diff", {"from_ref": main, "to_ref": "fix_prices"}))
    assert (d["rows_added"], d["rows_modified"]) == (1, 2), d
    log = ok(tools.call("log", {"ref": "fix_prices", "limit": 5}))
    assert isinstance(log, list) and log

    # main is unchanged until the merge
    def main_total():
        return ok(tools.call("sql", {"ref": main, "statements": [
            "SELECT count(*), sum(price) FROM item"]}))["statements"][0]["rows"][0]
    assert main_total()[0] == 3
    if impl["merge"] != "unsupported":
        ok(tools.call("merge", {"into": main, "source": "fix_prices", "message": "ship fix"}))
        total = main_total()
        assert total[0] == 4 and float(total[1]) == pytest.approx(3.0 + 4.5 + 0.75 + 3.0), total

    # undo on the branch: revert the commit, or reset to before it
    if impl["revert"] != "unsupported":
        ok(tools.call("revert", {"ref": "fix_prices", "commit": c1}))
    elif impl["reset"] != "unsupported":
        base = [e for e in ok(tools.call("log", {"ref": main, "limit": 10}))
                if e.get("message") == "seed"] or ok(tools.call("log", {"ref": "fix_prices", "limit": 10}))[-1:]
        ok(tools.call("reset", {"ref": "fix_prices", "to": base[0]["commit_hash"]}))
    if impl["revert"] != "unsupported" or impl["reset"] != "unsupported":
        n = ok(tools.call("sql", {"ref": "fix_prices", "statements": [
            "SELECT count(*) FROM item"]}))["statements"][0]["rows"][0][0]
        assert n == 3
    if impl["reset"] != "unsupported":
        ok(tools.call("reset", {"ref": "fix_prices", "to": c1}))     # back to the doubled prices
        n = ok(tools.call("sql", {"ref": "fix_prices", "statements": [
            "SELECT count(*) FROM item"]}))["statements"][0]["rows"][0][0]
        assert n == 4

    # protected branch can't be reset or deleted; the work branch can be deleted
    assert tools.call("delete", {"ref": main})["status"] == "REFUSED"
    ok(tools.call("delete", {"ref": "fix_prices"}))
    assert "fix_prices" not in ok(tools.call("list_branches"))["branches"]


def test_mcp_stdio_on_backend(backend, tmp_path):
    """The CLI end to end: a second database created by the server itself
    (its setup prints must not reach the protocol), dropped on exit."""
    pytest.importorskip("mcp")
    from mcp import ClientSession
    from mcp.client.stdio import StdioServerParameters, stdio_client
    key = backend[0]
    db = f"agentmcp_{key}"
    params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "dblib.mcp_server", "--backend", key, "--db", db, "--create", "--drop-on-exit",
              "--record", str(tmp_path)],
        cwd=ROOT, env={**os.environ, "PYTHONPATH": ROOT})

    async def go():
        async with stdio_client(params) as (r, w):
            async with ClientSession(r, w) as client:
                await client.initialize()
                main = json.loads((await client.call_tool("list_branches", {})).content[0].text)["result"]["default_branch"]
                calls = [
                    ("branch", {"name": "scratch"}),
                    ("sql", {"ref": "scratch", "statements": [
                        "CREATE TABLE note (id INT PRIMARY KEY, body VARCHAR(64))",
                        "INSERT INTO note VALUES (1, 'hello')", "SELECT body FROM note"]}),
                    ("commit", {"ref": "scratch", "message": "note"}),
                    ("diff", {"from_ref": main, "to_ref": "scratch"}),
                ]
                for name, args in calls:
                    res = await client.call_tool(name, args)
                    out = json.loads(res.content[0].text)
                    assert not res.isError and out["status"] == "OK", (name, out)
                assert out["result"]["schema"]["tables_added"] == ["note"] or out["result"]["schema"] is None
    asyncio.run(go())
    files = list(tmp_path.glob("*.parquet"))
    assert files, "nothing recorded"
    # --drop-on-exit: the server hands the delete to a process of its own,
    # which outlives the client's kill of the server.
    from dblib.connector import load_connector
    connector = load_connector(key)
    deadline = time.time() + 60
    while connector.exists(db) and time.time() < deadline:
        time.sleep(1)
    assert not connector.exists(db), f"{db} still exists after --drop-on-exit"
