"""dblib.agent_tools over the sqlite fake backend: tool definitions, every
tool, the guardrails, argument checking and JSON-safe results."""

import datetime as dt
import decimal
import json
import time

import jsonschema
import pytest

from dblib import result_pb2 as rslt
from dblib.agent_tools import AgentTools, Guardrails, jsonable
from dblib.db_api import Ref
from tests.fake_backend import FakeSuite


class MergingFake(FakeSuite):
    """The fake plus a naive merge (the source's tables replace the
    target's), enough to exercise merge guardrails."""

    def _merge_impl(self, into: Ref, source: Ref, message: str, on_conflict="ours"):
        self.branches[into.branch] = self.branches[source.branch].copy()
        if self._current_ref and self._current_ref.branch == into.branch:
            self.conn = self.branches[into.branch]
        return {"merged": True}


def seeded(cls=FakeSuite):
    s = cls()
    s.exec(["CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
            "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')"], timed=False)
    return s


def count(suite, ref="main"):
    return suite.exec(["SELECT count(*) FROM t"], refs=[ref], timed=False)[0].rows[0][0]


@pytest.fixture
def suite():
    return seeded()


@pytest.fixture
def tools(suite):
    return AgentTools(suite)


# --- definitions -----------------------------------------------------------

@pytest.mark.parametrize("style", ["anthropic", "openai", "mcp"])
def test_definitions_are_valid_json_schema(tools, style):
    defs = tools.definitions(style)
    json.dumps(defs)
    for d in defs:
        schema = d["function"]["parameters"] if style == "openai" else d["input_schema"]
        jsonschema.Draft202012Validator.check_schema(schema)
        assert schema["additionalProperties"] is False
        name = d["function"]["name"] if style == "openai" else d["name"]
        assert len((d["function"] if style == "openai" else d)["description"]) > 20, name


def test_definitions_accept_what_the_tools_accept(tools):
    schemas = {d["name"]: d["input_schema"] for d in tools.definitions("anthropic")}
    jsonschema.validate({"ref": "main", "statements": ["SELECT 1"], "atomic": True}, schemas["sql"])
    jsonschema.validate({"name": "fix_1", "from_ref": "main@abc"}, schemas["branch"])
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate({"name": "Bad-Name"}, schemas["branch"])
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate({"ref": "main", "statements": []}, schemas["sql"])


def test_unsupported_operations_are_hidden(tools):
    names = tools.tool_names()
    # the fake has no merge, rebase or revert
    assert {"merge", "rebase", "revert"}.isdisjoint(names)
    assert {"sql", "branch", "commit", "log", "diff", "reset", "delete",
            "list_branches", "capabilities"} <= set(names)
    r = tools.call("merge", {"into": "main", "source": "x"})
    assert r["status"] == "INVALID" and "not supported" in r["error"]


def test_unsupported_shown_when_asked(suite):
    tools = AgentTools(suite, hide_unsupported=False)
    assert "merge" in tools.tool_names()
    tools.call("branch", {"name": "feat"})
    r = tools.call("merge", {"into": "main", "source": "feat"})
    assert r["status"] == "UNSUPPORTED"
    assert "merge is not supported by the fake backend" in r["error"]


def test_mcp_annotations(tools):
    ann = {d["name"]: d["annotations"] for d in tools.definitions("mcp")}
    assert ann["diff"]["readOnlyHint"] and not ann["diff"]["destructiveHint"]
    assert ann["delete"]["destructiveHint"] and not ann["delete"]["readOnlyHint"]


# --- the tools -------------------------------------------------------------

def test_branch_change_diff_reset_delete_flow(tools, suite):
    lb = tools.call("list_branches")
    assert lb == {**lb, "status": "OK"}
    assert lb["result"] == {"branches": ["main"], "default_branch": "main", "protected": ["main"]}
    assert list(lb)[0] == "status"

    assert tools.call("branch", {"name": "feat"})["result"] == {"branch": "feat", "from": "main"}
    c0 = tools.call("commit", {"ref": "feat", "message": "start"})["result"]["commit"]
    r = tools.call("sql", {"ref": "feat", "statements": [
        "UPDATE t SET v = 'z' WHERE id >= 2", "INSERT INTO t VALUES (4, 'd')", "SELECT id, v FROM t ORDER BY id"]})
    assert r["status"] == "OK"
    upd, ins, sel = r["result"]["statements"]
    assert upd == {"sql": "UPDATE t SET v = 'z' WHERE id >= 2", "status": "OK", "rowcount": 2}
    assert ins["rowcount"] == 1 and "rows" not in ins
    assert sel["columns"] == ["id", "v"] and sel["rows"] == [[1, "a"], [2, "z"], [3, "z"], [4, "d"]]
    assert sel["truncated"] is False and sel["rowcount"] == 4
    assert count(suite, "main") == 3                       # main untouched

    tools.call("commit", {"ref": "feat", "message": "edit"})
    d = tools.call("diff", {"from_ref": "main", "to_ref": "feat"})["result"]
    assert (d["rows_added"], d["rows_modified"], d["rows_deleted"]) == (1, 2, 0)
    log = tools.call("log", {"ref": "feat", "limit": 5})["result"]
    assert [e["message"] for e in log] == ["edit", "start"]

    # read the branch as of the first commit
    r = tools.call("sql", {"ref": f"feat@{c0}", "statements": ["SELECT count(*) FROM t"]})
    assert r["result"]["statements"][0]["rows"] == [[3]]

    assert tools.call("reset", {"ref": "feat", "to": c0})["status"] == "OK"
    assert count(suite, "feat") == 3
    assert tools.call("delete", {"ref": "feat"})["result"] == {"deleted": "feat"}
    assert tools.call("list_branches")["result"]["branches"] == ["main"]


def test_capabilities(tools):
    c = tools.call("capabilities")["result"]
    assert c["backend"] == "fake" and c["sql_dialect"] == "postgres"
    assert c["branch_operations"]["merge"] == "unsupported"
    assert c["branch_operations"]["branch"] == "native"
    assert c["guardrails"]["protected"] == ["main"] and c["guardrails"]["read_only"] is False


def test_every_call_is_recorded_with_a_label(tools, suite):
    tools.call("branch", {"name": "feat"})
    tools.call("sql", {"ref": "feat", "statements": ["SELECT 1"]})
    rows = suite.result_collector.results
    labels = {(r.op_type, r.label) for r in rows}
    assert (rslt.OpType.BRANCH, "agent:branch") in labels
    assert (rslt.OpType.EXEC, "agent:sql") in labels
    assert (rslt.OpType.READ, "agent:sql") in labels


def test_results_are_json(tools):
    tools.call("branch", {"name": "feat"})
    for name, args in [("list_branches", {}), ("capabilities", {}), ("log", {"ref": "main"}),
                       ("diff", {"from_ref": "main", "to_ref": "feat"}),
                       ("sql", {"ref": "feat", "statements": ["SELECT * FROM t"]})]:
        out = tools.call(name, args)
        assert json.loads(json.dumps(out)) == out, name
        assert json.loads(tools.call_json(name, args))["status"] == "OK"


# --- sql specifics -----------------------------------------------------------

def test_sql_stops_at_first_failure_and_reports_it(tools, suite):
    tools.call("branch", {"name": "feat"})
    r = tools.call("sql", {"ref": "feat", "statements": [
        "INSERT INTO t VALUES (5, 'e')", "SELECT * FROM nope", "INSERT INTO t VALUES (6, 'f')"]})
    assert r["status"] == "FAILED" and "nope" in r["error"]
    first, bad = r["result"]["statements"]
    assert first["status"] == "OK" and bad["status"] == "FAILED" and "nope" in bad["error"]
    assert r["result"]["not_run"] == 1
    assert count(suite, "feat") == 4                       # each statement autocommits


def test_atomic_rolls_back_the_whole_list(tools, suite):
    tools.call("branch", {"name": "feat"})
    r = tools.call("sql", {"ref": "feat", "atomic": True, "statements": [
        "INSERT INTO t VALUES (5, 'e')", "INSERT INTO t VALUES (1, 'duplicate')"]})
    assert r["status"] == "FAILED" and r["result"]["rolled_back"] is True
    assert [s["sql"] for s in r["result"]["statements"]] == [
        "INSERT INTO t VALUES (5, 'e')", "INSERT INTO t VALUES (1, 'duplicate')"]
    assert count(suite, "feat") == 3
    ok = tools.call("sql", {"ref": "feat", "atomic": True, "statements": ["INSERT INTO t VALUES (5, 'e')"]})
    assert ok["status"] == "OK" and ok["result"]["rolled_back"] is False
    assert count(suite, "feat") == 4


def test_rows_and_cells_are_bounded(suite):
    tools = AgentTools(suite, guardrails=Guardrails(max_rows=2, max_cell_chars=5))
    tools.call("branch", {"name": "feat"})
    tools.call("sql", {"ref": "feat", "statements": ["UPDATE t SET v = 'abcdefghij' WHERE id = 1"]})
    st = tools.call("sql", {"ref": "feat", "statements": ["SELECT id, v FROM t ORDER BY id"]})["result"]["statements"][0]
    assert st["rows"] == [[1, "abcde...[5 more chars]"], [2, "b"]]
    assert st["truncated"] is True and st["rowcount"] == 3


def test_a_refused_statement_runs_nothing(tools, suite):
    tools.call("branch", {"name": "feat"})
    r = tools.call("sql", {"ref": "feat", "statements": [
        "DELETE FROM t", "SELECT dolt_checkout('main')"]})
    assert r["status"] == "REFUSED" and "statement 2" in r["error"] and "Nothing was run" in r["error"]
    assert count(suite, "feat") == 3


# --- guardrails ------------------------------------------------------------

def test_protected_branch_takes_no_direct_writes(tools, suite):
    before = len(suite.result_collector.results)
    for stmts in (["DELETE FROM t"], ["SELECT 1", "UPDATE t SET v = 'x'"], ["CREATE TABLE u (x int)"]):
        r = tools.call("sql", {"ref": "main", "statements": stmts})
        assert r["status"] == "REFUSED" and "protected" in r["error"]
    assert count(suite, "main") == 3
    assert len(suite.result_collector.results) == before          # nothing ran
    assert tools.call("sql", {"ref": "main", "statements": ["SELECT * FROM t"]})["status"] == "OK"
    c = tools.call("commit", {"ref": "main", "message": "snapshot"})
    assert c["status"] == "OK"                                     # a commit changes no data
    for name, args in [("reset", {"ref": "main", "to": c["result"]["commit"]}),
                       ("delete", {"ref": "main"})]:
        r = tools.call(name, args)
        assert r["status"] == "REFUSED" and "protected" in r["error"], name


def test_merge_into_protected(suite):
    s = seeded(MergingFake)
    tools = AgentTools(s)
    tools.call("branch", {"name": "feat"})
    tools.call("sql", {"ref": "feat", "statements": ["DELETE FROM t WHERE id = 3"]})
    r = tools.call("merge", {"into": "main", "source": "feat"})
    assert r["status"] == "OK" and count(s, "main") == 2

    strict = AgentTools(seeded(MergingFake), guardrails=Guardrails(allow_merge_into_protected=False))
    strict.call("branch", {"name": "feat"})
    r = strict.call("merge", {"into": "main", "source": "feat"})
    assert r["status"] == "REFUSED" and "protected" in r["error"]


def test_custom_and_no_protection(suite):
    tools = AgentTools(suite, guardrails=Guardrails(protected=("prod",)))
    assert tools.call("sql", {"ref": "main", "statements": ["DELETE FROM t WHERE id = 1"]})["status"] == "OK"
    tools.call("branch", {"name": "prod"})
    assert tools.call("sql", {"ref": "prod", "statements": ["DELETE FROM t"]})["status"] == "REFUSED"
    open_ = AgentTools(seeded(), guardrails=Guardrails(protected=()))
    assert open_.call("sql", {"ref": "main", "statements": ["DELETE FROM t"]})["status"] == "OK"


def test_commit_refs_are_read_only(tools, suite):
    tools.call("branch", {"name": "feat"})
    c = tools.call("commit", {"ref": "feat", "message": "x"})["result"]["commit"]
    for name, args in [("sql", {"ref": f"feat@{c}", "statements": ["DELETE FROM t"]}),
                       ("commit", {"ref": f"feat@{c}", "message": "m"}),
                       ("delete", {"ref": f"feat@{c}"})]:
        r = tools.call(name, args)
        assert r["status"] == "REFUSED" and "commit ref" in r["error"], name
    assert tools.call("branch", {"name": "from_commit", "from_ref": f"feat@{c}"})["status"] == "OK"


def test_read_only(suite):
    tools = AgentTools(suite, guardrails=Guardrails(read_only=True, protected=()))
    assert set(tools.tool_names()) == {"list_branches", "capabilities", "log", "diff", "sql"}
    assert "Read-only" in {d["name"]: d for d in tools.definitions("anthropic")}["sql"]["description"]
    r = tools.call("sql", {"ref": "main", "statements": ["UPDATE t SET v = 'x'"]})
    assert r["status"] == "REFUSED" and "read-only" in r["error"]
    assert tools.call("sql", {"ref": "main", "statements": ["SELECT count(*) FROM t"]})["status"] == "OK"
    r = tools.call("branch", {"name": "feat"})
    assert r["status"] == "INVALID" and "read-only" in r["error"]
    assert count(suite) == 3 and suite.list_branches() == ["main"]


def test_max_live_branches(suite):
    tools = AgentTools(suite, guardrails=Guardrails(max_live_branches=2))
    assert tools.call("branch", {"name": "b1"})["status"] == "OK"
    r = tools.call("branch", {"name": "b2"})
    assert r["status"] == "REFUSED" and "limit is 2" in r["error"]
    tools.call("delete", {"ref": "b1"})
    assert tools.call("branch", {"name": "b2"})["status"] == "OK"


class SlowFake(FakeSuite):
    def _branch_impl(self, name, from_ref):
        deadline = time.time() + 5
        while time.time() < deadline:
            self._check_cancel()
            time.sleep(0.01)
        super()._branch_impl(name, from_ref)


def test_timeout_cancels_a_long_call():
    tools = AgentTools(SlowFake(), guardrails=Guardrails(timeout_s=0.2))
    t0 = time.time()
    r = tools.call("branch", {"name": "slow"})
    assert time.time() - t0 < 2
    assert r["status"] == "FAILED" and r["error"].startswith("timed out after 0.2s")


# --- arguments ---------------------------------------------------------------

BAD_ARGS = [
    ("bogus", {}, "unknown tool"),
    ("branch", {}, "missing"),
    ("branch", {"name": "feat", "extra": 1}, "unknown argument"),
    ("branch", {"name": "Feat"}, "not a valid branch name"),
    ("branch", {"name": "x'; DROP TABLE t; --"}, "not a valid branch name"),
    ("branch", {"name": "feat", "from_ref": "main@ab'c"}, "not a commit id"),
    ("branch", {"name": 5}, "must be a string"),
    ("sql", {"ref": "main", "statements": "SELECT 1"}, "list of strings"),
    ("sql", {"ref": "main", "statements": []}, "non-empty"),
    ("sql", {"ref": "main", "statements": [1]}, "list of strings"),
    ("sql", {"ref": "main", "statements": ["SELECT 1"] * 51}, "at most 50"),
    ("sql", {"ref": "main", "statements": ["SELECT 1"], "atomic": "yes"}, "true or false"),
    ("sql", {"ref": "../etc", "statements": ["SELECT 1"]}, "not a valid branch name"),
    ("log", {"ref": "main", "limit": 0}, "between 1 and 100"),
    ("log", {"ref": "main", "limit": True}, "integer"),
    ("reset", {"ref": "feat", "to": "abc def"}, "not a commit id"),
    ("reset", {"ref": "feat", "to": "x');--"}, "not a commit id"),
]


@pytest.mark.parametrize("name,args,why", BAD_ARGS, ids=[f"{n}:{w}" for n, _, w in BAD_ARGS])
def test_bad_arguments(tools, name, args, why):
    r = tools.call(name, args)
    assert r["status"] == "INVALID", r
    assert why in r["error"]


def test_non_dict_arguments(tools):
    assert tools.call("list_branches", None)["status"] == "OK"
    assert tools.call("list_branches", ["x"])["status"] == "INVALID"


def test_backend_errors_never_escape(tools, suite, monkeypatch):
    def boom():
        raise ConnectionError("server went away")
    monkeypatch.setattr(suite, "list_branches", boom)
    r = tools.call("list_branches")
    assert r["status"] == "ERROR" and "server went away" in r["error"]
    r = tools.call("branch", {"name": "feat"})            # a failed op is FAILED, not raised
    assert r["status"] in ("OK", "FAILED")
    r = tools.call("branch", {"name": "feat"})
    assert r["status"] == "FAILED" and "exists" in r["error"]


def test_jsonable():
    assert jsonable(decimal.Decimal("12.50")) == "12.50"
    assert jsonable(dt.datetime(2026, 1, 2, 3, 4, 5)) == "2026-01-02T03:04:05"
    assert jsonable(dt.date(2026, 1, 2)) == "2026-01-02"
    assert jsonable(dt.timedelta(seconds=90)) == "0:01:30"
    assert jsonable(b"\x00\x01") == {"base64": "AAE="}
    assert jsonable(memoryview(b"ab")) == {"base64": "YWI="}
    assert jsonable(float("nan")) == "nan"
    assert jsonable({"k": (1, decimal.Decimal("2"))}) == {"k": [1, "2"]}
    assert jsonable("x" * 10, 4) == "xxxx...[6 more chars]"
    assert jsonable(object()).startswith("<object")


class UncancellableFake(FakeSuite):
    def _branch_impl(self, name, from_ref):
        time.sleep(0.4)                   # ignores cancel_current_op
        super()._branch_impl(name, from_ref)


def test_timeout_that_cannot_cancel_says_so():
    r = AgentTools(UncancellableFake(), guardrails=Guardrails(timeout_s=0.1)).call("branch", {"name": "b"})
    assert r["status"] == "OK" and "ran past the 0.1s timeout" in r["note"]


def test_exec_rows_record_the_statements(tools, suite):
    tools.call("branch", {"name": "feat"})
    tools.call("sql", {"ref": "feat", "statements": ["SELECT 1", "SELECT 2"]})
    tools.call("sql", {"ref": "feat", "atomic": True, "statements": ["SELECT 3"]})
    execs = [r.sql_query for r in suite.result_collector.results if r.op_type == rslt.OpType.EXEC]
    assert execs == ["SELECT 1\nSELECT 2", "-- atomic\nSELECT 3"]
