"""Scenario parameters: parsing the config's string map into each
scenario's Params dataclass, the deprecated per-scenario messages, and the
registry metadata the scripts and plots read."""

from dataclasses import fields
from pathlib import Path

import pytest
from google.protobuf import text_format

from macrobench import task_pb2 as tp
from macrobench.scenarios import SCENARIOS, listed_keys, load_params
from macrobench.scenarios.params import params_to_dict, parse_params

CONFIG_DIR = Path(__file__).resolve().parent.parent / "macrobench" / "configs"


def _cfg(text: str) -> tp.MacroBenchConfig:
    cfg = tp.MacroBenchConfig()
    text_format.Parse('run_id: "t" database_setup { db_name: "t" } ' + text, cfg)
    return cfg


def test_values_are_parsed_by_declared_type():
    p = parse_params("multi_agent", SCENARIOS["multi_agent"].Params,
                     {"agents": "12", "key_overlap": "0.25", "shared_scratch_table": "true"})
    assert (p.agents, p.key_overlap, p.shared_scratch_table) == (12, 0.25, True)
    assert p.rounds == 0                                   # left out: the default


@pytest.mark.parametrize("values, message", [
    ({"agnets": "4"}, "unknown parameter(s) agnets"),
    ({"agents": "four"}, "'agents' expects an integer"),
    ({"agents": "1.5"}, "'agents' expects an integer"),
    ({"key_overlap": "high"}, "'key_overlap' expects a number"),
    ({"shared_scratch_table": "maybe"}, "expects true/false"),
    ({"resolution_policy": "LLM_ASSISTED"}, "must be one of FIXED_WORKFLOW"),
])
def test_bad_values_name_the_scenario_and_parameter(values, message):
    with pytest.raises(ValueError) as e:
        parse_params("multi_agent", SCENARIOS["multi_agent"].Params, values)
    assert "scenario multi_agent" in str(e.value) and message in str(e.value)


def test_legacy_message_and_new_form_give_the_same_params():
    old = _cfg('workload { multi_agent { agents: 4 rounds: 2 key_overlap: 0.5 '
               'resolution_policy: FIXED_WORKFLOW spine_updates_per_round: 10 } }')
    new = _cfg('workload { scenario: "multi_agent" params { key: "agents" value: "4" } '
               'params { key: "rounds" value: "2" } params { key: "key_overlap" value: "0.5" } '
               'params { key: "spine_updates_per_round" value: "10" } }')
    assert load_params(old) == load_params(new)


def test_setting_both_forms_or_neither_is_an_error():
    with pytest.raises(ValueError, match="both"):
        load_params(_cfg('workload { scenario: "rl_env" rl_env { tasks: 1 } }'))
    with pytest.raises(ValueError, match="not set"):
        load_params(_cfg("workload { }"))
    with pytest.raises(ValueError, match="unknown scenario 'nope'"):
        load_params(_cfg('workload { scenario: "nope" }'))


@pytest.mark.parametrize("path", sorted(CONFIG_DIR.glob("*.textproto")), ids=lambda p: p.name)
def test_every_shipped_config_loads(path):
    cfg = tp.MacroBenchConfig()
    text_format.Parse(path.read_text(), cfg)
    key, params = load_params(cfg)
    assert params_to_dict(params)
    # A scenario with its own message keeps it in the shipped presets: the
    # leaderboard reads the scenario's name as a key of the workload and
    # digests the presets (TODO at Workload.legacy_scenario in task.proto).
    legacy = {f.name for f in tp.Workload.DESCRIPTOR.oneofs_by_name["legacy_scenario"].fields}
    if key in legacy:
        assert cfg.workload.WhichOneof("legacy_scenario") == key, path.name
        assert not cfg.workload.scenario and not cfg.workload.params, path.name
    else:
        assert cfg.workload.scenario == key, path.name


@pytest.mark.parametrize("key", listed_keys(include_examples=True))
def test_registry_metadata(key):
    cls = SCENARIOS[key]
    assert cls.name and cls.label and cls.plot_label and cls.title
    assert cls.group in ("branch", "data")
    for f in fields(cls.Params):
        if f.metadata.get("intensity"):
            assert f.type in (int, "int"), f"{key}.{f.name}: only int parameters can scale"
    for label, text in cls.expected_failures:
        assert label and text
