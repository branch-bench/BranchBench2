"""Every scenario runs on the fake backend (branch, commit, log, diff,
reset and delete only). Merge, rebase and revert come back UNSUPPORTED,
so these runs check the support contract: a scenario that needs none of
them completes with every check passed; one that does ends at its first
unsupported verb with UnsupportedOperationAbort, the verb recorded in the
support summary and named in the context, and no failed check."""

import pytest
from google.protobuf import text_format

from dblib import result_collector as rc
from macrobench import schema, task_pb2 as tp
from macrobench.datagen.ch import CHScale, seed_ch
from macrobench.scenarios import (InvariantRecorder, ScenarioContext, UnsupportedOperationAbort,
                                  create_scenario)
from tests.fake_backend import FakeSuite

MINI_SCHEMA = ("schema { base: CH_BENCH scale_factor: 1 items: 60 customers_per_district: 10 "
               "orders_per_district: 10 suppliers: 5 skip_foreign_keys: true }")

CONFIGS = {
    "rl_env": MINI_SCHEMA + """
        workload { branch_ops { commit_interval: 1 }
                   data_ops { statements_per_step: 3 rows_per_write: 5 write_fraction: 0.5 ddl_fraction: 0.5 }
                   rl_env { tasks: 2 group_size: 2 step_budget: 2 historical_fork_prob: 1.0 } }""",
    "context_mgmt": """schema { base: NO_BASE }
        workload { branch_ops { commit_interval: 1 retention_steps: 2 }
                   data_ops { statements_per_step: 3 rows_per_write: 4 write_fraction: 0.5 }
                   context_mgmt { compaction_interval: 2 fanout: 3 compaction_cycles: 2 rejected_cycle: 1 } }""",
    "multi_agent": """schema { base: NO_BASE }
        workload { branch_ops { commit_interval: 1 }
                   data_ops { statements_per_step: 3 rows_per_write: 2 write_fraction: 0.5 }
                   multi_agent { agents: 2 rounds: 1 key_overlap: 0.5 spine_updates_per_round: 2 } }""",
    "dev_agent": MINI_SCHEMA + """
        workload { branch_ops { commit_interval: 1 }
                   data_ops { statements_per_step: 2 rows_per_write: 5 write_fraction: 0.8 }
                   dev_agent { dev_branches: 1 concurrent_branches: 1 dev_phase_steps: 2 review_phase_steps: 1
                               review_modification_prob: 1.0 rebases_per_branch: 1 spine_migration_interval: 1 key_overlap: 0.5 } }""",
    "ops_agent": MINI_SCHEMA + """
        workload { branch_ops { commit_interval: 1 }
                   data_ops { statements_per_step: 2 rows_per_write: 2 }
                   ops_agent { spine_commits: 5 fault_commit_offset: 2 investigation_branches: 2 history_offset: 4 } }""",
    # The history window holds two commits, fewer than the four requested branches.
    "ops_agent/short_window": MINI_SCHEMA + """
        workload { branch_ops { commit_interval: 1 }
                   data_ops { statements_per_step: 2 rows_per_write: 2 }
                   ops_agent { spine_commits: 5 fault_commit_offset: 2 investigation_branches: 4 history_offset: 2 } }""",
    # The last of three incidents has two commits, so it investigates one branch, not two.
    "ops_agent/short_last_incident": MINI_SCHEMA + """
        workload { branch_ops { commit_interval: 1 }
                   data_ops { statements_per_step: 2 rows_per_write: 2 }
                   ops_agent { spine_commits: 10 commits_per_incident: 4 fault_commit_offset: 1 investigation_branches: 6
                               history_offset: 3 } }""",
    "data_agent": MINI_SCHEMA + """
        workload { branch_ops { commit_interval: 1 }
                   data_ops { statements_per_step: 2 rows_per_write: 2 write_fraction: 0.8 }
                   data_agent { batches: 2 concurrent_batches: 1 batch_rows: 4 days_back: 2 steps_per_batch: 2 resets_per_batch: 1 } }""",
}
# The first incident's one-commit history window lies after its bad deploy, so the
# reset cannot restore the spine and the second incident is skipped.
SKIPPED_INCIDENT = MINI_SCHEMA + """
    workload { branch_ops { commit_interval: 1 }
               data_ops { statements_per_step: 2 rows_per_write: 2 }
               ops_agent { spine_commits: 12 commits_per_incident: 6 fault_commit_offset: 5 investigation_branches: 4
                           history_offset: 1 } }"""

NEEDS_MERGE = {"context_mgmt", "multi_agent", "dev_agent", "data_agent"}


def _prepared(scenario_key: str, text: str):
    """One scenario config on a seeded fake backend, ready to run."""
    cfg = tp.MacroBenchConfig()
    text_format.Parse('run_id: "t" seed: 7 ' + text, cfg)
    collector = rc.ResultCollector()
    suite = FakeSuite(collector)
    inv = InvariantRecorder()
    ctx = ScenarioContext(config=cfg, suite_factory=lambda: suite, collector=collector,
                          spine="main", invariants=inv, log=lambda m: None)
    scenario = create_scenario(ctx)
    scenario.validate()
    stats = schema.apply_schema(suite, "main", cfg.schema, scenario_key, log=lambda m: None)
    assert stats["failed"] == []
    if cfg.schema.base == tp.BaseSchema.CH_BENCH:
        seed_ch(suite, "main", CHScale.from_config(cfg.schema), log=lambda m: None)
    scenario.seed(suite)
    suite.commit("main", "seed", timed=False)
    return ctx, scenario, suite, inv, collector


@pytest.mark.parametrize("key", sorted(CONFIGS))
def test_scenario_runs_to_completion_on_fake_backend(key):
    scenario_key = key.partition("/")[0]  # "ops_agent/short_window" is one more ops_agent config
    ctx, scenario, suite, inv, collector = _prepared(scenario_key, CONFIGS[key])

    if scenario_key in NEEDS_MERGE:
        with pytest.raises(UnsupportedOperationAbort):
            scenario.run()
        support = collector.support_summary()
        summary = inv.summary()
        assert support["workflow_supported"] is False
        assert any(op.startswith(("MERGE", "REBASE", "REVERT")) for op in support["unsupported_ops"])
        assert ctx.unsupported_abort.split(":")[0] in ("MERGE", "REBASE", "REVERT"), ctx.unsupported_abort
        assert ctx.stop_event.is_set()
        # A scenario that stops early never records more work than it announced.
        work = ctx.work.summary()
        assert work["failed"] == 0 and work["succeeded"] + work["skipped"] <= scenario.total_units(), work
    else:
        scenario.run()  # must not raise
        support = collector.support_summary()
        summary = inv.summary()
        assert support["workflow_supported"] is True, support
        assert ctx.unsupported_abort == ""
        assert summary["passed"] >= 1
        # Every branch the scenario made was deleted again.
        assert suite.list_branches() == ["main"], suite.list_branches()
        # A completed scenario records exactly the work units it announced.
        work = ctx.work.summary()
        assert (work["succeeded"], work["failed"], work["skipped"]) == (scenario.total_units(), 0, 0), work
    assert summary["failed"] == 0, [r for r in summary["results"] if r["passed"] is False]


def test_ops_agent_counts_a_skipped_incident_like_one_that_ran():
    ctx, scenario, suite, inv, _ = _prepared("ops_agent", SKIPPED_INCIDENT)
    scenario.run()
    work = ctx.work.summary()
    assert work["skipped"] > 0 and work["failed"] == 0, work
    assert work["succeeded"] + work["skipped"] == scenario.total_units(), work
    assert inv.summary()["failed"] == 0
    assert suite.list_branches() == ["main"], suite.list_branches()
