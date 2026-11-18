"""Workload intensity multipliers.

``Workload.branch_intensity`` and ``Workload.data_intensity`` are applied
once, before the run starts, by :func:`apply_intensity`. They multiply the
explicit knobs in place so the scenarios never see them and the e2e stats
record the effective values.

branch_intensity scales the number of branch operations a scenario issues:

* ``branch_ops.commit_interval`` is divided (more commits per step).
* Scenario counts of branches, forks, rounds, rebases, investigation
  points and S6's resets per batch (``BRANCH_FIELDS``). S2's rejections follow from its cycles
  (one per ``cycles_per_rejection`` cycles) and S5's incidents from its
  spine commits (one per ``commits_per_incident``), so they scale too.

data_intensity scales how much SQL runs on each branch:

* ``data_ops.statements_per_step``, ``rows_per_write`` and ``spine_clients``.
* Scenario per-branch step and row counts (``DATA_FIELDS``).

Threads (``worker_threads``, ``concurrent_*``), probabilities, fractions and
caps such as ``max_live_branches`` are left alone, because they bound the
run rather than size it. Scaled ints are rounded and never drop below 1;
a few fields have hard caps the scenarios enforce (S2 fanout <= 10, S4
rebases_per_branch <= 2, S1 group_size <= 32 with rollouts_per_round <= group_size)
and S5 keeps ``fault_commit_offset`` inside
``spine_commits``.
"""

from __future__ import annotations

from macrobench import task_pb2 as tp

# Per-scenario int fields that grow with branch_intensity.
BRANCH_FIELDS = {
    "rl_env": ("tasks", "group_size"),   # rollouts_per_round is concurrency, not size
    "context_mgmt": ("fanout", "compaction_cycles"),
    "multi_agent": ("agents", "rounds"),
    "dev_agent": ("dev_branches", "rebases_per_branch"),
    "ops_agent": ("spine_commits", "investigation_branches"),   # per-incident window/offset fixed
    "data_agent": ("batches", "resets_per_batch"),
}

# Per-scenario int fields that grow with data_intensity.
DATA_FIELDS = {
    "rl_env": ("step_budget",),
    "context_mgmt": ("compaction_interval",),
    "multi_agent": ("spine_updates_per_round",),
    "dev_agent": ("dev_phase_steps", "review_phase_steps", "spine_commit_every_txns"),
    "ops_agent": ("commit_every_txns",),
    "data_agent": ("batch_rows", "steps_per_batch"),
}

DATA_OPS_FIELDS = ("statements_per_step", "rows_per_write", "spine_clients")

# Hard caps the scenarios validate against.
CAPS = {
    ("context_mgmt", "fanout"): 10,
    ("dev_agent", "rebases_per_branch"): 2,
    # A GRPO group: more rollouts per task than this is not a realistic
    # group size, so branch intensity grows the number of tasks instead.
    ("rl_env", "group_size"): 32,
}


def _scale(value: int, factor: float, cap: int | None = None) -> int:
    if value <= 0:
        return value  # 0 keeps its "default/disabled" meaning
    out = max(1, int(round(value * factor)))
    if cap is not None:
        out = min(out, cap)
    return out


def _scale_fields(msg, fields, factor, key, changed):
    for name in fields:
        before = getattr(msg, name)
        after = _scale(before, factor, CAPS.get((key, name)))
        if after != before:
            setattr(msg, name, after)
            changed[f"{key}.{name}"] = (before, after)


def apply_intensity(workload: tp.Workload) -> dict:
    """Scale ``workload`` in place; return {field: (before, after)}."""
    key = workload.WhichOneof("scenario")
    bi = workload.branch_intensity or 1.0
    di = workload.data_intensity or 1.0
    if bi <= 0 or di <= 0:
        raise ValueError("branch_intensity and data_intensity must be > 0")
    changed: dict = {}
    if key is None or (bi == 1.0 and di == 1.0):
        return changed
    params = getattr(workload, key)

    if bi != 1.0:
        ci = workload.branch_ops.commit_interval
        if ci > 1:
            after = max(1, int(round(ci / bi)))
            if after != ci:
                workload.branch_ops.commit_interval = after
                changed["branch_ops.commit_interval"] = (ci, after)
        _scale_fields(params, BRANCH_FIELDS[key], bi, key, changed)
        if key == "rl_env":
            # Rounds never exceed the (capped) group.
            p = params
            if p.rollouts_per_round > p.group_size:
                changed["rl_env.rollouts_per_round"] = (changed.get("rl_env.rollouts_per_round", (p.rollouts_per_round,))[0], p.group_size)
                p.rollouts_per_round = p.group_size
        if key == "ops_agent":
            # The fault offset is per incident and not scaled; keep it
            # strictly inside an incident (or the whole spine).
            p = params
            limit = p.commits_per_incident or p.spine_commits
            off = min(p.fault_commit_offset, max(1, limit - 1))
            if off != p.fault_commit_offset:
                changed["ops_agent.fault_commit_offset"] = (p.fault_commit_offset, off)
                p.fault_commit_offset = off

    if di != 1.0:
        _scale_fields(workload.data_ops, DATA_OPS_FIELDS, di, "data_ops", changed)
        _scale_fields(params, DATA_FIELDS[key], di, key, changed)

    return changed
