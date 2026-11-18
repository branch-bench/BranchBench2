"""Workload intensity multipliers.

``Workload.branch_intensity`` and ``Workload.data_intensity`` are applied
once, before the run starts, by :func:`apply_intensity`. They multiply the
explicit knobs in place so the scenarios never see them and the e2e stats
record the effective values.

branch_intensity scales the number of branch operations a scenario issues:

* ``branch_ops.commit_interval`` is divided (more commits per step).
* The scenario parameters tagged ``intensity="branch"`` (counts of
  branches, forks, rounds, rebases, investigation points). S2's rejections
  follow from its cycles (one per ``cycles_per_rejection`` cycles) and S5's
  incidents from its spine commits (one per ``commits_per_incident``), so
  they scale too.

data_intensity scales how much SQL runs on each branch:

* ``data_ops.statements_per_step``, ``rows_per_write`` and ``spine_clients``.
* The scenario parameters tagged ``intensity="data"`` (per-branch step and
  row counts).

A scenario tags its parameters in its ``Params`` dataclass
(scenarios/params.py: ``param(..., intensity="branch", cap=32)``); a cap
bounds the scaled value, and the scenario's ``after_intensity`` hook keeps
dependent values consistent (S1 rounds within the group, S5's fault inside
an incident). Threads (``worker_threads``, ``concurrent_*``),
probabilities, fractions and caps such as ``max_live_branches`` are never
tagged, because they bound the run rather than size it. Scaled ints are
rounded and never drop below 1; 0 keeps its "default/disabled" meaning.
"""

from __future__ import annotations

from dataclasses import fields

from macrobench import task_pb2 as tp

DATA_OPS_FIELDS = ("statements_per_step", "rows_per_write", "spine_clients")


def _scale(value: int, factor: float, cap: int | None = None) -> int:
    if value <= 0:
        return value  # 0 keeps its "default/disabled" meaning
    out = max(1, int(round(value * factor)))
    if cap is not None:
        out = min(out, cap)
    return out


def _scale_message(msg, names, factor, prefix, changed):
    for name in names:
        before = getattr(msg, name)
        after = _scale(before, factor)
        if after != before:
            setattr(msg, name, after)
            changed[f"{prefix}.{name}"] = (before, after)


def _scale_params(params, kind, factor, key, changed):
    for f in fields(params):
        if f.metadata.get("intensity") != kind:
            continue
        before = getattr(params, f.name)
        after = _scale(before, factor, f.metadata.get("cap"))
        if after != before:
            setattr(params, f.name, after)
            changed[f"{key}.{f.name}"] = (before, after)


def apply_intensity(workload: tp.Workload, scenario_cls, params) -> dict:
    """Scale ``params`` (the scenario's Params instance) and the shared
    knobs of ``workload`` in place; return {"<where>.<field>": (before, after)}."""
    bi = workload.branch_intensity or 1.0
    di = workload.data_intensity or 1.0
    if bi <= 0 or di <= 0:
        raise ValueError("branch_intensity and data_intensity must be > 0")
    changed: dict = {}
    if bi == 1.0 and di == 1.0:
        return changed
    key = scenario_cls.key

    if bi != 1.0:
        ci = workload.branch_ops.commit_interval
        if ci > 1:
            after = max(1, int(round(ci / bi)))
            if after != ci:
                workload.branch_ops.commit_interval = after
                changed["branch_ops.commit_interval"] = (ci, after)
        _scale_params(params, "branch", bi, key, changed)
    if di != 1.0:
        _scale_message(workload.data_ops, DATA_OPS_FIELDS, di, "data_ops", changed)
        _scale_params(params, "data", di, key, changed)
    scenario_cls.after_intensity(params, changed)
    return changed
