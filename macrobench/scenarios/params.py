"""Scenario parameters: a dataclass per scenario, filled from the config.

A scenario declares its parameters as a nested dataclass::

    @dataclass
    class Params:
        agents: int = param(0, intensity="branch", doc="N agents")
        key_overlap: float = param(0.0, doc="share of shared rows")
        policy: str = param("FIXED_WORKFLOW", choices=("FIXED_WORKFLOW",))

The config gives values as text (``workload.params``, a string map); each
value is parsed by the field's declared type (int, float, bool or str), and
unknown names, unparsable values and values outside ``choices`` are errors
that name the scenario and the field. Older configs that set one of the
per-scenario proto messages are converted field by field.

Field metadata:

* ``intensity``: "branch" or "data". The field is multiplied by
  ``workload.branch_intensity`` or ``workload.data_intensity``
  (macrobench/intensity.py).
* ``cap``: upper bound applied after scaling.
* ``choices``: allowed values (strings).
* ``doc``: one line describing the parameter (the guide and ``--describe``
  print it).
"""

from __future__ import annotations

import dataclasses
from dataclasses import MISSING, dataclass, field, fields

__all__ = ["param", "parse_params", "params_from_legacy", "params_to_dict", "describe", "NoParams"]


def param(default, *, intensity: str = "", cap: int | None = None, choices: tuple = (), doc: str = ""):
    """A dataclass field with the metadata the benchmark reads."""
    if intensity not in ("", "branch", "data"):
        raise ValueError(f"intensity must be 'branch' or 'data', not {intensity!r}")
    return field(default=default, metadata={"intensity": intensity, "cap": cap,
                                            "choices": tuple(choices), "doc": doc})


@dataclass
class NoParams:
    """For a scenario without parameters."""


_TRUE = {"true", "1", "yes", "on"}
_FALSE = {"false", "0", "no", "off"}


def _field_type(f) -> type:
    t = f.type
    if isinstance(t, str):  # `from __future__ import annotations`
        t = {"int": int, "float": float, "bool": bool, "str": str}.get(t, t)
    if t not in (int, float, bool, str):
        raise TypeError(f"parameter {f.name}: type {f.type!r} is not one of int, float, bool, str")
    return t


def _convert(scenario: str, f, value):
    """``value`` (text from the config, or a typed value) as the field's type."""
    t = _field_type(f)
    where = f"scenario {scenario}: parameter {f.name!r}"
    if t is bool:
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if text in _TRUE:
            return True
        if text in _FALSE:
            return False
        raise ValueError(f"{where} expects true/false, got {value!r}")
    if t is int:
        if isinstance(value, bool):
            raise ValueError(f"{where} expects an integer, got {value!r}")
        if isinstance(value, int):
            return value
        if isinstance(value, float) and value.is_integer():
            return int(value)
        try:
            return int(str(value).strip())
        except ValueError:
            raise ValueError(f"{where} expects an integer, got {value!r}") from None
    if t is float:
        try:
            return float(value if not isinstance(value, str) else value.strip())
        except (TypeError, ValueError):
            raise ValueError(f"{where} expects a number, got {value!r}") from None
    return str(value)


def _check_choices(scenario: str, f, value) -> None:
    choices = f.metadata.get("choices") or ()
    if choices and value not in choices:
        raise ValueError(f"scenario {scenario}: parameter {f.name!r} must be one of "
                         f"{', '.join(map(str, choices))}, got {value!r}")


def parse_params(scenario: str, cls, values: dict):
    """An instance of the dataclass ``cls`` from ``{name: value}``; values
    may be text (from the config) or already typed."""
    known = {f.name: f for f in fields(cls)}
    unknown = sorted(set(values) - set(known))
    if unknown:
        raise ValueError(f"scenario {scenario}: unknown parameter(s) {', '.join(unknown)}; "
                         f"known: {', '.join(sorted(known)) or '(none)'}")
    kwargs = {}
    for name, f in known.items():
        if name in values:
            kwargs[name] = _convert(scenario, f, values[name])
        elif f.default is MISSING and f.default_factory is MISSING:
            raise ValueError(f"scenario {scenario}: parameter {name!r} is required")
    out = cls(**kwargs)
    for f in fields(out):
        _check_choices(scenario, f, getattr(out, f.name))
    return out


def params_from_legacy(scenario: str, cls, message):
    """Convert one of the per-scenario proto messages (the shipped presets
    still use them; see the TODO at Workload.legacy_scenario in
    macrobench/task.proto). A parameter the message has no field for keeps
    its dataclass default, so a new parameter of S1-S6 needs a proto field
    too until the presets move to the params map. Every
    field the dataclass declares is read from the message (unset fields
    read as the proto default, which is also the dataclass default), enum
    values become their names."""
    values = {}
    descriptors = {fd.name: fd for fd in message.DESCRIPTOR.fields}
    for f in fields(cls):
        fd = descriptors.get(f.name)
        if fd is None:
            continue
        v = getattr(message, f.name)
        if fd.enum_type is not None:
            v = fd.enum_type.values_by_number[v].name
        values[f.name] = v
    return parse_params(scenario, cls, values)


def params_to_dict(params) -> dict:
    return dataclasses.asdict(params)


def describe(cls) -> list:
    """[(name, type name, default, metadata)] for documentation."""
    out = []
    for f in fields(cls):
        out.append((f.name, _field_type(f).__name__, f.default, dict(f.metadata)))
    return out
