"""The macrobenchmark scenarios. Importing the package imports every module
in it, so a scenario module registers itself (``@register``) just by being
here; no list to edit."""

import importlib
import pkgutil

from macrobench.scenarios.base import (  # noqa: F401
    SCENARIOS,
    InvariantFailed,
    InvariantRecorder,
    Scenario,
    ScenarioContext,
    ScenarioStopped,
    UnsupportedOperationAbort,
    Worker,
    create_scenario,
    listed_keys,
    load_params,
    register,
    scenario_class,
    scenario_key,
)
from macrobench.scenarios.params import NoParams, param  # noqa: F401

_NOT_SCENARIOS = {"base", "params", "__main__"}

for _info in pkgutil.iter_modules(__path__):
    if _info.name not in _NOT_SCENARIOS and not _info.name.startswith("_"):
        importlib.import_module(f"{__name__}.{_info.name}")
