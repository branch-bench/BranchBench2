"""Provider-neutral benchmark target passed to the runners."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from dblib.db_api import DBToolSuite
from dblib.result_collector import ResultCollector


@dataclass(frozen=True)
class SuiteRequest:
    """State needed to open one independent suite for a benchmark worker."""

    result_collector: ResultCollector
    initial_ref: str
    worker_index: int
    setup_refs: tuple[str, ...] = ()
    measure_storage: bool = False


SuiteFactory = Callable[[SuiteRequest], DBToolSuite]


@dataclass(frozen=True)
class PreparedTarget:
    """A provisioned database and the factory that opens worker suites."""

    root_ref: str
    suite_type: type[DBToolSuite]
    open_suite: SuiteFactory


@dataclass(frozen=True)
class ExecutionState:
    """The current benchmark ref plus refs created during untimed setup."""

    working_ref: str
    setup_refs: tuple[str, ...] = ()
