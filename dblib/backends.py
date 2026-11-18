"""Open a backend's database in one call: a thin layer over the connectors.

    import dblib

    suite = dblib.open_suite("dolt", "mydb")              # an existing database
    with dblib.open_backend("matrixone", "scratch", create=True, drop_on_close=True) as backend:
        suites = [backend.suite() for _ in range(4)]      # one connection each

The benchmarks' runners prepare databases through the connectors in
``dblib/systems/<key>/connector.py`` (``dblib.connector.prepare_database``)
and hand the runners a ``PreparedTarget``. This module is the same lifecycle
for code that wants suites directly (an agent, another benchmark's harness):
it picks the connector, creates or attaches to the database, and opens
suites through the connector's target.
"""

from __future__ import annotations

import re
from pathlib import Path
from types import SimpleNamespace
from typing import Optional

from dblib import result_collector as rc
from dblib.connector import Connector, DatabaseHandle, load_connector
from dblib.target import PreparedTarget, SuiteRequest

_DB_NAME = re.compile(r"[a-z][a-z0-9_]{0,62}")


class BackendHandle:
    """A database prepared by its connector. ``suite()`` opens a new suite
    (its own connection) on the default branch; ``close()`` deletes the
    database if ``drop_on_close``."""

    def __init__(self, key: str, connector: Connector, database: DatabaseHandle,
                 target: PreparedTarget, drop_on_close: bool):
        self.key = key
        self.connector = connector
        self.database = database
        self.target = target
        self.drop_on_close = drop_on_close
        self._workers = 0
        self._closed = False

    @property
    def db_name(self) -> str:
        return self.database.name

    @property
    def default_branch(self) -> str:
        return self.target.root_ref

    @property
    def suite_type(self):
        return self.target.suite_type

    def suite(self, collector: Optional[rc.ResultCollector] = None,
              measure_storage: bool = False):
        request = SuiteRequest(result_collector=collector or rc.ResultCollector(),
                               initial_ref=self.target.root_ref,
                               worker_index=self._workers,
                               measure_storage=measure_storage)
        self._workers += 1
        suite = self.target.open_suite(request)
        suite.backend_handle = self
        return suite

    def close(self, drop: Optional[bool] = None) -> None:
        """Delete the database if ``drop`` (default: drop_on_close)."""
        if self._closed:
            return
        if self.drop_on_close if drop is None else drop:
            self.connector.delete_database(self.database)
        self._closed = True       # only once the delete went through, so a retry can repeat it

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def open_backend(backend: str, db_name: str, *, create: bool = False,
                 sql_dump: Optional[str] = None, branch_id: str = "",
                 neon_project_id: str = "", drop_on_close: bool = False,
                 state_dir: Optional[Path] = None) -> BackendHandle:
    """Prepare ``db_name`` on ``backend`` (a connector name: see
    ``dblib.connector.connector_names()``).

    ``create=True`` (re)creates the database (an existing one of that name
    is deleted first) and loads ``sql_dump`` if given; otherwise the
    database must exist (Neon also needs ``neon_project_id``). If loading
    fails, a database this call created is deleted again."""
    key = backend.lower()
    if not _DB_NAME.fullmatch(db_name):
        raise ValueError("db_name must start with a lowercase letter and contain only "
                         "lowercase letters, digits, or underscores (at most 63 characters)")
    dump = Path(sql_dump).expanduser() if sql_dump else None
    if dump is not None and not create:
        raise ValueError("sql_dump needs create=True")
    if dump is not None and not dump.is_file():
        raise ValueError(f"SQL dump does not exist: {dump}")
    connector = load_connector(key, state_dir=state_dir)
    connector.preflight()
    if create:
        if connector.exists(db_name):
            connector.delete(db_name)
        database = connector.provision(db_name)
    else:
        existing = SimpleNamespace(branch_id=branch_id, neon_project_id=neon_project_id)
        database = connector.attach(db_name, existing)
    try:
        if dump is not None:
            connector.load(database, dump)
        target = connector.prepare_target(database)
    except BaseException:
        if create:
            connector.delete_database(database)
        raise
    return BackendHandle(key, connector, database, target, drop_on_close)


def open_suite(backend: str, db_name: str, *, collector: Optional[rc.ResultCollector] = None,
               **kwargs):
    """``open_backend(...).suite(collector)``: one suite on the default
    branch. ``suite.backend_handle.close()`` deletes the database when it
    was opened with ``drop_on_close``."""
    return open_backend(backend, db_name, **kwargs).suite(collector)
