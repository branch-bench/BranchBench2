"""Built-in database selection and lifecycle for standalone benchmark commands."""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from importlib import import_module
from importlib.resources import files
from pathlib import Path

from dblib.target import PreparedTarget


@dataclass(frozen=True)
class DatabaseHandle:
    """The database name, initial branch, and provider-specific resource identity."""

    name: str
    root_ref: str
    state: object = None


class Connector(ABC):
    """Database lifecycle shared by standalone commands and external applications."""

    needs_psql: bool
    version_sql: str
    environment: tuple[str, ...] = ()
    root_ref = "main"

    def __init__(self, *, state_dir: Path | None = None) -> None:
        self.state_dir = (state_dir if state_dir is not None
                          else Path.home() / ".cache" / "branchbench" / "runs")

    @abstractmethod
    def scope(self) -> dict:
        """Return the deletion scope without secrets."""

    @abstractmethod
    def provision(self, db: str) -> DatabaseHandle:
        """Create db; on failure, remove only incomplete resources acquired here."""

    def attach(self, db: str, existing: object) -> DatabaseHandle:
        """Use existing data without provisioning or loading it."""
        if not self.exists(db):
            raise ValueError(f"database {db!r} does not exist")
        return DatabaseHandle(db, existing.branch_id or self.root_ref or db)

    @abstractmethod
    def load(self, database: DatabaseHandle, dump: Path) -> None:
        """Load a SQL dump into the database."""

    @abstractmethod
    def prepare_target(self, database: DatabaseHandle) -> PreparedTarget:
        """Supply the initial branch and worker adapter factory."""

    @abstractmethod
    def delete(self, db: str) -> None:
        """Delete resources named after db, including a partial provision."""

    def delete_database(self, database: DatabaseHandle) -> None:
        """Delete the resource identified by a successful provision or attach."""
        self.delete(database.name)

    @abstractmethod
    def exists(self, db: str) -> bool:
        """Whether resources named after db exist."""

    @abstractmethod
    def version(self) -> str:
        """Return the running database version."""

    def preflight(self) -> None:
        """Check hosted account access before creating resources."""

    def consumption_metrics(self, database: DatabaseHandle) -> dict | None:
        """Return provider consumption metrics when available."""
        return None


def connector_names() -> list[str]:
    """List built-in providers without importing their optional dependencies."""
    root = Path(str(files("dblib.systems")))
    return sorted(path.name for path in root.iterdir()
                  if path.is_dir() and not path.name.startswith("_")
                  and (path / "connector.py").is_file())


def load_connector(name: str, *, state_dir: Path | None = None) -> Connector:
    if name not in connector_names():
        raise ValueError(f"no built-in database connector named {name!r}")
    module = import_module(f"dblib.systems.{name}.connector")
    return module.CONNECTOR(state_dir=state_dir)


def prepare_database(
    backend: str, setup: object, *, state_dir: Path | None = None,
) -> tuple[Connector, DatabaseHandle, PreparedTarget]:
    """Prepare a database and return its lifecycle owner, handle, and target.

    Providers remove resources they acquired when provisioning fails, even when
    setup.cleanup is false. A failed CREATE never proves database ownership.
    """
    db = setup.db_name
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", db):
        raise ValueError("db_name must start with a lowercase letter and contain only "
                         "lowercase letters, digits, or underscores (at most 63 characters)")
    source = setup.WhichOneof("source")
    dump = Path(setup.sql_dump.sql_dump_path).expanduser() if source == "sql_dump" else None
    if dump is not None and not dump.is_file():
        raise ValueError(f"SQL dump does not exist: {dump}")
    connector = load_connector(backend, state_dir=state_dir)
    connector.preflight()
    if source == "existing_db":
        database = connector.attach(db, setup.existing_db)
    else:
        if connector.exists(db):
            connector.delete(db)
        database = connector.provision(db)
    try:
        if dump is not None:
            connector.load(database, dump)
        target = connector.prepare_target(database)
    except BaseException:
        if setup.cleanup:
            connector.delete_database(database)
        raise
    return connector, database, target


@contextmanager
def prepared_database(
    backend: str, setup: object, *, state_dir: Path | None = None,
) -> Iterator[PreparedTarget]:
    """Yield a prepared target and apply setup.cleanup when it exits."""
    connector, database, target = prepare_database(backend, setup, state_dir=state_dir)
    try:
        yield target
    finally:
        if setup.cleanup:
            connector.delete_database(database)
