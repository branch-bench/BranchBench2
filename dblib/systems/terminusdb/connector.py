from __future__ import annotations

from pathlib import Path
from urllib.parse import urlsplit

from dblib.connector import Connector, DatabaseHandle
from dblib.target import PreparedTarget, SuiteRequest

from . import adapter as terminusdb
from .adapter import TerminusDBError, TerminusDBToolSuite


class TerminusDB(Connector):
    """TerminusDB document and graph database: backend terminusdb."""

    environment = ("TERMINUSDB_",)
    needs_psql = False
    version_sql = ""

    def __init__(self, *, state_dir: Path | None = None) -> None:
        super().__init__(state_dir=state_dir)
        self._retained: set[str] = set()

    def scope(self) -> dict:
        endpoint = urlsplit(terminusdb.TERMINUSDB_URL)
        return {
            "scheme": endpoint.scheme,
            "host": endpoint.hostname or "",
            "port": endpoint.port,
            "team": terminusdb.TERMINUSDB_TEAM,
            "user": terminusdb.TERMINUSDB_USER,
        }

    def _call(self, method: str, path: str, **kwargs):
        client = terminusdb._Client()
        try:
            return client.call(method, path, **kwargs)
        finally:
            client.close()

    def preflight(self) -> None:
        self._call("GET", "info")

    def provision(self, db: str) -> DatabaseHandle:
        if terminusdb.REUSE_LOAD and self.exists(db):
            self._retained.add(db)
            return DatabaseHandle(db, terminusdb.MAIN_BRANCH)
        terminusdb._forget(db)
        self._call(
            "POST",
            f"db/{terminusdb._db_path(db)}",
            body={"label": db, "comment": "db-fork benchmark"},
        )
        return DatabaseHandle(db, terminusdb.MAIN_BRANCH)

    def load(self, database: DatabaseHandle, dump: Path) -> None:
        terminusdb.setup_database(database.name, str(dump))
        self._retained.discard(database.name)

    def prepare_target(self, database: DatabaseHandle) -> PreparedTarget:
        if database.name in self._retained:
            terminusdb.setup_database(database.name)
            self._retained.remove(database.name)

        def open_suite(request: SuiteRequest):
            return TerminusDBToolSuite.init_for_bench(
                request.result_collector,
                database.name,
                request.initial_ref,
                request.measure_storage,
            )

        return PreparedTarget(database.root_ref, TerminusDBToolSuite, open_suite)

    def delete(self, db: str) -> None:
        terminusdb.drop_database(db)
        if not terminusdb.REUSE_LOAD and self.exists(db):
            raise RuntimeError(f"TerminusDB database '{db}' still exists after deletion")

    def exists(self, db: str) -> bool:
        try:
            self._call("GET", f"db/{terminusdb._db_path(db)}")
        except TerminusDBError as error:
            if error.status in (400, 404):
                return False
            raise
        return True

    def version(self) -> str:
        info = self._call("GET", "info")
        if isinstance(info, dict):
            details = info.get("api:info")
            for source in (info, details if isinstance(details, dict) else {}):
                for key in ("api:version", "terminusdb_version", "version"):
                    if source.get(key):
                        return str(source[key])
        return "unknown"


CONNECTOR = TerminusDB
