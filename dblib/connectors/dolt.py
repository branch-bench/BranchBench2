from __future__ import annotations

import shutil
from contextlib import closing
from pathlib import Path

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from dblib.target import PreparedTarget, SuiteRequest
from dblib.connector import Connector, DatabaseHandle
from dblib import dolt as dolt_pg
from dblib.dolt import DOLT_DATA_DIR, DoltToolSuite, commit_dolt_schema
from util.import_db import load_sql_file


class Doltgres(Connector):
    """Doltgres over the Postgres protocol: backend dolt."""

    environment = ("DOLT_",)
    needs_psql = True
    version_sql = "SELECT dolt_version()"
    data_dir = Path(DOLT_DATA_DIR).expanduser()

    def scope(self) -> dict:
        return {"host": dolt_pg.DOLT_HOST, "port": dolt_pg.DOLT_PORT, "user": dolt_pg.DOLT_USER,
                "data_dir": str(self.data_dir.resolve())}

    def _admin(self) -> psycopg2.extensions.connection:
        conn = psycopg2.connect(DoltToolSuite.get_default_connection_uri())
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return conn

    def provision(self, db: str) -> DatabaseHandle:
        with closing(self._admin()) as conn, conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE {db}")
        return DatabaseHandle(db, "main")

    def load(self, database: DatabaseHandle, dump: Path) -> None:
        uri = DoltToolSuite.get_initial_connection_uri(database.name)
        load_sql_file(uri, dump)
        commit_dolt_schema(uri)

    def prepare_target(self, database: DatabaseHandle) -> PreparedTarget:
        def open_suite(request: SuiteRequest):
            return DoltToolSuite.init_for_bench(
                request.result_collector,
                database.name,
                request.initial_ref,
                request.measure_storage,
            )

        return PreparedTarget(database.root_ref, DoltToolSuite, open_suite)

    def delete(self, db: str) -> None:
        with closing(self._admin()) as conn, conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {db}")
        dropped = self.data_dir / ".dolt_dropped_databases" / db
        if dropped.exists():
            shutil.rmtree(dropped)
        if self.exists(db):
            raise RuntimeError(f"Doltgres database '{db}' still exists after deletion")

    def exists(self, db: str) -> bool:
        with closing(self._admin()) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (db,))
            listed = cur.fetchone() is not None
        return listed or (self.data_dir / db).exists() or (self.data_dir / ".dolt_dropped_databases" / db).exists()

    def version(self) -> str:
        with closing(self._admin()) as conn, conn.cursor() as cur:
            cur.execute(self.version_sql)
            return str(cur.fetchone()[0])


CONNECTOR = Doltgres
