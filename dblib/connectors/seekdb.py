from __future__ import annotations

from contextlib import closing
from pathlib import Path

from dblib.target import PreparedTarget, SuiteRequest
from dblib.connector import Connector, DatabaseHandle
from dblib import seekdb


class SeekDB(Connector):
    """SeekDB (OceanBase) over the MySQL protocol: backend seekdb."""

    environment = ("SEEKDB_",)
    needs_psql = False
    version_sql = "SELECT version()"
    data_dir = Path(seekdb.SEEKDB_DATA_DIR).expanduser()

    def scope(self) -> dict:
        return {"host": seekdb.SEEKDB_HOST, "port": seekdb.SEEKDB_PORT,
                "user": seekdb.SEEKDB_USER, "data_dir": str(self.data_dir.resolve())}

    def provision(self, db: str) -> DatabaseHandle:
        with closing(seekdb.connect()) as conn, conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE {seekdb._quote(db)}")
        return DatabaseHandle(db, seekdb.MAIN_BRANCH)

    def load(self, database: DatabaseHandle, dump: Path) -> None:
        seekdb.load_sql_dump(database.name, str(dump))

    def prepare_target(self, database: DatabaseHandle) -> PreparedTarget:
        def open_suite(request: SuiteRequest):
            return seekdb.SeekDBToolSuite.init_for_bench(
                request.result_collector,
                database.name,
                request.initial_ref,
                request.measure_storage,
            )

        return PreparedTarget(database.root_ref, seekdb.SeekDBToolSuite, open_suite)

    def delete(self, db: str) -> None:
        seekdb.drop_database(db)
        if self.exists(db):
            raise RuntimeError(f"SeekDB database '{db}' still exists after deletion")

    def exists(self, db: str) -> bool:
        with closing(seekdb.connect()) as conn, conn.cursor() as cur:
            cur.execute("SHOW DATABASES")
            return any(row[0] == db for row in cur.fetchall()) or bool(seekdb._branch_databases(cur, db))

    def version(self) -> str:
        with closing(seekdb.connect()) as conn, conn.cursor() as cur:
            cur.execute(self.version_sql)
            return str(cur.fetchone()[0])


CONNECTOR = SeekDB
