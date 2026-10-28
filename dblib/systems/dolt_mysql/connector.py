from __future__ import annotations

import shutil
from contextlib import closing
from pathlib import Path

from dblib.target import PreparedTarget, SuiteRequest
from dblib.connector import Connector, DatabaseHandle

from . import adapter as dolt_mysql


class Dolt(Connector):
    """Dolt over the MySQL protocol: backend dolt_mysql."""

    environment = ("DOLT_MYSQL_",)
    needs_psql = False
    version_sql = "SELECT dolt_version()"
    data_dir = Path(dolt_mysql.DOLT_MYSQL_DATA_DIR)
    suite_type = dolt_mysql.DoltMySQLToolSuite

    def scope(self) -> dict:
        return {"host": dolt_mysql.DOLT_MYSQL_HOST, "port": dolt_mysql.DOLT_MYSQL_PORT,
                "user": dolt_mysql.DOLT_MYSQL_USER, "data_dir": str(self.data_dir.resolve())}

    def provision(self, db: str) -> DatabaseHandle:
        with closing(dolt_mysql.connect()) as conn, conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE {db}")
        return DatabaseHandle(db, "main")

    def load(self, database: DatabaseHandle, dump: Path) -> None:
        dolt_mysql.load_sql_dump(database.name, str(dump))
        with closing(dolt_mysql.connect(database.name)) as conn, conn.cursor() as cur:
            cur.execute("CALL DOLT_COMMIT('-Am', 'Load SQL schema')")

    def prepare_target(self, database: DatabaseHandle) -> PreparedTarget:
        def open_suite(request: SuiteRequest):
            return self.suite_type.init_for_bench(
                request.result_collector,
                database.name,
                request.initial_ref,
                request.measure_storage,
            )

        return PreparedTarget(database.root_ref, self.suite_type, open_suite)

    def delete(self, db: str) -> None:
        dolt_mysql.drop_database(db)
        dropped = self.data_dir / ".dolt_dropped_databases" / db
        if dropped.exists():
            shutil.rmtree(dropped)
        if self.exists(db):
            raise RuntimeError(f"Dolt database '{db}' still exists after deletion")

    def exists(self, db: str) -> bool:
        with closing(dolt_mysql.connect()) as conn, conn.cursor() as cur:
            cur.execute("SHOW DATABASES")
            listed = any(row[0] == db for row in cur.fetchall())
        return listed or (self.data_dir / db).exists() or (self.data_dir / ".dolt_dropped_databases" / db).exists()

    def version(self) -> str:
        with closing(dolt_mysql.connect()) as conn, conn.cursor() as cur:
            cur.execute(self.version_sql)
            return str(cur.fetchone()[0])


CONNECTOR = Dolt
