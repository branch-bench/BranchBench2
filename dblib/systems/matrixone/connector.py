from __future__ import annotations

from contextlib import closing
from pathlib import Path

from dblib.target import PreparedTarget, SuiteRequest
from dblib.connector import Connector, DatabaseHandle

from . import adapter as matrixone


class MatrixOne(Connector):
    """MatrixOne over the MySQL protocol: backend matrixone."""

    environment = ("MO_",)
    needs_psql = False
    version_sql = "SELECT version()"
    data_dir = Path(matrixone.MO_DATA_DIR).expanduser()

    def scope(self) -> dict:
        return {"host": matrixone.MO_HOST, "port": matrixone.MO_PORT,
                "user": matrixone.MO_USER, "data_dir": str(self.data_dir.resolve())}

    def provision(self, db: str) -> DatabaseHandle:
        with closing(matrixone.connect()) as conn, conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE {matrixone._quote(db)}")
        return DatabaseHandle(db, matrixone.MAIN_BRANCH)

    def load(self, database: DatabaseHandle, dump: Path) -> None:
        matrixone.load_sql_dump(database.name, str(dump))

    def prepare_target(self, database: DatabaseHandle) -> PreparedTarget:
        def open_suite(request: SuiteRequest):
            return matrixone.MatrixOneToolSuite.init_for_bench(
                request.result_collector,
                database.name,
                request.initial_ref,
                request.measure_storage,
            )

        return PreparedTarget(database.root_ref, matrixone.MatrixOneToolSuite, open_suite)

    def delete(self, db: str) -> None:
        matrixone.drop_database(db)
        if self.exists(db):
            raise RuntimeError(f"MatrixOne database or snapshot for '{db}' still exists after deletion")

    def exists(self, db: str) -> bool:
        with closing(matrixone.connect()) as conn, conn.cursor() as cur:
            cur.execute("SHOW DATABASES")
            if any(row[0] == db for row in cur.fetchall()) or matrixone._branch_databases(cur, db):
                return True
            prefix = matrixone.snapshot_prefix(db)
            cur.execute("SHOW SNAPSHOTS")
            return any(str(r[0]).startswith(prefix) for r in cur.fetchall())

    def version(self) -> str:
        with closing(matrixone.connect()) as conn, conn.cursor() as cur:
            cur.execute(self.version_sql)
            return str(cur.fetchone()[0])


CONNECTOR = MatrixOne
