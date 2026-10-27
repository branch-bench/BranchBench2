from __future__ import annotations

from contextlib import closing
import os
from pathlib import Path

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from dblib.target import PreparedTarget, SuiteRequest
from dblib.connector import Connector, DatabaseHandle
from dblib import file_copy
from dblib.file_copy import FileCopyToolSuite
from util.import_db import load_sql_file


def _quote(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


class FileCopy(Connector):
    """PostgreSQL CREATE DATABASE ... STRATEGY = FILE_COPY: backend file_copy."""

    environment = ("FILE_COPY_",)
    needs_psql = True
    version_sql = "SELECT current_setting('server_version')"
    root_ref = ""
    data_dir = Path(file_copy.PGSQL_DATA_DIR or "~/pg_file_copy/data").expanduser()
    prev_method_file: Path | None = None

    def _prev_method_path(self) -> Path:
        if self.prev_method_file is not None:
            return self.prev_method_file
        return self.state_dir / "file_copy.prev_method"

    def scope(self) -> dict:
        return {"host": file_copy.PGSQL_HOST, "port": file_copy.PGSQL_PORT,
                "user": file_copy.PGSQL_USER, "data_dir": str(self.data_dir.resolve())}

    def _admin(self) -> psycopg2.extensions.connection:
        conn = psycopg2.connect(FileCopyToolSuite.get_default_connection_uri())
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return conn

    def _record_prev_method(self, method: str | None) -> None:
        path = self._prev_method_path()
        if path.exists():
            existing = path.read_text(encoding="utf-8").strip()
            if not existing:
                raise RuntimeError(f"persisted file_copy_method file {path} is empty")
            self._prev_method = existing
            return
        if not method or not str(method).strip():
            raise RuntimeError("cannot persist empty file_copy_method")
        clean = str(method).strip()
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".prev_method.tmp")
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            with open(fd, "w", encoding="utf-8") as fh:
                fh.write(f"{clean}\n")
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, path)
        finally:
            tmp.unlink(missing_ok=True)
        self._prev_method = clean

    def _read_prev_method(self) -> str | None:
        path = self._prev_method_path()
        if path.exists():
            existing = path.read_text(encoding="utf-8").strip()
            if not existing:
                raise RuntimeError(f"persisted file_copy_method file {path} is empty")
            self._prev_method = existing
            return existing
        prev = getattr(self, "_prev_method", None)
        return prev if prev else None

    def capture_prev_method(self) -> None:
        """Persist the server's pre-benchmark file_copy_method if no recovery file exists yet."""
        path = self._prev_method_path()
        if path.exists():
            self._read_prev_method()
            return
        with closing(self._admin()) as conn, conn.cursor() as cur:
            cur.execute("SHOW file_copy_method;")
            row = cur.fetchone()
            if not row or not isinstance(row[0], str) or not row[0].strip():
                raise RuntimeError("SHOW file_copy_method returned no value")
            self._record_prev_method(row[0])

    def provision(self, db: str) -> DatabaseHandle:
        self._validate_name(db)
        with closing(self._admin()) as conn, conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE {_quote(db)}")
        try:
            self.capture_prev_method()
            with closing(self._admin()) as conn, conn.cursor() as cur:
                cur.execute("ALTER SYSTEM SET file_copy_method = %s;", (file_copy.FILE_COPY_METHOD,))
                cur.execute("SELECT pg_reload_conf();")
        except BaseException:
            self.delete(db)
            raise
        return DatabaseHandle(db, db)

    @staticmethod
    def _validate_name(db: str) -> None:
        # Longer names lose the db__ ownership prefix when branch names are shortened.
        if len(db) > 30:
            raise ValueError("FILE_COPY db_name must contain at most 30 characters")

    def attach(self, db: str, existing: object) -> DatabaseHandle:
        self._validate_name(db)
        if existing.branch_id and existing.branch_id != db:
            raise ValueError("FILE_COPY existing_db supports only the root database branch")
        return super().attach(db, existing)

    def load(self, database: DatabaseHandle, dump: Path) -> None:
        uri = FileCopyToolSuite.get_initial_connection_uri(database.name)
        load_sql_file(uri, dump)

    def prepare_target(self, database: DatabaseHandle) -> PreparedTarget:
        def open_suite(request: SuiteRequest):
            suite = FileCopyToolSuite.init_for_bench(
                request.result_collector,
                database.name,
                request.initial_ref,
                measure_storage=request.measure_storage,
            )
            if request.worker_index > 0 and request.setup_refs:
                suite.exec([], refs=[request.setup_refs[0]], timed=False)[0].raise_for_status()
            return suite

        return PreparedTarget(database.root_ref, FileCopyToolSuite, open_suite)

    def delete(self, db: str) -> None:
        restored = False
        try:
            file_copy.drop_database(db)
        finally:
            prev = self._read_prev_method()
            if prev:
                with closing(self._admin()) as conn, conn.cursor() as cur:
                    cur.execute("ALTER SYSTEM SET file_copy_method = %s;", (prev,))
                    cur.execute("SELECT pg_reload_conf();")
                    restored = True
        if self.exists(db):
            raise RuntimeError(f"FileCopy database '{db}' still exists after deletion")
        if restored:
            self._prev_method_path().unlink(missing_ok=True)
            self._prev_method = None

    def exists(self, db: str) -> bool:
        with closing(self._admin()) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s OR datname LIKE %s",
                (db, db.replace("_", r"\_") + r"\_\_%"),
            )
            return cur.fetchone() is not None

    def version(self) -> str:
        with closing(self._admin()) as conn, conn.cursor() as cur:
            cur.execute(self.version_sql)
            return str(cur.fetchone()[0])


CONNECTOR = FileCopy
