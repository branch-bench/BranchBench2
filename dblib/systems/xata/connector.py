from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
import hashlib
import os
from pathlib import Path

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from dblib.target import PreparedTarget, SuiteRequest
from dblib.connector import Connector, DatabaseHandle
from util.import_db import load_sql_file

from . import adapter as xata
from .adapter import XataToolSuite


@dataclass(frozen=True)
class XataState:
    project_id: str
    branch_id: str


class Xata(Connector):
    """Xata branching Postgres: backend xata."""

    environment = ("XATA_",)
    needs_psql = True
    version_sql = "SELECT current_setting('server_version')"
    region = "us-east-1"
    service = {
        "region": "us-east-1",
        "instance_type": "xata.medium",
        "image": "postgres:18.0",
        "replicas": 0,
        "scale_to_zero_minutes": 30,
    }

    def _sync_env(self) -> None:
        xata.API_KEY = os.environ.get("XATA_API_KEY", xata.API_KEY)
        xata.XATA_ORGANIZATION_ID = os.environ.get("XATA_ORGANIZATION_ID", xata.XATA_ORGANIZATION_ID)
        xata.XATA_API_BASE_URL = f"https://api.xata.tech/organizations/{xata.XATA_ORGANIZATION_ID}/"

    def scope(self) -> dict:
        org = os.environ.get("XATA_ORGANIZATION_ID", xata.XATA_ORGANIZATION_ID or "")
        key = os.environ.get("XATA_API_KEY", xata.API_KEY or "")
        account = hashlib.sha256(key.encode()).hexdigest()[:12] if key else ""
        return {"provider": "xata", "organization_id": org, "account": account, "region": self.region}

    def preflight(self) -> None:
        self._sync_env()
        XataToolSuite._request("GET", "projects")

    def provision(self, db: str) -> DatabaseHandle:
        self._sync_env()
        project_id, branch_id, branch_name, default_uri = XataToolSuite.create_xata_project(f"project_{db}")
        try:
            with closing(psycopg2.connect(default_uri)) as conn:
                conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                with conn.cursor() as cur:
                    cur.execute(f'CREATE DATABASE "{db}"')
            return DatabaseHandle(db, branch_name, XataState(project_id, branch_id))
        except BaseException:
            XataToolSuite.delete_project(project_id)
            raise

    def load(self, database: DatabaseHandle, dump: Path) -> None:
        self._sync_env()
        state = database.state
        uri = XataToolSuite._get_xata_connection_uri(
            state.project_id, state.branch_id, database.name
        )
        load_sql_file(uri, dump)

    def attach(self, db: str, existing: object) -> DatabaseHandle:
        raise NotImplementedError("Xata requires database setup")

    def delete_database(self, database: DatabaseHandle) -> None:
        self._sync_env()
        XataToolSuite.delete_project(database.state.project_id)

    def prepare_target(self, database: DatabaseHandle) -> PreparedTarget:
        state = database.state

        def open_suite(request: SuiteRequest):
            return XataToolSuite.init_for_bench(
                request.result_collector,
                state.project_id,
                state.branch_id,
                request.initial_ref,
                database.name,
                request.measure_storage,
            )

        return PreparedTarget(database.root_ref, XataToolSuite, open_suite)

    def delete(self, db: str) -> None:
        self._sync_env()
        target = f"project_{db}"
        resp = XataToolSuite._request("GET", "projects")
        for proj in resp.get("projects", []):
            if proj.get("name") == target:
                XataToolSuite.delete_project(proj["id"])
        if self.exists(db):
            raise RuntimeError(f"Xata project '{target}' still exists after deletion")

    def exists(self, db: str) -> bool:
        self._sync_env()
        target = f"project_{db}"
        resp = XataToolSuite._request("GET", "projects")
        return any(proj.get("name") == target for proj in resp.get("projects", []))

    def version(self) -> str:
        return "18.0"


CONNECTOR = Xata
