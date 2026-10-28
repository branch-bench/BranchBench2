from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import time

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from dblib.target import PreparedTarget, SuiteRequest
from dblib.connector import Connector, DatabaseHandle
from util.import_db import load_sql_file

from . import adapter as neon
from .adapter import NeonToolSuite


@dataclass(frozen=True)
class NeonState:
    project_id: str
    branch_id: str


class Neon(Connector):
    """Neon serverless Postgres: backend neon."""

    environment = ("NEON_",)
    needs_psql = True
    version_sql = "SELECT current_setting('server_version')"
    region = neon.NEON_REGION
    service = {
        "region": neon.NEON_REGION,
        "pg_version": int(neon.NEON_PG_VERSION),
        "endpoint_type": "read_write",
        "cpu_millis": int(round(neon.NEON_COMPUTE_CU * 1000)),
    }

    def _sync_env(self) -> None:
        neon.API_KEY = os.environ.get("NEON_API_KEY_ORG", neon.API_KEY)

    def scope(self) -> dict:
        key = os.environ.get("NEON_API_KEY_ORG", neon.API_KEY or "")
        account = hashlib.sha256(key.encode()).hexdigest()[:12] if key else ""
        return {"provider": "neon", "account": account, "region": self.region, "pg_version": int(neon.NEON_PG_VERSION)}

    def preflight(self) -> None:
        self._sync_env()
        neon.api_request("GET", "projects?limit=1")

    def provision(self, db: str) -> DatabaseHandle:
        self._sync_env()
        project = NeonToolSuite.create_neon_project(f"project_{db}")
        project_id = project["project"]["id"]
        try:
            default_uri = project["connection_uris"][0]["connection_uri"] if project.get("connection_uris") else ""
            branch_id = project["branch"]["id"]
            branch_name = project["branch"]["name"]
            with closing(psycopg2.connect(default_uri)) as conn:
                conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                with conn.cursor() as cur:
                    cur.execute(f"CREATE DATABASE {neon._q(db)}")
            return DatabaseHandle(db, branch_name, NeonState(project_id, branch_id))
        except BaseException:
            neon.api_request("DELETE", f"projects/{project_id}")
            raise

    def load(self, database: DatabaseHandle, dump: Path) -> None:
        self._sync_env()
        state = database.state
        uri = NeonToolSuite._get_neon_connection_uri(
            state.project_id, state.branch_id, database.name
        )
        load_sql_file(uri, dump)

    def attach(self, db: str, existing: object) -> DatabaseHandle:
        self._sync_env()
        project_id = existing.neon_project_id
        if not project_id:
            raise ValueError("existing_db.neon_project_id is required for Neon")
        branches = NeonToolSuite.get_project_branches(project_id)["branches"]
        branch = next((branch for branch in branches
                       if (branch["id"] == existing.branch_id if existing.branch_id else branch.get("default"))), None)
        if branch is None:
            raise ValueError(f"Neon project {project_id!r} has no matching branch")
        return DatabaseHandle(db, branch["name"], NeonState(project_id, branch["id"]))

    def delete_database(self, database: DatabaseHandle) -> None:
        self._sync_env()
        neon.api_request("DELETE", f"projects/{database.state.project_id}")

    def prepare_target(self, database: DatabaseHandle) -> PreparedTarget:
        state = database.state

        def open_suite(request: SuiteRequest):
            return NeonToolSuite.init_for_bench(
                request.result_collector,
                state.project_id,
                state.branch_id,
                request.initial_ref,
                database.name,
                False,  # never per operation on Neon: pg_database_size per branch is too slow
            )

        return PreparedTarget(database.root_ref, NeonToolSuite, open_suite)

    def consumption_metrics(self, database: DatabaseHandle) -> dict | None:
        """Wait for Neon's delayed project consumption metrics."""
        self._sync_env()
        print("Waiting 15 min for Neon consumption metrics (after)...", flush=True)
        for elapsed_min in range(15):
            time.sleep(60)
            if (elapsed_min + 1) % 3 == 0 or elapsed_min == 14:
                print(f"  {elapsed_min + 1}/15 min elapsed...", flush=True)
        for attempt in range(10):
            result = NeonToolSuite.get_consumption_metrics(database.state.project_id)
            if result and result.get("all_metrics"):
                return result
            if attempt < 9:
                time.sleep(60)
        print("  WARNING: No Neon consumption metrics for after", flush=True)
        return None

    def delete(self, db: str) -> None:
        self._sync_env()
        target = f"project_{db}"
        for proj in neon.list_projects():
            if proj.get("name") == target:
                neon.api_request("DELETE", f"projects/{proj['id']}")
        if self.exists(db):
            raise RuntimeError(f"Neon project '{target}' still exists after deletion")

    def exists(self, db: str) -> bool:
        self._sync_env()
        target = f"project_{db}"
        return any(proj.get("name") == target for proj in neon.list_projects())

    def version(self) -> str:
        return str(neon.NEON_PG_VERSION)


CONNECTOR = Neon
