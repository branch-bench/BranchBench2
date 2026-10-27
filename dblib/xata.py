"""Xata backend.

Branches are Xata branches (REST API), each its own Postgres instance, so
connecting means opening a new connection. Xata has no commits, merge or
restore: only branch, delete and exec are supported.
"""

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Tuple
import psycopg2
import requests

from psycopg2.extensions import connection as _pgconn
from dblib.db_api import DBToolSuite, Ref
import dblib.result_collector as rc

API_KEY = os.environ.get("XATA_API_KEY", "")

# Everything runs in one pre-created organization.
XATA_ORGANIZATION_ID = os.environ.get("XATA_ORGANIZATION_ID", "")
XATA_API_BASE_URL = f"https://api.xata.tech/organizations/{XATA_ORGANIZATION_ID}/"


class XataToolSuite(DBToolSuite):
    BACKEND_NAME = "xata"
    SUPPORTS_COMMIT_REFS = False
    SUPPORTS_MULTI_REF_EXEC = False

    # ------------------------------------------------------------------
    # Project-level helpers
    # ------------------------------------------------------------------

    @classmethod
    def add_db_name_to_connection_string(cls, connection_string: str, db_name: str) -> str:
        conn_components = connection_string.split("/")
        conn_components[-1] = db_name
        return "/".join(conn_components) + "?sslmode=require"

    @classmethod
    def create_xata_project(cls, project_name: str) -> Tuple[str, str, str, str]:
        """Create a project with a "main" branch; returns (project id,
        branch id, branch name, connection uri for the postgres db)."""
        project_details = cls._request("POST", "projects", json={"name": project_name})
        endpoint = f"projects/{project_details['id']}/branches"
        branch_payload = {
            "mode": "custom",
            "name": "main",
            "scaleToZero": {"enabled": True, "inactivityPeriodMinutes": 30},
            "configuration": {
                "region": "us-east-1",
                "instanceType": "xata.medium",
                "image": "postgres:18.0",
                "replicas": 0,
            },
        }
        try:
            default_branch = cls._request("POST", endpoint, json=branch_payload)
            conn_string = cls._poll_branch_active(
                project_details["id"],
                default_branch["id"],
                initial_conn_string=default_branch.get("connectionString"),
                initial_status_type=(default_branch.get("status") or {}).get("statusType", ""),
            )
            return (
                project_details["id"],
                default_branch["id"],
                default_branch["name"],
                cls.add_db_name_to_connection_string(conn_string, "postgres"),
            )
        except BaseException:
            cls.delete_project(project_details["id"])
            raise

    @classmethod
    def delete_project(cls, project_id: str) -> None:
        """Delete every branch, then the project (as the API requires)."""
        response = cls._request("GET", f"projects/{project_id}/branches")
        for branch in response.get("branches", []):
            cls._request("DELETE", f"projects/{project_id}/branches/{branch['id']}")
        time.sleep(2)
        cls._request("DELETE", f"projects/{project_id}")

    @classmethod
    def init_for_bench(
        cls,
        result_collector: rc.ResultCollector,
        project_id: str,
        branch_id: str,
        branch_name: str,
        database_name: str,
        measure_storage: bool = False,
    ):
        uri = cls._get_xata_connection_uri(project_id, branch_id, database_name)
        conn = psycopg2.connect(uri)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return cls(conn, result_collector, project_id, branch_name, branch_id,
                   database_name, measure_storage)

    @classmethod
    def _request(cls, method: str, endpoint: str, **kwargs):
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {API_KEY}"
        headers["Accept"] = "application/json"
        headers["Content-Type"] = "application/json"
        r = requests.request(method, XATA_API_BASE_URL + endpoint, headers=headers, **kwargs)
        r.raise_for_status()
        if r.status_code == 204 or not r.content:
            return {}
        return r.json()

    _READY_STATUSES = {"STATUS_TYPE_ACTIVE", "STATUS_TYPE_HEALTHY"}

    @classmethod
    def _poll_branch_active(
        cls,
        project_id: str,
        branch_id: str,
        initial_conn_string: str = None,
        initial_status_type: str = "",
        max_attempts: int = 30,
        interval: float = 10.0,
    ) -> str:
        """Block until the branch has a connection string and a ready status
        (a string can appear while the compute is still transient)."""
        conn_string = initial_conn_string
        status_type = initial_status_type
        endpoint = f"projects/{project_id}/branches/{branch_id}"
        for _ in range(max_attempts):
            if conn_string and status_type in cls._READY_STATUSES:
                return conn_string
            time.sleep(interval)
            details = cls._request("GET", endpoint)
            conn_string = details.get("connectionString")
            status_type = (details.get("status") or {}).get("statusType", "")
        if not conn_string:
            raise RuntimeError(
                f"Branch {branch_id} connection string not available after {max_attempts} attempts"
            )
        raise RuntimeError(
            f"Branch {branch_id} not active after {max_attempts} attempts (status: {status_type})"
        )

    @classmethod
    def _get_xata_connection_uri(cls, project_id: str, branch_id: str, db_name: str) -> str:
        response = cls._request("GET", f"projects/{project_id}/branches/{branch_id}")
        return cls.add_db_name_to_connection_string(response["connectionString"], db_name)

    # ------------------------------------------------------------------

    def __init__(
        self,
        connection: _pgconn,
        result_collector: rc.ResultCollector,
        project_id: str,
        branch_name: str,
        branch_id: str,
        database_name: str = None,
        measure_storage: bool = False,
    ):
        super().__init__(connection, result_collector, measure_storage)
        self.project_id = project_id
        self.db_name = database_name or connection.get_dsn_parameters()["dbname"]
        self.current_branch_id = branch_id
        branch_name = branch_name or "main"
        self._all_branches = {branch_name: (branch_id, None)}
        self._current_ref = Ref(branch_name)

    def _get_xata_branches(self) -> dict:
        response = self.__class__._request("GET", f"projects/{self.project_id}/branches")
        return {r["name"]: (r["id"], r.get("parent_id", None)) for r in response["branches"]}

    def _branch_id(self, name: str) -> str:
        info = self._all_branches.get(name)
        if info and info[0]:
            return info[0]
        all_branches = self._get_xata_branches()
        if name not in all_branches:
            raise ValueError(f"Branch '{name}' does not exist.")
        bid = all_branches[name][0]
        self._all_branches[name] = (bid, None)
        return bid

    def _uri_for(self, name: str) -> str:
        bid = self._branch_id(name)
        uri = self._all_branches[name][1]
        if not uri:
            uri = self.__class__._get_xata_connection_uri(self.project_id, bid, self.db_name)
            self._all_branches[name] = (bid, uri)
        return uri

    def list_branches(self) -> list:
        return list(self._get_xata_branches().keys())

    # ------------------------------------------------------------------
    # Hooks
    # ------------------------------------------------------------------

    def _connect_impl(self, ref: Ref) -> None:
        uri = self._uri_for(ref.branch)
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
        self.conn = psycopg2.connect(uri)
        self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        self.current_branch_id = self._all_branches[ref.branch][0]

    def _branch_impl(self, name: str, from_ref: Ref) -> None:
        """Create a branch inheriting from its parent and wait until its
        compute is ready (that wait is part of the BRANCH latency)."""
        parent_id = self._branch_id(from_ref.branch)
        res = self.__class__._request(
            "POST",
            f"projects/{self.project_id}/branches",
            json={"mode": "inherit", "name": name, "parentID": parent_id},
        )
        branch_id = res["id"]
        conn_string = self.__class__._poll_branch_active(
            self.project_id,
            branch_id,
            initial_conn_string=res.get("connectionString"),
            initial_status_type=(res.get("status") or {}).get("statusType", ""),
        )
        uri = self.__class__.add_db_name_to_connection_string(conn_string, self.db_name)
        self._all_branches[name] = (branch_id, uri)

    def _delete_impl(self, ref: Ref) -> None:
        bid = self._branch_id(ref.branch)
        self.__class__._request("DELETE", f"projects/{self.project_id}/branches/{bid}")
        self._all_branches.pop(ref.branch, None)
        if self._current_ref and self._current_ref.branch == ref.branch:
            self._current_ref = None

    def _get_branch_instance_ids(self, branch_id: str) -> list:
        details = self.__class__._request("GET", f"projects/{self.project_id}/branches/{branch_id}")
        instances = details.get("status", {}).get("instances", [])
        return [inst["id"] for inst in instances]

    def _get_branch_disk_bytes(self, branch_id: str) -> int:
        instance_ids = self._get_branch_instance_ids(branch_id)
        if not instance_ids:
            return 0
        end = datetime.now(timezone.utc)
        start = end - timedelta(minutes=5)
        payload = {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "metric": "disk",
            "instances": instance_ids,
            "aggregations": ["max"],
        }
        response = self.__class__._request(
            "POST", f"projects/{self.project_id}/branches/{branch_id}/metrics", json=payload
        )
        max_bytes = 0
        for series in response.get("series", []):
            for point in series.get("values", []):
                max_bytes = max(max_bytes, point.get("value", 0))
        return int(max_bytes)

    def _storage_bytes(self) -> int:
        """Sum of the ``disk`` metric over all branches. This is the
        logical per-instance size, so shared copy-on-write blocks are
        counted once per branch."""
        try:
            return sum(
                self._get_branch_disk_bytes(bid)
                for _, (bid, _) in self._get_xata_branches().items()
            )
        except Exception as e:
            print(f"Warning: Could not get Xata storage metrics: {e}")
            return 0

    # ------------------------------------------------------------------
    # Async
    # ------------------------------------------------------------------

    async def open_async_pool(self, size: int) -> None:
        from psycopg_pool import AsyncConnectionPool

        if self._current_ref is None:
            raise ValueError("Not connected to a branch")
        self._pool_branch = self._current_ref.branch
        self.async_pool = AsyncConnectionPool(
            self._uri_for(self._pool_branch),
            min_size=size, max_size=size, kwargs={"autocommit": True}, open=False,
        )
        await self.async_pool.open(wait=True)

    async def _connect_impl_async(self, conn, ref: Ref):
        if ref.branch == self._pool_branch:
            return conn
        import psycopg

        return await psycopg.AsyncConnection.connect(
            self._uri_for(ref.branch), autocommit=True
        )
