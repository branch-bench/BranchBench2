"""Alpha demo database implemented with Dolt's MySQL protocol."""

from __future__ import annotations

from ..dolt_mysql.adapter import DoltMySQLToolSuite


class InkeunDemoDBToolSuite(DoltMySQLToolSuite):
    """Mock adapter with a separate alpha result identity."""

    BACKEND_NAME = "inkeun_demo_db"
