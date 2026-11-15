"""IK-DB-test adapter: a MySQL-protocol database with Git-style branches."""

from __future__ import annotations

from ..dolt_mysql.adapter import DoltMySQLToolSuite


class IKDBTestToolSuite(DoltMySQLToolSuite):
    """IK-DB-test speaks Dolt's MySQL dialect, so it reuses that adapter under its own identity."""

    BACKEND_NAME = "ik_db_test"
