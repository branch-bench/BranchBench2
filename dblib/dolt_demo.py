"""Alpha demo database implemented with Dolt's MySQL protocol."""

from __future__ import annotations

from dblib.dolt_mysql import DoltMySQLToolSuite


class DoltDemoToolSuite(DoltMySQLToolSuite):
    """Dolt-backed adapter with a distinct demo result identity."""

    BACKEND_NAME = "dolt_demo"
