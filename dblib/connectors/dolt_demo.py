"""Lifecycle connector for the Dolt-backed alpha demo database."""

from dblib import dolt_demo
from dblib.connectors.dolt_mysql import Dolt


class DoltDemo(Dolt):
    """Alpha demo connector that reuses Dolt's database lifecycle."""

    suite_type = dolt_demo.DoltDemoToolSuite


CONNECTOR = DoltDemo
