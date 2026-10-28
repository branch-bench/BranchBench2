"""Lifecycle connector for IK-DB."""

from ..dolt_mysql.connector import Dolt
from . import adapter as ik_db


class IKDB(Dolt):
    """IK-DB connector: Dolt's lifecycle with the IK-DB adapter."""

    suite_type = ik_db.IKDBToolSuite


CONNECTOR = IKDB
