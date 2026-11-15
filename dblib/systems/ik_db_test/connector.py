"""Lifecycle connector for IK-DB-test."""

from ..dolt_mysql.connector import Dolt
from . import adapter as ik_db_test


class IKDBTest(Dolt):
    """IK-DB-test connector: Dolt's lifecycle with the IK-DB-test adapter."""

    suite_type = ik_db_test.IKDBTestToolSuite


CONNECTOR = IKDBTest
