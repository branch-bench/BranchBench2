"""Lifecycle connector for the alpha-only inkeun-demo-db."""

from ..dolt_mysql.connector import Dolt
from . import adapter as inkeun_demo_db


class InkeunDemoDB(Dolt):
    """Mock database connector that uses Dolt as its test implementation."""

    suite_type = inkeun_demo_db.InkeunDemoDBToolSuite


CONNECTOR = InkeunDemoDB
