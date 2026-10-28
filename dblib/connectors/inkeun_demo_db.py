"""Lifecycle connector for the alpha-only inkeun-demo-db."""

from dblib import inkeun_demo_db
from dblib.connectors.dolt_mysql import Dolt


class InkeunDemoDB(Dolt):
    """Mock database connector that uses Dolt as its test implementation."""

    suite_type = inkeun_demo_db.InkeunDemoDBToolSuite


CONNECTOR = InkeunDemoDB
