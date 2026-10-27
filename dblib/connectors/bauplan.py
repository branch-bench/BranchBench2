from __future__ import annotations

from importlib import metadata
from pathlib import Path

from dblib import bauplan
from dblib.bauplan import BauplanToolSuite
from dblib.connector import Connector, DatabaseHandle
from dblib.target import PreparedTarget, SuiteRequest


class Bauplan(Connector):
    """Bauplan lakehouse branches over S3: backend bauplan."""

    environment = ("BAUPLAN_", "AWS_")
    needs_psql = False
    version_sql = ""

    def scope(self) -> dict:
        return {
            "provider": "bauplan",
            "username": bauplan.BAUPLAN_USERNAME,
            "source_ref": bauplan.BAUPLAN_SOURCE_REF,
        }

    def preflight(self) -> None:
        if not bauplan.BAUPLAN_S3_URI.startswith("s3://"):
            raise RuntimeError("BAUPLAN_S3_URI must be set to s3://<bucket>/<prefix>")
        bauplan.new_client().info()

    def provision(self, db: str) -> DatabaseHandle:
        client = bauplan.new_client()
        root = bauplan.root_branch(db, client)
        client.create_branch(root, from_ref=bauplan.BAUPLAN_SOURCE_REF)
        try:
            client.create_namespace(bauplan.namespace(db), root)
        except BaseException:
            client.delete_branch(root, if_exists=True)
            raise
        return DatabaseHandle(db, bauplan.MAIN_BRANCH)

    def load(self, database: DatabaseHandle, dump: Path) -> None:
        raise NotImplementedError(
            "The Bauplan backend does not load SQL dumps yet; use generated data"
        )

    def prepare_target(self, database: DatabaseHandle) -> PreparedTarget:
        def open_suite(request: SuiteRequest):
            return BauplanToolSuite.init_for_bench(
                request.result_collector,
                database.name,
                request.initial_ref,
                False,  # table metadata calls are too expensive per operation
            )

        return PreparedTarget(database.root_ref, BauplanToolSuite, open_suite)

    def delete(self, db: str) -> None:
        bauplan.drop_database(db)
        if self.exists(db):
            raise RuntimeError(f"Bauplan run '{db}' still exists after deletion")

    def exists(self, db: str) -> bool:
        client = bauplan.new_client()
        root = bauplan.root_branch(db, client)
        branches = client.get_branches(name=root)
        if any(branch.name == root or branch.name.startswith(root + "__")
               for branch in branches):
            return True
        return any(tag.name.startswith(root + ".")
                   for tag in client.get_tags(filter_by_name=root))

    def version(self) -> str:
        return metadata.version("bauplan")


CONNECTOR = Bauplan
