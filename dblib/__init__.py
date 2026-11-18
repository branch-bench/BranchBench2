"""Git-like API over branchable databases.

    import dblib
    suite = dblib.open_suite("dolt", "mydb")        # see dblib/backends.py

``dblib.db_api`` defines the API (DBToolSuite, Ref, OpResult). Each
database system is a package in ``dblib/systems/<key>/`` (adapter and
connector), imported only when that backend is opened.
"""

from dblib.backends import BackendHandle, open_backend, open_suite  # noqa: F401
from dblib.connector import connector_names  # noqa: F401
