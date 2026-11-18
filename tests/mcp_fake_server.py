"""Runs dblib.mcp_server's CLI with dblib.open_backend swapped for the
sqlite fake (tests/test_mcp_server.py starts it as a subprocess). Setup
prints to stdout on purpose, as real backends do, to check that nothing
but protocol reaches the client."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import dblib  # noqa: E402
from dblib import mcp_server  # noqa: E402
from tests.fake_backend import FakeSuite  # noqa: E402


class FakeHandle:
    default_branch = "main"

    def __init__(self, *a, **kw):
        print("Database created successfully.")          # noise on stdout, like real setup
        os.write(1, b"raw fd-1 noise\n")

    def suite(self, collector=None, measure_storage=False):
        s = FakeSuite(collector)
        s.exec(["CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
                "INSERT INTO t VALUES (1, 'a'), (2, 'b')"], timed=False)
        return s

    def close(self):
        print("Database deleted.")


dblib.open_backend = FakeHandle

if __name__ == "__main__":
    sys.exit(mcp_server.main())
