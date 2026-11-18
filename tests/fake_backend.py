"""An in-memory sqlite backend for testing DBToolSuite without a server.

Each branch is its own sqlite database. Commits are snapshots, so the
fake supports commit, log, diff (row-count based), reset and delete, and
leaves merge, rebase and revert unsupported to exercise that path.
"""

import hashlib
import sqlite3
import time
from contextlib import asynccontextmanager, contextmanager

from dblib.db_api import DBToolSuite, Ref


class _Cursor:
    """sqlite3 cursor proxy: context manager, and execute(sql, None) works."""

    def __init__(self, cur):
        self._cur = cur

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self._cur.close()

    def execute(self, q, vars=None):
        # Scripts use DB-API "%s" placeholders (psycopg2/pymysql); sqlite
        # wants "?".
        if vars is not None and "%s" in q:
            q = q.replace("%s", "?")
        self._cur.execute(q, vars if vars is not None else ())

    def fetchall(self):
        return self._cur.fetchall()

    def fetchone(self):
        return self._cur.fetchone()

    @property
    def description(self):
        return self._cur.description

    @property
    def rowcount(self):
        return self._cur.rowcount

    def close(self):
        self._cur.close()


class Conn:
    """sqlite3 connection whose cursor() works as a context manager."""

    def __init__(self, raw=None):
        self.raw = raw or sqlite3.connect(":memory:", check_same_thread=False)
        self.raw.isolation_level = None  # autocommit
        self.closed = False

    def cursor(self):
        return _Cursor(self.raw.cursor())

    def close(self):
        self.closed = True

    def copy(self) -> "Conn":
        dst = Conn()
        self.raw.backup(dst.raw)
        return dst


class _AsyncCursor:
    def __init__(self, cur):
        self._cur = cur
        self.description = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        self._cur.close()

    async def execute(self, q, vars=None):
        if vars is not None and "%s" in q:
            q = q.replace("%s", "?")
        self._cur.execute(q, vars if vars is not None else ())
        self.description = self._cur.description

    async def fetchall(self):
        return self._cur.fetchall()


class AsyncConn:
    def __init__(self, conn: Conn):
        self.conn = conn
        self.closed = False

    def cursor(self):
        return _AsyncCursor(self.conn.raw.cursor())

    async def close(self):
        self.closed = True


class FakePool:
    def __init__(self, suite):
        self.suite = suite
        self.closed = False
        self.borrowed = 0

    @asynccontextmanager
    async def connection(self):
        self.borrowed += 1
        yield AsyncConn(self.suite.branches[self.suite.pool_branch])

    async def close(self):
        self.closed = True


class FakeSuite(DBToolSuite):
    BACKEND_NAME = "fake"
    SUPPORTS_COMMIT_REFS = True
    SUPPORTS_MULTI_REF_EXEC = False

    def __init__(self, result_collector=None, measure_storage=False):
        self.branches = {"main": Conn()}
        self.commits = {}  # hash -> (branch, Conn snapshot, message, ts)
        self.history = {"main": []}  # branch -> [hash, ...] newest last
        self.pool_branch = "main"
        super().__init__(self.branches["main"], result_collector,
                         measure_storage)
        self._connect_impl(Ref("main"))
        self._current_ref = Ref("main")

    # --- hooks ---------------------------------------------------------

    def _storage_bytes(self) -> int:
        total = 0
        for c in self.branches.values():
            cur = c.raw.cursor()
            cur.execute("PRAGMA page_count")
            pages = cur.fetchone()[0]
            cur.execute("PRAGMA page_size")
            total += pages * cur.fetchone()[0]
        return total

    def _connect_impl(self, ref: Ref) -> None:
        if ref.commit:
            if ref.commit not in self.commits:
                raise ValueError(f"unknown commit {ref.commit}")
            self.conn = self.commits[ref.commit][1].copy()
            return
        if ref.branch not in self.branches:
            raise ValueError(f"unknown branch {ref.branch}")
        self.conn = self.branches[ref.branch]

    def _schema_at(self, ref: Ref) -> dict:
        """sqlite's catalog: table list plus PRAGMA table_info per table."""
        self._connect_impl(ref)
        out = {}
        with self.conn.cursor() as cur:
            cur.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE '_bb_%'")
            tables = [r[0] for r in cur.fetchall()]
            for t in tables:
                cur.execute(f"PRAGMA table_info({t})")
                out[t] = {r[1]: str(r[2]).lower() for r in cur.fetchall()}
        return out

    def _branch_impl(self, name: str, from_ref: Ref) -> None:
        if name in self.branches:
            raise ValueError(f"branch {name} exists")
        src = (self.commits[from_ref.commit][1] if from_ref.commit
               else self.branches[from_ref.branch])
        self.branches[name] = src.copy()
        self.history[name] = list(self.history.get(from_ref.branch, []))

    def _commit_impl(self, ref: Ref, message: str) -> str:
        snap = self.branches[ref.branch].copy()
        h = hashlib.sha1(f"{ref.branch}{message}{time.time()}".encode()).hexdigest()[:12]
        self.commits[h] = (ref.branch, snap, message, time.time())
        self.history[ref.branch].append(h)
        return h

    def _log_impl(self, ref: Ref, limit: int) -> list:
        hashes = list(reversed(self.history[ref.branch]))[:limit]
        return [{"hash": h, "message": self.commits[h][2]} for h in hashes]

    def _diff_impl(self, a: Ref, b: Ref):
        """Row counts per state plus rows_added / rows_deleted /
        rows_modified over every user table, keyed by primary key (whole
        row for a keyless table)."""
        def state(ref):
            c = (self.commits[ref.commit][1] if ref.commit
                 else self.branches[ref.branch])
            cur = c.raw.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type = 'table' "
                        "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '\\_bb\\_%' ESCAPE '\\'")
            out = {}
            for (name,) in cur.fetchall():
                cur.execute(f'PRAGMA table_info("{name}")')
                cols = cur.fetchall()
                pk = [i for i, col in enumerate(cols) if col[5]]
                cur.execute(f'SELECT * FROM "{name}"')
                rows = cur.fetchall()
                out[name] = {(tuple(r[i] for i in pk) if pk else r): r for r in rows}
            return out
        sa, sb = state(a), state(b)
        added = deleted = modified = 0
        for t in set(sa) | set(sb):
            ra, rb = sa.get(t, {}), sb.get(t, {})
            added += len(set(rb) - set(ra))
            deleted += len(set(ra) - set(rb))
            modified += sum(1 for k in set(ra) & set(rb) if ra[k] != rb[k])
        return {"rows_a": sum(len(v) for v in sa.values()), "rows_b": sum(len(v) for v in sb.values()),
                "rows_added": added, "rows_deleted": deleted, "rows_modified": modified}

    def _reset_impl(self, ref: Ref, to: str) -> None:
        self.branches[ref.branch] = self.commits[to][1].copy()
        if self._current_ref and self._current_ref.branch == ref.branch:
            self.conn = self.branches[ref.branch]

    def _delete_impl(self, ref: Ref) -> None:
        if ref.branch == "main":
            raise ValueError("cannot delete the default branch")
        if self._current_ref and self._current_ref.branch == ref.branch:
            # Like Dolt: move the connection off the branch first.
            self._connect_impl(Ref("main"))
            self._current_ref = Ref("main")
        del self.branches[ref.branch]

    def list_branches(self):
        return list(self.branches)

    # --- async ---------------------------------------------------------

    async def open_async_pool(self, size: int) -> None:
        self.async_pool = FakePool(self)

    async def _connect_impl_async(self, conn, ref: Ref):
        if ref.branch == self.pool_branch and not ref.commit:
            return conn
        target = (self.commits[ref.commit][1].copy() if ref.commit
                  else self.branches[ref.branch])
        return AsyncConn(target)
