"""Neon backend (Lakebase Postgres on Neon).

Branches are Neon branches, each with one read-write compute; the benchmark
runs in a project of its own (``BackendManager`` creates and deletes it).
A branch is its own Postgres endpoint, so connecting to one means opening
a new connection.

Neon versions storage but has no commits, diff or merge. The verbs the
API lacks are built from what it does have:

* branching at a point in time (``parent_lsn``) and ``restore`` to an LSN;
* ``postgres_fdw``, with which one branch's compute can read another's
  tables, and plain Postgres on each compute.

    branch     POST /branches with parent_id (+ parent_lsn for a commit
               ref) and a read-write compute; the verb waits for Neon's
               operations to finish, then writes a fork row on the branch
    commit     a row in the branch's _bb_commits table plus the branch's
               pg_current_wal_flush_lsn() as the commit's point in time
    log        SELECT FROM _bb_commits
    diff       per-table, per-key-bucket row hashes on each side (local
               to each compute); only the differing buckets are pulled over
               postgres_fdw into temp tables and compared locally
    merge      SQL three-way merge on the target's compute: base = a
               temporary branch at the source's fork LSN (created and
               timed inside the verb), theirs = the source branch over
               postgres_fdw; only the key buckets theirs changed are
               pulled into temp tables; conflicts per on_conflict as on
               the other backends
    rebase     the same three-way merge of the upstream into the branch,
               after which a rebase row moves the branch's base to the
               upstream's LSN so the merge back only sees newer changes
    revert     temporary branches at the commit's LSN and its predecessor,
               inverse delta applied locally
    reset      POST /branches/{id}/restore to the commit's LSN (native;
               Neon keeps the pre-restore state in a backup branch that
               cannot be deleted before the project)
    delete     DELETE /branches/{id} (a branch with children is deferred
               and retried once they are gone)

Quotas are treated as capacity. Neon counts *active computes* per project
(20 on the Launch plan, the default branch exempt), not branches. The
process keeps a registry of the computes it activated and, before it
needs one more above ``NEON_ACTIVE_BUDGET``, suspends the least recently
used compute no connection is using; a suspended compute resumes on the
next connect, which lands in CONNECT. All API calls go
through one process-wide token bucket sized under the documented 700
requests/minute, with waits recorded as API_RETRY_WAIT rows; 429/423/503
responses are retried as the backstop.

Multi-branch queries (``exec(mode="multi")``) are declared unsupported:
Neon has no multi-branch query semantics, and routing the scenarios'
cross-branch reads over postgres_fdw would be slower than running them on
each branch, which the scenarios do when the flag is off.
"""

import os
import random
import re
import threading
import time
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse

import psycopg2
import requests
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from dblib.db_api import DBToolSuite, Ref, Session
from dblib.pg_merge import META_PREFIX, PgMergeMixin, _q, empty_diff
from dblib import result_pb2 as rslt
import dblib.result_collector as rc

API_KEY = os.environ.get("NEON_API_KEY_ORG", "")
NEON_API_BASE_URL = "https://console.neon.tech/api/v2/"
NEON_REGION = os.environ.get("NEON_REGION", "aws-us-east-1")
# PostgreSQL major version of new projects (Neon supports 14-18).
NEON_PG_VERSION = int(os.environ.get("NEON_PG_VERSION", "18"))
# Fixed compute size for every endpoint (min = max), in CU.
NEON_COMPUTE_CU = float(os.environ.get("NEON_COMPUTE_CU", "2"))
# How long Neon keeps history (commit refs are LSNs inside this window).
NEON_HISTORY_RETENTION_SEC = int(os.environ.get("NEON_HISTORY_RETENTION_SEC", str(2 * 86400)))
# Scale-to-zero of branch computes: -1 (never) by default. Neon's
# autosuspend counts only running statements as activity, not open
# connections: with the plan default (5 min on Launch, its minimum) a
# branch compute that a worker holds but has not queried for 5 minutes
# (waiting for a lock, in a quiet review phase, or read over postgres_fdw
# by a merge that is busy elsewhere) is suspended under its connections,
# which then fail with "SSL connection has been closed unexpectedly".
# The active-compute budget below suspends computes explicitly instead.
NEON_BRANCH_SUSPEND_TIMEOUT_SEC = int(os.environ.get("NEON_BRANCH_SUSPEND_TIMEOUT_SEC", "-1"))
# Active computes the process keeps at most (the plan allows 20 besides the
# default branch): when a new compute is needed above this, the least
# recently used idle one is suspended first.
NEON_ACTIVE_BUDGET = int(os.environ.get("NEON_ACTIVE_BUDGET", "18"))
# Also suspend a branch's compute whenever the connection moves off it
# (off by default: the budget above only suspends when room is needed).
NEON_SUSPEND_ON_SWITCH = os.environ.get("NEON_SUSPEND_ON_SWITCH", "0").lower() in ("1", "true", "yes")
# Token bucket for the API: requests per minute and burst.
NEON_API_RATE_PER_MIN = float(os.environ.get("NEON_API_RATE_PER_MIN", "600"))
NEON_API_BURST = int(os.environ.get("NEON_API_BURST", "20"))
# Projects whose name starts with this are the benchmark's and are deleted
# before a run (stale ones from interrupted runs) and after it.
PROJECT_PREFIX = "project_macro_"

_LSN_RE = re.compile(r"^[0-9A-Fa-f]+/[0-9A-Fa-f]+$")
_OP_POLL_SEC = 0.5
_OP_TIMEOUT_SEC = 600
_ACTIVE_LIMIT_MSG = "concurrently active endpoints"

COMMITS_TABLE = META_PREFIX + "commits"
_COMMITS_DDL = f"""
CREATE TABLE IF NOT EXISTS "{COMMITS_TABLE}" (
    seq BIGINT PRIMARY KEY,
    id TEXT NOT NULL,
    kind TEXT NOT NULL,
    message TEXT,
    lsn TEXT,
    branch_id TEXT,
    prev_lsn TEXT,
    prev_branch_id TEXT,
    ts TIMESTAMPTZ DEFAULT now()
)
"""

def lsn_to_int(lsn: str) -> int:
    hi, lo = lsn.split("/")
    return (int(hi, 16) << 32) + int(lo, 16)


# ----------------------------------------------------------------------------
# API plumbing: token bucket, retries, operations
# ----------------------------------------------------------------------------


class _TokenBucket:
    """Process-wide limiter: tokens refill at ``rate`` per second up to
    ``burst``; acquire() blocks until one is available and returns the
    seconds waited."""

    def __init__(self, rate_per_sec: float, burst: int):
        self.rate = rate_per_sec
        self.burst = burst
        self.tokens = float(burst)
        self.updated = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self) -> float:
        waited = 0.0
        while True:
            with self.lock:
                now = time.monotonic()
                self.tokens = min(self.burst, self.tokens + (now - self.updated) * self.rate)
                self.updated = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return waited
                need = (1 - self.tokens) / self.rate
            time.sleep(need)
            waited += need


_BUCKET = _TokenBucket(NEON_API_RATE_PER_MIN / 60.0, NEON_API_BURST)


class NeonAPIError(RuntimeError):
    def __init__(self, status: int, text: str):
        super().__init__(f"HTTP {status}: {text[:300]}")
        self.status = status
        self.text = text


def api_request(method: str, endpoint: str, record=None, max_retries: int = 8, **kwargs):
    """One Neon API call through the token bucket, retrying 408/423/429/5xx
    (and connection errors) with Retry-After or jittered backoff.
    ``record(seconds)`` is told about every wait."""
    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {API_KEY}"
    headers["Accept"] = "application/json"
    headers["Content-Type"] = "application/json"
    kwargs.setdefault("timeout", 60)
    for attempt in range(max_retries):
        waited = _BUCKET.acquire()
        if waited:
            NeonToolSuite.observe("api_bucket_wait_sec", waited)
        try:
            r = requests.request(method, NEON_API_BASE_URL + endpoint, headers=headers, **kwargs)
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                raise
            delay = 0.5 * (2 ** attempt) * (0.5 + random.random())
            if record:
                record(delay)
            time.sleep(delay)
            continue
        if r.status_code < 400:
            if r.status_code == 204 or not r.content:
                return {}
            return r.json()
        if r.status_code in (408, 423, 429, 500, 502, 503, 504) and attempt < max_retries - 1:
            retry_after = r.headers.get("Retry-After")
            try:
                delay = float(retry_after) if retry_after else 0.5 * (2 ** attempt)
            except ValueError:
                delay = 0.5 * (2 ** attempt)
            delay *= 0.5 + random.random()
            if record:
                record(delay)
            time.sleep(delay)
            continue
        raise NeonAPIError(r.status_code, r.text)
    raise NeonAPIError(0, "retries exhausted")


def wait_operations(project_id: str, operations, record=None, timeout: float = _OP_TIMEOUT_SEC,
                    check=None) -> float:
    """Poll each operation until it finishes; returns seconds spent.
    ``check()`` runs before each poll (a cancellation hook)."""
    t0 = time.perf_counter()
    for op in operations or []:
        op_id = op.get("id") if isinstance(op, dict) else op
        if not op_id:
            continue
        if isinstance(op, dict) and op.get("status") == "finished":
            continue
        while True:
            if check is not None:
                check()
            o = api_request("GET", f"projects/{project_id}/operations/{op_id}", record=record)["operation"]
            status = o.get("status")
            if status == "finished":
                break
            if status in ("failed", "error", "cancelled", "skipped"):
                raise RuntimeError(f"Neon operation {o.get('action')} {status}: {o.get('error')}")
            if time.perf_counter() - t0 > timeout:
                raise RuntimeError(f"Neon operation {o.get('action')} did not finish in {timeout}s")
            time.sleep(_OP_POLL_SEC)
    return time.perf_counter() - t0


def _endpoint_spec(suspend_timeout: int) -> dict:
    return {
        "type": "read_write",
        "autoscaling_limit_min_cu": NEON_COMPUTE_CU,
        "autoscaling_limit_max_cu": NEON_COMPUTE_CU,
        "suspend_timeout_seconds": suspend_timeout,
    }


# ----------------------------------------------------------------------------
# Project helpers (BackendManager)
# ----------------------------------------------------------------------------


def create_project(project_name: str) -> dict:
    """A project with a fixed-size, never-suspending default compute and
    a history window long enough for a run's commit refs."""
    body = {
        "project": {
            "name": project_name,
            "pg_version": NEON_PG_VERSION,
            "region_id": NEON_REGION,
            "history_retention_seconds": NEON_HISTORY_RETENTION_SEC,
            "default_endpoint_settings": {
                "autoscaling_limit_min_cu": NEON_COMPUTE_CU,
                "autoscaling_limit_max_cu": NEON_COMPUTE_CU,
                "suspend_timeout_seconds": -1,
            },
        }
    }
    resp = api_request("POST", "projects", json=body)
    try:
        wait_operations(resp["project"]["id"], resp.get("operations"))
    except BaseException:
        api_request("DELETE", f"projects/{resp['project']['id']}")
        raise
    return resp


def delete_project(project_id: str) -> None:
    api_request("DELETE", f"projects/{project_id}")
    print(f"Neon project {project_id} deleted.")


def list_projects() -> list:
    out, cursor = [], None
    while True:
        q = "projects?limit=100" + (f"&cursor={cursor}" if cursor else "")
        resp = api_request("GET", q)
        out.extend(resp.get("projects", []))
        cursor = (resp.get("pagination") or {}).get("cursor")
        if not cursor or not resp.get("projects"):
            break
    return out


def delete_stale_projects(prefix: str = PROJECT_PREFIX) -> list:
    """Delete every project of this org whose name starts with ``prefix``
    (leftovers of interrupted runs)."""
    deleted = []
    for p in list_projects():
        if str(p.get("name", "")).startswith(prefix):
            try:
                delete_project(p["id"])
                deleted.append(p["name"])
            except Exception as e:
                print(f"Warning: could not delete stale Neon project {p['name']}: {e}")
    return deleted


def connection_uri(project_id: str, branch_id: str, db_name: str, role: str = "neondb_owner",
                   record=None, max_retries: int = 20) -> str:
    """Connection URI of a branch; 404 (branch not visible yet) is retried."""
    # Direct (unpooled) endpoint: the pooler runs PgBouncer in transaction
    # mode, where session state (temp tables, the search_path postgres_fdw
    # sets on its remote sessions) leaks between clients.
    endpoint = (f"projects/{project_id}/connection_uri?branch_id={branch_id}"
                f"&database_name={db_name}&role_name={role}&pooled=false")
    for attempt in range(max_retries):
        try:
            return api_request("GET", endpoint, record=record)["uri"]
        except NeonAPIError as e:
            if e.status == 404 and attempt < max_retries - 1:
                time.sleep(0.5)
                continue
            raise
    raise RuntimeError("connection URI unavailable")


def get_project_branches(project_id: str) -> dict:
    return api_request("GET", f"projects/{project_id}/branches")


# ----------------------------------------------------------------------------


class _State:
    """A ref resolved to a branch (id, name) and, for a commit, its LSN."""

    __slots__ = ("branch", "branch_id", "lsn", "seq")

    def __init__(self, branch: str, branch_id: str, lsn: str = None, seq: int = None):
        self.branch = branch
        self.branch_id = branch_id
        self.lsn = lsn
        self.seq = seq


class NeonToolSuite(PgMergeMixin, DBToolSuite):
    BACKEND_NAME = "neon"
    STORAGE_SCOPE = "project"   # root logical size + child branches' WAL since fork
    SUPPORTS_COMMIT_REFS = True
    SCHEMA_DIALECT = "postgres"
    SUPPORTS_MULTI_REF_EXEC = False
    IMPLEMENTATION = {
        "branch": "native",
        "commit": "native",
        "diff": "simulated",
        "log": "simulated",
        "merge": "simulated",
        "rebase": "simulated",
        "revert": "simulated",
        "reset": "native",
        "delete": "native",
        "commit_refs": "native",
        "multi_ref_exec": "unsupported",
    }
    IMPLEMENTATION_NOTES = {
        "branch": "POST /branches with parent_id (+ parent_lsn for a commit) and a 2 CU compute "
                  "that never auto-suspends, waiting for the operations; computes are suspended "
                  "by the process's active-compute budget (LRU) and on delete (CONNECT carries "
                  "the resume)",
        "commit": "_bb_commits row + pg_current_wal_flush_lsn() as the point in time",
        "log": "SELECT from the _bb_commits bookkeeping table",
        "diff": "per-table row hashes on each compute, differing tables pulled over postgres_fdw "
                "and compared locally",
        "merge": "SQL three-way merge on the target's compute; base = temporary branch at the "
                 "fork LSN (created inside the verb), theirs pulled over postgres_fdw",
        "rebase": "three-way merge of the upstream into the branch; the base moves to the "
                  "upstream's LSN",
        "revert": "temporary branches at the commit's LSN and its predecessor; inverse delta "
                  "applied locally",
        "reset": "POST /branches/{id}/restore to the commit's LSN (preserve_under_name leaves an "
                 "undeletable backup branch)",
        "delete": "DELETE /branches/{id}; a branch with children is deferred until they are gone",
        "commit_refs": "parent_lsn / restore to LSN; exec on a commit ref uses a temporary branch",
        "multi_ref_exec": "not provided: no multi-branch query semantics; the scenarios fall back "
                          "to running cross-branch reads once per branch",
    }

    # Facts observed during a run (shared by every suite of the process):
    # throttling, compute-limit waits, deferred deletes.
    OBSERVED: dict = {}
    _OBS_LOCK = threading.Lock()
    # Computes this process activated: branch id -> last use (monotonic),
    # and how many open connections each one has. Shared by every suite.
    _ACTIVE: dict = {}
    _HOLDS: dict = {}
    # Storage accounting, project-wide: branch id -> parent LSN (None for a
    # root) and branch id -> last flush LSN seen (see _storage_bytes).
    _LINEAGE: dict = {}
    _HEAD_LSN: dict = {}
    _SIZE_LOCK = threading.Lock()
    _SUSPENDING: set = set()     # branch ids with a suspend request in flight
    _ACT_LOCK = threading.Lock()

    @classmethod
    def observe(cls, key: str, amount=1) -> None:
        with cls._OBS_LOCK:
            cls.OBSERVED[key] = cls.OBSERVED.get(key, 0) + amount

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    def get_default_connection_uri(cls) -> str:
        return f"neon://{NEON_REGION}"

    @classmethod
    def create_neon_project(cls, project_name: str) -> dict:
        return create_project(project_name)

    @classmethod
    def delete_project(cls, project_id: str, timeout: int = 60) -> None:
        try:
            delete_project(project_id)
        except Exception as e:
            print(f"Warning: Neon project deletion failed: {e}")

    @classmethod
    def get_project_branches(cls, project_id: str) -> dict:
        return get_project_branches(project_id)

    @classmethod
    def _get_neon_connection_uri(cls, project_id: str, branch_id: str, db_name: str) -> str:
        return connection_uri(project_id, branch_id, db_name)

    @classmethod
    def init_for_bench(cls, result_collector: rc.ResultCollector, project_id: str,
                       branch_id: str, branch_name: str, database_name: str,
                       measure_storage: bool = False):
        return cls(None, result_collector, project_id, branch_name, branch_id,
                   database_name, measure_storage)

    def __init__(self, connection, result_collector: rc.ResultCollector, project_id: str,
                 branch_name: str, branch_id: str, database_name: str = None,
                 measure_storage: bool = False):
        super().__init__(connection, result_collector, measure_storage)
        self.project_id = project_id
        self.db_name = database_name
        self.default_branch = branch_name
        self._branches: dict = {}        # name -> {"id", "parent_id", "parent_lsn"}
        self._uris: dict = {}            # branch id -> connection uri
        self._endpoints: dict = {}       # branch id -> endpoint id
        self._temp: dict = {}            # (branch id, lsn) -> temp branch name
        self._deferred_deletes: list = []
        self._fdw_ready: set = set()     # branch ids whose db has postgres_fdw
        self._conn_branch_id = None      # branch whose compute self.conn is on
        self._register(branch_name, branch_id, None, None)
        self._ensure_meta(branch_name)
        if connection is None:
            self._connect_impl(Ref(branch_name))
            self._current_ref = Ref(branch_name)

    # ------------------------------------------------------------------
    # API helpers that record waits on this suite's collector
    # ------------------------------------------------------------------

    def _wait_row(self, seconds: float, label: str = "api_wait") -> None:
        self._check_cancel()
        self.observe(f"{label}_sec", seconds)
        self.observe(f"{label}_count", 1)
        now = time.time()
        self.result_collector.emit(
            rslt.OpType.API_RETRY_WAIT, status=rslt.OpStatus.OK, latency=seconds,
            start_time=now - seconds, end_time=now, ref="", label=label, num_keys_touched=0)

    def _api(self, method: str, endpoint: str, **kwargs):
        return api_request(method, endpoint, record=self._wait_row, **kwargs)

    def _wait(self, operations) -> float:
        return wait_operations(self.project_id, operations, record=self._wait_row,
                               check=self._check_cancel)

    # ------------------------------------------------------------------
    # Branch registry
    # ------------------------------------------------------------------

    def _register(self, name: str, branch_id: str, parent_id, parent_lsn) -> None:
        self._branches[name] = {"id": branch_id, "parent_id": parent_id, "parent_lsn": parent_lsn}
        with type(self)._SIZE_LOCK:
            type(self)._LINEAGE[branch_id] = lsn_to_int(parent_lsn) if parent_lsn else None

    def _note_head(self, conn, branch_id: str) -> None:
        """Remember the branch's flush LSN (its WAL since the fork is its
        storage); best effort, on a connection that is still open."""
        if conn is None or getattr(conn, "closed", 0) or not branch_id:
            return
        try:
            lsn = self._exec(conn, "SELECT pg_current_wal_flush_lsn()::text")[0][0]
        except Exception:
            return
        with type(self)._SIZE_LOCK:
            type(self)._HEAD_LSN[branch_id] = lsn_to_int(lsn)

    def _refresh_branches(self) -> None:
        resp = self._api("GET", f"projects/{self.project_id}/branches")
        for b in resp.get("branches", []):
            self._register(b["name"], b["id"], b.get("parent_id"), b.get("parent_lsn"))

    def _branch_info(self, name: str) -> dict:
        info = self._branches.get(name)
        if info is None:
            self._refresh_branches()
            info = self._branches.get(name)
        if info is None:
            raise ValueError(f"Branch '{name}' does not exist")
        return info

    def _branch_id(self, name: str) -> str:
        return self._branch_info(name)["id"]

    def _uri(self, branch_id: str) -> str:
        uri = self._uris.get(branch_id)
        if not uri:
            uri = connection_uri(self.project_id, branch_id, self.db_name, record=self._wait_row)
            self._uris[branch_id] = uri
        return uri

    def _endpoint_id(self, branch_id: str):
        ep = self._endpoints.get(branch_id)
        if ep is None:
            resp = self._api("GET", f"projects/{self.project_id}/branches/{branch_id}/endpoints")
            for e in resp.get("endpoints", []):
                if e.get("type") == "read_write":
                    ep = e["id"]
                    break
            self._endpoints[branch_id] = ep or ""
        return ep or None

    def _suspend(self, branch_id: str) -> None:
        """Suspend a branch's compute (never the default branch's)."""
        if branch_id == self._branch_id(self.default_branch):
            return
        ep = self._endpoint_id(branch_id)
        if not ep:
            return
        cls = type(self)
        with cls._ACT_LOCK:
            # Chosen as a victim while unheld, but a thread may have taken
            # it since: a suspend would close that connection.
            if cls._HOLDS.get(branch_id, 0) > 0 or branch_id in cls._SUSPENDING:
                self.observe("suspend_skipped_held")
                return
            last = cls._ACTIVE.pop(branch_id, None)
            cls._SUSPENDING.add(branch_id)
        try:
            self._api("POST", f"projects/{self.project_id}/endpoints/{ep}/suspend")
        except NeonAPIError as e:
            if e.status not in (409, 422, 423):   # already suspended / in transition
                raise
            # In transition (most likely starting for another connection):
            # still active as far as the quota goes, so it stays a candidate
            # for the next time room is needed.
            self.observe("suspend_in_transition")
            with cls._ACT_LOCK:
                if last is not None:
                    cls._ACTIVE.setdefault(branch_id, last)
        finally:
            with cls._ACT_LOCK:
                cls._SUSPENDING.discard(branch_id)

    def _make_room(self, target: str) -> None:
        """Keep the process under NEON_ACTIVE_BUDGET active computes: before
        ``target`` is (re)activated, suspend least-recently-used computes
        that no connection holds. The default branch is exempt."""
        default_id = self._branches[self.default_branch]["id"]
        if target == default_id:
            return
        cls = type(self)
        with cls._ACT_LOCK:
            if target in cls._ACTIVE:
                cls._ACTIVE[target] = time.monotonic()
                return
            victims = []
            now = time.monotonic()
            while len(cls._ACTIVE) + 1 > NEON_ACTIVE_BUDGET:
                # Computes nobody holds, those untouched for 20 s first (a
                # worker switching branches releases one for a moment);
                # when that is not enough, the least recently used of the
                # rest: a resume later is the cost of the quota.
                unheld = [b for b in cls._ACTIVE if cls._HOLDS.get(b, 0) <= 0 and b != default_id]
                if not unheld:
                    break
                idle = [b for b in unheld if now - cls._ACTIVE[b] > 20.0]
                lru = min(idle or unheld, key=lambda b: cls._ACTIVE[b])
                del cls._ACTIVE[lru]
                victims.append(lru)
            cls._ACTIVE[target] = time.monotonic()
        for b in victims:
            try:
                self._suspend(b)
                self.observe("lru_suspends")
            except Exception as e:
                print(f"Warning: could not suspend compute of {b}: {e}")

    def _reconcile_actives(self) -> int:
        """Rebuild the registry from Neon's own view of the project's
        computes and suspend active ones no connection holds, least
        recently used first, until the budget has room. Called when a
        connect is refused for the active-compute limit: the registry may
        have missed a compute (a suspend refused in transition, a compute
        another path started), and a compute used a moment ago is still
        a legitimate victim when the alternative is waiting. Returns the
        number of suspends issued."""
        resp = self._api("GET", f"projects/{self.project_id}/endpoints")
        default_id = self._branches[self.default_branch]["id"]
        cls = type(self)
        victims = []
        with cls._ACT_LOCK:
            now = time.monotonic()
            seen = set()
            for e in resp.get("endpoints", []):
                if e.get("type") != "read_write":
                    continue
                b = e.get("branch_id")
                self._endpoints.setdefault(b, e["id"])
                # A compute on its way to idle (our suspend in flight) is
                # not active; one on its way up is.
                if (e.get("pending_state") or e.get("current_state")) == "active":
                    seen.add(b)
                    if b != default_id and b not in cls._ACTIVE:
                        cls._ACTIVE[b] = now - 60.0
                        self.observe("active_compute_unregistered")
            for b in [b for b in cls._ACTIVE if b not in seen]:
                del cls._ACTIVE[b]
            unheld = sorted((b for b in cls._ACTIVE if cls._HOLDS.get(b, 0) <= 0 and b != default_id),
                            key=lambda b: cls._ACTIVE[b])
            while len(cls._ACTIVE) + 1 > NEON_ACTIVE_BUDGET and unheld:
                b = unheld.pop(0)
                del cls._ACTIVE[b]
                victims.append(b)
        for b in victims:
            try:
                self._suspend(b)
                self.observe("limit_suspends")
            except Exception as e:
                print(f"Warning: could not suspend compute of {b}: {e}")
        return len(victims)

    def _reserve(self, branch_id: str, n: int = 1) -> None:
        """Hold (or release, n=-1) a compute without a connection, so the
        LRU never suspends it mid-verb."""
        cls = type(self)
        with cls._ACT_LOCK:
            cls._HOLDS[branch_id] = max(0, cls._HOLDS.get(branch_id, 0) + n)

    def _close(self, conn) -> None:
        """Close a connection opened by _open and release its hold."""
        if conn is None:
            return
        bid = getattr(self, "_conn_owner", {}).pop(id(conn), None)
        if bid:
            self._note_head(conn, bid)
        try:
            conn.close()
        except Exception:
            pass
        if bid:
            with type(self)._ACT_LOCK:
                type(self)._HOLDS[bid] = max(0, type(self)._HOLDS.get(bid, 0) - 1)

    def _open(self, branch_id: str, max_wait: float = 300.0):
        """A new autocommit connection to the branch's compute, counted in
        the process's active-compute registry; a connect refused for the
        active-compute limit (or failing while the compute changes state)
        is retried, with the waits recorded."""
        uri = self._uri(branch_id)
        cls = type(self)
        self._make_room(branch_id)
        # Held from here on: a compute being connected to is not a victim
        # for the LRU or the quota reconcile of another thread. A suspend
        # already in flight for it is allowed to land first, so the new
        # connection is not the one it closes.
        deadline = time.monotonic() + 30.0
        while True:
            with cls._ACT_LOCK:
                if branch_id not in cls._SUSPENDING or time.monotonic() > deadline:
                    cls._HOLDS[branch_id] = cls._HOLDS.get(branch_id, 0) + 1
                    break
            time.sleep(0.05)
        t0 = time.perf_counter()
        attempt = 0
        try:
            conn = self._connect_loop(uri, branch_id, max_wait, t0)
        except BaseException:
            with cls._ACT_LOCK:
                cls._HOLDS[branch_id] = max(0, cls._HOLDS.get(branch_id, 0) - 1)
            raise
        with cls._ACT_LOCK:
            if branch_id != self._branches[self.default_branch]["id"]:
                cls._ACTIVE[branch_id] = time.monotonic()
        if not hasattr(self, "_conn_owner"):
            self._conn_owner = {}
        self._conn_owner[id(conn)] = branch_id
        return conn

    def _connect_loop(self, uri: str, branch_id: str, max_wait: float, t0: float):
        attempt = 0
        while True:
            self._check_cancel()
            try:
                conn = psycopg2.connect(uri, connect_timeout=30)
                conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                return conn
            except psycopg2.OperationalError as e:
                msg = str(e)
                attempt += 1
                if time.perf_counter() - t0 > max_wait:
                    raise
                if _ACTIVE_LIMIT_MSG in msg:
                    self.observe("active_compute_limit_hits")
                    # Make room from Neon's view of what is active, then retry.
                    try:
                        freed = self._reconcile_actives()
                    except Exception as re:
                        freed = 0
                        print(f"Warning: could not reconcile active computes: {re}")
                    delay = (0.5 if freed else 2.0) + random.random()
                    label = "compute_wait"
                else:
                    delay = min(3.0, 0.5 * attempt)
                    label = "connect_retry"
                    self.observe("connect_error: " + " ".join(msg.split())[:70])
                self._wait_row(delay, label)
                time.sleep(delay)

    def list_branches(self) -> list:
        self._refresh_branches()
        return [n for n in self._branches if not n.startswith("tmp_") and "_bk_" not in n]

    # ------------------------------------------------------------------
    # Connection hook
    # ------------------------------------------------------------------

    def _connect_impl(self, ref: Ref) -> None:
        if ref.commit:
            target = self._temp_branch(self._state(ref))
        else:
            target = self._branch_id(ref.branch)
        previous = self._conn_branch_id
        if self.conn:
            self._close(self.conn)
            self.conn = None
        if NEON_SUSPEND_ON_SWITCH and previous and previous != target:
            try:
                self._suspend(previous)
            except Exception as e:
                print(f"Warning: could not suspend compute of {previous}: {e}")
        self.conn = self._open(target)
        self._conn_branch_id = target

    def _connect(self, ref: Ref, exec_id: int, timed: bool, label: str, fallback: bool):
        """A branch restore or a compute restart closes existing
        connections; a dead connection is reopened on the next exec and
        recorded as a CONNECT like any other switch."""
        if self.conn is not None and getattr(self.conn, "closed", 0):
            self._current_ref = None
            self._conn_branch_id = None
        return super()._connect(ref, exec_id, timed, label, fallback)

    def _on(self, ref: Ref) -> str:
        """Point self.conn at the branch head; returns its id."""
        if self.conn is not None and getattr(self.conn, "closed", 0):
            self._current_ref = None
            self._conn_branch_id = None
        if self._current_ref != Ref(ref.branch) or self.conn is None:
            self._connect_impl(Ref(ref.branch))
            self._current_ref = Ref(ref.branch)
        return self._branch_id(ref.branch)

    def _temp_branch(self, st: _State) -> str:
        """A branch at ``st``'s LSN for reading a commit; cached, suspended
        between uses, deleted when the connection closes."""
        key = (st.branch_id, st.lsn)
        if key in self._temp:
            return self._temp[key]
        name = f"tmp_{lsn_to_int(st.lsn):x}_{os.getpid() % 10000}_{threading.get_ident() % 10000}_{len(self._temp)}"
        resp = self._api("POST", f"projects/{self.project_id}/branches", json={
            "endpoints": [_endpoint_spec(NEON_BRANCH_SUSPEND_TIMEOUT_SEC)],
            "branch": {"name": name, "parent_id": st.branch_id, "parent_lsn": st.lsn},
        })
        self._wait(resp.get("operations"))
        b = resp["branch"]
        self._register(name, b["id"], b.get("parent_id"), b.get("parent_lsn"))
        for e in resp.get("endpoints", []):
            if e.get("type") == "read_write":
                self._endpoints[b["id"]] = e["id"]
        self._temp[key] = b["id"]
        return b["id"]

    # ------------------------------------------------------------------
    # Bookkeeping (commit log)
    # ------------------------------------------------------------------

    _LOG_COLS = ("seq", "id", "kind", "message", "lsn", "branch_id", "prev_lsn", "prev_branch_id", "ts")

    def _exec(self, conn, sql: str, args=None):
        self._check_cancel()
        with conn.cursor() as cur:
            cur.execute(sql, args)
            if cur.description is not None:
                return cur.fetchall()
            return None

    def _flush_lsn(self, conn) -> str:
        return self._exec(conn, "SELECT pg_current_wal_flush_lsn()::text")[0][0]

    def _ensure_meta(self, branch: str) -> None:
        conn = self._open(self._branch_id(branch))
        try:
            self._exec(conn, _COMMITS_DDL)
            n = self._exec(conn, f'SELECT COUNT(*) FROM "{COMMITS_TABLE}"')[0][0]
            if not n:
                self._add_commit(conn, self._branch_id(branch), "root", "initial state")
        finally:
            self._close(conn)

    def _add_commit(self, conn, branch_id: str, kind: str, message: str, commit_id: str = None,
                    prev_lsn: str = None, prev_branch_id: str = None, lsn: str = None,
                    seq: int = None) -> tuple:
        """Append a log row; unless ``lsn`` is given, the branch's flush LSN
        right after the row is stored as the commit's point in time (so a
        branch created there carries the row). Returns (id, lsn)."""
        seq = seq or time.time_ns()
        commit_id = commit_id or f"{seq:x}"
        for _ in range(8):
            try:
                self._exec(conn, f'INSERT INTO "{COMMITS_TABLE}" (seq, id, kind, message, lsn, branch_id, '
                                 "prev_lsn, prev_branch_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                           (seq, commit_id, kind, message or "", lsn, branch_id, prev_lsn, prev_branch_id))
                break
            except psycopg2.IntegrityError:
                seq += 1
        else:
            raise RuntimeError("could not record commit")
        if lsn is None:
            lsn = self._flush_lsn(conn)
            self._exec(conn, f'UPDATE "{COMMITS_TABLE}" SET lsn = %s WHERE seq = %s', (lsn, seq))
        return commit_id, lsn

    def _log_rows(self, conn, limit: int = None, max_seq: int = None) -> list:
        sql = f'SELECT {", ".join(self._LOG_COLS)} FROM "{COMMITS_TABLE}"'
        args = []
        if max_seq is not None:
            sql += " WHERE seq <= %s"
            args.append(int(max_seq))
        sql += " ORDER BY seq DESC"
        if limit:
            sql += f" LIMIT {int(limit)}"
        rows = self._exec(conn, sql, tuple(args) if args else None) or []
        return [dict(zip(self._LOG_COLS, r)) for r in rows]

    def _find_commit(self, conn, commit_id: str) -> dict:
        rows = self._exec(conn, f'SELECT {", ".join(self._LOG_COLS)} FROM "{COMMITS_TABLE}" '
                                "WHERE id = %s ORDER BY seq DESC LIMIT 1", (str(commit_id),))
        if not rows:
            raise ValueError(f"unknown commit '{commit_id}'")
        return dict(zip(self._LOG_COLS, rows[0]))

    def _base_row(self, conn) -> dict:
        rows = self._exec(conn, f'SELECT {", ".join(self._LOG_COLS)} FROM "{COMMITS_TABLE}" '
                                "WHERE kind IN ('fork', 'rebase', 'root') ORDER BY seq DESC LIMIT 1")
        return dict(zip(self._LOG_COLS, rows[0]))

    def _state(self, ref: Ref) -> _State:
        bid = self._branch_id(ref.branch)
        if not ref.commit:
            return _State(ref.branch, bid)
        conn = self._conn_for(bid)
        try:
            row = self._find_commit(conn, ref.commit)
        finally:
            self._release(conn)
        if not row["lsn"]:
            raise ValueError(f"commit '{ref.commit}' has no LSN recorded on {ref.branch}")
        return _State(ref.branch, row["branch_id"] or bid, row["lsn"], int(row["seq"]))

    def _conn_for(self, branch_id: str):
        """self.conn when it is on that branch, else a short-lived one
        (callers close it through _release)."""
        if self.conn is not None and self._conn_branch_id == branch_id:
            return self.conn
        return self._open(branch_id)

    def _release(self, conn) -> None:
        if conn is not None and conn is not self.conn:
            self._close(conn)

    # ------------------------------------------------------------------
    # postgres_fdw: read another branch's tables from this connection
    # ------------------------------------------------------------------

    def _fdw_schema(self, conn, local_branch_id: str, remote_branch_id: str, tables: list) -> str:
        """Import ``tables`` of the remote branch into a schema on this
        connection's database; returns the schema name."""
        if local_branch_id not in self._fdw_ready:
            self._exec(conn, "CREATE EXTENSION IF NOT EXISTS postgres_fdw")
            self._fdw_ready.add(local_branch_id)
        pr = urlparse(self._uri(remote_branch_id))
        srv = f"bb_srv_{remote_branch_id.replace('-', '_')}"
        schema = f"bb_{remote_branch_id.replace('-', '_')}"
        self._exec(conn, f"CREATE SERVER IF NOT EXISTS {_q(srv)} FOREIGN DATA WRAPPER postgres_fdw "
                         f"OPTIONS (host '{pr.hostname}', dbname '{self.db_name}', port '5432', "
                         f"sslmode 'require', fetch_size '10000', "
                         f"options '-c idle_in_transaction_session_timeout=0 -c statement_timeout=0')")
        self._exec(conn, f"CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER SERVER {_q(srv)} "
                         f"OPTIONS (user '{pr.username}', password '{pr.password}')")
        self._exec(conn, f"DROP SCHEMA IF EXISTS {_q(schema)} CASCADE")
        self._exec(conn, f"CREATE SCHEMA {_q(schema)}")
        if tables:
            names = ", ".join(_q(t) for t in tables)
            self._exec(conn, f"IMPORT FOREIGN SCHEMA public LIMIT TO ({names}) FROM SERVER {_q(srv)} INTO {_q(schema)}")
        return schema

    def _fdw_cleanup(self, conn, remote_branch_id: str) -> None:
        srv = f"bb_srv_{remote_branch_id.replace('-', '_')}"
        schema = f"bb_{remote_branch_id.replace('-', '_')}"
        for sql in (f"DROP SCHEMA IF EXISTS {_q(schema)} CASCADE", f"DROP SERVER IF EXISTS {_q(srv)} CASCADE"):
            try:
                self._exec(conn, sql)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Verbs
    # ------------------------------------------------------------------

    def _storage_bytes(self) -> int:
        """The project's storage as Neon defines it: the default branch's
        logical size plus, for every other branch, the changes it
        accumulated since its fork point. The logical size is
        ``pg_cluster_size()`` of the ``neon`` extension, the pageserver's
        figure read on demand over a dedicated connection to the root (the
        API's ``logical_size`` is the same number published minutes late;
        its project totals are deprecated and always 0). A child's changes
        are its WAL since its parent LSN, from the flush LSN noted whenever
        a connection to it closes (and for the current connection, now),
        so no other compute is touched. Neon caps a child at its logical
        size; the cap is not applied here, nor are the backup branches a
        restore leaves, and a deleted branch drops out at once although
        Neon keeps its data for the retention window."""
        if getattr(self, "_closed", False):
            return 0  # the run's project is gone (after cleanup)
        root_id = self._branch_id(self.default_branch)
        root = 0
        for attempt in range(2):
            try:
                if getattr(self, "_size_conn", None) is None:
                    self._size_conn = self._open(root_id, max_wait=15)
                    try:
                        self._exec(self._size_conn, "CREATE EXTENSION IF NOT EXISTS neon")
                        self._cluster_size_ok = True
                    except Exception as e:
                        print(f"Warning: neon extension unavailable, storage falls back to "
                              f"pg_database_size: {e}")
                        self._cluster_size_ok = False
                if self.conn is not None and self._conn_branch_id and self._conn_branch_id != root_id:
                    self._note_head(self.conn, self._conn_branch_id)
                q = ("SELECT pg_cluster_size()" if getattr(self, "_cluster_size_ok", False)
                     else "SELECT pg_database_size(current_database())")
                root = int(self._exec(self._size_conn, q)[0][0])
                break
            except Exception:
                try:
                    if getattr(self, "_size_conn", None) is not None:
                        self._close(self._size_conn)
                except Exception:
                    pass
                self._size_conn = None
                if attempt == 1:
                    raise
        cls = type(self)
        with cls._SIZE_LOCK:
            children = sum(max(0, cls._HEAD_LSN.get(b, p) - p)
                           for b, p in cls._LINEAGE.items() if b != root_id and p is not None)
        return root + children

    def _branch_impl(self, name: str, from_ref: Ref) -> None:
        st = self._state(from_ref)
        body = {"endpoints": [_endpoint_spec(NEON_BRANCH_SUSPEND_TIMEOUT_SEC)],
                "branch": {"name": name, "parent_id": st.branch_id}}
        if st.lsn:
            body["branch"]["parent_lsn"] = st.lsn
        resp = self._api("POST", f"projects/{self.project_id}/branches", json=body)
        self._wait(resp.get("operations"))
        b = resp["branch"]
        self._register(name, b["id"], b.get("parent_id"), b.get("parent_lsn"))
        for e in resp.get("endpoints", []):
            if e.get("type") == "read_write":
                self._endpoints[b["id"]] = e["id"]
        # The fork row: the branch's own LSN right after creation, and the
        # parent point it was cloned from (the base of later merges).
        conn = self._open(b["id"])
        try:
            if st.seq is not None:
                self._exec(conn, f'DELETE FROM "{COMMITS_TABLE}" WHERE seq > %s', (st.seq,))
                # The commit's row predates its own LSN (written after the
                # row); give the copy the value the parent has.
                self._exec(conn, f'UPDATE "{COMMITS_TABLE}" SET lsn = %s WHERE seq = %s', (st.lsn, st.seq))
            self._add_commit(conn, b["id"], "fork", f"fork from {from_ref}",
                             prev_lsn=b.get("parent_lsn") or st.lsn, prev_branch_id=st.branch_id)
        finally:
            self._close(conn)
        if NEON_SUSPEND_ON_SWITCH:
            self._suspend(b["id"])

    def _commit_impl(self, ref: Ref, message: str) -> str:
        bid = self._on(ref)
        head = self._log_rows(self.conn, limit=1)[0]
        commit_id, _ = self._add_commit(self.conn, bid, "commit", message,
                                        prev_lsn=head["lsn"], prev_branch_id=head["branch_id"])
        return commit_id

    def _log_impl(self, ref: Ref, limit: int) -> list:
        bid = self._branch_id(ref.branch)
        conn = self._conn_for(bid)
        try:
            max_seq = int(self._find_commit(conn, ref.commit)["seq"]) if ref.commit else None
            return [{"commit_hash": r["id"], "kind": r["kind"], "committer": "neon", "email": "",
                     "date": r["ts"], "message": r["message"], "lsn": r["lsn"]}
                    for r in self._log_rows(conn, limit=limit, max_seq=max_seq)]
        finally:
            self._release(conn)

    # -- diff ----------------------------------------------------------------

    def _reading_branch(self, st: _State) -> str:
        """Branch id whose compute holds ``st``: the branch itself for a
        head, a temporary branch for a commit."""
        if st.lsn and not self._is_head_lsn(st):
            return self._temp_branch(st)
        return st.branch_id

    def _is_head_lsn(self, st: _State) -> bool:
        """True when nothing was written on the branch since the commit
        (its flush LSN still equals the commit's)."""
        conn = self._conn_for(st.branch_id)
        try:
            return self._flush_lsn(conn) == st.lsn
        finally:
            self._release(conn)

    def _diff_impl(self, ref_a: Ref, ref_b: Ref) -> dict:
        a_id = self._reading_branch(self._state(ref_a))
        b_id = self._reading_branch(self._state(ref_b))
        ca, cb = self._open(a_id), (self._conn_for(b_id) if b_id != a_id else None)
        try:
            if cb is None:
                return empty_diff()
            return self._diff_tables(ca, a_id, cb, b_id)
        finally:
            self._close(ca)
            self._release(cb)

    def _merge_base(self, ours_id: str, theirs_conn, theirs_id: str, ours_conn) -> _State:
        """The fork/rebase point of whichever side descends from the other."""
        rt = self._base_row(theirs_conn)
        ro = self._base_row(ours_conn)
        cands = []
        if rt["prev_branch_id"] == ours_id and rt["kind"] in ("fork", "rebase"):
            cands.append((rt, _State(None, rt["prev_branch_id"], rt["prev_lsn"], int(rt["seq"]))))
        if ro["prev_branch_id"] == theirs_id and ro["kind"] in ("fork", "rebase"):
            cands.append((ro, _State(None, ro["prev_branch_id"], ro["prev_lsn"], int(ro["seq"]))))
        if not cands:
            return None
        return max(cands, key=lambda c: int(c[0]["seq"]))[1]

    def _copy_log(self, into_conn, from_conn, max_seq=None) -> None:
        rows = self._log_rows(from_conn)
        have = {r["id"] for r in self._log_rows(into_conn)}
        for r in reversed(rows):
            if r["kind"] not in ("commit", "merge") or r["id"] in have:
                continue
            if max_seq is not None and int(r["seq"]) > max_seq:
                continue
            self._add_commit(into_conn, r["branch_id"], r["kind"], r["message"], commit_id=r["id"],
                             prev_lsn=r["prev_lsn"], prev_branch_id=r["prev_branch_id"], lsn=r["lsn"],
                             seq=int(r["seq"]))

    def _merge_impl(self, into: Ref, source: Ref, message: str, on_conflict="ours") -> dict:
        src = self._state(source)
        theirs_id = self._reading_branch(src)
        self._reserve(theirs_id)
        try:
            ours_id = self._on(into)
            if theirs_id == ours_id:
                raise ValueError(f"Cannot merge branch '{source.branch}' into itself")
            return self._merge_reserved(into, source, message, on_conflict, src, theirs_id, ours_id)
        finally:
            self._reserve(theirs_id, -1)

    def _merge_reserved(self, into, source, message, on_conflict, src, theirs_id, ours_id) -> dict:
        ours = self.conn
        theirs_conn = self._open(theirs_id)
        base_conn, base_id = None, None
        try:
            base = self._merge_base(ours_id, theirs_conn, src.branch_id, ours)
            info = {"two_way": base is None}
            if base is not None:
                base_id = self._reading_branch(base)
                base_conn = self._open(base_id)
            else:
                print(f"Warning: {source.branch} and {into.branch} share no fork point: merging two-way")
                base_id = theirs_id
                base_conn = self._open(theirs_id)
                self._exec(base_conn, "SET search_path TO public")
            schema = self._reconcile_schema(ours, ours_id, theirs_conn, theirs_id)
            keep_ours = callable(on_conflict) or on_conflict == "ours"
            if base is None:
                # No common ancestor: everything theirs has is "their change".
                tw_info, conflicts = {"conflicts": 0, "conflict_tables": [], "ours_changes": 0,
                                      "theirs_changes": 0, "merged_tables": [], "skipped_tables": {}}, []
                for t in sorted(set(self._tables(ours)) & set(self._tables(theirs_conn)) - set(schema["added_tables"])):
                    cols_o = [c[0] for c in self._columns(ours, t)]
                    cols = [c for c in cols_o if c in {c2[0] for c2 in self._columns(theirs_conn, t)}]
                    pk = [c for c in self._pk(ours, t) if c in cols]
                    s_t = self._fdw_schema(ours, ours_id, theirs_id, [t])
                    if pk:
                        self._exec(ours, self._upsert(_q(t), cols, pk, f"SELECT {', '.join(_q(c) for c in cols)} FROM {_q(s_t)}.{_q(t)}")
                                   if not keep_ours else
                                   f"INSERT INTO {_q(t)} ({', '.join(_q(c) for c in cols)}) SELECT {', '.join(_q(c) for c in cols)} FROM {_q(s_t)}.{_q(t)} ON CONFLICT DO NOTHING")
                    tw_info["merged_tables"].append(t)
                info.update(tw_info)
            else:
                tw_info, conflicts = self._three_way(ours, ours_id, theirs_conn, theirs_id, base_conn, base_id,
                                                     on_conflict, into, schema["added_tables"], keep_ours)
                info.update(tw_info)
            if conflicts:
                info["resolved"] = "custom"
                info["resolution"] = on_conflict(self._conflict_session(into), conflicts)
            elif info.get("conflicts"):
                info["resolved"] = on_conflict
            head = self._log_rows(ours, limit=1)[0]  # before the source's commits join the log
            self._copy_log(ours, theirs_conn, max_seq=src.seq)
            src_head = self._find_commit(theirs_conn, source.commit) if src.lsn else self._log_rows(theirs_conn, limit=1)[0]
            commit_id = src_head["id"] if src_head["kind"] == "commit" else None
            info["hash"], _ = self._add_commit(ours, ours_id, "merge", message or f"Merge {source.branch} into {into.branch}",
                                               commit_id=commit_id, prev_lsn=head["lsn"], prev_branch_id=head["branch_id"])
            info["fast_forward"] = info.get("ours_changes", 0) == 0 or info.get("theirs_changes", 0) == 0
            info["schema_changes"] = schema
            return info
        finally:
            self._close(theirs_conn)
            if base_conn is not None:
                self._close(base_conn)
            for bid in {theirs_id, base_id}:
                if bid:
                    self._fdw_cleanup(ours, bid)

    def _rebase_impl(self, ref: Ref, onto: Ref, on_conflict="ours") -> dict:
        ours_id = self._on(ref)
        ours = self.conn
        up = self._state(onto)
        if up.branch_id == ours_id:
            raise ValueError(f"Cannot rebase branch '{ref.branch}' onto itself")
        up_conn = self._open(up.branch_id)
        base_conn, base_id = None, None
        theirs_conn, theirs_id = None, None
        try:
            # Read the upstream at one point (a temporary branch at its LSN,
            # since a spine under load keeps moving): that LSN becomes the
            # branch's new base.
            up_lsn = up.lsn or self._flush_lsn(up_conn)
            theirs_id = self._temp_branch(_State(up.branch, up.branch_id, up_lsn))
            theirs_conn = self._open(theirs_id)
            base = self._merge_base(ours_id, up_conn, up.branch_id, ours)
            if base is None:
                print(f"Warning: {ref.branch} and {onto.branch} share no fork point: upstream conflicts not detected")
            info = {"up_to_date": False}
            base_id = self._reading_branch(base) if base is not None else None
            base_conn = self._open(base_id) if base_id else None
            schema = self._reconcile_schema(ours, ours_id, theirs_conn, theirs_id)
            # git: "ours" is the upstream, "theirs" the branch's own commits.
            keep_branch = (on_conflict == "theirs")
            if base_conn is not None:
                tw_info, conflicts = self._three_way(ours, ours_id, theirs_conn, theirs_id, base_conn, base_id,
                                                     on_conflict, ref, schema["added_tables"], keep_ours=keep_branch or callable(on_conflict))
            else:
                tw_info, conflicts = {"conflicts": 0, "conflict_tables": [], "ours_changes": 0, "theirs_changes": 0,
                                      "merged_tables": [], "skipped_tables": {}}, []
            # Swap sides for the callable: our_* = upstream, their_* = branch.
            for cf in conflicts:
                for row in cf["rows"]:
                    for c in list(row):
                        if c.startswith("our_") and not c.endswith("_diff_type"):
                            row[c], row["their_" + c[4:]] = row["their_" + c[4:]], row[c]
                    row["our_diff_type"], row["their_diff_type"] = row["their_diff_type"], row["our_diff_type"]
            info.update(tw_info)
            if conflicts:
                info["resolved"] = "custom"
                info["resolution"] = on_conflict(self._conflict_session(ref), conflicts)
            elif info.get("conflicts"):
                info["resolved"] = on_conflict
            info["up_to_date"] = info.get("theirs_changes", 0) == 0 and not schema["added_tables"] and not schema["added_columns"]
            self._copy_log(ours, up_conn, max_seq=up.seq)
            self._add_commit(ours, ours_id, "rebase", f"rebase onto {onto}", prev_lsn=up_lsn,
                             prev_branch_id=up.branch_id)
            info["schema_changes"] = schema
            return info
        finally:
            self._close(up_conn)
            if theirs_conn is not None:
                self._close(theirs_conn)
            if base_conn is not None:
                self._close(base_conn)
            for bid in {theirs_id, base_id}:
                if bid:
                    self._fdw_cleanup(ours, bid)

    # -- reset / revert ------------------------------------------------------

    def _ancestor_holding(self, branch_id: str, lsn: str) -> str:
        """The branch, or the nearest ancestor, whose timeline contains
        ``lsn``: walk up while the LSN is older than the branch's fork
        point (its parent_lsn)."""
        by_id = {info["id"]: info for info in self._branches.values()}
        target = lsn_to_int(lsn)
        cur = branch_id
        while True:
            info = by_id.get(cur)
            if not info or not info.get("parent_id") or not info.get("parent_lsn"):
                return cur
            if target >= lsn_to_int(info["parent_lsn"]):
                return cur
            cur = info["parent_id"]

    def _reset_impl(self, ref: Ref, to: str) -> None:
        bid = self._on(ref)
        row = self._find_commit(self.conn, to)
        if not row["lsn"]:
            raise ValueError(f"commit {to} has no LSN")
        # A restore with preserve_under_name makes the backup branch the
        # parent of the restored branch at the restore LSN, so a later
        # reset to an older commit must name the ancestor whose timeline
        # holds that LSN (Neon refuses "LSN is older than parent" from the
        # branch itself).
        self._refresh_branches()
        src = self._ancestor_holding(row["branch_id"] or bid, row["lsn"])
        n = len([k for k in self._branches if k.startswith(f"{ref.branch}_bk_")])
        body = {"source_branch_id": src, "source_lsn": row["lsn"],
                "preserve_under_name": f"{ref.branch}_bk_{n + 1}_{int(time.time()) % 100000}"}
        resp = self._api("POST", f"projects/{self.project_id}/branches/{bid}/restore", json=body)
        self._wait(resp.get("operations"))
        self._refresh_branches()
        # The compute restarts: reconnect, then cut the log at the commit.
        if self.conn is not None:
            self._close(self.conn)
            self.conn = None
        self._current_ref = None
        self._conn_branch_id = None
        self._on(ref)
        self._exec(self.conn, f'DELETE FROM "{COMMITS_TABLE}" WHERE seq > %s', (int(row["seq"]),))
        self._exec(self.conn, f'UPDATE "{COMMITS_TABLE}" SET lsn = %s WHERE seq = %s', (row["lsn"], int(row["seq"])))

    def _revert_impl(self, ref: Ref, commit: str) -> None:
        bid = self._on(ref)
        ours = self.conn
        row = self._find_commit(ours, commit)
        if not row["prev_lsn"]:
            raise ValueError(f"commit {commit} has no predecessor to revert to")
        after = _State(None, row["branch_id"] or bid, row["lsn"])
        before = _State(None, row["prev_branch_id"] or after.branch_id, row["prev_lsn"])
        a_id, b_id = self._reading_branch(after), self._reading_branch(before)
        ca, cb = self._open(a_id), self._open(b_id)
        try:
            if not self._revert_apply(ours, bid, ca, a_id, cb, b_id):
                return
            head = self._log_rows(ours, limit=1)[0]
            self._add_commit(ours, bid, "commit", f"revert {commit}", prev_lsn=head["lsn"], prev_branch_id=head["branch_id"])
        finally:
            self._close(ca)
            self._close(cb)

    # -- delete --------------------------------------------------------------

    def _delete_branch(self, branch_id: str) -> None:
        with type(self)._SIZE_LOCK:
            type(self)._LINEAGE.pop(branch_id, None)
            type(self)._HEAD_LSN.pop(branch_id, None)
        resp = self._api("DELETE", f"projects/{self.project_id}/branches/{branch_id}")
        try:
            self._wait(resp.get("operations"))
        except RuntimeError as e:
            # A compute suspend that failed alongside the delete is harmless
            # once the branch is gone.
            try:
                self._api("GET", f"projects/{self.project_id}/branches/{branch_id}")
            except NeonAPIError as g:
                if g.status == 404:
                    print(f"Warning: delete of {branch_id}: {e} (branch is gone)")
                else:
                    raise e
            else:
                raise
        with type(self)._ACT_LOCK:
            type(self)._ACTIVE.pop(branch_id, None)
            type(self)._HOLDS.pop(branch_id, None)

    def _flush_deferred(self) -> None:
        still = []
        for name, bid in self._deferred_deletes:
            try:
                self._delete_branch(bid)
                self._branches.pop(name, None)
            except NeonAPIError as e:
                if "child" in e.text.lower():
                    still.append((name, bid))
                else:
                    print(f"Warning: deferred delete of {name} failed: {e}")
        self._deferred_deletes = still

    def _delete_impl(self, ref: Ref) -> None:
        bid = self._branch_id(ref.branch)
        if self._current_ref and self._current_ref.branch == ref.branch:
            if self.conn:
                self._close(self.conn)
                self.conn = None
            self._current_ref = None
            self._conn_branch_id = None
        try:
            self._delete_branch(bid)
        except NeonAPIError as e:
            if "child" in e.text.lower():
                # Neon refuses to delete a branch with children: retried once
                # they are gone (later deletes, close_connection).
                self._deferred_deletes.append((ref.branch, bid))
                self.observe("deferred_deletes")
                return
            raise
        self._branches.pop(ref.branch, None)
        self._uris.pop(bid, None)
        self._endpoints.pop(bid, None)
        if self._deferred_deletes:
            self._flush_deferred()

    def _qualified_table(self, ref: Ref, table: str) -> str:
        raise self._unsupported("multi_ref_exec", "no multi-branch query semantics; "
                                "cross-branch reads run once per branch")

    def close_connection(self) -> None:
        self._closed = True
        if getattr(self, "_size_conn", None) is not None:
            try:
                self._close(self._size_conn)
            except Exception:
                pass
            self._size_conn = None
        for key, bid in list(self._temp.items()):
            try:
                self._delete_branch(bid)
            except Exception as e:
                print(f"Warning: could not delete temporary branch {bid}: {e}")
        self._temp.clear()
        if self._deferred_deletes:
            self._flush_deferred()
        if self.conn is not None:
            self._close(self.conn)
            self.conn = None
        super().close_connection()

    # ------------------------------------------------------------------
    # Async: a psycopg pool on the branch the suite is on when the pool is
    # opened; a script on another branch gets its own connection.
    # ------------------------------------------------------------------

    async def open_async_pool(self, size: int) -> None:
        from psycopg_pool import AsyncConnectionPool

        if self._current_ref is None:
            raise ValueError("Not connected to a branch")
        self._pool_branch = self._current_ref.branch
        self.async_pool = AsyncConnectionPool(
            self._uri(self._branch_id(self._pool_branch)),
            min_size=size, max_size=size, kwargs={"autocommit": True}, open=False,
        )
        await self.async_pool.open(wait=True)

    async def _connect_impl_async(self, conn, ref: Ref):
        if ref.branch == self._pool_branch:
            return conn
        import psycopg

        return await psycopg.AsyncConnection.connect(
            self._uri(self._branch_id(ref.branch)), autocommit=True
        )

    # ------------------------------------------------------------------
    # Consumption metrics (macrobench accounting)
    # ------------------------------------------------------------------

    @classmethod
    def get_consumption_metrics(cls, project_id, org_id=None):
        """All consumption entries for a project in a 2-day window around
        now (hourly granularity), plus a summary of the most recent entry.
        Returns None on failure."""
        org_id = org_id or os.environ.get("NEON_ORG_ID", "")
        if not org_id:
            try:
                orgs = api_request("GET", "users/me/organizations").get("organizations", [])
                org_id = orgs[0]["id"] if orgs else ""
            except Exception:
                org_id = ""
        if not org_id:
            print("Warning: NEON_ORG_ID not set, skipping consumption metrics")
            return None
        now = datetime.now(timezone.utc)
        start = (now - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        end = (now + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        endpoint = (
            f"consumption_history/v2/projects"
            f"?org_id={org_id}&project_ids={project_id}"
            f"&from={start}&to={end}&granularity=hourly"
            f"&metrics=root_branch_bytes_month,child_branch_bytes_month,"
            f"compute_unit_seconds,public_network_transfer_bytes,"
            f"private_network_transfer_bytes"
        )
        try:
            resp = api_request("GET", endpoint)
        except Exception as e:
            print(f"Warning: consumption metrics request failed: {e}")
            return None
        try:
            projects = resp.get("projects", [])
            if not projects:
                return None
            all_entries = []
            for period in projects[0].get("periods", []):
                for entry in period.get("consumption", []):
                    metrics = entry.get("metrics", [])
                    if metrics:
                        d = {"period_id": period.get("period_id", ""), "timestamp": entry.get("timestamp", "")}
                        for m in metrics:
                            d[m["metric_name"]] = m["value"]
                        all_entries.append(d)
            if not all_entries:
                return None
            most_recent = all_entries[-1]
            return {"all_metrics": all_entries, "count": len(all_entries),
                    "summary": {k: v for k, v in most_recent.items() if k not in ("period_id", "timestamp")}}
        except (KeyError, IndexError) as e:
            print(f"Warning: could not parse consumption metrics: {e}")
            return None
