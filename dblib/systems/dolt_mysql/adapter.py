"""Dolt backend over the MySQL wire protocol (dolt sql-server).

Same model as the Doltgres adapter with CALL syntax; multi-ref scripts address
other branches as `<db>/<branch>`.<table>.
"""

import os

import aiomysql
import pymysql
from pymysql.constants import CLIENT

from dblib.db_api import DBToolSuite, Ref
from dblib import util as dbutil, mysql_common
import dblib.result_collector as rc

DOLT_MYSQL_USER = os.environ.get("DOLT_MYSQL_USER", "root")
DOLT_MYSQL_PASSWORD = os.environ.get("DOLT_MYSQL_PASSWORD", "")
DOLT_MYSQL_HOST = os.environ.get("DOLT_MYSQL_HOST") or os.environ.get("DBFORK_SERVER_HOST", "127.0.0.1")
DOLT_MYSQL_PORT = int(os.environ.get("DOLT_MYSQL_PORT", "3306"))
DOLT_MYSQL_DATA_DIR = os.path.expanduser(
    os.environ.get("DOLT_MYSQL_DATA_DIR", "~/dolt/databases")
)


def connect(db_name: str = None, autocommit: bool = True, **kwargs):
    """Open a PyMySQL connection to the Dolt sql-server."""
    return pymysql.connect(
        host=DOLT_MYSQL_HOST,
        port=DOLT_MYSQL_PORT,
        user=DOLT_MYSQL_USER,
        password=DOLT_MYSQL_PASSWORD,
        database=db_name,
        autocommit=autocommit,
        **kwargs,
    )


async def create_pool_async(db_name: str, size: int, autocommit: bool = True):
    """Open an aiomysql pool of `size` connections to the Dolt sql-server."""
    return await aiomysql.create_pool(
        minsize=size,
        maxsize=size,
        host=DOLT_MYSQL_HOST,
        port=DOLT_MYSQL_PORT,
        user=DOLT_MYSQL_USER,
        password=DOLT_MYSQL_PASSWORD,
        db=db_name,
        autocommit=autocommit,
    )


def load_sql_dump(db_name: str, sql_path: str) -> None:
    """Load a SQL file (plain SQL or pg_dump output) into db_name."""
    conn = connect(db_name, client_flag=CLIENT.MULTI_STATEMENTS)
    try:
        mysql_common.load_sql_dump(conn, sql_path)
    finally:
        conn.close()


def setup_database(db_name: str, sql_path: str = None) -> None:
    """(Re)create db_name, load sql_path into it (if given), and commit it
    on main."""
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {db_name};")
            cur.execute(f"CREATE DATABASE {db_name};")
        print("Database created successfully.")
    finally:
        conn.close()

    if not sql_path:
        return
    load_sql_dump(db_name, sql_path)

    conn = connect(db_name)
    try:
        with conn.cursor() as cur:
            cur.execute("CALL DOLT_COMMIT('-Am', 'Load SQL schema');")
        print("Dolt schema committed: Load SQL schema")
    finally:
        conn.close()


def drop_database(db_name: str) -> None:
    """Drop db_name and purge Dolt's copy of dropped databases, which it
    keeps under .dolt_dropped_databases until purged."""
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {db_name};")
            cur.execute("CALL DOLT_PURGE_DROPPED_DATABASES();")
        print(f"Database '{db_name}' deleted successfully.")
    finally:
        conn.close()


class DoltMySQLToolSuite(DBToolSuite):
    BACKEND_NAME = "dolt_mysql"
    SUPPORTS_COMMIT_REFS = True
    SCHEMA_DIALECT = "mysql"
    SUPPORTS_MULTI_REF_EXEC = True

    @classmethod
    def get_default_connection_uri(cls) -> str:
        return cls.get_initial_connection_uri("")

    @classmethod
    def get_initial_connection_uri(cls, db_name: str) -> str:
        return (
            f"mysql://{DOLT_MYSQL_USER}:{DOLT_MYSQL_PASSWORD}"
            f"@{DOLT_MYSQL_HOST}:{DOLT_MYSQL_PORT}/{db_name}"
        )

    @classmethod
    def init_for_bench(
        cls,
        collector: rc.ResultCollector,
        db_name: str,
        default_branch_name: str = "main",
        measure_storage: bool = False,
    ):
        return cls(connect(db_name), collector, db_name, default_branch_name,
                   measure_storage)

    def __init__(
        self,
        connection,
        collector: rc.ResultCollector,
        db_name: str,
        default_branch_name: str = "main",
        measure_storage: bool = False,
    ):
        super().__init__(connection, collector, measure_storage)
        self.db_name = db_name
        self.default_branch = default_branch_name
        self._temp_branches: set = set()
        self._connect_impl(Ref(default_branch_name))
        self._current_ref = Ref(default_branch_name)

    # ------------------------------------------------------------------

    def _checkout(self, branch: str) -> None:
        self._execute("CALL DOLT_CHECKOUT(%s);", (branch,))

    def _temp_branch_name(self, commit: str) -> str:
        return f"_at_{commit[:12]}_t{rc.get_current_thread_id()}"

    def _on(self, ref: Ref) -> None:
        if self._current_ref != Ref(ref.branch):
            self._checkout(ref.branch)
            self._current_ref = Ref(ref.branch)

    @staticmethod
    def _spec(ref: Ref) -> str:
        return ref.commit or ref.branch

    def _rows_as_dicts(self, query: str, vars=None) -> list:
        with self.conn.cursor() as cur:
            cur.execute(query, vars)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def _get_table_columns(self, table_name: str) -> list:
        rows = self._execute(mysql_common.TABLE_COLUMNS_QUERY, (table_name,))
        return mysql_common.normalize_column_types(rows)

    # ------------------------------------------------------------------
    # Hooks
    # ------------------------------------------------------------------

    def _storage_bytes(self) -> int:
        if not self.db_name:
            return 0
        return dbutil.get_directory_size_bytes(
            os.path.join(DOLT_MYSQL_DATA_DIR, self.db_name),
            ssh=dbutil.ssh_target("DOLT_MYSQL_SSH"),
        )

    def _gc_impl(self) -> None:
        # dolt_gc needs a current database.
        self._execute(f"USE `{self.db_name}`;")
        self._execute("CALL DOLT_GC();")

    def _connect_impl(self, ref: Ref) -> None:
        if not ref.commit:
            self._checkout(ref.branch)
            return
        tmp = self._temp_branch_name(ref.commit)
        if tmp in self._temp_branches:
            self._checkout(tmp)
            return
        try:
            self._execute("CALL DOLT_CHECKOUT(%s, '-b', %s);", (ref.commit, tmp))
        except Exception:
            self._checkout(tmp)
        self._temp_branches.add(tmp)

    def _branch_impl(self, name: str, from_ref: Ref) -> None:
        self._execute("CALL DOLT_BRANCH(%s, %s);", (name, self._spec(from_ref)))

    def _commit_impl(self, ref: Ref, message: str) -> str:
        self._on(ref)
        self._execute("CALL DOLT_ADD('-A');")
        try:
            rows = self._execute("CALL DOLT_COMMIT('-m', %s);", (message,))
            return rows[0][0]
        except Exception as e:
            if "nothing to commit" not in str(e).lower():
                raise
            return self._execute("SELECT HASHOF('HEAD');")[0][0]

    def _diff_impl(self, ref_a: Ref, ref_b: Ref) -> dict:
        tables = self._rows_as_dicts(
            "SELECT * FROM DOLT_DIFF_STAT(%s, %s);",
            (self._spec(ref_a), self._spec(ref_b)),
        )
        return {
            "tables": tables,
            "rows_added": sum(t.get("rows_added") or 0 for t in tables),
            "rows_deleted": sum(t.get("rows_deleted") or 0 for t in tables),
            "rows_modified": sum(t.get("rows_modified") or 0 for t in tables),
        }

    SCHEMA_DIFF_HALF = "native"   # dolt_schema_diff

    def _schema_diff_impl(self, ref_a: Ref, ref_b: Ref) -> dict:
        """dolt_schema_diff names the tables whose schema differs (added,
        dropped or changed); the column detail of changed tables comes from
        the catalog at each ref."""
        rows = self._rows_as_dicts("SELECT from_table_name, to_table_name FROM dolt_schema_diff(%s, %s);",
                                   (self._spec(ref_a), self._spec(ref_b)))
        # Doltgres qualifies the names with the schema ("public.t").
        bare = lambda n: str(n).split(".")[-1] if n else n
        added = sorted(bare(r["to_table_name"]) for r in rows if not r["from_table_name"])
        dropped = sorted(bare(r["from_table_name"]) for r in rows if not r["to_table_name"])
        changed = sorted(bare(r["from_table_name"]) for r in rows if r["from_table_name"] and r["to_table_name"])
        out = {"tables_added": added, "tables_dropped": dropped,
               "columns_added": {}, "columns_dropped": {}, "columns_changed": {}}
        if changed:
            a, b = self._schema_at(ref_a), self._schema_at(ref_b)
            cmp = self._compare_schemas({t: a.get(t, {}) for t in changed}, {t: b.get(t, {}) for t in changed})
            for k in ("columns_added", "columns_dropped", "columns_changed"):
                out[k] = cmp[k]
        out["changed"] = bool(rows)
        return out

    def _log_impl(self, ref: Ref, limit: int) -> list:
        return self._rows_as_dicts(
            "SELECT commit_hash, committer, email, date, message "
            "FROM DOLT_LOG(%s) LIMIT %s;",
            (self._spec(ref), int(limit)),
        )

    # ------------------------------------------------------------------
    # Conflicts (merge and rebase)
    # ------------------------------------------------------------------

    def _allow_conflicts(self) -> None:
        """Under autocommit Dolt rolls a conflicting merge back unless the
        session allows committing conflicts; set once per connection."""
        if not getattr(self, "_conflicts_allowed", False):
            self._execute("SET @@dolt_allow_commit_conflicts = 1;")
            self._conflicts_allowed = True

    def _mark_resolved(self, session, table: str) -> None:
        session.sql(f"DELETE FROM dolt_conflicts_{table}")

    def _conflict_tables(self) -> list:
        rows = self._execute('SELECT `table` FROM dolt_conflicts;')
        return [r[0] for r in rows or []]

    def _conflict_row_count(self) -> int:
        """Conflicting rows over every table of the working set: the count
        the other backends report as ``conflicts`` (Dolt's own number is
        per merge or per replayed commit)."""
        rows = self._execute("SELECT COALESCE(SUM(num_conflicts), 0) FROM dolt_conflicts;")
        return int(rows[0][0]) if rows else 0

    def _schema_conflicts(self) -> list:
        rows = self._execute(
            "SELECT table_name, description FROM dolt_schema_conflicts;"
        )
        return [{"table": r[0], "description": r[1]} for r in rows or []]

    def _conflict_rows(self, table: str) -> list:
        """Every conflict row: a resolve callable must see them all, or the
        rows it never saw keep the working set's stale version."""
        return self._rows_as_dicts(f"SELECT * FROM dolt_conflicts_{table};")

    def _resolve_conflicts(self, ref: Ref, on_conflict) -> dict:
        """Resolve the data conflicts in the working set per on_conflict.
        Returns {"conflict_tables", "resolved", "resolution"}."""
        tables = self._conflict_tables()
        info = {"conflict_tables": tables, "resolved": "", "resolution": None,
                "conflict_rows": self._conflict_row_count()}
        if not tables:
            return info
        if callable(on_conflict):
            conflicts = [
                {"table": t, "rows": self._conflict_rows(t)} for t in tables
            ]
            info["resolution"] = on_conflict(self._conflict_session(ref), conflicts)
            info["resolved"] = "custom"
            remaining = self._conflict_tables()
            if remaining:
                for t in remaining:
                    self._resolve_table(t, "--ours", info)
                info["resolved"] = f"custom+ours({','.join(remaining)})"
        else:
            flag = "--theirs" if on_conflict == "theirs" else "--ours"
            for t in tables:
                self._resolve_table(t, flag, info)
            info["resolved"] = on_conflict
        return info

    def _resolve_table(self, table: str, flag: str, info: dict) -> None:
        """dolt_conflicts_resolve. Dolt refuses it when the table's columns
        differ between the sides (e.g. the target added a column since the
        fork); then --ours keeps the working set's rows and --theirs is
        applied by hand from the conflict table."""
        try:
            self._execute("CALL DOLT_CONFLICTS_RESOLVE(%s, %s);", (flag, table))
        except Exception as e:
            if "schema" not in str(e).lower():
                raise
            if flag == "--theirs":
                self._resolve_theirs_by_hand(table)
                info.setdefault("theirs_by_hand", []).append(table)
            else:
                info.setdefault("fallback_ours", []).append(table)
            self._execute(f"DELETE FROM dolt_conflicts_{table}")

    def _resolve_theirs_by_hand(self, table: str) -> None:
        """What --theirs does, for a table whose columns differ between the
        sides: each conflicting row becomes theirs (removed, or every column
        both sides have set to their value); columns only the working set
        has keep their working value."""
        key = [r[0] for r in self._execute(
            "SELECT column_name FROM information_schema.key_column_usage "
            "WHERE table_name = %s AND table_schema = DATABASE() "
            "AND constraint_name = 'PRIMARY' ORDER BY ordinal_position;", (table,))]
        current = {r[0] for r in self._execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = %s AND table_schema = DATABASE();", (table,))}
        if not key:
            raise RuntimeError(f"cannot resolve {table} by hand: no primary key")
        for row in self._rows_as_dicts(f"SELECT * FROM dolt_conflicts_{table};"):
            theirs = {c[len("their_"):]: v for c, v in row.items()
                      if c.startswith("their_") and c != "their_diff_type"
                      and c[len("their_"):] in current}
            if row["their_diff_type"] == "removed":
                where = " AND ".join(f"`{k}` = %s" for k in key)
                self._execute(f"DELETE FROM `{table}` WHERE {where};",
                              [row[f"base_{k}"] for k in key])
            elif row["our_diff_type"] != "removed" and row.get(f"our_{key[0]}") is not None:
                sets = ", ".join(f"`{c}` = %s" for c in theirs)
                where = " AND ".join(f"`{k}` = %s" for k in key)
                self._execute(f"UPDATE `{table}` SET {sets} WHERE {where};",
                              list(theirs.values()) + [row[f"our_{k}"] for k in key])
            else:
                cols = ", ".join(f"`{c}`" for c in theirs)
                marks = ", ".join(["%s"] * len(theirs))
                self._execute(f"INSERT INTO `{table}` ({cols}) VALUES ({marks});",
                              list(theirs.values()))

    def _merge_impl(self, into: Ref, source: Ref, message: str,
                    on_conflict="ours") -> dict:
        self._on(into)
        # dolt_merge needs a clean working set.
        self._execute("CALL DOLT_ADD('-A');")
        try:
            self._execute("CALL DOLT_COMMIT('-m', 'pre-merge commit');")
        except Exception as e:
            if "nothing to commit" not in str(e).lower():
                raise
        self._allow_conflicts()
        message = message or f"Merge {source.branch} into {into.branch}"
        rows = self._execute(
            "CALL DOLT_MERGE(%s, '-m', %s);", (self._spec(source), message)
        )
        row = rows[0] if rows else ()
        info = {
            "hash": row[0] if len(row) > 0 else "",
            "fast_forward": bool(row[1]) if len(row) > 1 else False,
            "conflicts": int(row[2]) if len(row) > 2 else 0,
            "message": row[3] if len(row) > 3 else "",
            "conflict_tables": [],
            "schema_conflicts": [],
        }
        if info["conflicts"] > 0:
            schema = self._schema_conflicts()
            if schema:
                # Dolt cannot resolve these in place; abort so the branch
                # is usable again and report why.
                self._execute("CALL DOLT_MERGE('--abort');")
                names = ", ".join(c["table"] for c in schema)
                raise RuntimeError(
                    f"schema conflict on {names}; merge aborted: "
                    f"{schema[0]['description']}"
                )
            info.update(self._resolve_conflicts(into, on_conflict))
            rows = self._execute("CALL DOLT_COMMIT('-Am', %s);", (message,))
            info["hash"] = rows[0][0] if rows else ""
        return info

    def _rebase_impl(self, ref: Ref, onto: Ref, on_conflict="ours") -> dict:
        self._on(ref)
        self._allow_conflicts()
        info = {"conflicts": 0, "conflict_rows": 0, "conflict_tables": [], "resolved": "",
                "up_to_date": False}
        # dolt_rebase refuses to start with uncommitted changes.
        self._execute("CALL DOLT_ADD('-A');")
        try:
            self._execute("CALL DOLT_COMMIT('-m', 'pre-rebase commit');")
        except Exception as e:
            if "nothing to commit" not in str(e).lower():
                raise
        try:
            self._execute("CALL DOLT_REBASE('-i', %s);", (self._spec(onto),))
        except Exception as e:
            if "identify any commits" in str(e).lower():
                # Nothing to replay: ref is already on top of onto.
                info["up_to_date"] = True
                return info
            raise
        plan = self._execute("SELECT COUNT(*) FROM dolt_rebase;")
        rounds = int(plan[0][0]) + 1 if plan else 2
        for _ in range(rounds):
            try:
                self._execute("CALL DOLT_REBASE('--continue');")
                self._current_ref = Ref(ref.branch)
                return info
            except Exception as e:
                msg = str(e).lower()
                if "conflict" not in msg:
                    self._abort_rebase()
                    raise
                if "automatically aborted" in msg or not self._conflict_tables():
                    # Schema conflict: Dolt aborts the rebase itself and
                    # leaves the branch as it was.
                    self._current_ref = None
                    raise RuntimeError(f"schema conflict; rebase aborted: {e}")
                resolved = self._resolve_conflicts(ref, on_conflict)
                info["conflicts"] += 1
                info["conflict_rows"] += resolved.get("conflict_rows", 0)
                info["conflict_tables"] = sorted(
                    set(info["conflict_tables"]) | set(resolved["conflict_tables"])
                )
                info["resolved"] = resolved["resolved"]
                self._execute("CALL DOLT_ADD('-A');")
        self._abort_rebase()
        raise RuntimeError("rebase did not finish after resolving conflicts")

    def _abort_rebase(self) -> None:
        """Abort a rebase; a failed conflict resolution can leave the
        replay's merge open, which must be aborted first."""
        rb = "CALL DOLT_REBASE('--abort');"
        mg = "CALL DOLT_MERGE('--abort');"
        for stmt in (rb, mg, rb):
            try:
                self._execute(stmt)
                if stmt == rb:
                    break
            except Exception:
                pass
        self._current_ref = None  # Dolt leaves us on dolt_rebase_<branch>

    def _revert_impl(self, ref: Ref, commit: str) -> None:
        self._on(ref)
        self._execute("CALL DOLT_REVERT(%s);", (commit,))

    def _reset_impl(self, ref: Ref, to: str) -> None:
        self._on(ref)
        self._execute("CALL DOLT_RESET('--hard', %s);", (to,))

    def _delete_impl(self, ref: Ref) -> None:
        if self._current_ref and self._current_ref.branch == ref.branch:
            self._checkout(self.default_branch)
            self._current_ref = Ref(self.default_branch)
        self._execute("CALL DOLT_BRANCH('-D', %s);", (ref.branch,))

    def _qualified_table(self, ref: Ref, table: str) -> str:
        return f"`{self.db_name}/{self._spec(ref)}`.`{table}`"

    def list_branches(self) -> list:
        rows = self._execute("SELECT name FROM dolt_branches;")
        return [r[0] for r in rows or []]

    def close_connection(self) -> None:
        if self.conn and self._temp_branches:
            try:
                self._checkout(self.default_branch)
                for tmp in list(self._temp_branches):
                    self._execute("CALL DOLT_BRANCH('-D', %s);", (tmp,))
            except Exception as e:
                print(f"Warning: could not drop temporary Dolt branches: {e}")
            self._temp_branches.clear()
        super().close_connection()

    # ------------------------------------------------------------------
    # Async
    # ------------------------------------------------------------------

    async def open_async_pool(self, size: int) -> None:
        self.async_pool = await create_pool_async(self.db_name, size)

    def _pool_connection(self):
        return mysql_common.aiomysql_pool_connection(self.async_pool)

    async def close_async_pool(self) -> None:
        if self.async_pool:
            self.async_pool.close()
            await self.async_pool.wait_closed()
            self.async_pool = None

    async def _connect_impl_async(self, conn, ref: Ref):
        target = ref.branch
        if ref.commit:
            target = self._temp_branch_name(ref.commit)
            if target not in self._temp_branches:
                async with conn.cursor() as cur:
                    try:
                        await cur.execute(
                            "CALL DOLT_CHECKOUT(%s, '-b', %s);", (ref.commit, target)
                        )
                    except Exception:
                        await cur.execute("CALL DOLT_CHECKOUT(%s);", (target,))
                self._temp_branches.add(target)
                conn._dolt_branch = target
                return conn
        if getattr(conn, "_dolt_branch", None) != target:
            async with conn.cursor() as cur:
                await cur.execute("CALL DOLT_CHECKOUT(%s);", (target,))
            conn._dolt_branch = target
        return conn
