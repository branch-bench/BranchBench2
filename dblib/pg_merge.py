"""SQL three-way merge, diff and revert for Postgres-based backends.

Shared by the backends that have Postgres on each branch but no native
merge (Neon, file_copy). The mixin expects the concrete class to provide

* ``_exec(conn, sql, args=None)``: run one statement on a DB-API
  connection and return its rows (None without a result set);
* ``_fdw_schema(conn, local_id, remote_id, tables)``: import ``tables`` of
  the branch ``remote_id`` into a schema of the database ``conn`` is on,
  through postgres_fdw, and return the schema name;
* ``_fdw_cleanup(conn, remote_id)``: drop that schema and server.

A branch "id" is whatever the backend uses to name one side (a Neon
branch id, a database name). All work runs on ``ours``, the target's
connection: the other sides are pulled over postgres_fdw into temp tables
(only the key buckets that changed), and the comparison runs locally.
Bookkeeping tables (prefix ``_bb_``) are never merged.
"""

from dblib.db_api import Ref  # noqa: F401 (type annotation)

META_PREFIX = "_bb_"


def _q(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def empty_diff() -> dict:
    return {"tables": [], "rows_added": 0, "rows_deleted": 0, "rows_modified": 0}


class PgMergeMixin:
    """Postgres SQL machinery behind diff/merge/rebase/revert."""

    # ------------------------------------------------------------------
    # Table metadata (Postgres)
    # ------------------------------------------------------------------
    def _tables(self, conn) -> list:
        rows = self._exec(conn, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' "
                                "AND table_type = 'BASE TABLE' AND table_name NOT LIKE %s ORDER BY table_name",
                          (META_PREFIX.replace("_", r"\_") + "%",))
        return [r[0] for r in rows or []]

    def _columns(self, conn, table: str) -> list:
        rows = self._exec(conn, "SELECT column_name, data_type, character_maximum_length, numeric_precision, "
                                "numeric_scale, is_nullable, column_default FROM information_schema.columns "
                                "WHERE table_schema = 'public' AND table_name = %s ORDER BY ordinal_position", (table,))
        return [tuple(r) for r in rows or []]

    def _pk(self, conn, table: str) -> list:
        rows = self._exec(conn, "SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid "
                                "AND a.attnum = ANY(i.indkey) WHERE i.indrelid = %s::regclass AND i.indisprimary "
                                "ORDER BY array_position(i.indkey, a.attnum)", (f'public."{table}"',))
        return [r[0] for r in rows or []]

    def _table_hash(self, conn, table: str) -> tuple:
        """(row count, order-independent hash) of a table, computed where
        the table lives."""
        rows = self._exec(conn, f'SELECT COUNT(*), COALESCE(SUM(hashtext(t::text)::bigint), 0) FROM {_q(table)} t')
        return (int(rows[0][0]), int(rows[0][1]))

    _NB = 256  # hash buckets per table for change detection and partial pulls

    def _bucket_expr(self, conn, table: str, pk: list, alias: str = None) -> str:
        """SQL for a row's bucket (0..255) from its integer primary-key
        columns, or None when the key has none. Built from immutable
        functions only, so postgres_fdw ships the predicate to the remote
        compute and only matching rows travel."""
        types = {c[0]: c[1] for c in self._columns(conn, table)}
        parts = []
        for c in pk:
            col = f"{alias}.{_q(c)}" if alias else _q(c)
            t = types.get(c)
            if t in ("integer", "smallint"):
                parts.append(f"hashint4({col}::int)")
            elif t == "bigint":
                parts.append(f"hashint8({col})")
        if not parts:
            return None
        return "((" + " # ".join(parts) + f") & {self._NB - 1})"

    def _bucket_hashes(self, conn, table: str, bexpr: str) -> dict:
        """{bucket: (rows, hash)} computed where the table lives."""
        rows = self._exec(conn, f"SELECT {bexpr} AS b, COUNT(*), COALESCE(SUM(hashtext(t::text)::bigint), 0) "
                                f"FROM {_q(table)} t GROUP BY 1") or []
        return {int(b): (int(n), int(h)) for b, n, h in rows}

    @staticmethod
    def _changed_buckets(a: dict, b: dict) -> list:
        return sorted(k for k in set(a) | set(b) if a.get(k, (0, 0)) != b.get(k, (0, 0)))

    @staticmethod
    def _type_sql(col) -> str:
        name, dtype, char_len, prec, scale, nullable, default = col
        t = dtype
        if dtype in ("character varying", "character") and char_len:
            t = f"{dtype}({char_len})"
        elif dtype == "numeric" and prec is not None:
            t = f"numeric({prec},{scale})"
        return t


    def _fk_order(self, conn, tables: list) -> list:
        """``tables`` sorted so that referenced tables come before the
        tables that reference them (inserts in this order, deletes in
        reverse keep foreign keys satisfied without superuser rights)."""
        rows = self._exec(conn, "SELECT c.conrelid::regclass::text, c.confrelid::regclass::text FROM pg_constraint c "
                                "WHERE c.contype = 'f' AND c.connamespace = 'public'::regnamespace") or []
        deps = {t: set() for t in tables}
        for child, parent in rows:
            child, parent = child.strip('"'), parent.strip('"')
            if child in deps and parent in deps and child != parent:
                deps[child].add(parent)
        out, done = [], set()
        while len(out) < len(tables):
            ready = [t for t in tables if t not in done and deps[t] <= done]
            if not ready:
                ready = [t for t in tables if t not in done]  # cycle: give up on order
            for t in ready:
                out.append(t)
                done.add(t)
        return out

    def _pull(self, conn, schema: str, table: str, local_name: str, cols: list, available=None,
              types: dict = None, where: str = None) -> None:
        """Copy a foreign table into a local temp table with exactly ``cols``;
        columns the remote table lacks (``available`` lists what it has)
        come out NULL, cast to the local column's type (``types``)."""
        def col(c):
            if available is None or c in available:
                return _q(c)
            return f"NULL::{types[c]} AS {_q(c)}" if types and c in types else f"NULL AS {_q(c)}"
        select = ", ".join(col(c) for c in cols)
        self._exec(conn, f"DROP TABLE IF EXISTS {_q(local_name)}")
        self._exec(conn, f"CREATE TEMP TABLE {_q(local_name)} AS SELECT {select} FROM {_q(schema)}.{_q(table)}"
                         + (f" WHERE {where}" if where else ""))


    # ------------------------------------------------------------------
    # SQL fragments (Postgres)
    # ------------------------------------------------------------------

    @staticmethod
    def _join(a, b, pk):
        return " AND ".join(f"{a}.{_q(c)} = {b}.{_q(c)}" for c in pk)

    @staticmethod
    def _same(a, b, cols):
        if not cols:
            return "TRUE"
        return " AND ".join(f"{a}.{_q(c)} IS NOT DISTINCT FROM {b}.{_q(c)}" for c in cols)

    @staticmethod
    def _key(alias, pk):
        return "concat_ws('|', " + ", ".join(f"{alias}.{_q(c)}::text" for c in pk) + ")"

    @staticmethod
    def _cols(alias, cols):
        return ", ".join(f"{alias}.{_q(c)}" for c in cols)

    @staticmethod
    def _upsert(dst, cols, pk, select):
        rest = [c for c in cols if c not in pk]
        head = f"INSERT INTO {dst} ({', '.join(_q(c) for c in cols)}) {select}"
        keys = ", ".join(_q(c) for c in pk)
        if not rest:
            return f"{head} ON CONFLICT ({keys}) DO NOTHING"
        sets = ", ".join(f"{_q(c)} = EXCLUDED.{_q(c)}" for c in rest)
        return f"{head} ON CONFLICT ({keys}) DO UPDATE SET {sets}"


    # ------------------------------------------------------------------
    # diff: counts of added/deleted/modified rows per table from a to b
    # ------------------------------------------------------------------

    def _diff_tables(self, ca, a_id: str, cb, b_id: str) -> dict:
        """Differences from side a (connection ``ca``) to side b (``cb``):
        tables hashed per key bucket on each side, only the differing
        buckets pulled over postgres_fdw onto b and compared there."""
        changed_fdw = False
        try:
            ta, tb = set(self._tables(ca)), set(self._tables(cb))
            out = []
            changed = {}
            for t in sorted(ta | tb):
                if t in ta and t not in tb:
                    out.append({"table_name": t, "rows_added": 0, "rows_deleted": self._table_hash(ca, t)[0], "rows_modified": 0})
                elif t in tb and t not in ta:
                    out.append({"table_name": t, "rows_added": self._table_hash(cb, t)[0], "rows_deleted": 0, "rows_modified": 0})
                else:
                    pk0 = self._pk(cb, t)
                    bexpr = self._bucket_expr(cb, t, pk0) if pk0 else None
                    if bexpr is None:
                        if self._table_hash(ca, t) != self._table_hash(cb, t):
                            changed[t] = None
                    else:
                        diffb = self._changed_buckets(self._bucket_hashes(ca, t, bexpr), self._bucket_hashes(cb, t, bexpr))
                        if diffb:
                            changed[t] = f"{bexpr} IN ({', '.join(str(b) for b in diffb)})"
            if changed:
                changed_fdw = True
                schema = self._fdw_schema(cb, b_id, a_id, list(changed))
                for t, where in changed.items():
                    cols_b = [c[0] for c in self._columns(cb, t)]
                    cols_a = {c[0] for c in self._columns(ca, t)}
                    cols = [c for c in cols_b if c in cols_a]
                    pk = [c for c in self._pk(cb, t) if c in cols] or cols
                    rest = [c for c in cols if c not in pk]
                    self._pull(cb, schema, t, "_bb_a", cols, available=cols_a,
                               types={c[0]: self._type_sql(c) for c in self._columns(cb, t)}, where=where)
                    self._exec(cb, 'DROP TABLE IF EXISTS "_bb_bsub"')
                    self._exec(cb, f'CREATE TEMP TABLE "_bb_bsub" AS SELECT {", ".join(_q(c) for c in cols)} FROM {_q(t)}'
                                   + (f" WHERE {where}" if where else ""))
                    B = '"_bb_bsub"'
                    added = self._exec(cb, f'SELECT COUNT(*) FROM {B} y LEFT JOIN "_bb_a" x ON {self._join("x", "y", pk)} WHERE x.{_q(pk[0])} IS NULL')[0][0]
                    deleted = self._exec(cb, f'SELECT COUNT(*) FROM "_bb_a" x LEFT JOIN {B} y ON {self._join("x", "y", pk)} WHERE y.{_q(pk[0])} IS NULL')[0][0]
                    modified = 0
                    if rest:
                        modified = self._exec(cb, f'SELECT COUNT(*) FROM "_bb_a" x JOIN {B} y ON {self._join("x", "y", pk)} WHERE NOT ({self._same("x", "y", rest)})')[0][0]
                    out.append({"table_name": t, "rows_added": int(added), "rows_deleted": int(deleted), "rows_modified": int(modified)})
                    self._exec(cb, 'DROP TABLE IF EXISTS "_bb_a"; DROP TABLE IF EXISTS "_bb_bsub"')
            return {"tables": out, "rows_added": sum(x["rows_added"] for x in out),
                    "rows_deleted": sum(x["rows_deleted"] for x in out),
                    "rows_modified": sum(x["rows_modified"] for x in out)}
        finally:
            if changed_fdw:
                self._fdw_cleanup(cb, a_id)

    # ------------------------------------------------------------------
    # three-way merge
    # ------------------------------------------------------------------

    def _reconcile_schema(self, ours, ours_id: str, theirs_conn, theirs_id: str) -> dict:
        """Give ours the tables and columns theirs has and it lacks (data
        of new tables copied over fdw); a differing primary key is a
        schema conflict raised before any change."""
        ours_tables = set(self._tables(ours))
        theirs_tables = self._tables(theirs_conn)
        new_tables = sorted(set(theirs_tables) - ours_tables)
        plan_cols = []
        for t in sorted(ours_tables & set(theirs_tables)):
            if self._pk(ours, t) != self._pk(theirs_conn, t):
                raise RuntimeError(f"schema conflict on {t}: primary keys differ")
            have = {c[0]: c for c in self._columns(ours, t)}
            for col in self._columns(theirs_conn, t):
                if col[0] not in have:
                    plan_cols.append((t, col))
                elif self._type_sql(col) != self._type_sql(have[col[0]]):
                    # The same column added or altered on both sides with
                    # different types: nothing to merge automatically.
                    raise RuntimeError(f"schema conflict on {t}.{col[0]}: types differ "
                                       f"(ours {self._type_sql(have[col[0]])}, theirs {self._type_sql(col)})")
        if new_tables:
            schema = self._fdw_schema(ours, ours_id, theirs_id, new_tables)
            for t in new_tables:
                cols = self._columns(theirs_conn, t)
                defs = ", ".join(f"{_q(c[0])} {self._type_sql(c)}" + (" NOT NULL" if c[5] == "NO" else "") for c in cols)
                pk = self._pk(theirs_conn, t)
                if pk:
                    defs += f", PRIMARY KEY ({', '.join(_q(c) for c in pk)})"
                self._exec(ours, f"CREATE TABLE {_q(t)} ({defs})")
                self._exec(ours, f"INSERT INTO {_q(t)} SELECT {', '.join(_q(c[0]) for c in cols)} FROM {_q(schema)}.{_q(t)}")
        for t, col in plan_cols:
            ddl = f"ALTER TABLE {_q(t)} ADD COLUMN {_q(col[0])} {self._type_sql(col)}"
            if col[6] is not None:
                ddl += f" DEFAULT {col[6]}"
            self._exec(ours, ddl)
        return {"added_tables": new_tables, "added_columns": [f"{t}.{c[0]}" for t, c in plan_cols]}

    def _three_way(self, ours, ours_id: str, theirs_conn, theirs_id: str, base_conn, base_id: str,
                   on_conflict, ref: Ref, new_tables: list, keep_ours: bool) -> tuple:
        """Apply theirs' changes since base onto ours (self.conn's branch).
        Per table, theirs and base are pulled over postgres_fdw into temp
        tables and the three-way comparison runs locally; deletes are
        applied in reverse foreign-key order and upserts in forward order.
        Returns (info, conflicts-for-callable)."""
        info = {"conflicts": 0, "conflict_tables": [], "ours_changes": 0, "theirs_changes": 0,
                "merged_tables": [], "skipped_tables": {}}
        conflicts = []
        want_rows = callable(on_conflict)
        base_tables = set(self._tables(base_conn))
        tables = sorted((set(self._tables(ours)) & set(self._tables(theirs_conn))) - set(new_tables))
        # Change detection per hash bucket of the key, each side hashing
        # its own rows locally; only buckets theirs changed are pulled.
        buckets = {}   # table -> (bexpr, theirs-changed buckets, ours-changed buckets) or None
        changed = []
        for t in tables:
            pk = self._pk(ours, t)
            bexpr = self._bucket_expr(ours, t, pk) if pk else None
            if bexpr is None:
                if t not in base_tables or self._table_hash(theirs_conn, t) != self._table_hash(base_conn, t):
                    changed.append(t)
                    buckets[t] = None
                continue
            h_t = self._bucket_hashes(theirs_conn, t, bexpr)
            h_b = self._bucket_hashes(base_conn, t, bexpr) if t in base_tables else {}
            tb = self._changed_buckets(h_t, h_b)
            if not tb:
                continue
            h_o = self._bucket_hashes(ours, t, bexpr)
            ob = self._changed_buckets(h_o, h_b)
            info["ours_changes"] += len(ob)
            changed.append(t)
            buckets[t] = (bexpr, tb, ob)
        if not changed:
            return info, conflicts
        changed = self._fk_order(ours, changed)
        s_t = self._fdw_schema(ours, ours_id, theirs_id, changed)
        s_b = self._fdw_schema(ours, ours_id, base_id, [t for t in changed if t in base_tables])
        plans = {}
        temps = []
        self._exec(ours, "BEGIN")
        try:
            for i, t in enumerate(changed):
                ocols = self._columns(ours, t)
                types = {c[0]: self._type_sql(c) for c in ocols}
                cols_o = [c[0] for c in ocols]
                cols_t = {c[0] for c in self._columns(theirs_conn, t)}
                cols = [c for c in cols_o if c in cols_t]
                pk = [c for c in self._pk(ours, t) if c in cols]
                T, B, OS = f"_bb_t{i}", f"_bb_b{i}", f"_bb_o{i}"
                temps += [T, B, OS]
                where = None
                if buckets.get(t):
                    bexpr, tb, _ = buckets[t]
                    where = f"{bexpr} IN ({', '.join(str(b) for b in tb)})"
                self._pull(ours, s_t, t, T, cols, where=where)
                if t in base_tables:
                    self._pull(ours, s_b, t, B, cols, available={c[0] for c in self._columns(base_conn, t)}, types=types, where=where)
                else:
                    self._exec(ours, f'CREATE TEMP TABLE {_q(B)} AS SELECT {", ".join(_q(c) for c in cols)} FROM {_q(T)} WHERE FALSE')
                # Our rows in the same buckets (the only ones theirs can conflict with).
                self._exec(ours, f'CREATE TEMP TABLE {_q(OS)} AS SELECT {", ".join(_q(c) for c in cols)} FROM {_q(t)}'
                                 + (f" WHERE {where}" if where else ""))
                plans[t] = {"cols": cols, "pk": pk, "T": _q(T), "B": _q(B), "O": _q(t), "OS": _q(OS)}
                if not pk:
                    continue
                rest = [c for c in cols if c not in pk]
                kt, ko, kb = self._key("t", pk), self._key("o", pk), self._key("b", pk)
                O, Tq, Bq = _q(OS), _q(T), _q(B)
                d_tb = f"b.{_q(pk[0])} IS NULL" + (f" OR NOT ({self._same('t', 'b', rest)})" if rest else "")
                d_ob = f"b.{_q(pk[0])} IS NULL" + (f" OR NOT ({self._same('o', 'b', rest)})" if rest else "")
                TK, OK, CK = f"_bb_tk{i}", f"_bb_ok{i}", f"_bb_ck{i}"
                temps += [TK, OK, CK]
                self._exec(ours, f'CREATE TEMP TABLE {_q(TK)} AS SELECT {kt} AS k, \'u\' AS side FROM {Tq} t LEFT JOIN {Bq} b ON {self._join("t", "b", pk)} WHERE {d_tb} '
                                 f'UNION ALL SELECT {kb}, \'d\' FROM {Bq} b LEFT JOIN {Tq} t ON {self._join("t", "b", pk)} WHERE t.{_q(pk[0])} IS NULL')
                self._exec(ours, f'CREATE TEMP TABLE {_q(OK)} AS SELECT {ko} AS k, \'u\' AS side FROM {O} o LEFT JOIN {Bq} b ON {self._join("o", "b", pk)} WHERE {d_ob} '
                                 f'UNION ALL SELECT {kb}, \'d\' FROM {Bq} b LEFT JOIN {O} o ON {self._join("o", "b", pk)} WHERE o.{_q(pk[0])} IS NULL')
                self._exec(ours, f'CREATE INDEX ON {_q(TK)} (k); CREATE INDEX ON {_q(OK)} (k)')
                n_t = int(self._exec(ours, f'SELECT COUNT(*) FROM {_q(TK)}')[0][0])
                n_o = int(self._exec(ours, f'SELECT COUNT(*) FROM {_q(OK)}')[0][0])
                info["theirs_changes"] += n_t
                if not buckets.get(t):
                    info["ours_changes"] += n_o
                same_ot = self._same("o", "t", rest) if rest else "TRUE"
                self._exec(ours, f'CREATE TEMP TABLE {_q(CK)} AS SELECT a.k FROM {_q(TK)} a JOIN {_q(OK)} b ON b.k = a.k '
                                 f'LEFT JOIN {O} o ON {ko} = a.k LEFT JOIN {Tq} t ON {kt} = a.k '
                                 f"WHERE NOT (a.side = 'd' AND b.side = 'd') AND NOT (a.side = 'u' AND b.side = 'u' AND {same_ot})")
                self._exec(ours, f'CREATE INDEX ON {_q(CK)} (k)')
                n_c = int(self._exec(ours, f'SELECT COUNT(*) FROM {_q(CK)}')[0][0])
                plans[t].update({"rest": rest, "TK": _q(TK), "CK": _q(CK), "n_t": n_t, "n_c": n_c})
            # Their deletes, children first; then their upserts, parents first.
            for t in reversed(changed):
                pl = plans[t]
                if not pl["pk"] or not pl.get("n_t"):
                    continue
                not_c = f'NOT EXISTS (SELECT 1 FROM {pl["CK"]} c WHERE c.k = s.k)'
                apply_c = "" if (keep_ours or want_rows) else " OR TRUE"
                self._exec(ours, f'DELETE FROM {pl["O"]} o WHERE {self._key("o", pl["pk"])} IN '
                                 f'(SELECT s.k FROM {pl["TK"]} s WHERE s.side = \'d\' AND ({not_c}{apply_c}))')
            for t in changed:
                pl = plans[t]
                cols, pk = pl["cols"], pl["pk"]
                if not pk:
                    # Rows theirs added since base, as a multiset difference:
                    # EXCEPT ALL hashes (NULLs compare equal), where an anti-join
                    # on IS NOT DISTINCT FROM over every column is a nested loop
                    # that grows with |theirs| x |base| (the history table of a
                    # dev_agent run made that the rebase's dominant cost).
                    cl = ", ".join(_q(c) for c in cols)
                    self._exec(ours, f'INSERT INTO {pl["O"]} ({cl}) SELECT {cl} FROM '
                                     f'(SELECT {cl} FROM {pl["T"]} EXCEPT ALL SELECT {cl} FROM {pl["B"]}) x')
                    info["merged_tables"].append(t)
                    info["skipped_tables"][t] = "no primary key: new rows only"
                    continue
                if not pl.get("n_t"):
                    continue
                not_c = f'NOT EXISTS (SELECT 1 FROM {pl["CK"]} c WHERE c.k = s.k)'
                apply_c = "" if (keep_ours or want_rows) else " OR TRUE"
                self._exec(ours, self._upsert(pl["O"], cols, pk, f'SELECT {self._cols("t", cols)} FROM {pl["T"]} t WHERE {self._key("t", pk)} IN '
                                                                 f'(SELECT s.k FROM {pl["TK"]} s WHERE s.side = \'u\' AND ({not_c}{apply_c}))'))
                info["merged_tables"].append(t)
                if pl["n_c"]:
                    info["conflicts"] += pl["n_c"]
                    info["conflict_tables"].append(t)
                    if want_rows:
                        conflicts.append({"table": t, "rows": self._conflict_rows(ours, pl["OS"], pl["T"], pl["B"], pl["CK"], cols, pk)})
            self._exec(ours, "COMMIT")
        except BaseException:
            try:
                self._exec(ours, "ROLLBACK")
            except Exception:
                pass
            raise
        finally:
            for tmp in temps:
                try:
                    self._exec(ours, f'DROP TABLE IF EXISTS {_q(tmp)}')
                except Exception:
                    pass
        return info, conflicts

    def _conflict_rows(self, ours, O, T, B, CK, cols, pk) -> list:
        select = ", ".join([f"b.{_q(c)}" for c in cols] + [f"o.{_q(c)}" for c in cols] + [f"t.{_q(c)}" for c in cols])
        rows = self._exec(ours, f'SELECT {select}, b.{_q(pk[0])} IS NULL, o.{_q(pk[0])} IS NULL, t.{_q(pk[0])} IS NULL '
                                f'FROM {CK} c LEFT JOIN {B} b ON {self._key("b", pk)} = c.k '
                                f'LEFT JOIN {O} o ON {self._key("o", pk)} = c.k LEFT JOIN {T} t ON {self._key("t", pk)} = c.k '
                                ) or []
        names = [f"base_{c}" for c in cols] + [f"our_{c}" for c in cols] + [f"their_{c}" for c in cols]
        out = []
        for r in rows:
            d = dict(zip(names, r[:len(names)]))
            no_base, no_ours, no_theirs = r[len(names):]

            def diff_type(missing):
                return "removed" if missing else ("added" if no_base else "modified")
            d["our_diff_type"] = diff_type(no_ours)
            d["their_diff_type"] = diff_type(no_theirs)
            out.append(d)
        return out


    # ------------------------------------------------------------------
    # revert: apply the inverse of (before -> after) onto ours
    # ------------------------------------------------------------------

    def _revert_apply(self, ours, ours_id: str, ca, a_id: str, cb, b_id: str) -> list:
        """Undo on ``ours`` the changes between side b (the state before a
        commit, connection ``cb``) and side a (the state after it, ``ca``):
        rows the commit added are deleted, rows it deleted or modified get
        their earlier version back. Returns the tables that changed."""
        try:
            tables = [t for t in self._tables(ours) if t in set(self._tables(ca)) and t in set(self._tables(cb))]
            changed = [t for t in tables if self._table_hash(ca, t) != self._table_hash(cb, t)]
            if not changed:
                return []
            s_a = self._fdw_schema(ours, ours_id, a_id, changed)
            s_b = self._fdw_schema(ours, ours_id, b_id, changed)
            changed = self._fk_order(ours, changed)
            self._exec(ours, "BEGIN")
            try:
                for t in reversed(changed):
                    cols = [c[0] for c in self._columns(ours, t) if c[0] in {x[0] for x in self._columns(ca, t)}]
                    pk = [c for c in self._pk(ours, t) if c in cols]
                    if not pk:
                        continue
                    rest = [c for c in cols if c not in pk]
                    types = {c[0]: self._type_sql(c) for c in self._columns(ours, t)}
                    self._pull(ours, s_a, t, "_bb_a", cols, available={c[0] for c in self._columns(ca, t)}, types=types)
                    self._pull(ours, s_b, t, "_bb_b", cols, available={c[0] for c in self._columns(cb, t)}, types=types)
                    O = _q(t)
                    # Rows the commit added: delete them (children first).
                    self._exec(ours, f'DELETE FROM {O} o WHERE EXISTS (SELECT 1 FROM "_bb_a" e LEFT JOIN "_bb_b" s ON {self._join("s", "e", pk)} '
                                     f'WHERE s.{_q(pk[0])} IS NULL AND {self._join("e", "o", pk)})')
                    self._exec(ours, f'ALTER TABLE "_bb_a" RENAME TO "_bb_a_{t}"; ALTER TABLE "_bb_b" RENAME TO "_bb_b_{t}"')
                for t in changed:
                    cols = [c[0] for c in self._columns(ours, t) if c[0] in {x[0] for x in self._columns(ca, t)}]
                    pk = [c for c in self._pk(ours, t) if c in cols]
                    if not pk:
                        continue
                    rest = [c for c in cols if c not in pk]
                    O = _q(t)
                    # Rows it deleted or modified: put the earlier version back (parents first).
                    differs = f"e.{_q(pk[0])} IS NULL" + (f" OR NOT ({self._same('s', 'e', rest)})" if rest else "")
                    self._exec(ours, self._upsert(O, cols, pk, f'SELECT {self._cols("s", cols)} FROM "_bb_b_{t}" s LEFT JOIN "_bb_a_{t}" e ON {self._join("s", "e", pk)} WHERE {differs}'))
                self._exec(ours, "COMMIT")
            except BaseException:
                self._exec(ours, "ROLLBACK")
                raise
            finally:
                for t in changed:
                    for tmp in (f"_bb_a_{t}", f"_bb_b_{t}", "_bb_a", "_bb_b"):
                        try:
                            self._exec(ours, f'DROP TABLE IF EXISTS {_q(tmp)}')
                        except Exception:
                            pass
            return changed
        finally:
            for x in {a_id, b_id}:
                self._fdw_cleanup(ours, x)
