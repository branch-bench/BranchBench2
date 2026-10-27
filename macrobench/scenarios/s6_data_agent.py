"""S6: Data agent.

The spine is a warehouse under a mostly analytical load. Each ingestion
batch opens a branch from the spine, writes its manifest row (the
sentinel), loads ``batch_rows`` historical orders dated within the last
``days_back`` days in ``steps_per_batch`` steps, maintains the derived
tables (daily_revenue, customer_summary, nation_sales), validates each
step by recomputing the affected days from the base tables, and commits
at a steady pace. ``resets_per_batch`` times per batch a step "fails"
(at a random non-final step after a commit) and the batch resets to one
of its earlier commits (or continues on a branch from that commit when the
backend has no reset) and redoes the work from there. A finished
batch rebases onto the spine and merges; conflicts on the derived tables
are resolved additively (ours + theirs - base). Merged branches are
deleted.

Sentinel: the batch_manifest row is invisible on the spine until the merge.

Invariants: S6.1 the manifest is hidden until the merge; S6.2 a reset
restores exactly the rows loaded up to its commit; S6.4 the merge carries
every row the batch loaded; S6.3 after each merge the spine's daily_revenue
and customer_summary equal the seed roll-ups plus a recomputation over the
merged batch orders (what the additive resolver must produce).
"""

import math
import threading
from datetime import datetime, timedelta

from macrobench.scenarios.base import Scenario, Worker, register
from macrobench.datagen.ch import DISTRICTS_PER_WAREHOUSE, insert_rows

ORDER_ID_BASE = 10_000_000

DERIVED = {
    "daily_revenue": (("day", "w_id"), ("revenue", "orders"), None),
    "nation_sales": (("n_nationkey", "day"), ("revenue",), None),
    "customer_summary": (("c_w_id", "c_d_id", "c_id"), ("order_count", "total_amount"), "last_order"),
}


def additive_resolver(db, conflicts):
    """Derived-table conflicts: both sides added deltas, so combine them
    (ours + theirs - base); take the later timestamp for last_order."""
    out = {}
    for cf in conflicts:
        table = cf["table"].split(".")[-1]
        if table not in DERIVED:
            continue
        keys, sums, latest = DERIVED[table]
        n = 0
        for row in cf["rows"]:
            if row.get("our_diff_type") == "removed" or row.get("their_diff_type") == "removed":
                continue
            sets, vals = [], []
            for col in sums:
                ours = float(row.get(f"our_{col}") or 0)
                theirs = float(row.get(f"their_{col}") or 0)
                base = float(row.get(f"base_{col}") or 0)
                sets.append(f"{col} = %s")
                vals.append(round(ours + theirs - base, 2))
            if latest:
                a, b = row.get(f"our_{latest}"), row.get(f"their_{latest}")
                sets.append(f"{latest} = %s")
                vals.append(max(x for x in (a, b) if x is not None) if (a or b) else None)
            where = " AND ".join(f"{k} = %s" for k in keys)
            vals.extend(row.get(f"our_{k}") for k in keys)
            db.sql(f"UPDATE {table} SET {', '.join(sets)} WHERE {where}", tuple(vals))
            n += 1
        db.resolve(table)
        out[table] = n
    return out


@register
class DataAgentScenario(Scenario):
    key = "data_agent"
    name = "S6 data agent"
    uses_spine_load = True

    def validate(self):
        p = self.params
        if p.batches <= 0 or p.batch_rows <= 0 or p.steps_per_batch <= 0 or p.days_back <= 0:
            raise ValueError("data_agent needs batches, batch_rows, steps_per_batch and days_back > 0")
        if p.resets_per_batch < 0:
            raise ValueError("data_agent resets_per_batch must be >= 0")
        if p.resets_per_batch and p.steps_per_batch < 2:
            raise ValueError("data_agent resets need steps_per_batch >= 2")

    def total_units(self) -> int:
        return self.params.batches * self.params.steps_per_batch

    # ------------------------------------------------------------------

    def seed(self, suite) -> dict:
        """Derived tables computed from the seeded CH orders. The per-order
        totals come from one join query; the day/customer/nation roll-ups
        are done in Python and inserted in batches, which keeps the seed
        independent of each backend's date functions."""
        batch_rows = self.ctx.config.schema.seed_batch_rows or 500

        def script(db):
            rows = db.sql(
                "SELECT o.o_w_id, o.o_d_id, o.o_id, o.o_c_id, o.o_entry_d, c.c_n_nationkey, "
                "SUM(ol.ol_amount) FROM orders o "
                "JOIN order_line ol ON ol.ol_w_id = o.o_w_id AND ol.ol_d_id = o.o_d_id AND ol.ol_o_id = o.o_id "
                "JOIN customer c ON c.c_w_id = o.o_w_id AND c.c_d_id = o.o_d_id AND c.c_id = o.o_c_id "
                "GROUP BY o.o_w_id, o.o_d_id, o.o_id, o.o_c_id, o.o_entry_d, c.c_n_nationkey")
            daily, cust, nation = {}, {}, {}
            for w, d, o, c, entry, nk, amount in rows or []:
                if isinstance(entry, str):
                    entry = datetime.fromisoformat(entry)
                amount = float(amount or 0)
                day = entry.date()
                a, n = daily.get((day, w), (0.0, 0))
                daily[(day, w)] = (a + amount, n + 1)
                a, n, last = cust.get((w, d, c), (0.0, 0, entry))
                cust[(w, d, c)] = (a + amount, n + 1, max(last, entry))
                nation[(nk or 0, day)] = nation.get((nk or 0, day), 0.0) + amount
            # Kept for the S6.3 oracle: the derived tables must equal these
            # seed roll-ups plus the merged batches' orders.
            self._seed_daily = {(str(day), int(w)): (round(a, 2), n) for (day, w), (a, n) in daily.items()}
            self._seed_cust = {(int(w), int(d), int(c)): (round(a, 2), n) for (w, d, c), (a, n, _) in cust.items()}
            counts = {}
            counts["daily_revenue"] = insert_rows(
                db, "daily_revenue", ["day", "w_id", "revenue", "orders"],
                ((day, w, round(a, 2), n) for (day, w), (a, n) in daily.items()), batch_rows)
            counts["customer_summary"] = insert_rows(
                db, "customer_summary",
                ["c_w_id", "c_d_id", "c_id", "order_count", "total_amount", "last_order"],
                ((w, d, c, n, round(a, 2), last) for (w, d, c), (a, n, last) in cust.items()), batch_rows)
            counts["nation_sales"] = insert_rows(
                db, "nation_sales", ["n_nationkey", "day", "revenue"],
                ((nk, day, round(a, 2)) for (nk, day), a in nation.items()), batch_rows)
            return counts
        res = suite.exec(script, refs=[self.ctx.spine], timed=False)[0]
        res.raise_for_status()
        return res.value

    def run(self):
        ctx, p = self.ctx, self.params
        self._merge_lock = threading.Lock()
        ctx.run_workers(list(range(1, p.batches + 1)), self._batch,
                        threads=max(1, p.concurrent_batches or ctx.worker_threads))

    # ------------------------------------------------------------------

    def _batch(self, w: Worker, b: int):
        ctx, p, inv, suite, rng = self.ctx, self.params, self.ctx.invariants, w.suite, w.rng
        branch = f"batch_{b}"
        res = ctx.branch(suite, branch, ctx.spine, label="batch_branch")
        if not res.ok:
            ctx.note(f"batch {b}: branch {res.status_name} {res.error}")
            ctx.tick(p.steps_per_batch, outcome="failed")
            return None
        branches = [branch]
        w.exec([("INSERT INTO batch_manifest (batch_id, branch_name, days_back, rows_loaded, status, created_at) "
                 "VALUES (%s, %s, %s, 0, 'loading', CURRENT_TIMESTAMP)", (b, branch, p.days_back))],
               branch, label="manifest")

        def manifest_on_spine():
            r = suite.exec([("SELECT COUNT(*) FROM batch_manifest WHERE batch_id = %s", (b,))],
                           refs=[ctx.spine], label="invariant")[0]
            return int(r.rows[0][0]) if r.rows else None
        before = manifest_on_spine()

        rows_per_step = max(1, math.ceil(p.batch_rows / p.steps_per_batch))
        # The batch's order ids: step s loads ids [first + s*rows, first + (s+1)*rows).
        first_id = ORDER_ID_BASE + b * p.steps_per_batch * rows_per_step
        last_id = first_id + p.steps_per_batch * rows_per_step

        def batch_orders_on(ref):
            r = suite.exec([("SELECT COUNT(*) FROM orders WHERE o_id >= %s AND o_id < %s",
                             (first_id, last_id))], refs=[ref], label="invariant")[0]
            return int(r.rows[0][0]) if r.ok and r.rows else None

        committed = []   # (step, hash)
        step = 0
        loaded_by_step = {}   # a redone step replaces its earlier count
        resets = 0
        ticked = 0
        mismatches = 0
        while step < p.steps_per_batch:
            ctx.check_stop()
            r = w.exec(self._load_step(b, step, rows_per_step, rng), branch, label="ingest")
            if r.ok and isinstance(r.value, dict):
                loaded_by_step[step] = r.value["rows"]
                mismatches += r.value["mismatches"]
            last = step == p.steps_per_batch - 1
            if ctx.should_commit(step, last_step=last):
                c = w.commit(branch, f"batch {b} step {step}", label="batch_commit")
                if c.ok and c.value:
                    committed.append((step, c.value))
            # Pipeline stage failures: exactly resets_per_batch per batch,
            # placed at random among the non-final steps that follow a
            # commit, and forced onto the last eligible steps when needed.
            # A redone step is a fresh decision point.
            left = (p.steps_per_batch - 1) - step
            need = p.resets_per_batch - resets
            if (not last and committed and need > 0
                    and (need >= left or rng.random() < need / left)):
                # Pipeline stage failed: go back to an earlier commit.
                # Prefer a commit that leaves room for the remaining resets
                # (a non-final step after it per reset still owed).
                room = [c for c in committed if (p.steps_per_batch - 2) - c[0] >= need - 1] or committed
                back_step, back_hash = rng.choice(room)
                if suite.supports("reset"):
                    rs = ctx.retry(lambda: suite.reset(branch, back_hash, label="batch_reset"))
                    ok = rs.ok
                else:
                    nb = f"{branch}_r{resets + 1}"
                    rs = ctx.branch(suite, nb, f"{branch}@{back_hash}", label="batch_reset_branch")
                    ok = rs.ok
                    if ok:
                        branch = nb
                        branches.append(nb)
                # S6.2: after the reset the branch holds exactly the rows
                # loaded up to the commit it went back to, nothing from
                # the steps after it. A failed reset leaves the later
                # rows in place and fails the check.
                expected = sum(loaded_by_step.get(st, 0) for st in range(back_step + 1))
                inv.expect(f"S6.2 batch {b} reset to step {back_step} restores its rows",
                           batch_orders_on(branch), expected,
                           f"reset {rs.status_name} {rs.error[:80]}".rstrip())
                if ok:
                    resets += 1
                    committed = [c for c in committed if c[0] <= back_step]
                    loaded_by_step = {st: n for st, n in loaded_by_step.items() if st <= back_step}
                    step = back_step + 1
                    # The steps up to that commit are done, this one
                    # included when it went back to its own commit.
                    if step > ticked:
                        ctx.tick(step - ticked)
                        ticked = step
                    continue
            step += 1
            if step > ticked:  # steps replayed after a reset are not new units
                ctx.tick()
                ticked = step

        loaded = sum(loaded_by_step.values())
        w.exec([("UPDATE batch_manifest SET status = 'validated', rows_loaded = %s WHERE batch_id = %s",
                 (loaded, b))], branch, label="manifest")
        w.commit(branch, f"batch {b} validated", label="batch_commit")

        # The spine load is paused while merging so its uncommitted writes
        # cannot land between the pre-merge commit and the merge.
        with self._merge_lock, self.quiesced():
            rb = ctx.retry(lambda: suite.rebase(branch, ctx.spine, on_conflict=additive_resolver, label="batch_rebase"))
            m = ctx.retry(lambda: suite.merge(ctx.spine, branch, message=f"merge batch {b}",
                                              on_conflict=additive_resolver, label="batch_merge"))
        if rb.ok and isinstance(rb.value, dict):
            ctx.bump_metric("rebase_conflicts", int(rb.value.get("conflicts", 0)))
        if m.ok:
            ff = bool(isinstance(m.value, dict) and m.value.get("fast_forward"))
            ctx.bump_metric("merges_fast_forward" if ff else "merges_three_way")
            if isinstance(m.value, dict):
                ctx.bump_metric("merge_conflicts", int(m.value.get("conflicts", 0)))
        elif m.failed:
            ctx.bump_metric("merges_failed")
            ctx.note(f"batch {b}: merge {m.error[:120]}")
        after = manifest_on_spine()
        inv.check(f"S6.1 manifest hidden until merge (batch {b})",
                  before == 0 and after == 1,
                  f"before={before} after={after} (merge {m.status_name})")
        # S6.4: the merge carried every row the batch loaded. S6.3: the
        # derived tables on the spine equal a recomputation from the
        # merged batch orders of every batch so far (the additive
        # resolver's output is what this checks).
        with self._merge_lock, self.quiesced():
            on_spine = batch_orders_on(ctx.spine)
            mism = self._derived_mismatches(suite, ctx.spine)
        inv.expect(f"S6.4 batch {b} rows all on the spine after merge", on_spine, loaded,
                   f"merge {m.status_name}")
        inv.expect(f"S6.3 derived tables match merged batch orders (after batch {b})", mism, 0,
                   "daily_revenue/customer_summary rows differing from the base tables")
        ctx.bump_metric("resets", resets)
        ctx.bump_metric("integrity_mismatches", mismatches)
        ctx.bump_metric("rows_loaded", loaded)
        for name in branches:
            ctx.delete(suite, name, label="batch_delete")
        return {"batch": b, "rows": loaded, "resets": resets}

    def _derived_mismatches(self, suite, ref) -> int:
        """Rows of daily_revenue and customer_summary on ``ref`` that differ
        from the seed roll-ups plus a recomputation over the batch orders
        (o_id >= ORDER_ID_BASE) and their order lines, for every day /
        customer the batches touched. Amounts compared to the cent.
        -1 when a probe failed."""
        def q(sql):
            r = suite.exec([sql], refs=[ref], label="invariant")[0]
            if not r.ok:
                self.ctx.note(f"derived-table probe failed: {r.error[:160]}")
                return None
            return r.rows or []
        batch_daily = q("SELECT CAST(o.o_entry_d AS DATE), o.o_w_id, COUNT(DISTINCT o.o_id), SUM(l.ol_amount) "
                        "FROM orders o JOIN order_line l ON l.ol_o_id = o.o_id AND l.ol_d_id = o.o_d_id AND l.ol_w_id = o.o_w_id "
                        f"WHERE o.o_id >= {ORDER_ID_BASE} GROUP BY CAST(o.o_entry_d AS DATE), o.o_w_id")
        batch_cust = q("SELECT o.o_w_id, o.o_d_id, o.o_c_id, COUNT(DISTINCT o.o_id), SUM(l.ol_amount) "
                       "FROM orders o JOIN order_line l ON l.ol_o_id = o.o_id AND l.ol_d_id = o.o_d_id AND l.ol_w_id = o.o_w_id "
                       f"WHERE o.o_id >= {ORDER_ID_BASE} GROUP BY o.o_w_id, o.o_d_id, o.o_c_id")
        daily = q("SELECT day, w_id, revenue, orders FROM daily_revenue")
        cust = q("SELECT c_w_id, c_d_id, c_id, total_amount, order_count FROM customer_summary")
        if None in (batch_daily, batch_cust, daily, cust):
            return -1
        seed_daily = getattr(self, "_seed_daily", {})
        seed_cust = getattr(self, "_seed_cust", {})
        have_daily = {(str(day)[:10], int(w)): (float(rev or 0), int(n or 0)) for day, w, rev, n in daily}
        have_cust = {(int(w), int(d), int(c)): (float(amt or 0), int(n or 0)) for w, d, c, amt, n in cust}
        bad = 0
        for day, w, n, amt in batch_daily:
            key = (str(day)[:10], int(w))
            sa, sn = seed_daily.get(key, (0.0, 0))
            exp = (sa + float(amt or 0), sn + int(n or 0))
            got = have_daily.get(key)
            if got is None or got[1] != exp[1] or abs(got[0] - exp[0]) > 0.05:
                bad += 1
        for w, d, c, n, amt in batch_cust:
            key = (int(w), int(d), int(c))
            sa, sn = seed_cust.get(key, (0.0, 0))
            exp = (sa + float(amt or 0), sn + int(n or 0))
            got = have_cust.get(key)
            if got is None or got[1] != exp[1] or abs(got[0] - exp[0]) > 0.05:
                bad += 1
        return bad

    def _load_step(self, b: int, step: int, rows: int, rng):
        ctx, p = self.ctx, self.params
        now = datetime.now().replace(microsecond=0)
        base_id = ORDER_ID_BASE + (b * p.steps_per_batch + step) * rows
        lines_per_order = max(1, min(5, ctx.rows_per_write))

        def script(db):
            touched = {}      # (day, w) -> (amount, orders)
            customers = {}    # (w, d, c) -> (amount, count, entry)
            nations = {}      # (nation, day) -> amount
            for i in range(rows):
                o_id = base_id + i
                w_id = rng.randint(1, ctx.scale.warehouses)
                d_id = rng.randint(1, DISTRICTS_PER_WAREHOUSE)
                c_id = rng.randint(1, ctx.scale.customers_per_district)
                entry = now - timedelta(days=rng.randint(0, p.days_back - 1), hours=rng.randint(0, 23))
                db.sql("INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) "
                       "VALUES (%s, %s, %s, %s, %s, %s, %s, 1)",
                       (o_id, d_id, w_id, c_id, entry, rng.randint(1, 10), lines_per_order))
                amount = 0.0
                for n in range(1, lines_per_order + 1):
                    a = round(rng.uniform(1, 500), 2)
                    amount += a
                    db.sql("INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, "
                           "ol_delivery_d, ol_quantity, ol_amount, ol_dist_info) VALUES (%s, %s, %s, %s, %s, %s, %s, 5, %s, 'batch')",
                           (o_id, d_id, w_id, n, rng.randint(1, ctx.scale.items), w_id, entry, a))
                amount = round(amount, 2)
                day = entry.date()
                t = touched.get((day, w_id), (0.0, 0))
                touched[(day, w_id)] = (round(t[0] + amount, 2), t[1] + 1)
                cs = customers.get((w_id, d_id, c_id), (0.0, 0, entry))
                customers[(w_id, d_id, c_id)] = (round(cs[0] + amount, 2), cs[1] + 1, max(cs[2], entry))
                nat = db.sql("SELECT c_n_nationkey FROM customer WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s",
                             (w_id, d_id, c_id))
                nk = int(nat[0][0]) if nat and nat[0][0] is not None else 0
                nations[(nk, day)] = round(nations.get((nk, day), 0.0) + amount, 2)
            # Derived tables.
            for (day, w_id), (amount, n) in touched.items():
                ex = db.sql("SELECT COUNT(*) FROM daily_revenue WHERE day = %s AND w_id = %s", (day, w_id))
                if int(ex[0][0]):
                    db.sql("UPDATE daily_revenue SET revenue = revenue + %s, orders = orders + %s "
                           "WHERE day = %s AND w_id = %s", (amount, n, day, w_id))
                else:
                    db.sql("INSERT INTO daily_revenue (day, w_id, revenue, orders) VALUES (%s, %s, %s, %s)",
                           (day, w_id, amount, n))
            for (w_id, d_id, c_id), (amount, n, entry) in customers.items():
                ex = db.sql("SELECT COUNT(*) FROM customer_summary WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s",
                            (w_id, d_id, c_id))
                if int(ex[0][0]):
                    db.sql("UPDATE customer_summary SET order_count = order_count + %s, total_amount = total_amount + %s, "
                           "last_order = %s WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s",
                           (n, amount, entry, w_id, d_id, c_id))
                else:
                    db.sql("INSERT INTO customer_summary (c_w_id, c_d_id, c_id, order_count, total_amount, last_order) "
                           "VALUES (%s, %s, %s, %s, %s, %s)", (w_id, d_id, c_id, n, amount, entry))
            for (nk, day), amount in nations.items():
                ex = db.sql("SELECT COUNT(*) FROM nation_sales WHERE n_nationkey = %s AND day = %s", (nk, day))
                if int(ex[0][0]):
                    db.sql("UPDATE nation_sales SET revenue = revenue + %s WHERE n_nationkey = %s AND day = %s",
                           (amount, nk, day))
                else:
                    db.sql("INSERT INTO nation_sales (n_nationkey, day, revenue) VALUES (%s, %s, %s)",
                           (nk, day, amount))
            # Integrity: recompute this step's revenue per touched day from
            # the base tables and compare with the delta applied to
            # daily_revenue.
            mismatches = 0
            for (day, w_id), (amount, _n) in touched.items():
                lo = datetime.combine(day, datetime.min.time())
                hi = lo + timedelta(days=1)
                truth = db.sql("SELECT CAST(SUM(ol.ol_amount) AS DECIMAL(14,2)) FROM orders o JOIN order_line ol "
                               "ON ol.ol_w_id = o.o_w_id AND ol.ol_d_id = o.o_d_id AND ol.ol_o_id = o.o_id "
                               "WHERE o.o_w_id = %s AND o.o_id BETWEEN %s AND %s "
                               "AND o.o_entry_d >= %s AND o.o_entry_d < %s",
                               (w_id, base_id, base_id + rows - 1, lo, hi))
                t = float(truth[0][0] or 0) if truth else 0.0
                if abs(t - amount) > 0.05:
                    mismatches += 1
                db.sql("SELECT revenue, orders FROM daily_revenue WHERE day = %s AND w_id = %s", (day, w_id))
            return {"rows": rows, "mismatches": mismatches, "days": len(touched)}
        return script
