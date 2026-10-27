"""S4: Development agent.

The spine is production: TPC-C traffic from the spine load, a checkpoint
commit every ``spine_commit_every_txns`` spine transactions, and a schema
migration from the catalog every ``spine_migration_interval`` dev steps. Dev branches are created from the
spine (``concurrent_branches`` alive at once, ``dev_branches`` in total).
Each implements the loyalty feature: loyalty_tier and loyalty_event tables,
customer.c_tier, and a chunked backfill of customer rows (a share
``key_overlap`` of which also rewrites c_data, which Payment transactions
on the spine rewrite too); each dev step is ``statements_per_step``
statements split by ``write_fraction`` into backfill chunks and reads
of the feature (tier counts, event counts, top customers of a tier). The branch is hot for ``dev_phase_steps``, then
quiet for ``review_phase_steps`` with occasional modifications, rebases
onto the spine once or twice (conflicts resolved in the branch's favour),
and is deleted without merging.

Sentinels: loyalty_tier rows exist only on dev branches; item 100001+i is
inserted on the spine after branch i exists and must be visible on the
branch only after a rebase.

Schema conflict: the ``schema_conflict_migration``-th spine migration adds
``customer.c_tier INT`` while the feature branch has already added
``c_tier VARCHAR(8)``. Every branch alive at that point meets it at its
next rebase, which must fail naming the conflict; the agent then resolves
it on its side (moves its column to ``c_loyalty_tier``: add, copy, drop;
commits) and retries the rebase; a backend that replays history meets the original
commit again, and the branch then merges the spine in instead and keeps
integrating by merge. Either way the spine's changes must arrive with
both columns present. A branch forked after that migration finds c_tier
taken and adds ``c_loyalty_tier`` instead, as the agent would; it has no
conflict and is checked like every other branch.

Invariants: S4.1 the feature table never reaches the spine; S4.2a/b the
spine sentinel is invisible before and visible after the first rebase;
S4.4 a rebase keeps both sides (the branch's own tables, column and rows,
and every migration the spine has applied, counted through migration_log);
S4.3a the schema-conflict rebase fails naming a schema conflict and leaves
the branch intact (or succeeds with both sides present); S4.3b after the
rename the branch incorporates the spine (rebase retry, else a merge of
the spine into the branch) with its rows and values, the spine's column
and every spine migration present.
"""

import threading
import time

from macrobench.scenarios.base import Scenario, Worker, register
from macrobench.datagen.ch import DISTRICTS_PER_WAREHOUSE

SPINE_SENTINEL_BASE = 100_000
REVIEW_IDLE_SEC = 0.5   # idle time per review step without an edit (2026-10-16, was 0.05)
DEFAULT_SPINE_COMMIT_EVERY_TXNS = 200
SPINE_COMMIT_PERIOD_SEC = 2.0     # checkpoint period without a spine load

# Spine migration catalog M1-M5: (name, target table, ddl(k)).
MIGRATIONS = [
    ("M1_customer_segment", "customer",
     lambda k: f"ALTER TABLE customer ADD COLUMN c_segment_{k} VARCHAR(8)"),
    ("M2_stock_reorder", "stock",
     lambda k: f"ALTER TABLE stock ADD COLUMN s_reorder_{k} INT"),
    ("M3_item_category", "item",
     lambda k: f"ALTER TABLE item ADD COLUMN i_category_{k} VARCHAR(16) DEFAULT 'general'"),
    ("M4_promo_table", "promo",
     lambda k: f"CREATE TABLE promo_{k} (promo_id INT NOT NULL, i_id INT, "
               f"discount DECIMAL(4,2), PRIMARY KEY (promo_id))"),
    ("M5_district_manager", "district",
     lambda k: f"ALTER TABLE district ADD COLUMN d_manager_{k} VARCHAR(16)"),
]
# Applied as the schema_conflict_migration-th migration: the same column
# the feature branch adds (c_tier VARCHAR(8)) with a different type.
SCHEMA_CONFLICT_MIGRATION = ("M6_c_tier_conflict", "customer",
                             lambda k: "ALTER TABLE customer ADD COLUMN c_tier INT")


@register
class DevAgentScenario(Scenario):
    key = "dev_agent"
    name = "S4 development agent"
    uses_spine_load = True
    spine_touches_c_data = True

    def validate(self):
        p = self.params
        if p.dev_branches <= 0 or p.dev_phase_steps <= 0:
            raise ValueError("dev_agent needs dev_branches and dev_phase_steps > 0")

    def total_units(self) -> int:
        p = self.params
        return p.dev_branches * (p.dev_phase_steps + p.review_phase_steps)

    # ------------------------------------------------------------------

    def run(self):
        ctx, p = self.ctx, self.params
        self._spine_lock = threading.Lock()
        self._dev_steps = 0
        self._migrations = 0
        self._schema_conflict_applied = False
        self._counter_lock = threading.Lock()
        done = threading.Event()
        prod = threading.Thread(target=self._production_loop, args=(done,),
                                daemon=True, name="production")
        prod.start()
        try:
            ctx.run_workers(list(range(1, p.dev_branches + 1)), self._dev_branch,
                            threads=max(1, p.concurrent_branches or ctx.worker_threads))
        finally:
            done.set()
            prod.join(60)
        ctx.add_metric("dev_steps", self._dev_steps)
        ctx.add_metric("spine_migrations", self._migrations)

    def _production_loop(self, done: threading.Event):
        """Spine checkpoints and migrations while dev branches work."""
        import dblib.result_collector as rc
        rc.set_current_thread_id(800)
        ctx, p = self.ctx, self.params
        suite = ctx.new_suite()
        rng = ctx.worker_rng(800)
        last_commit = time.time()
        next_migration_at = p.spine_migration_interval or 0
        every = p.spine_commit_every_txns or DEFAULT_SPINE_COMMIT_EVERY_TXNS
        load = self.spine_load

        def txns():
            return load.total if load is not None else 0
        next_commit_at = txns() + every
        try:
            while not done.is_set():
                with self._counter_lock:
                    steps = self._dev_steps
                if next_migration_at and steps >= next_migration_at:
                    self._migrate(suite, rng)
                    next_migration_at += p.spine_migration_interval
                    last_commit = time.time()
                    next_commit_at = txns() + every
                elif (txns() >= next_commit_at if load is not None
                      else time.time() - last_commit >= SPINE_COMMIT_PERIOD_SEC):
                    self._spine_commit(suite, "production checkpoint")
                    last_commit = time.time()
                    next_commit_at = txns() + every
                done.wait(0.2)
        finally:
            ctx.close_suite(suite)

    def _spine_commit(self, suite, message, label="spine_commit"):
        with self._spine_lock, self.quiesced():
            return suite.commit(self.ctx.spine, message, label=label)

    def _migrate(self, suite, rng):
        ctx, p = self.ctx, self.params
        # Counter, conflict flag, statement and commit all under the spine
        # lock, so a rebase that holds the lock sees a spine whose committed
        # migrations are exactly what the counters say.
        with self._spine_lock, self.quiesced():
            self._migrations += 1
            k = self._migrations
            if p.schema_conflict_migration and k == p.schema_conflict_migration:
                # The feature branch added customer.c_tier VARCHAR(8); the
                # spine now adds the same column as INT: a schema conflict for
                # every branch alive at this point, met at its next rebase.
                name, table, ddl = SCHEMA_CONFLICT_MIGRATION
            elif rng.random() < p.key_overlap:
                name, table, ddl = MIGRATIONS[0]        # targets customer
            else:
                name, table, ddl = rng.choice(MIGRATIONS[1:])
            stmt = ddl(k)

            def script(db):
                db.sql(stmt)
                db.sql("INSERT INTO migration_log (migration_id, name, applied_step, applied_at) "
                       "VALUES (%s, %s, %s, CURRENT_TIMESTAMP)", (k, name, self._dev_steps))
            res = suite.exec(script, refs=[ctx.spine], label="spine_migration")[0]
            suite.commit(ctx.spine, f"migration {name} #{k}", label="migration_commit")
            if res.ok and name == SCHEMA_CONFLICT_MIGRATION[0]:
                self._schema_conflict_applied = True
        if not res.ok:
            ctx.note(f"migration {name}: {res.status_name} {res.error[:100]}")

    # ------------------------------------------------------------------

    def _dev_branch(self, w: Worker, i: int):
        ctx, p, inv, suite, rng = self.ctx, self.params, self.ctx.invariants, w.suite, w.rng
        branch = f"dev_{i}"
        res = ctx.branch(suite, branch, ctx.spine, label="dev_branch")
        if not res.ok:
            ctx.note(f"dev branch {branch}: {res.status_name} {res.error}")
            ctx.tick(p.dev_phase_steps + p.review_phase_steps, outcome="failed")
            return None
        # The branch's feature column. An agent looks before it adds: a fork
        # taken after the spine's conflicting migration already has c_tier
        # (INT), so the feature goes in as c_loyalty_tier and never conflicts.
        taken = suite.exec(["SELECT COUNT(c_tier) FROM customer WHERE 1 = 0"],
                           refs=[branch], label="invariant")[0].ok
        col = "c_loyalty_tier" if taken else "c_tier"
        # Spine sentinel: inserted on the spine after the branch exists.
        sentinel_id = SPINE_SENTINEL_BASE + i
        with self._spine_lock, self.quiesced():
            suite.exec([("INSERT INTO item (i_id, i_im_id, i_name, i_price, i_data) "
                         "VALUES (%s, 1, %s, 1.00, 'spine sentinel')",
                         (sentinel_id, f"spine_sentinel_{i}"))],
                       refs=[ctx.spine], label="spine_sentinel")
            suite.commit(ctx.spine, f"spine sentinel {i}", label="spine_commit")

        by_merge = False          # after a schema conflict a replaying backend cannot rebase past
                                  # the original commit; the branch then merges the spine in instead
        rebase_steps = {p.dev_phase_steps}
        if p.rebases_per_branch >= 2 and p.dev_phase_steps >= 2:
            rebase_steps.add(p.dev_phase_steps // 2)
        rebases_done = 0
        chunk = 0

        def sentinel_visible():
            r = w.exec([("SELECT COUNT(*) FROM item WHERE i_id = %s", (sentinel_id,))],
                       branch, label="invariant")
            return int(r.rows[0][0]) if r.rows else None

        # Dev phase.
        for step in range(1, p.dev_phase_steps + 1):
            ctx.check_stop()
            if step == 1:
                w.exec(self._feature_ddl(i, col), branch, label="feature_ddl")
            else:
                w.exec(self._backfill_script(i, step, chunk, rng, col), branch, label="backfill")
                chunk += self._backfill_writes()
            committed = ctx.should_commit(step - 1, last_step=(step == p.dev_phase_steps))
            if committed:
                w.commit(branch, f"dev {i} step {step}", label="dev_commit")
            with self._counter_lock:
                self._dev_steps += 1
            if step in rebase_steps:
                if not committed:  # a rebase needs a clean working set
                    w.commit(branch, f"dev {i} step {step} (pre-rebase)", label="dev_commit")
                if rebases_done == 0:
                    inv.expect(f"S4.2a spine sentinel invisible on {branch} before rebase",
                               sentinel_visible(), 0)
                with self._spine_lock:
                    conflict_due = self._schema_conflict_applied
                    schema_before = self._branch_schema(suite, branch, col)
                    events_before, tiered_before = schema_before["events"], schema_before["tiered"]
                    if by_merge:
                        rb = ctx.retry(lambda: suite.merge(branch, ctx.spine, message=f"dev {i} merge spine",
                                                           on_conflict="ours", label="spine_merge"))
                    else:
                        rb = ctx.retry(lambda: suite.rebase(branch, ctx.spine, on_conflict="theirs", label="dev_rebase"))
                    schema_after = self._branch_schema(suite, branch, col)
                    spine_migrations = self._migrations
                rebases_done += 1
                if rb.ok and isinstance(rb.value, dict):
                    ctx.bump_metric("rebase_conflicts", int(rb.value.get("conflicts", 0)))
                elif rb.failed:
                    ctx.bump_metric("rebase_failed")
                    ctx.note(f"{branch} rebase: {rb.error[:120]}")
                # S4.4: a rebase keeps both sides: the branch's own tables,
                # column and rows, and every migration the spine has applied
                # (migration_log rows are written by the migrations).
                # S4.3: once the spine carries the conflicting c_tier, the
                # rebase either fails naming a schema conflict and leaves the
                # branch intact, or succeeds with both sides present; a
                # silently dropped side fails either check.
                def is_intact(sc):
                    return (sc["tiers"] == 3 and sc["events"] == events_before
                            and sc["feature_col"] and sc["tiered"] == tiered_before)
                intact = is_intact(schema_after)
                if conflict_due and col == "c_tier":
                    # First integration after the conflicting migration; a
                    # branch that already moved its column, or was born with
                    # c_loyalty_tier, is checked as usual.
                    ctx.bump_metric("schema_conflict_rebases")
                    if rb.failed:
                        inv.check(f"S4.3a schema-conflict rebase of {branch} fails cleanly",
                                  "schema conflict" in (rb.error or "").lower() and intact,
                                  f"error={rb.error[:100]!r} tiers={schema_after['tiers']} events={schema_after['events']}/{events_before} "
                                  f"tiered={schema_after['tiered']}/{tiered_before}")
                        # The agent resolves on its side: rename its column,
                        # commit, rebase again. The retry must bring the
                        # spine in with both columns present.
                        # Move the column rather than rename it: RENAME COLUMN
                        # is missing on some dialects, and Doltgres implements
                        # it by recreating the table, which its merge then
                        # refuses as a table deletion.
                        new_col = "c_loyalty_tier"
                        rn = w.exec([f"ALTER TABLE customer ADD COLUMN {new_col} VARCHAR(8)",
                                     f"UPDATE customer SET {new_col} = {col} WHERE {col} IS NOT NULL",
                                     f"ALTER TABLE customer DROP COLUMN {col}"], branch, label="schema_resolve")
                        if rn.ok:
                            col = new_col
                            w.commit(branch, f"dev {i} resolve schema conflict: rename {new_col}", label="dev_commit")

                            def brought_in(sc, n):
                                return is_intact(sc) and sc["spine_col"] and sc["migrations"] == n

                            with self._spine_lock:
                                rb2 = ctx.retry(lambda: suite.rebase(branch, ctx.spine, on_conflict="theirs",
                                                                     label="dev_rebase_retry"))
                                schema_retry = self._branch_schema(suite, branch, col)
                                spine_migrations = self._migrations
                            resolved_by, last = None, rb2
                            if rb2.ok and brought_in(schema_retry, spine_migrations):
                                resolved_by = "rebase"
                            elif rb2.failed and "schema conflict" in (rb2.error or "").lower():
                                # A backend that replays history meets the
                                # original commit again: merge the spine in
                                # (final states) and integrate by merge from
                                # now on.
                                with self._spine_lock:
                                    m2 = ctx.retry(lambda: suite.merge(
                                        branch, ctx.spine, message=f"dev {i} merge spine after schema conflict",
                                        on_conflict="ours", label="spine_merge_retry"))
                                    schema_retry = self._branch_schema(suite, branch, col)
                                    spine_migrations = self._migrations
                                last = m2
                                if m2.ok and brought_in(schema_retry, spine_migrations):
                                    resolved_by, by_merge = "merge", True
                            if last.ok and isinstance(last.value, dict):
                                ctx.bump_metric("rebase_conflicts", int(last.value.get("conflicts", 0)))
                            if resolved_by:
                                ctx.bump_metric(f"schema_conflict_resolved_by_{resolved_by}")
                            else:
                                ctx.bump_metric("rebase_failed")
                            inv.check(f"S4.3b {branch} incorporates the spine after moving its column",
                                      resolved_by is not None,
                                      f"via {resolved_by or 'nothing'}: last {last.op} {last.status_name} {last.error[:80]!r} "
                                      f"tiers={schema_retry['tiers']} events={schema_retry['events']}/{events_before} "
                                      f"tiered={schema_retry['tiered']}/{tiered_before} spine c_tier={schema_retry['spine_col']} "
                                      f"migrations={schema_retry['migrations']}/{spine_migrations}")
                            rb = last
                        else:
                            inv.check(f"S4.3b {branch} incorporates the spine after moving its column", False,
                                      f"column move failed: {rn.error[:120]}")
                    else:
                        inv.check(f"S4.3a schema-conflict rebase of {branch} keeps both sides",
                                  intact and schema_after["migrations"] == spine_migrations,
                                  f"tiers={schema_after['tiers']} events={schema_after['events']}/{events_before} "
                                  f"tiered={schema_after['tiered']}/{tiered_before} migrations={schema_after['migrations']}/{spine_migrations}")
                elif rb.ok:
                    inv.check(f"S4.4 {'merge' if by_merge else 'rebase'} of {branch} keeps both sides (integration {rebases_done})",
                              intact and schema_after["migrations"] == spine_migrations,
                              f"tiers={schema_after['tiers']} events={schema_after['events']}/{events_before} "
                              f"tiered={schema_after['tiered']}/{tiered_before} migrations={schema_after['migrations']}/{spine_migrations}")
                if rebases_done == 1:
                    inv.expect(f"S4.2b spine sentinel visible on {branch} after rebase",
                               sentinel_visible(), 1, f"rebase {rb.status_name}")
            ctx.tick()

        # Review phase: quiet, occasional modification.
        modified = False
        for step in range(1, p.review_phase_steps + 1):
            ctx.check_stop()
            if rng.random() < p.review_modification_prob:
                w.exec([("UPDATE loyalty_tier SET min_payment = min_payment + 1 WHERE tier_id = 2", None),
                        ("SELECT COUNT(*) FROM loyalty_event", None)],
                       branch, label="review_edit")
                modified = True
                if ctx.should_commit(step - 1):
                    w.commit(branch, f"dev {i} review {step}", label="dev_commit")
                    modified = False
            else:
                time.sleep(REVIEW_IDLE_SEC)
            ctx.tick()
        if modified:
            w.commit(branch, f"dev {i} review end", label="dev_commit")

        # Invariant: feature data never leaks into production.
        # MySQL-protocol backends list every database's tables in
        # information_schema, so count only the current one there; the
        # dialect probe is untimed so its miss on Postgres is not a
        # recorded failure.
        r = suite.exec([("SELECT COUNT(*) FROM information_schema.tables "
                         "WHERE table_name = %s AND table_schema = DATABASE()",
                         ("loyalty_tier",))], refs=[ctx.spine], label="invariant")[0]
        if not r.ok:
            r = suite.exec([("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = %s",
                             ("loyalty_tier",))], refs=[ctx.spine], label="invariant")[0]
        if r.ok and r.rows:
            tables = int(r.rows[0][0])
        else:
            # No information_schema: probe the table itself (untimed, since a
            # missing table is the expected outcome, not a failed op).
            probe = suite.exec(["SELECT COUNT(*) FROM loyalty_tier"], refs=[ctx.spine],
                               timed=False)[0]
            tables = 0 if probe.failed else 1
        inv.expect(f"S4.1 loyalty_tier never on the spine (dev {i})", tables, 0,
                   "loyalty_tier tables on the spine")

        for _ in range(max(0, ctx.branch_ops.retention_steps)):
            time.sleep(REVIEW_IDLE_SEC)
        ctx.delete(suite, branch, label="dev_delete")
        return rebases_done

    def _branch_schema(self, suite, branch: str, col: str = "c_tier") -> dict:
        """What the branch holds of its own feature and of the spine's
        migrations: loyalty_tier rows, loyalty_event rows, whether
        customer.c_tier exists, migration_log rows. Recorded as "invariant" rows
        (kept out of the figures)."""
        def count(sql):
            r = suite.exec([sql], refs=[branch], label="invariant")[0]
            return int(r.rows[0][0]) if r.ok and r.rows else -1
        return {"tiers": count("SELECT COUNT(*) FROM loyalty_tier"),
                "events": count("SELECT COUNT(*) FROM loyalty_event"),
                "feature_col": count(f"SELECT COUNT({col}) FROM customer WHERE 1 = 0") >= 0,
                # The spine's column (c_tier INT after the conflicting
                # migration); same as the feature column before any rename.
                "spine_col": count("SELECT COUNT(c_tier) FROM customer WHERE 1 = 0") >= 0,
                # Rows the backfill tiered: readable as the branch wrote them,
                # whatever a merge did to the column's type.
                "tiered": count(f"SELECT COUNT(*) FROM customer WHERE {col} IN ('gold', 'silver', 'bronze')"),
                "migrations": count("SELECT COUNT(*) FROM migration_log")}

    def _feature_ddl(self, i: int, col: str = "c_tier"):
        def script(db):
            db.sql("CREATE TABLE loyalty_tier (tier_id INT NOT NULL, name VARCHAR(16) NOT NULL, "
                   "min_payment DECIMAL(12,2) NOT NULL, PRIMARY KEY (tier_id))")
            db.sql("CREATE TABLE loyalty_event (event_id INT NOT NULL, c_w_id INT, c_d_id INT, "
                   "c_id INT, tier_id INT, created_at TIMESTAMP, PRIMARY KEY (event_id))")
            db.sql(f"ALTER TABLE customer ADD COLUMN {col} VARCHAR(8)")
            db.sql("INSERT INTO loyalty_tier (tier_id, name, min_payment) VALUES "
                   "(1, 'bronze', 0), (2, 'silver', 5000), (3, 'gold', 9000)")
        return script

    def _backfill_writes(self) -> int:
        """Backfill chunks per dev step: statements_per_step split by
        write_fraction, as in S1 and S2; the rest of the step is reads."""
        n = self.ctx.statements_per_step
        return max(1, round(n * self.ctx.data_ops.write_fraction)) if n > 1 else 1

    def _backfill_script(self, i: int, step: int, chunk: int, rng, col: str = "c_tier"):
        """One dev step: ``writes`` backfill chunks (a range update of
        rows_per_write customers plus an event insert each) and then
        ``reads`` queries a developer would run against the feature."""
        ctx = self.ctx
        n = ctx.statements_per_step
        writes = self._backfill_writes()
        reads = max(0, n - writes)
        rows = ctx.rows_per_write
        customers = ctx.scale.customers_per_district
        per_district = max(1, (customers + rows - 1) // rows)
        overlap = self.params.key_overlap
        read_kinds = [rng.randrange(4) for _ in range(reads)]

        def script(db):
            last = (1, 1)
            for k in range(writes):
                idx = chunk + k
                w_id = 1 + (idx // (per_district * DISTRICTS_PER_WAREHOUSE)) % ctx.scale.warehouses
                d_id = 1 + (idx // per_district) % DISTRICTS_PER_WAREHOUSE
                lo = 1 + (idx % per_district) * rows
                hi = min(customers, lo + rows - 1)
                if rng.random() < overlap:
                    db.sql(f"UPDATE customer SET {col} = CASE WHEN c_ytd_payment > 9000 THEN 'gold' "
                           "WHEN c_ytd_payment > 5000 THEN 'silver' ELSE 'bronze' END, "
                           "c_data = 'loyalty backfill' "
                           "WHERE c_w_id = %s AND c_d_id = %s AND c_id BETWEEN %s AND %s",
                           (w_id, d_id, lo, hi))
                else:
                    db.sql(f"UPDATE customer SET {col} = CASE WHEN c_ytd_payment > 9000 THEN 'gold' "
                           "WHEN c_ytd_payment > 5000 THEN 'silver' ELSE 'bronze' END "
                           "WHERE c_w_id = %s AND c_d_id = %s AND c_id BETWEEN %s AND %s",
                           (w_id, d_id, lo, hi))
                db.sql("INSERT INTO loyalty_event (event_id, c_w_id, c_d_id, c_id, tier_id, created_at) "
                       "VALUES (%s, %s, %s, %s, 1, CURRENT_TIMESTAMP)",
                       (i * 1_000_000 + step * 1000 + k, w_id, d_id, lo))
                last = (w_id, d_id)
            w_id, d_id = last
            for kind in read_kinds:
                if kind == 0:
                    db.sql(f"SELECT {col}, COUNT(*) FROM customer GROUP BY {col}")
                elif kind == 1:
                    db.sql("SELECT COUNT(*) FROM loyalty_event WHERE c_w_id = %s", (w_id,))
                elif kind == 2:
                    db.sql(f"SELECT c_id, c_ytd_payment FROM customer WHERE c_w_id = %s AND c_d_id = %s "
                           f"AND {col} = 'gold' ORDER BY c_ytd_payment DESC LIMIT 10", (w_id, d_id))
                else:
                    db.sql("SELECT t.name, COUNT(e.event_id) FROM loyalty_tier t "
                           "LEFT JOIN loyalty_event e ON e.tier_id = t.tier_id GROUP BY t.name")
        return script
