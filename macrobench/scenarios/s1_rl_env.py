"""S1: Agentic RL environment.

Root -> T task branches (fault injected, task commit) -> per task a group of
G rollout leaves of up to S steps. Each rollout step is one exec() (reads,
writes, occasional DDL) followed by a commit; later rollouts fork from a
random recorded step (branch@commit) and consume the remaining budget. When
the group is done, a cross-branch read scores every leaf on the TPC-C
consistency conditions, each leaf is diffed against the task commit for
collateral change, and the task's branches are deleted.

Parallelism is two-level, as in a GRPO rollout loop: ``worker_threads``
tasks run at once (a batch of prompts), and within a task the group is
sampled in rounds of ``rollouts_per_round`` rollouts that run concurrently
on an inner pool, each on its own connection. Fork points for a round are
chosen before it starts, so a round forks from earlier rounds' commits but
not its own. Live rollout branches = worker_threads x rollouts_per_round.

Sentinels (item rows at reserved ids): 100000+t is written in the task
state and never touched; 200000+t gets a rollout-specific price on every
path; task B must not see task A's rows.

Invariants: S1.1 the kept sentinel is readable from every leaf; S1.2 the
path sentinel differs per leaf; S1.3 a task cannot see another task's
sentinel; S1.4 the diff of a leaf against the task commit reports exactly
the rows the path inserted (its repair_audit rows, plus what the fault's
repair re-inserts on a repaired path), at least the existing stock rows
and the path sentinel it updated, and no deletion other than the
delete-plus-add some backends report for a repair's update of a row in a
table without a primary key; S1.5 the schema diff of a leaf against the
commit it forked from reports exactly the DDL the rollout ran (the
repair_audit table when the rollout started from the task commit, else
the note_<step> columns it added; indexes are not part of the schema half of a diff).
"""

import random

from macrobench import faults
from macrobench.scenarios.base import Scenario, ScenarioStopped, Worker, register

KEEP_SENTINEL_BASE = 100_000
PATH_SENTINEL_BASE = 200_000


def sentinel_name(task_id: int) -> str:
    return f"sentinel_keep_{task_id}"


@register
class RlEnvScenario(Scenario):
    key = "rl_env"
    name = "S1 RL environment"

    def validate(self):
        p = self.params
        if p.tasks <= 0 or p.group_size <= 0 or p.step_budget <= 0:
            raise ValueError("rl_env needs tasks, group_size and step_budget > 0")
        if p.group_size > 32:
            raise ValueError("rl_env group_size must be <= 32 (a GRPO group); raise tasks instead")
        if p.rollouts_per_round > p.group_size:
            raise ValueError("rl_env rollouts_per_round must be <= group_size")

    def total_units(self) -> int:
        return self.params.tasks * (1 + self.params.group_size)

    # ------------------------------------------------------------------

    def run(self):
        p = self.params
        ctx = self.ctx
        results = ctx.run_workers(list(range(1, p.tasks + 1)), self._run_task)
        scores = [r for r in results if isinstance(r, dict)]
        ctx.add_metric("tasks_completed", len(scores))
        ctx.add_metric("leaves", sum(r["leaves"] for r in scores))
        ctx.add_metric("rollout_steps", sum(r["steps"] for r in scores))
        ctx.add_metric("historical_forks", sum(r["historical_forks"] for r in scores))
        if scores:
            ctx.add_metric("mean_leaf_reward",
                           sum(r["mean_reward"] for r in scores) / len(scores))

    # ------------------------------------------------------------------

    def _run_task(self, w: Worker, task_id: int) -> dict:
        ctx, p = self.ctx, self.params
        suite, rng = w.suite, w.rng
        task_branch = f"task_{task_id}"
        self._touched_steps = getattr(self, "_touched_steps", {})   # (branch, step) -> stock keys so far
        self._touched = getattr(self, "_touched", {})               # leaf branch -> distinct stock rows updated
        self._ddl = getattr(self, "_ddl", {})                       # leaf branch -> (fork ref, leaf ref, table added?, columns added)
        fault, fw, fd = faults.pick_fault(rng, ctx.scale.warehouses)

        # 1. Task-specific state: fault + sentinels + registry row, committed.
        ctx.check_stop()
        ctx.branch(suite, task_branch, ctx.spine, label="task_branch")

        def task_setup(db):
            state = fault.inject(db, fw, fd)
            db.sql("INSERT INTO item (i_id, i_im_id, i_name, i_price, i_data) "
                   "VALUES (%s, 1, %s, 1.00, 'sentinel kept')",
                   (KEEP_SENTINEL_BASE + task_id, sentinel_name(task_id)))
            db.sql("INSERT INTO item (i_id, i_im_id, i_name, i_price, i_data) "
                   "VALUES (%s, 1, %s, 0.00, 'sentinel path')",
                   (PATH_SENTINEL_BASE + task_id, f"sentinel_path_{task_id}"))
            db.sql("INSERT INTO rl_task (task_id, fault_id, fault_w_id, fault_d_id, "
                   "status, created_at) VALUES (%s, %s, %s, %s, 'open', CURRENT_TIMESTAMP)",
                   (task_id, fault.fault_id, fw, fd))
            return state

        res = w.exec(task_setup, task_branch, label="task_setup")
        fault_state = res.value if res.ok else {}
        commit = w.commit(task_branch, f"task {task_id}: {fault.name}", label="task_commit")
        task_commit = commit.value if commit.ok else None
        task_ref = f"{task_branch}@{task_commit}" if task_commit else task_branch
        ctx.tick()

        # 2. Rollouts, in rounds. recorded = [(ref, step)] of every committed
        # step so far; the rollouts of one round are sampled concurrently on
        # an inner pool (GRPO samples a group in parallel from one policy
        # snapshot), each from a fork point chosen before the round starts,
        # so a round can fork from earlier rounds' paths but not from its own.
        recorded = [(task_ref, 0)]
        leaves = []          # (branch, final price of the path sentinel)
        steps_total = 0
        historical = 0
        per_round = p.rollouts_per_round or p.group_size
        rollout = 0
        while len(leaves) < p.group_size:
            ctx.check_stop()
            plan = []
            for _ in range(min(per_round, p.group_size - len(leaves))):
                rollout += 1
                fork_ref, fork_step = recorded[0]
                # Historical fork: any recorded step with budget left.
                forkable = [r for r in recorded[1:] if r[1] < p.step_budget]
                if forkable and rng.random() < p.historical_fork_prob:
                    fork_ref, fork_step = rng.choice(forkable)
                    historical += 1
                plan.append((rollout, fork_ref, fork_step))

            def run_rollout(rw: Worker, item, task_id=task_id, task_ref=task_ref,
                            fault=fault, fw=fw, fd=fd, fault_state=fault_state):
                return self._run_rollout(rw, task_id, item, fault, fw, fd, fault_state)

            results = ctx.run_workers(plan, run_rollout, threads=len(plan),
                                      thread_base=1000 * task_id)
            for item, r in zip(plan, results):
                if not isinstance(r, dict):
                    if isinstance(r, ScenarioStopped):
                        raise r
                    continue        # the pool accounted the crashed rollout
                steps_total += r["steps"]
                recorded.extend(r["recorded"])
                with ctx._lock:
                    self._touched_steps.update(r["touched_steps"])
                    self._touched[r["branch"]] = r["touched"]
                    self._ddl[r["branch"]] = r["ddl"]
                if r["leaf"]:
                    leaves.append(r["leaf"])
                ctx.tick(outcome="succeeded" if r["leaf"] else "failed")

        # 3. Score the group: cross-branch consistency read + diff per leaf.
        leaf_refs = [b for b, _ in leaves]
        rewards = self._score(w, task_id, leaf_refs, task_ref, fault, fw, fd)

        # 4. Invariance checks.
        self._check_invariants(w, task_id, leaves)

        # 5. Delete the task's branches.
        for b in leaf_refs:
            ctx.delete(suite, b, label="rollout_delete")
        ctx.delete(suite, task_branch, label="task_delete")
        return {
            "task": task_id, "leaves": len(leaves), "steps": steps_total,
            "historical_forks": historical,
            "mean_reward": (sum(rewards) / len(rewards)) if rewards else 0.0,
        }

    def _run_rollout(self, w: Worker, task_id: int, item, fault, fw: int, fd: int,
                     fault_state: dict) -> dict:
        """One rollout of a task on its own worker: branch from the fork
        point, run the remaining step budget with a commit per
        commit_interval, and return what the task thread needs to record.
        The rng is seeded per (task, rollout) so the path does not depend
        on which thread runs it."""
        ctx, p = self.ctx, self.params
        rollout, fork_ref, fork_step = item
        rng = random.Random(ctx.seed * 7919 + task_id * 1000 + rollout)
        suite = w.suite
        branch = f"task_{task_id}_r{rollout}"
        res = ctx.branch(suite, branch, fork_ref, label="rollout_branch")
        if not res.ok:
            ctx.note(f"[T{task_id}] rollout branch {branch}: {res.status_name} {res.error}")
            return {"branch": branch, "leaf": None, "steps": 0, "recorded": [],
                    "touched_steps": {}, "touched": 0, "ddl": (fork_ref, branch, False, [])}
        price = round(rollout * 1.25, 2)
        budget = p.step_budget - fork_step
        # Stock rows this path has updated, inherited from the fork point
        # (the parent's rows up to that step).
        with ctx._lock:
            touched = set(self._touched_steps.get((fork_ref.split("@")[0], fork_step), set()))
        repair_at = rng.randint(1, max(1, budget)) if rng.random() < 0.5 else None
        table_added, cols_added, leaf_ref = False, [], branch
        recorded, touched_steps, steps = [], {}, 0
        for i in range(budget):
            ctx.check_stop()
            step = fork_step + i + 1
            script, keys, ddl = self._step_script(task_id, rollout, step, i == 0, price,
                                                  fault, fw, fd, fault_state,
                                                  repair=(repair_at == i + 1), rng=rng,
                                                  create_audit=(fork_step == 0))
            res = w.exec(script, branch, label="rollout_step")
            if res.ok:
                touched |= keys
                if ddl and ddl[0] == "table":
                    table_added = True
                elif ddl and ddl[0] == "column":
                    cols_added.append(ddl[1])
            steps += 1
            if ctx.should_commit(i, last_step=(i == budget - 1)):
                c = w.commit(branch, f"rollout {rollout} step {step}", label="step_commit")
                if c.ok and c.value:
                    recorded.append((f"{branch}@{c.value}", step))
                    touched_steps[(branch, step)] = set(touched)
                    leaf_ref = f"{branch}@{c.value}"
        return {"branch": branch, "leaf": (branch, price), "steps": steps,
                "recorded": recorded, "touched_steps": touched_steps,
                "touched": len(touched), "ddl": (fork_ref, leaf_ref, table_added, cols_added)}

    # ------------------------------------------------------------------

    def _check_schema_diff(self, w: Worker, fork_ref: str, leaf_ref: str, branch: str,
                           table_added: bool, cols_added: list) -> None:
        """S1.5: the schema half of a diff from the fork point to the leaf
        lists exactly the rollout's DDL: the repair_audit table if this
        rollout created it (its columns are then not listed separately),
        otherwise the note_<step> columns it added; nothing dropped or
        retyped. An engine diff that covers rows only (no schema in the
        result) fails the check."""
        ctx, inv = self.ctx, self.ctx.invariants
        name = f"S1.5 schema diff of {branch} against its fork point reports the rollout's DDL"
        sd = ctx.retry(lambda: w.suite.diff(fork_ref, leaf_ref, label="fork_diff"))
        if not sd.ok or not isinstance(sd.value, dict):
            inv.check(name, False, f"diff failed: {sd.error[:100]}")
            return
        v = sd.value.get("schema")
        if not isinstance(v, dict):
            inv.check(name, False, "the diff reports no schema (rows only)")
            return
        want_tables = ["repair_audit"] if table_added else []
        want_cols = [] if table_added else sorted(cols_added)
        got_cols = sorted(v.get("columns_added", {}).get("repair_audit", []))
        other_cols = {t: c for t, c in v.get("columns_added", {}).items() if t != "repair_audit"}
        inv.check(name,
                  v.get("tables_added") == want_tables and not v.get("tables_dropped")
                  and got_cols == want_cols and not other_cols
                  and not v.get("columns_dropped") and not v.get("columns_changed"),
                  f"tables_added={v.get('tables_added')} (want {want_tables}) "
                  f"columns_added[repair_audit]={got_cols} (want {want_cols}) "
                  f"other={other_cols} dropped={v.get('tables_dropped')}/{v.get('columns_dropped')} "
                  f"changed={v.get('columns_changed')}")

    def _step_script(self, task_id, rollout, step, first, price, fault, fw, fd,
                     fault_state, repair, rng, create_audit=True):
        """One rollout step: a mix of reads and writes sized by DataOps.
        The rollout's first step sets the path sentinel and, when the
        rollout starts from the task commit, creates repair_audit; later
        steps add a column or an index with probability ddl_fraction."""
        ctx = self.ctx
        n = ctx.statements_per_step
        writes = max(1, round(n * ctx.data_ops.write_fraction)) if n > 1 else 1
        reads = max(0, n - writes)
        rows = ctx.rows_per_write
        do_ddl = first or rng.random() < ctx.data_ops.ddl_fraction
        if first and create_audit:
            ddl = ("table", "repair_audit")
        elif do_ddl:
            ddl = ("column", f"note_{step}") if step % 2 == 0 else ("index", f"idx_audit_r{rollout}_s{step}")
        else:
            ddl = None
        start_item = rng.randint(1, max(1, ctx.scale.items - rows))
        conditions = [rng.choice(faults.CONDITIONS) for _ in range(reads)]
        # The ranges after the first can run past the last item; only rows
        # that exist count towards what the diff must report as modified.
        keys = {(fw, i) for k in range(writes) for j in range(rows)
                for i in (start_item + k * rows + j,) if i <= ctx.scale.items}

        def script(db):
            if first:
                db.sql("UPDATE item SET i_price = %s WHERE i_id = %s",
                       (price, PATH_SENTINEL_BASE + task_id))
            if first and create_audit:
                db.sql("CREATE TABLE repair_audit (audit_id INT NOT NULL, task_id INT, "
                       "step INT, action VARCHAR(40), detail VARCHAR(200), "
                       "PRIMARY KEY (audit_id))")
            elif do_ddl:
                if step % 2 == 0:
                    db.sql(f"ALTER TABLE repair_audit ADD COLUMN note_{step} VARCHAR(20)")
                else:
                    db.sql(f"CREATE INDEX idx_audit_r{rollout}_s{step} ON repair_audit (task_id, step)")
            for k in range(writes):
                lo = start_item + k * rows
                db.sql("UPDATE stock SET s_quantity = s_quantity + 1, s_remote_cnt = s_remote_cnt + 1 "
                       "WHERE s_w_id = %s AND s_i_id BETWEEN %s AND %s", (fw, lo, lo + rows - 1))
                db.sql("INSERT INTO repair_audit (audit_id, task_id, step, action, detail) "
                       "VALUES (%s, %s, %s, %s, %s)",
                       (rollout * 100_000 + step * 100 + k, task_id, step, "probe",
                        f"stock {lo}-{lo + rows - 1}"))
            if repair:
                fault.repair(db, fw, fd, fault_state)
                db.sql("INSERT INTO repair_audit (audit_id, task_id, step, action, detail) "
                       "VALUES (%s, %s, %s, 'repair', %s)",
                       (rollout * 100_000 + step * 100 + 99, task_id, step, fault.name))
            qs = faults.consistency_queries()
            for cond in conditions:
                db.sql(qs[cond])
            db.sql("SELECT COUNT(*) FROM repair_audit WHERE task_id = %s", (task_id,))
        return script, keys, ddl

    def _score(self, w: Worker, task_id: int, leaf_refs: list, task_ref: str,
               fault, fw: int, fd: int) -> list:
        """Group-relative reward: -violations per leaf (cross-branch read),
        plus a data diff of each leaf against the task commit and a schema
        diff against the commit it forked from."""
        ctx = self.ctx
        if not leaf_refs:
            return []

        def script_for(multi: bool):
            if multi:
                def script(db):
                    out = {}
                    for ref in leaf_refs:
                        out[ref] = faults.check_consistency(db, lambda t, r=ref: db.table(r, t))
                    return out
                return script
            return lambda db: faults.check_consistency(db)

        results = ctx.cross_branch_exec(w.suite, script_for, leaf_refs, label="score")
        rewards = []
        if len(results) == 1 and results[0].ok and isinstance(results[0].value, dict) \
                and leaf_refs[0] in results[0].value:
            per_leaf = results[0].value
            for ref in leaf_refs:
                v = per_leaf.get(ref, {})
                rewards.append(-sum(x for x in v.values() if x > 0))
        else:
            for res in results:
                v = res.value if (res.ok and isinstance(res.value, dict)) else {}
                rewards.append(-sum(x for x in v.values() if x > 0))
        inv = ctx.invariants
        for ref in leaf_refs:
            # Schema diff of the leaf against the commit it forked from
            # (against the task commit the audit table is always new, so
            # no column change would be visible); S1.5.
            if ref in self._ddl:
                fork_ref, leaf_ref, table_added, cols_added = self._ddl[ref]
                self._check_schema_diff(w, fork_ref, leaf_ref, ref, table_added, cols_added)
            d = ctx.retry(lambda: w.suite.diff(task_ref, ref, label="score_diff"))
            if d.ok and isinstance(d.value, dict):
                ctx.bump_metric("diff_rows_modified", int(d.value.get("rows_modified") or 0))
                # S1.4: the diff of a leaf against the task commit must show
                # exactly the rows the path inserted: its repair_audit rows
                # (the table did not exist at the task commit) plus whatever
                # the fault's repair re-inserts once the path has repaired;
                # at least the existing stock rows and the path sentinel it
                # updated; and no deletion, except that a backend may report
                # the repair's update of a row in a keyless table (history)
                # as one deleted plus one added row.
                def counts(db):
                    audit = int(db.sql("SELECT COUNT(*) FROM repair_audit")[0][0])
                    repaired = int(db.sql("SELECT COUNT(*) FROM repair_audit WHERE action = 'repair'")[0][0]) > 0
                    keyless = (fault.repair_keyless_rows(db, fw, fd)
                               if repaired and fault.repair_keyless_rows else 0)
                    return audit, (fault.repair_inserts if repaired else 0), keyless
                cnt = w.exec(counts, ref, label="invariant")
                n_audit, inserts, keyless = cnt.value if cnt.ok else (-1, 0, 0)
                v = d.value
                added, deleted, modified = (int(v.get("rows_added") or 0), int(v.get("rows_deleted") or 0),
                                            int(v.get("rows_modified") or 0))
                min_mod = self._touched.get(ref, 0) + 1
                want_added = n_audit + inserts
                inv.check(f"S1.4 diff of {ref} against the task commit reports the path's changes",
                          n_audit >= 0 and deleted in (0, keyless)
                          and added - deleted == want_added and modified >= min_mod,
                          f"added={added} deleted={deleted} (want added-deleted={want_added}: audit rows {n_audit}"
                          f" + repair inserts {inserts}; deleted 0 or {keyless} keyless rows) "
                          f"modified={modified} (>= {min_mod} expected)")
        return rewards

    def _check_invariants(self, w: Worker, task_id: int, leaves: list):
        inv = self.ctx.invariants
        if not leaves:
            inv.not_applicable(f"S1.task{task_id}", "no leaves")
            return
        first_branch, first_price = leaves[0]
        res = w.exec([("SELECT i_name FROM item WHERE i_id = %s", (KEEP_SENTINEL_BASE + task_id,))],
                     first_branch, label="invariant")
        inv.expect(f"S1.1 kept sentinel readable from leaf (task {task_id})",
                   res.rows[0][0] if res.rows else None, sentinel_name(task_id))
        if len(leaves) > 1:
            other_branch, other_price = leaves[-1]
            r1 = w.exec([("SELECT i_price FROM item WHERE i_id = %s", (PATH_SENTINEL_BASE + task_id,))],
                        first_branch, label="invariant")
            r2 = w.exec([("SELECT i_price FROM item WHERE i_id = %s", (PATH_SENTINEL_BASE + task_id,))],
                        other_branch, label="invariant")
            v1 = float(r1.rows[0][0]) if r1.rows else None
            v2 = float(r2.rows[0][0]) if r2.rows else None
            inv.check(f"S1.2 path-divergent sentinel differs per leaf (task {task_id})",
                      v1 == first_price and v2 == other_price and v1 != v2,
                      f"{first_branch}={v1} (want {first_price}), {other_branch}={v2} (want {other_price})")
        other_task = task_id - 1 if task_id > 1 else self.params.tasks
        if other_task != task_id:
            res = w.exec([("SELECT COUNT(*) FROM item WHERE i_id = %s", (KEEP_SENTINEL_BASE + other_task,))],
                         first_branch, label="invariant")
            inv.expect(f"S1.3 task {task_id} cannot see task {other_task}'s sentinel",
                       int(res.rows[0][0]) if res.rows else None, 0)
