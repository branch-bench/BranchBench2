"""S3: Multi-agent collaboration.

The spine holds shared task state (task, dependency). Each round the spine
is checkpointed, N agents branch from that checkpoint, revise the plan
(``statements_per_step`` statements split by ``write_fraction``: task
status/assignee edits with a configurable key overlap and subtask
inserts with their edges, then reads; plus three fixed probes: the
sentinel update, a contradictory dependency edge and a scratch table
created and dropped), commit
with a message carrying their id, and merge into the spine one at a time
while the spine keeps receiving task updates. Merges resolve conflicts with the fixed policy:
spine wins on task rows except the sentinel task 0 (last merger wins, it
models a completion counter), and dependency edges are unioned and any
cycle broken by dropping the merging agent's edge. Side branches are
deleted after integration.

Sentinel: task 0's status must show three different values before the
first merge, between the two merges and after the second.

Invariants: S3.1 the shared sentinel takes three distinct values across
the first two merges; S3.2 after every successful merge the sentinel task
carries the merging agent's value (the resolver takes theirs on that row).
"""

import threading
import time

from macrobench.scenarios.base import Scenario, Worker, register

SENTINEL_TASK = 0
SHARED_SCRATCH = "scratch_shared"


def _find_cycle_edge(edges, prefer_by):
    """Return one edge (task_id, depends_on) that closes a cycle, preferring
    one added by ``prefer_by``; None if the graph is acyclic."""
    adj = {}
    for t, d, by in edges:
        adj.setdefault(t, []).append(d)
    WHITE, GREY, BLACK = 0, 1, 2
    color = {}
    stack = []

    def dfs(u):
        color[u] = GREY
        stack.append(u)
        for v in adj.get(u, []):
            if color.get(v, WHITE) == GREY:
                cycle = stack[stack.index(v):] + [v]
                return [(cycle[i], cycle[i + 1]) for i in range(len(cycle) - 1)]
            if color.get(v, WHITE) == WHITE:
                found = dfs(v)
                if found:
                    return found
        stack.pop()
        color[u] = BLACK
        return None

    for node in list(adj):
        if color.get(node, WHITE) == WHITE:
            cyc = dfs(node)
            if cyc:
                by = {(t, d): b for t, d, b in edges}
                for e in cyc:
                    if by.get(e) == prefer_by:
                        return e
                for e in cyc:
                    if by.get(e) != "seed":
                        return e
                return cyc[0]
    return None


def fixed_policy(db, conflicts):
    """Spine wins on task rows, except the sentinel task (theirs); other
    tables are left to the backend's 'ours'."""
    out = {}
    for cf in conflicts:
        table = cf["table"].split(".")[-1]
        if table != "task":
            continue
        for row in cf["rows"]:
            if row.get("their_task_id") == SENTINEL_TASK:
                db.sql("UPDATE task SET status = %s, assignee = %s, version = %s, updated_step = %s "
                       "WHERE task_id = %s",
                       (row.get("their_status"), row.get("their_assignee"),
                        row.get("their_version"), row.get("their_updated_step"), SENTINEL_TASK))
        db.resolve(table)
        out[table] = len(cf["rows"])
    return out


@register
class MultiAgentScenario(Scenario):
    key = "multi_agent"
    name = "S3 multi-agent collaboration"

    def validate(self):
        p = self.params
        if p.agents <= 0 or p.rounds <= 0:
            raise ValueError("multi_agent needs agents and rounds > 0")

    def total_units(self) -> int:
        return self.params.rounds * self.params.agents

    @property
    def num_tasks(self) -> int:
        return max(20, self.params.agents * 5)

    def seed(self, suite) -> dict:
        n = self.num_tasks
        rng = self.ctx.rng

        def script(db):
            db.sql("INSERT INTO task (task_id, parent_id, title, status, assignee, version, updated_step) "
                   "VALUES (%s, NULL, 'sentinel', 'open', NULL, 0, 0)", (SENTINEL_TASK,))
            roots = max(1, n // 4)
            for t in range(1, n + 1):
                parent = None if t <= roots else rng.randint(1, roots)
                db.sql("INSERT INTO task (task_id, parent_id, title, status, assignee, version, updated_step) "
                       "VALUES (%s, %s, %s, 'open', NULL, 0, 0)", (t, parent, f"task {t}"))
            for t in range(2, n + 1):
                db.sql("INSERT INTO dependency (task_id, depends_on, added_by) VALUES (%s, %s, 'seed')",
                       (t, rng.randint(1, t - 1)))
            if self.params.shared_scratch_table:
                db.sql(f"CREATE TABLE {SHARED_SCRATCH} (id INT NOT NULL, v INT, PRIMARY KEY (id))")
                db.sql(f"INSERT INTO {SHARED_SCRATCH} (id, v) VALUES (1, 0)")
        suite.exec(script, refs=[self.ctx.spine], timed=False)[0].raise_for_status()
        return {"task": n + 1, "dependency": n - 1}

    # ------------------------------------------------------------------

    def run(self):
        ctx, p = self.ctx, self.params
        suite = ctx.new_suite()
        self._merge_lock = threading.Lock()
        self._next_task_id = self.num_tasks + 1
        self._id_lock = threading.Lock()
        try:
            for r in range(1, p.rounds + 1):
                ctx.check_stop()
                self._round(suite, r)
        finally:
            ctx.close_suite(suite)

    def _round(self, suite, r):
        ctx, p, inv = self.ctx, self.params, self.ctx.invariants
        c = suite.commit(ctx.spine, f"checkpoint {r}", label="checkpoint")
        checkpoint = f"{ctx.spine}@{c.value}" if (c.ok and c.value) else ctx.spine
        self._merge_order = []
        before = self._sentinel_status(suite)

        stop_updates = threading.Event()
        updater = threading.Thread(target=self._spine_updates, args=(r, stop_updates),
                                   daemon=True, name=f"spine-updates-{r}")
        updater.start()
        try:
            ctx.run_workers(list(range(1, p.agents + 1)),
                            lambda w, a: self._agent(w, r, a, checkpoint),
                            thread_base=100 * r)
        finally:
            stop_updates.set()
            updater.join(60)

        # Invariants for the round.
        merged = [m for m in self._merge_order if m["ok"]]
        if len(merged) < 2:
            inv.not_applicable(f"S3.1 three sentinel values across two merges (round {r})",
                               f"only {len(merged)} successful merges")
        else:
            v1, v2 = merged[0]["sentinel_after"], merged[1]["sentinel_after"]
            inv.check(f"S3.1 three sentinel values across two merges (round {r})",
                      before != v1 and v1 != v2 and before != v2
                      and v1 == merged[0]["sentinel_value"] and v2 == merged[1]["sentinel_value"],
                      f"before={before!r} after first merge={v1!r} after second={v2!r}")
        # S3.2: after every successful merge the sentinel task carries the
        # merging agent's value (the resolver takes theirs on that row).
        wrong = [m["agent"] for m in merged if m["sentinel_after"] != m["sentinel_value"]]
        if merged:
            inv.check(f"S3.2 sentinel carries each merging agent's value (round {r})", not wrong,
                      f"{len(wrong)} of {len(merged)} merges left another value (agents {wrong[:5]})")
        ctx.add_metric(f"merge_order_round_{r}", [m["agent"] for m in self._merge_order])
        ctx.bump_metric("merges_ok", len(merged))
        ctx.bump_metric("merges_failed", sum(1 for m in self._merge_order if not m["ok"]))
        ctx.bump_metric("merge_conflicts", sum(m["conflicts"] for m in self._merge_order))

    def _sentinel_status(self, suite):
        res = suite.exec([("SELECT status FROM task WHERE task_id = %s", (SENTINEL_TASK,))],
                         refs=[self.ctx.spine], label="invariant")[0]
        return res.rows[0][0] if res.rows else None

    def _spine_updates(self, r, stop):
        """Task/progress updates on the spine while agents work."""
        ctx, p = self.ctx, self.params
        import dblib.result_collector as rc
        rc.set_current_thread_id(900 + r)
        suite = ctx.new_suite()
        rng = ctx.worker_rng(900 + r)
        shared = self._shared_keys()
        try:
            for k in range(p.spine_updates_per_round):
                if stop.is_set():
                    break
                t = rng.choice(shared) if shared else rng.randint(1, self.num_tasks)

                def script(db, t=t, k=k):
                    db.sql("UPDATE task SET status = 'in_progress', version = version + 1, "
                           "updated_step = %s WHERE task_id = %s", (k, t))
                    if p.shared_scratch_table:
                        db.sql(f"UPDATE {SHARED_SCRATCH} SET v = v + 1 WHERE id = 1")
                with self._merge_lock:
                    suite.exec(script, refs=[ctx.spine], label="spine_update")
                time.sleep(0.01)
        finally:
            ctx.close_suite(suite)

    def _shared_keys(self):
        n = self.num_tasks
        k = max(1, int(round(n * max(0.0, min(1.0, self.params.key_overlap)))))
        return list(range(1, k + 1))

    def _agent(self, w: Worker, r: int, a: int, checkpoint: str):
        ctx, suite = self.ctx, w.suite
        branch = f"agent_r{r}_a{a}"
        res = ctx.branch(suite, branch, checkpoint, label="agent_branch")
        if not res.ok:
            ctx.note(f"agent {a}: branch {res.status_name} {res.error}")
            ctx.tick(outcome="failed")
            return None
        sentinel_value = f"r{r}-a{a}"
        w.exec(self._revision_script(w, r, a, sentinel_value), branch, label="revise")
        w.commit(branch, f"agent {a} plan revision (round {r})", label="agent_commit")
        with self._merge_lock:
            m = ctx.retry(lambda: suite.merge(ctx.spine, branch, message=f"merge agent {a} round {r}",
                                              on_conflict=fixed_policy, label="agent_merge"))
            conflicts = int(m.value.get("conflicts", 0)) if (m.ok and isinstance(m.value, dict)) else 0
            if m.ok:
                w.exec(self._integration_script(a), ctx.spine, label="integrate")
            after = self._sentinel_status(suite)
            self._merge_order.append({"agent": a, "ok": m.ok, "status": m.status_name,
                                      "conflicts": conflicts, "sentinel_value": sentinel_value,
                                      "sentinel_after": after, "error": m.error})
        if not m.ok:
            ctx.note(f"agent {a} round {r}: merge {m.status_name}: {m.error[:120]}")
        ctx.delete(suite, branch, label="agent_delete")
        ctx.tick()
        return m.status_name

    def _revision_script(self, w: Worker, r: int, a: int, sentinel_value: str):
        ctx, p, rng = self.ctx, self.params, w.rng
        # Plan work sized by DataOps like S1 and S2: statements_per_step
        # split by write_fraction into writes and reads. A third of the
        # writes (at least one) are subtask inserts, each with its
        # dependency edge; the rest are task edits. The sentinel update,
        # the contradictory edge and the scratch table are fixed probes.
        n = ctx.statements_per_step
        writes = max(1, round(n * ctx.data_ops.write_fraction)) if n > 1 else 1
        reads = max(0, n - writes)
        subtasks = max(1, writes // 3)
        n_edits = max(1, writes - subtasks)
        shared = self._shared_keys()
        private = [t for t in range(1, self.num_tasks + 1) if t not in shared] or shared
        edits = []
        for _ in range(n_edits):
            pool = shared if (shared and rng.random() < p.key_overlap) else private
            edits.append(rng.choice(pool))
        agent = f"agent-{a}"
        # Contradictory edges: odd agents add reconcile -> fulfil (2 -> 1),
        # even agents the reverse.
        edge = (2, 1) if a % 2 else (1, 2)
        with self._id_lock:
            sub_base = self._next_task_id
            self._next_task_id += subtasks

        def script(db):
            db.sql(f"CREATE TABLE scratch_{a} (id INT NOT NULL, v INT, PRIMARY KEY (id))")
            db.sql(f"INSERT INTO scratch_{a} (id, v) VALUES (1, %s)", (r,))
            for t in edits:
                db.sql("UPDATE task SET status = %s, assignee = %s, version = version + 1, "
                       "updated_step = %s WHERE task_id = %s",
                       (rng.choice(["in_progress", "done", "blocked"]), agent, r, t))
            parent = edits[0]
            for i in range(subtasks):
                db.sql("INSERT INTO task (task_id, parent_id, title, status, assignee, version, updated_step) "
                       "VALUES (%s, %s, %s, 'open', %s, 0, %s)",
                       (sub_base + i, parent, f"subtask {sub_base + i} of {parent}", agent, r))
                db.sql("INSERT INTO dependency (task_id, depends_on, added_by) VALUES (%s, %s, %s)",
                       (parent, sub_base + i, agent))
            present = db.sql("SELECT 1 FROM dependency WHERE task_id = %s AND depends_on = %s", edge)
            if not present:  # another agent's cycle edge may already be in the checkpoint
                db.sql("INSERT INTO dependency (task_id, depends_on, added_by) VALUES (%s, %s, %s)",
                       (edge[0], edge[1], agent))
            db.sql("UPDATE task SET status = %s, version = version + 1, updated_step = %s "
                   "WHERE task_id = %s", (sentinel_value, r, SENTINEL_TASK))
            for _ in range(reads):
                db.sql("SELECT t.task_id, t.status, COUNT(d.depends_on) FROM task t "
                       "LEFT JOIN dependency d ON d.task_id = t.task_id "
                       "WHERE t.assignee = %s GROUP BY t.task_id, t.status", (agent,))
            db.sql(f"DROP TABLE scratch_{a}")
            if p.shared_scratch_table:
                db.sql(f"DROP TABLE {SHARED_SCRATCH}")
        return script

    def _integration_script(self, a: int):
        agent = f"agent-{a}"

        def script(db):
            removed = 0
            for _ in range(10):
                rows = db.sql("SELECT task_id, depends_on, added_by FROM dependency")
                edge = _find_cycle_edge([tuple(r) for r in rows or []], agent)
                if edge is None:
                    break
                db.sql("DELETE FROM dependency WHERE task_id = %s AND depends_on = %s", edge)
                removed += 1
            if removed:
                self.ctx.bump_metric("cycle_edges_removed", removed)
            return removed
        return script
