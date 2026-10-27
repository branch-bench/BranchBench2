"""S2: Agent context management.

A single spine records an assistant's interactions (turn and tool_call
rows). Every ``compaction_interval`` steps the spine is committed and
``fanout`` candidate branches try a curation strategy each (A: summarize
old turns, B: replace large tool results by references, C: deduplicate
turns), commit, and read their curated context; with probability
``data_ops.ddl_fraction`` a candidate also creates a view over that
context and an index on turn and reads through the view. The
candidates run concurrently, one worker each (``worker_threads``, capped
by ``fanout``), while the spine is driven from the main thread. A
cross-branch policy check picks one candidate and the spine fast-forwards
to it. The cycles form blocks of ``cycles_per_rejection``; at the end of
each block the strategy chosen at the block's ``rejected_cycle`` is
rejected: its compaction commit is reverted on the spine, the spine is
rebased onto the retained alternative candidate and fast-forwarded again.
Branch intensity scales the number of cycles, so it scales the number of
rejections (``cycles_per_rejection`` 0 = one rejection at the end).
Candidates are deleted ``retention_steps`` interaction steps after their
compaction point (the alternative kept for the rejection is deleted last).

The rebase replays every spine commit since the compaction point onto the
alternative, including the rejected compaction commit and its revert;
these cancel out, so the result equals replaying the work commits only.
Conflict-freedom is a schema property: candidates rewrite old rows and add
notes (and views and indexes); the spine only appends new turn/tool_call rows.

Sentinels: the ``note`` row with kind 'compaction' naming the candidate.

Invariants, per rejection: S2.1 the rejected candidate's note is gone from
the spine after the revert; S2.2 the alternative's note is present after
the rebase and merge and was absent before; S2.3 the compaction notes on
the spine are exactly the retained set: every other cycle's chosen
candidate so far (so the rebase replayed the later cycles' compaction
commits and earlier rejections survived) plus the alternatives of the
rejected cycles.
"""

from macrobench.scenarios.base import Scenario, register

STRATEGIES = ["summarize", "reference", "dedup"]
INDEX_COLUMNS = {"summarize": "(step)", "reference": "(session_id, step)",
                 "dedup": "(role, step)"}
LETTERS = "ABCDEFGHIJ"


@register
class ContextMgmtScenario(Scenario):
    key = "context_mgmt"
    name = "S2 context management"

    def validate(self):
        p = self.params
        if p.compaction_interval <= 0 or p.fanout <= 0 or p.compaction_cycles <= 0:
            raise ValueError("context_mgmt needs compaction_interval, fanout and compaction_cycles > 0")
        if p.fanout > len(LETTERS):
            raise ValueError("context_mgmt fanout must be <= 10")

    def total_units(self) -> int:
        p = self.params
        cpr = p.cycles_per_rejection or p.compaction_cycles
        return p.compaction_cycles * (p.compaction_interval + 1) + -(-p.compaction_cycles // cpr)

    # ------------------------------------------------------------------

    def run(self):
        ctx, p = self.ctx, self.params
        suite = ctx.new_suite()
        rng = ctx.rng
        self._turn_id = 0
        self._call_id = 0
        self._note_id = 0
        step = 0
        pending_delete = []    # (branch, delete_at_step)
        chosen = {}            # cycle -> dict(branch, commit, strategy)
        alternatives = {}      # cycle -> dict(branch, commit, strategy)
        retention = max(0, ctx.branch_ops.retention_steps)
        ff_count = 0
        cpr = p.cycles_per_rejection or p.compaction_cycles   # cycles per rejection block
        self._rejected = {}    # rejected cycle -> alternative branch applied instead
        rejections = 0

        try:
            for cycle in range(1, p.compaction_cycles + 1):
                block_offset = (cycle - 1) % cpr + 1
                # Interactions on the spine.
                for k in range(p.compaction_interval):
                    ctx.check_stop()
                    suite.exec(self._interaction_script(step, rng), refs=[ctx.spine],
                               label="interaction")
                    if ctx.should_commit(k):
                        suite.commit(ctx.spine, f"work step {step}", label="work_commit")
                    step += 1
                    pending_delete = self._retire(suite, pending_delete, step)
                    ctx.tick()

                # Compaction point.
                ctx.check_stop()
                suite.commit(ctx.spine, f"pre-compaction {cycle}", label="pre_compaction_commit")
                # The candidates are tried concurrently, one worker each.
                items = [(f"cand_{cycle}_{LETTERS[j]}", STRATEGIES[j % len(STRATEGIES)])
                         for j in range(p.fanout)]

                def try_candidate(w, item, cycle=cycle, step=step):
                    name, strategy = item
                    res = ctx.branch(w.suite, name, ctx.spine, label="candidate_branch")
                    if not res.ok:
                        ctx.note(f"candidate {name}: {res.status_name} {res.error}")
                        return None
                    # DDL (a view over the curated context and an index) with
                    # probability ddl_fraction; without it the candidate reads
                    # its context straight from the turn table.
                    ddl = w.rng.random() < ctx.data_ops.ddl_fraction
                    w.exec(self._curation_script(name, strategy, step, w.rng, ddl), name, label="curation")
                    c = w.commit(name, f"compaction {cycle} {strategy} ({name})",
                                 label="curation_commit")
                    cutoff = max(0, step - 2)
                    context_sql = (f"SELECT COUNT(*), SUM(token_count) FROM context_v_{name}" if ddl else
                                   f"SELECT COUNT(*), SUM(token_count) FROM turn WHERE step >= {cutoff}")
                    w.exec([context_sql], name, label="curated_context")
                    return {"branch": name, "commit": c.value if c.ok else None,
                            "strategy": strategy}

                results = ctx.run_workers(items, try_candidate,
                                          threads=max(1, min(p.fanout, ctx.worker_threads)),
                                          thread_base=100 * cycle)
                cands = [r for r in results if isinstance(r, dict)]
                # Accounted here, on the caller's thread: the compaction
                # unit failed when any candidate did not come back.
                outcome = "succeeded" if len(cands) == len(items) else "failed"
                if not cands:
                    ctx.tick(outcome=outcome)
                    continue

                pick = self._policy_check(suite, cands)
                if block_offset == p.rejected_cycle and len(cands) >= 2:
                    pick, alt = 1, 0
                    alternatives[cycle] = cands[alt]
                sel = cands[pick]
                chosen[cycle] = sel
                m = ctx.retry(lambda: suite.merge(ctx.spine, sel["branch"],
                                                  message=f"fast-forward to {sel['branch']}", label="fast_forward"))
                if m.ok:
                    ff = bool(isinstance(m.value, dict) and m.value.get("fast_forward"))
                    ff_count += int(ff)
                    if not ff:
                        ctx.bump_metric("non_ff_merges")
                for c in cands:
                    if c is not alternatives.get(cycle):
                        pending_delete.append((c["branch"], step + retention))
                ctx.tick(outcome=outcome)

                # End of a block: reject the block's chosen compaction.
                if cycle % cpr == 0 or cycle == p.compaction_cycles:
                    rejections += 1
                    rc = (cycle - 1) // cpr * cpr + p.rejected_cycle
                    self._reject(suite, chosen, alternatives, rc, rejections)
                    alt = alternatives.pop(rc, None)
                    if alt is not None:
                        ctx.delete(suite, alt["branch"], label="candidate_delete")
                    ctx.tick()
        finally:
            for branch, _ in pending_delete:
                ctx.delete(suite, branch, label="candidate_delete")
            for c in alternatives.values():
                ctx.delete(suite, c["branch"], label="candidate_delete")
            ctx.add_metric("interaction_steps", step)
            ctx.add_metric("fast_forwards", ff_count)
            ctx.add_metric("rejections", rejections)
            ctx.close_suite(suite)

    # ------------------------------------------------------------------

    def _retire(self, suite, pending, step):
        keep = []
        for branch, at in pending:
            if step >= at:
                self.ctx.delete(suite, branch, label="candidate_delete")
            else:
                keep.append((branch, at))
        return keep

    def _interaction_script(self, step, rng):
        ctx = self.ctx
        n = ctx.statements_per_step
        writes = max(1, round(n * ctx.data_ops.write_fraction)) if n > 1 else 1
        reads = max(0, n - writes)
        blob = "x" * (100 * ctx.rows_per_write)   # tool-result size: 100 chars per "row"
        topic = rng.randint(1, 20)  # repeats -> duplicates for strategy C

        def script(db):
            for k in range(writes):
                self._turn_id += 1
                db.sql("INSERT INTO turn (turn_id, session_id, step, role, content, token_count, created_at) "
                       "VALUES (%s, 1, %s, 'user', %s, %s, CURRENT_TIMESTAMP)",
                       (self._turn_id, step, f"question about order {topic}", 12))
                self._turn_id += 1
                db.sql("INSERT INTO turn (turn_id, session_id, step, role, content, token_count, created_at) "
                       "VALUES (%s, 1, %s, 'assistant', %s, %s, CURRENT_TIMESTAMP)",
                       (self._turn_id, step, f"answer about order {topic} at step {step}", 40))
                self._call_id += 1
                db.sql("INSERT INTO tool_call (call_id, turn_id, tool, args, result, result_size) "
                       "VALUES (%s, %s, 'lookup_order', %s, %s, %s)",
                       (self._call_id, self._turn_id, f'{{"order": {topic}}}', blob, len(blob)))
            for _ in range(reads):
                db.sql("SELECT turn_id, role, content FROM turn ORDER BY turn_id DESC LIMIT 20")
        return script

    def _next_note_id(self) -> int:
        with self.ctx._lock:
            self._note_id += 1
            return self._note_id

    def _curation_script(self, name, strategy, step, rng, ddl: bool = True):
        cutoff = max(0, step - 2)   # "older" = before the last two steps

        def script(db):
            if strategy == "summarize":
                db.sql("UPDATE turn SET content = 'summarized', token_count = 2 WHERE step < %s",
                       (cutoff,))
                note_id = self._next_note_id()
                db.sql("INSERT INTO note (note_id, kind, candidate, content, created_at) "
                       "VALUES (%s, 'summary', %s, %s, CURRENT_TIMESTAMP)",
                       (note_id, name, f"summary of steps before {cutoff}"))
            elif strategy == "reference":
                rows = db.sql("SELECT c.call_id FROM tool_call c JOIN turn t ON t.turn_id = c.turn_id "
                              "WHERE t.step < %s AND c.result_size > 100", (cutoff,))
                for (call_id,) in rows or []:
                    db.sql("UPDATE tool_call SET result = %s, result_size = 8 WHERE call_id = %s",
                           (f"ref:{call_id}", call_id))
            else:  # dedup
                rows = db.sql("SELECT role, content, MIN(turn_id), COUNT(*) FROM turn "
                              "WHERE step < %s GROUP BY role, content HAVING COUNT(*) > 1", (cutoff,))
                for role, content, keep, _cnt in rows or []:
                    db.sql("DELETE FROM turn WHERE role = %s AND content = %s AND turn_id <> %s "
                           "AND step < %s", (role, content, keep, cutoff))
            note_id = self._next_note_id()
            db.sql("INSERT INTO note (note_id, kind, candidate, content, created_at) "
                   "VALUES (%s, 'compaction', %s, %s, CURRENT_TIMESTAMP)",
                   (note_id, name, f"strategy {strategy}"))
            if not ddl:
                return
            db.sql(f"CREATE VIEW context_v_{name} AS SELECT turn_id, step, role, content, token_count "
                   f"FROM turn WHERE step >= {cutoff}")
            # One index per strategy, each on its own column set: Dolt
            # cannot merge two indexes covering the same columns, and a
            # strategy chosen earlier may already have put its index on
            # the spine (then the CREATE fails and is ignored).
            index_ddl = f"idx_turn_{strategy} ON turn {INDEX_COLUMNS[strategy]}"
            try:
                db.sql(f"CREATE INDEX IF NOT EXISTS {index_ddl}")
            except Exception:
                try:  # MySQL dialects lack IF NOT EXISTS
                    db.sql(f"CREATE INDEX {index_ddl}")
                except Exception:
                    pass
        return script

    def _policy_check(self, suite, cands) -> int:
        """Cross-branch read of each candidate's context size; returns the
        index of the smallest."""
        refs = [c["branch"] for c in cands]

        def script_for(multi):
            if multi:
                def script(db):
                    out = {}
                    for ref in refs:
                        rows = db.sql(f"SELECT COUNT(*), SUM(token_count) FROM {db.table(ref, 'turn')}")
                        out[ref] = float(rows[0][1] or 0) if rows else 0.0
                    return out
                return script

            def one(db):
                rows = db.sql("SELECT COUNT(*), SUM(token_count) FROM turn")
                return float(rows[0][1] or 0) if rows else 0.0
            return one

        results = self.ctx.cross_branch_exec(suite, script_for, refs, label="policy_check")
        sizes = {}
        if len(results) == 1 and isinstance(results[0].value, dict):
            sizes = results[0].value
        else:
            for ref, res in zip(refs, results):
                sizes[ref] = res.value if (res.ok and res.value is not None) else float("inf")
        best = min(range(len(refs)), key=lambda i: sizes.get(refs[i], float("inf")))
        return best

    def _reject(self, suite, chosen, alternatives, cycle, n):
        """Reject the compaction chosen at ``cycle`` (the n-th rejection)."""
        ctx, inv, p = self.ctx, self.ctx.invariants, self.params
        tag = f" (rejection {n}, cycle {cycle})"
        n1 = "S2.1 rejected candidate's sentinel removed after revert" + tag
        n2 = "S2.2 alternative's sentinel present after rebase+merge" + tag
        n3 = "S2.3 compaction notes on the spine are exactly the retained candidates" + tag
        if cycle not in chosen or cycle not in alternatives:
            why = "no rejected cycle configured or fewer than 2 candidates"
            inv.not_applicable(n1, why)
            inv.not_applicable(n2, why)
            inv.not_applicable(n3, why)
            return
        rejected, alt = chosen[cycle], alternatives[cycle]

        def present(candidate):
            res = suite.exec([("SELECT COUNT(*) FROM note WHERE kind = 'compaction' AND candidate = %s",
                               (candidate,))], refs=[ctx.spine], label="invariant")[0]
            return int(res.rows[0][0]) if res.rows else None

        alt_before = present(alt["branch"])
        if rejected["commit"] is None:
            inv.not_applicable(n1, "backend has no commits to revert")
        else:
            r = ctx.retry(lambda: suite.revert(ctx.spine, rejected["commit"], label="revert_compaction"))
            inv.expect(n1, present(rejected["branch"]), 0, f"revert {r.status_name} {r.error}")
        rb = ctx.retry(lambda: suite.rebase(ctx.spine, alt["branch"], on_conflict="theirs", label="rebase_onto_alternative"))
        if rb.ok and isinstance(rb.value, dict):
            ctx.add_metric("rebase_conflicts", rb.value.get("conflicts", 0))
        m = ctx.retry(lambda: suite.merge(ctx.spine, alt["branch"], message="fast-forward to alternative",
                                          label="fast_forward"))
        inv.check(n2, alt_before == 0 and present(alt["branch"]) == 1,
                  f"before={alt_before} after={present(alt['branch'])} "
                  f"(rebase {rb.status_name}, merge {m.status_name})")
        # S2.3: the retained set, across every block so far.
        self._rejected[cycle] = alt["branch"]
        want = {c["branch"] for cy, c in chosen.items() if cy not in self._rejected}
        want |= set(self._rejected.values())
        res = suite.exec([("SELECT candidate FROM note WHERE kind = 'compaction'", None)],
                         refs=[ctx.spine], label="invariant")[0]
        got = {row[0] for row in (res.rows or [])}
        inv.check(n3, got == want,
                  f"missing={sorted(want - got)} extra={sorted(got - want)}")
