"""S5: Operations agent.

The spine is production under TPC-C load with dense commits: one commit
every ``commit_every_txns`` spine transactions, so each commit captures the
same amount of production change on every backend and phase 1 runs at the
backend's own transaction rate (without a spine load the scenario thread
runs those transactions itself). A bad
deployment ``fault_commit_offset`` commits before the end injects a fault
from the catalog and writes the deploy_log row that is the head sentinel.
Investigation branches are created at ``investigation_branches`` commit
points among the last ``history_offset`` commits; each creates
incident_finding, evaluates the TPC-C consistency conditions plus
diagnostic reads and writes finding rows (``statements_per_step``
statements split by ``write_fraction``; the conditions always run),
diffs against the corrupted head and commits its findings. The spine is then
reset hard to the latest commit at which every condition held, and the
investigation branches are deleted.

Incidents: the spine commits form incidents of ``commits_per_incident``
commits (0 = one incident), each with its own bad deploy, an equal share
of the investigation branches, and its own reset; production continues
from the reset head into the next incident, so branch intensity k gives
k incidents. When an incident's reset fails, the spine keeps the fault's
schema, so the later incidents are reported as not applicable rather
than run on a broken schema. With more than one
incident the sentinel version and the check names carry the incident
number.

Sentinels: the bad deploy's deploy_log row is present at the head, absent
on a branch created from an earlier commit, and absent after the reset.

Schema fault (``schema_fault``): the bad deploy also creates a table
(feature_flag), adds a column (deploy_log.rollout_pct), widens a column
type (deploy_log.note) and drops a column holding data
(deploy_log.approved_by, 'ops' on every routine deploy); each DDL runs
on its own so a dialect that rejects one still applies the others.
S5.3: after the reset the schema diff from the target commit to the
spine reports no change. S5.4: the dropped column is back with its
values (one 'ops' per routine deploy up to the target commit). A backend
that cannot restore a state older than a dropped column reports the
reset (and a branch from such a commit) as unsupported, which ends the
run.

Lineage across the reset (S5.5): the spine's history must still read
correctly after a reset. S5.5a: right after the reset, a diff from the
target commit to the spine reports no change. S5.5b: after one probe
deploy on the spine (one deploy_log row inserted, one routine deploy's
note updated, committed), the same diff reports exactly one added and
one modified row and nothing deleted. A reset that rebuilds the tables
under new identities (MatrixOne's RESTORE DATABASE) fails here: the
later diff lists the updated row as deleted plus added on both sides.
"""

import time
from datetime import datetime

from macrobench import faults, tpcc
from macrobench.scenarios.base import Scenario, Worker, register

DEFAULT_COMMIT_EVERY_TXNS = 20
BAD_VERSION = "v-bad-deploy"


@register
class OpsAgentScenario(Scenario):
    key = "ops_agent"
    name = "S5 operations agent"
    uses_spine_load = True

    def validate(self):
        p = self.params
        if p.spine_commits <= 0 or p.investigation_branches <= 0:
            raise ValueError("ops_agent needs spine_commits and investigation_branches > 0")
        if p.fault_commit_offset <= 0 or p.fault_commit_offset >= p.spine_commits:
            raise ValueError("ops_agent.fault_commit_offset must be in (0, spine_commits)")
        if p.commits_per_incident and p.fault_commit_offset >= p.commits_per_incident:
            raise ValueError("ops_agent.fault_commit_offset must be below commits_per_incident")

    def _incidents(self) -> tuple:
        """(number of incidents, commits per incident, branches per incident)."""
        p = self.params
        cpi = p.commits_per_incident or p.spine_commits
        n = -(-p.spine_commits // cpi)
        return n, cpi, max(1, p.investigation_branches // n)

    def total_units(self) -> int:
        n, _, per = self._incidents()
        return self.params.spine_commits + n * per + n

    # ------------------------------------------------------------------

    def run(self):
        ctx, p, inv = self.ctx, self.params, self.ctx.invariants
        suite = ctx.new_suite()
        rng = ctx.rng
        every = p.commit_every_txns or DEFAULT_COMMIT_EVERY_TXNS
        incidents, cpi, per_inc = self._incidents()
        self._deploy_ks = set()     # routine deploys on the spine (survivors of resets)
        k = 0
        next_commit_at = every
        prev_why = None             # why the previous incident left the spine unrestored
        load_stopped = False
        try:
            for inc in range(1, incidents + 1):
                n_commits = min(cpi, p.spine_commits - k)
                tag = f" (incident {inc})" if incidents > 1 else ""
                bad_version = BAD_VERSION if incidents == 1 else f"{BAD_VERSION}-{inc}"
                if prev_why is not None:
                    why = f"spine not restored by incident {inc - 1}: {prev_why}"
                    for n in ("S5.1 head sentinel absent on investigation branches",
                              "S5.2 head sentinel gone after reset",
                              "S5.3 schema restored by the reset",
                              "S5.4 dropped column's values restored by the reset"):
                        inv.not_applicable(n + tag, why)
                    ctx.note(f"incident {inc} not run: {why}")
                    ctx.tick(n_commits + per_inc + 1, outcome="skipped")
                    k += n_commits
                    next_commit_at += every * n_commits
                    continue
                if load_stopped and self.spine_load is not None:
                    self.spine_load.restart()
                    load_stopped = False
                fault, fw, fd = faults.pick_fault(rng, ctx.scale.warehouses)
                fault_k = k + n_commits - p.fault_commit_offset
                commits = []   # (k, hash or None, iso timestamp) of this incident

                # 1. Production with dense commits, one per ``every`` spine
                # transactions; the bad deploy lands at fault_k.
                for _ in range(n_commits):
                    ctx.check_stop()
                    if self.spine_load is None:
                        for _ in range(every):
                            suite.exec(lambda db: tpcc.run_transaction(
                                tpcc.pick_transaction(rng), db, rng, ctx.scale),
                                refs=[ctx.spine], label="spine")
                    elif not self.spine_load.wait_for_total(next_commit_at, stop=ctx.stop_event):
                        ctx.note(f"spine load stopped before commit {k}; committing without waiting")
                    next_commit_at += every
                    with self.quiesced():
                        if k == fault_k:
                            suite.exec(self._deploy_script(k, fault, fw, fd, bad=True, version=bad_version),
                                       refs=[ctx.spine], label="bad_deploy")
                            if p.schema_fault:
                                self._schema_fault(suite)
                        elif k % 5 == 0:
                            r = suite.exec(self._deploy_script(k, None, fw, fd, bad=False),
                                           refs=[ctx.spine], label="deploy")[0]
                            if r.ok:
                                self._deploy_ks.add(k)
                        c = suite.commit(ctx.spine, f"prod commit {k}", label="spine_commit")
                    commits.append((k, c.value if (c.ok and c.value) else None,
                                    datetime.now().isoformat(timespec="seconds")))
                    k += 1
                    ctx.tick()

                if self.spine_load is not None and not p.load_during_investigation:
                    self.spine_load.stop()
                    load_stopped = True

                # 2. Investigation branches at commit points near the head.
                t_start = time.time()
                window = commits[-(p.history_offset + 1):-1] if p.history_offset > 0 else commits[:-1]
                if not window:
                    window = commits[:-1] or commits
                n = min(per_inc, len(window))
                picks = [window[int(i * (len(window) - 1) / max(1, n - 1))] for i in range(n)] if n > 1 else [window[-1]]
                picks = sorted(set(picks))
                findings = ctx.run_workers(
                    picks, lambda w, c: self._investigate(w, c, fault_k, bad_version, tag),
                    thread_base=100 * inc)
                good = [f for f in findings if isinstance(f, dict) and f["all_hold"]]
                if good:
                    target = max(good, key=lambda f: f["k"])
                else:
                    target = min((f for f in findings if isinstance(f, dict)), key=lambda f: f["k"], default=None)

                # 3. Reset the spine (PITR).
                sfx = f"_{inc}" if incidents > 1 else ""
                reset = None
                if target is None:
                    inv.not_applicable("S5.2 head sentinel gone after reset" + tag, "no investigation results")
                else:
                    to = target["hash"] or target["timestamp"]
                    # The load stays paused from the reset through the
                    # checks: S5.5 diffs the spine against the target
                    # commit, so no spine transaction may land in between.
                    with self.quiesced():
                        reset = ctx.retry(lambda: suite.reset(ctx.spine, to, label="pitr_reset"))
                        ctx.add_metric("reset_status" + sfx, reset.status_name)
                        ctx.add_metric("reset_target_commit" + sfx, target["k"])
                        ctx.add_metric("fault_commit" + sfx, fault_k)
                        if reset.ok:
                            self._deploy_ks = {x for x in self._deploy_ks if x <= target["k"]}
                        r = suite.exec([("SELECT COUNT(*) FROM deploy_log WHERE version = %s", (bad_version,))],
                                       refs=[ctx.spine], label="invariant")[0]
                        present = int(r.rows[0][0]) if r.rows else None
                        if target["k"] < fault_k:
                            inv.expect("S5.2 head sentinel gone after reset" + tag, present, 0,
                                       f"reset {reset.status_name} to commit {target['k']}")
                        else:
                            inv.not_applicable("S5.2 head sentinel gone after reset" + tag,
                                               f"selected commit {target['k']} is after the bad deploy {fault_k}")
                        if p.schema_fault:
                            self._check_schema_reset(suite, target, fault_k, reset, tag)
                        self._check_diff_across_reset(suite, target, fault_k, reset, inc, tag)
                ctx.add_metric("time_to_recovery_sec" + sfx, round(time.time() - t_start, 3))
                ctx.tick()

                # 4. Delete the investigation branches.
                for f in findings:
                    if isinstance(f, dict):
                        ctx.delete(suite, f["branch"], label="investigation_delete")
                        ctx.tick()
                # The next incident needs the spine back at a pre-fault state;
                # otherwise its deploys run on the fault's schema and its
                # checks would be vacuous, so it is reported as not applicable.
                if target is None:
                    prev_why = "no investigation results"
                elif reset is None or not reset.ok:
                    prev_why = f"reset {reset.status_name if reset else 'not run'}"
                elif target["k"] >= fault_k:
                    prev_why = f"reset target commit {target['k']} is after the bad deploy {fault_k}"
                else:
                    prev_why = None
            ctx.add_metric("incidents", incidents)
        finally:
            ctx.close_suite(suite)

    # ------------------------------------------------------------------

    def _schema_fault(self, suite) -> None:
        """The bad deploy's schema changes, one exec each (a failure of one,
        e.g. a dialect without the retype form, leaves the others applied)."""
        dialect = getattr(type(suite), "SCHEMA_DIALECT", None)
        retype = ("ALTER TABLE deploy_log MODIFY note VARCHAR(400)" if dialect == "mysql"
                  else "ALTER TABLE deploy_log ALTER COLUMN note TYPE VARCHAR(400)")
        applied = []
        for name, stmts in (("table", ["CREATE TABLE feature_flag (flag_id INT NOT NULL, name VARCHAR(40), "
                                       "enabled INT, PRIMARY KEY (flag_id))",
                                       "INSERT INTO feature_flag (flag_id, name, enabled) VALUES (1, 'new_checkout', 1)"]),
                            ("column", ["ALTER TABLE deploy_log ADD COLUMN rollout_pct INT"]),
                            ("retype", [retype]),
                            ("drop", ["ALTER TABLE deploy_log DROP COLUMN approved_by"])):
            r = suite.exec(stmts, refs=[self.ctx.spine], label="bad_deploy_ddl")[0]
            if r.ok:
                applied.append(name)
            else:
                self.ctx.note(f"bad deploy schema change '{name}': {r.status_name} {r.error[:100]}")
        self._schema_fault_applied = applied
        self.ctx.add_metric("schema_fault_applied", ",".join(applied))

    def _check_schema_reset(self, suite, target: dict, fault_k: int, reset, tag: str = "") -> None:
        """S5.3: the schema at the spine after the reset equals the schema
        at the target commit (the diff's schema half reports no change). S5.4: the
        dropped column's values are back (one 'ops' per routine deploy up
        to the target commit)."""
        inv, ctx = self.ctx.invariants, self.ctx
        names = ("S5.3 schema restored by the reset" + tag,
                 "S5.4 dropped column's values restored by the reset" + tag)
        applied = ",".join(getattr(self, "_schema_fault_applied", []))
        if target["k"] >= fault_k:
            for n in names:
                inv.not_applicable(n, f"selected commit {target['k']} is after the bad deploy {fault_k}")
            return
        if not target["hash"]:
            for n in names:
                inv.not_applicable(n, "backend has no commits")
            return
        sd = ctx.retry(lambda: suite.diff(f"{ctx.spine}@{target['hash']}", ctx.spine, label="schema_diff"))
        if not sd.ok or not isinstance(sd.value, dict):
            inv.check(names[0], False, f"diff failed: {sd.error[:100]}")
        elif not isinstance(sd.value.get("schema"), dict):
            inv.check(names[0], False, "the diff reports no schema (rows only)")
        else:
            v = sd.value["schema"]
            inv.check(names[0], not v.get("changed"),
                      f"tables_added={v.get('tables_added')} tables_dropped={v.get('tables_dropped')} "
                      f"columns_added={v.get('columns_added')} columns_dropped={v.get('columns_dropped')} "
                      f"columns_changed={v.get('columns_changed')} (bad deploy applied: {applied})")
        if "drop" not in applied:
            inv.not_applicable(names[1], "the bad deploy's DROP COLUMN was not applied")
            return
        # Routine deploys still on the spine up to the target (earlier
        # incidents' resets removed the ones past their own targets).
        want = len([x for x in self._deploy_ks if x <= target["k"]])
        r = suite.exec([("SELECT COUNT(*) FROM deploy_log WHERE approved_by = %s", ("ops",))],
                       refs=[ctx.spine], label="invariant")[0]
        got = int(r.rows[0][0]) if r.ok and r.rows else None
        inv.check(names[1], got == want,
                  f"rows with approved_by='ops' = {got} (want {want}: routine deploys up to commit {target['k']})"
                  + ("" if r.ok else f"; read failed: {r.error[:80]}"))

    def _check_diff_across_reset(self, suite, target: dict, fault_k: int, reset, inc: int,
                                 tag: str = "") -> None:
        """S5.5a: a diff from the target commit to the reset spine reports
        no change. S5.5b: after a probe deploy (one deploy_log row inserted,
        one routine deploy's note updated, committed), the diff reports
        exactly 1 added, 1 modified, 0 deleted rows. Runs inside the
        caller's quiesced block, so the spine load (which keeps running
        in the full config) cannot add rows between the reset and the
        diffs."""
        inv, ctx = self.ctx.invariants, self.ctx
        names = ("S5.5a diff from the target commit to the reset spine is empty" + tag,
                 "S5.5b diff across the reset reports exactly the probe deploy" + tag)
        if target["k"] >= fault_k:
            for n in names:
                inv.not_applicable(n, f"selected commit {target['k']} is after the bad deploy {fault_k}")
            return
        if not target["hash"]:
            for n in names:
                inv.not_applicable(n, "backend has no commits")
            return
        if not reset.ok:
            for n in names:
                inv.check(n, False, f"reset {reset.status_name}: {reset.error[:100]}")
            return
        target_ref = f"{ctx.spine}@{target['hash']}"

        def counts(d):
            v = d.value if (d.ok and isinstance(d.value, dict)) else {}
            return tuple(int(v.get(k) or 0) for k in ("rows_added", "rows_deleted", "rows_modified"))

        d0 = ctx.retry(lambda: suite.diff(target_ref, ctx.spine, label="invariant"))
        a0, dl0, m0 = counts(d0)
        inv.check(names[0], d0.ok and (a0, dl0, m0) == (0, 0, 0),
                  f"added={a0} deleted={dl0} modified={m0}" + ("" if d0.ok else f"; diff failed: {d0.error[:80]}"))
        # Probe: one routine deploy's note updated (the latest one still on
        # the spine, if any) and one new deploy_log row, as production would
        # write them; committed so the diff is between two commits.
        updated = max((x for x in self._deploy_ks if x <= target["k"]), default=None)
        probe_id = 900000 + inc

        def probe(db):
            if updated is not None:
                db.sql("UPDATE deploy_log SET note = %s WHERE deploy_id = %s", (f"post-reset probe {inc}", updated))
            db.sql("INSERT INTO deploy_log (deploy_id, version, deployed_at, note) "
                   "VALUES (%s, %s, CURRENT_TIMESTAMP, 'post-reset probe')", (probe_id, f"probe{inc}"))
        with self.quiesced():
            r = suite.exec(probe, refs=[ctx.spine], label="invariant")[0]
            c = suite.commit(ctx.spine, f"post-reset probe {inc}", label="invariant")
        if not (r.ok and c.ok):
            inv.check(names[1], False, f"probe {'write' if not r.ok else 'commit'} failed: {(r.error or c.error)[:100]}")
            return
        d1 = ctx.retry(lambda: suite.diff(target_ref, ctx.spine, label="invariant"))
        a1, dl1, m1 = counts(d1)
        want_mod = 1 if updated is not None else 0
        inv.check(names[1], d1.ok and (a1, dl1, m1) == (1, 0, want_mod),
                  f"added={a1} deleted={dl1} modified={m1} (want 1/0/{want_mod})"
                  + ("" if d1.ok else f"; diff failed: {d1.error[:80]}"))

    def _deploy_script(self, k, fault, fw, fd, bad, version=BAD_VERSION):
        def script(db):
            if bad:
                fault.inject(db, fw, fd)
                db.sql("INSERT INTO deploy_log (deploy_id, version, deployed_at, note) "
                       "VALUES (%s, %s, CURRENT_TIMESTAMP, %s)",
                       (k, version, f"sentinel: {fault.name} in w{fw} d{fd}"))
            else:
                db.sql("INSERT INTO deploy_log (deploy_id, version, deployed_at, note, approved_by) "
                       "VALUES (%s, %s, CURRENT_TIMESTAMP, 'routine', 'ops')", (k, f"v{k}"))
        return script

    def _investigate(self, w: Worker, commit, fault_k, bad_version=BAD_VERSION, tag: str = "") -> dict:
        ctx, inv, suite = self.ctx, self.ctx.invariants, w.suite
        k, h, ts = commit
        branch = f"inv_{k}"
        ref = f"{ctx.spine}@{h}" if h else ctx.spine
        res = ctx.branch(suite, branch, ref, label="investigation_branch")
        if not res.ok:
            ctx.note(f"investigation branch {branch}: {res.status_name} {res.error}")
            ctx.tick(outcome="failed")
            return None

        # Sized by DataOps like S1 and S2: statements_per_step split by
        # write_fraction. The consistency conditions always run (the
        # recovery point depends on all of them) and count as the first
        # reads; further reads are diagnostics from a pool. Writes are
        # finding rows: one per condition, then one per diagnostic.
        n = ctx.statements_per_step
        writes = max(1, round(n * ctx.data_ops.write_fraction)) if n > 1 else 1
        reads = max(0, n - writes)
        diagnostics = [
            "SELECT deploy_id, version, deployed_at FROM deploy_log ORDER BY deploy_id DESC LIMIT 10",
            "SELECT COUNT(*) FROM orders WHERE o_carrier_id IS NULL",
            "SELECT w_id, w_ytd FROM warehouse ORDER BY w_ytd DESC LIMIT 5",
            "SELECT COUNT(*) FROM stock WHERE s_quantity < 10",
        ]

        def script(db):
            db.sql("CREATE TABLE incident_finding (finding_id INT NOT NULL, branch_name VARCHAR(64), "
                   "commit_hash VARCHAR(64), condition_name VARCHAR(8), violations INT, "
                   "PRIMARY KEY (finding_id))")
            violations = faults.check_consistency(db)
            findings = list(violations.items())
            for j in range(max(0, reads - len(violations))):
                rows = db.sql(diagnostics[j % len(diagnostics)])
                findings.append((f"D{j + 1}", len(rows or [])))
            for i, (cond, v) in enumerate(findings[:writes]):
                db.sql("INSERT INTO incident_finding (finding_id, branch_name, commit_hash, "
                       "condition_name, violations) VALUES (%s, %s, %s, %s, %s)",
                       (k * 100 + i, branch, h or "", cond, v))
            rows = db.sql("SELECT COUNT(*) FROM deploy_log WHERE version = %s", (bad_version,))
            return {"violations": violations, "bad_deploy_rows": int(rows[0][0]) if rows else None}
        r = w.exec(script, branch, label="investigate")
        value = r.value if (r.ok and isinstance(r.value, dict)) else {"violations": {}, "bad_deploy_rows": None}
        ctx.retry(lambda: suite.diff(ctx.spine, branch, label="investigate_diff"))
        w.commit(branch, f"findings at commit {k}", label="findings_commit")
        if k < fault_k:
            if h is None:
                inv.not_applicable(f"S5.1 head sentinel absent on branch from commit {k}{tag}",
                                   "backend has no commits; branch is at the head")
            else:
                inv.expect(f"S5.1 head sentinel absent on branch from commit {k}{tag}",
                           value["bad_deploy_rows"], 0)
        ctx.tick()
        return {"k": k, "hash": h, "timestamp": ts, "branch": branch,
                "all_hold": faults.all_hold(value["violations"]),
                "violations": value["violations"]}
