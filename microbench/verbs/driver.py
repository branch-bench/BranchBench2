"""Branch-verb microbenchmark: the cost of each verb against the one
quantity it depends on, on the macrobenchmark's schema and data.

    from microbench.verbs import driver
    driver.main(["--scale", "1", "--outdir", out, "--verbs", "branch,commit"],
                target=prepared_target, system_name="dolt")

The embedding application provisions the database and passes it as a
dblib.target.PreparedTarget; deleting it afterwards is the application's
job too. One run is one (system, W): the CH-benCHmark data is generated at W
warehouses and committed as the base, then every point of the grid
(microbench/verbs/grid.py) runs on a branch forked from the base and
deleted afterwards, so no point sees another's state. A point is
``warmup`` untimed reps and ``reps`` timed ones; each rep prepares the
verb's state untimed (fork, writes, commits), then runs the verb timed
with the backend's storage read before and after (after a flush), and,
for branch and commit, once more after FOLLOW_UP_ROWS updates on the new
branch. The control point measures those follow-up writes alone.

Outputs, under --outdir: ``micro_verbs_<system>_w<W>.parquet`` (one row
per timed verb, as the macrobench records them) and
``micro_verbs_<system>_w<W>_stats.json`` (per point: parameters, status,
the latency and storage delta of every rep, and their quantiles).
"""

import argparse
import json
import os
import statistics
import sys
import threading
import time
import traceback

from dblib import result_collector as rc
from dblib.db_api import native_only
from dblib.target import PreparedTarget, SuiteRequest
from macrobench import schema as mschema
from macrobench import task_pb2 as tp
from macrobench.datagen.ch import CHScale, seed_ch
from macrobench.runner import implementation_report
from microbench.verbs.grid import BASE, FOLLOW_UP_ROWS, STOCK_ROWS, VERBS, points_for_run
from microbench.verbs.ops import CONTROL, VERB_IMPL, Ctx, drop, follow_up, fork

CONTROL_REPS = 3


def _flush(suite, spine):
    suite.flush(spine)


def _quantiles(xs):
    xs = sorted(xs)
    if not xs:
        return {}
    q = statistics.quantiles(xs, n=10, method="inclusive") if len(xs) >= 2 else [xs[0]] * 9
    return {"n": len(xs), "p50": statistics.median(xs), "p10": q[0], "p90": q[8], "min": xs[0], "max": xs[-1]}


def run_point(point_id, verb, params, suite, spine, point_branch, *, reps, warmup,
              storage_reps, stock_rows, log, new_suite=None):
    """All reps of one point on ``point_branch``. Returns the summary."""
    ctx = Ctx(suite=suite, point=point_branch, stock_rows=stock_rows, log=log, new_suite=new_suite)
    out = {"id": point_id, "verb": verb.name, "params": params, "status": "ok", "reps": [],
           "errors": [], "prep_sec": 0.0}
    t0 = time.time()
    ps = verb.prepare_point(ctx, params)
    out["prep_sec"] += time.time() - t0
    try:
        for i in range(warmup + reps):
            timed = i >= warmup
            ctx.rep = i
            measure = timed and (storage_reps <= 0 or i - warmup < storage_reps)
            rep = {"timed": timed}
            s_pre = None
            if measure:
                # What the rep's own preparation adds (a commit's rows are
                # written before the commit; that is where Dolt pays).
                _flush(suite, spine)
                s_pre = suite._safe_storage()
            t1 = time.time()
            rs = verb.prepare_rep(ctx, params, ps)
            out["prep_sec"] += time.time() - t1
            try:
                s0 = s1 = s2 = None
                if measure:
                    _flush(suite, spine)
                    s0 = suite._safe_storage()
                    rep["prep_delta"] = s0 - s_pre
                res = verb.run(ctx, params, ps, rs, timed, point_id)
                if res is not None:
                    rep["status"] = res.status_name
                    rep["latency"] = res.latency
                    if isinstance(res.value, dict) and "parallel" in res.value:
                        rep["per_delete"] = res.value["per_delete"]
                        rep["sum_latency"] = res.value["sum"]
                    if isinstance(res.value, dict) and "conflicts" in res.value:
                        # Rows on every backend: Dolt's ``conflicts`` counts
                        # merges or replayed commits, its ``conflict_rows`` rows.
                        v = res.value
                        rep["conflicts"] = int(v.get("conflict_rows", v["conflicts"]) or 0)
                    if not res.ok:
                        rep["error"] = res.error[:300]
                        out["errors"].append(res.error[:300])
                    if res.unsupported:
                        out["status"] = "unsupported"
                        out["unsupported"] = res.error
                        return out
                if measure and res is not None:
                    _flush(suite, spine)
                    s1 = suite._safe_storage()
                    rep["storage_delta"] = s1 - s0
                ok = res is None or res.ok
                if verb.deferred and ok and verb.deferred_ref(rs):
                    t2 = time.time()
                    follow_up(suite, verb.deferred_ref(rs), i, stock_rows)
                    out["prep_sec"] += time.time() - t2
                    if measure:
                        _flush(suite, spine)
                        s2 = suite._safe_storage()
                        rep["deferred_delta"] = s2 - s0
            finally:
                verb.cleanup_rep(ctx, rs)
            if timed:
                out["reps"].append(rep)
    finally:
        verb.cleanup_point(ctx, ps)
    lat = [r["latency"] for r in out["reps"] if r.get("status") == "OK"]
    out["latency"] = _quantiles(lat)
    out["storage"] = _quantiles([r["storage_delta"] for r in out["reps"] if "storage_delta" in r])
    out["deferred"] = _quantiles([r["deferred_delta"] for r in out["reps"] if "deferred_delta" in r])
    out["failed"] = sum(1 for r in out["reps"] if r.get("status") not in (None, "OK"))
    if out["reps"] and out["failed"] == len(out["reps"]) and verb is not CONTROL:
        out["status"] = "failed"
    return out


def _fmt_bytes(b):
    if b is None:
        return "-"
    sign = "-" if b < 0 else "+"
    b = abs(b)
    for unit in ("B", "KB", "MB", "GB"):
        if b < 1024 or unit == "GB":
            return f"{sign}{b:,.0f} {unit}" if unit == "B" else f"{sign}{b:.1f} {unit}"
        b /= 1024


def main(argv: list, *, target: PreparedTarget, system_name: str) -> dict:
    """Run the grid on ``target`` and return the stats written to --outdir."""
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scale", type=int, default=BASE["scale"], help="W warehouses of CH data (default 1)")
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--verbs", help="comma-separated subset of " + ",".join(VERBS))
    ap.add_argument("--axes", help="comma-separated axis names (rows, scale, ...) or verb:axis keys")
    ap.add_argument("--reps", type=int, default=10, help="timed reps per point (default 10)")
    ap.add_argument("--warmup", type=int, default=3, help="untimed reps before them (default 3)")
    ap.add_argument("--storage-reps", type=int, default=0,
                    help="timed reps that also read storage (default 0 = all of them)")
    ap.add_argument("--no-gc", action="store_true", help="no backend GC between points")
    ap.add_argument("--no-control", action="store_true", help="skip the follow-up-writes control point")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--suffix", default="", help="appended to the run id and file names (a supplementary run)")
    ap.add_argument("--skip-done", metavar="STATS_JSON", action="append", default=[],
                    help="skip points already measured (status ok) in these stats files; repeatable")
    ap.add_argument("--stock-rows", type=int, default=STOCK_ROWS, help=argparse.SUPPRESS)
    ap.add_argument("--no-progress", action="store_true", help=argparse.SUPPRESS)
    args = ap.parse_args(argv)

    backend_name = system_name
    verbs = args.verbs.split(",") if args.verbs else None
    axes = args.axes.split(",") if args.axes else None
    for v in verbs or []:
        if v not in VERBS:
            print(f"Error: unknown verb {v!r}; one of {', '.join(VERBS)}")
            sys.exit(1)
    points = points_for_run(args.scale, verbs, axes)
    done = set()
    for f in args.skip_done:
        done |= {p["id"] for p in json.load(open(f)).get("points", []) if p.get("status") == "ok"}
    if done:
        points = [pt for pt in points if pt.id not in done]
        print(f"Skipping {len(done)} points already measured")
        if not points:
            print("Nothing left to measure at this scale")
            return
    if not points:
        print("Error: no points selected for this scale, verbs and axes")
        sys.exit(1)
    run_id = f"micro_verbs_{backend_name.lower()}_w{args.scale}{args.suffix}"
    os.makedirs(args.outdir, exist_ok=True)
    stats_path = os.path.join(args.outdir, f"{run_id}_stats.json")
    log = print
    log(f"Run ID: {run_id}\nBackend: {backend_name}\nScale: W={args.scale}\n"
        f"Points: {len(points)} ({args.warmup} warm-up + {args.reps} timed reps each)"
        + (f"\nNative-only mode" if native_only() else ""))

    spine = target.root_ref
    collector = rc.ResultCollector(run_id=run_id, output_dir=args.outdir, spill_rows=200_000)
    collector.set_context(table_name="micro_verbs", table_schema=f"w{args.scale}", initial_db_size=0,
                          seed=args.seed)
    rc.set_current_thread_id(1)

    suite_index = 0
    suite_index_lock = threading.Lock()

    def new_suite():
        nonlocal suite_index
        with suite_index_lock:
            worker_index = suite_index
            suite_index += 1
        return target.open_suite(SuiteRequest(
            result_collector=collector,
            initial_ref=spine,
            worker_index=worker_index,
        ))

    def close(suite):
        try:
            suite.close_connection()
        except Exception:
            pass

    stats = {"run_id": run_id, "backend": backend_name, "scale": args.scale, "seed": args.seed,
             "reps": args.reps, "warmup": args.warmup, "storage_reps": args.storage_reps,
             "follow_up_rows": FOLLOW_UP_ROWS, "base": BASE, "native_only": native_only(),
             "implementation": implementation_report(target.suite_type), "setup": {}, "points": [], "status": "running"}

    def save():
        with open(stats_path, "w") as f:
            json.dump(stats, f, indent=1, default=str)

    # -- data: schema and CH-benCHmark at W, committed as the base --------
    suite = new_suite()
    t0 = time.time()
    schema = tp.SchemaConfig(base=tp.BaseSchema.CH_BENCH, scale_factor=args.scale)
    sch = mschema.apply_schema(suite, spine, schema, "micro_verbs", log=log)
    scale = CHScale.from_config(schema)
    log(f"Seeding CH-benCHmark at W={scale.warehouses}...")
    counts = seed_ch(suite, spine, scale, seed=args.seed, batch_rows=500, log=log)
    base = suite.commit(spine, "base", timed=False)
    stats["setup"] = {"schema_statements": sch["statements"], "schema_failed": sch["failed"],
                      "ch_rows": counts, "base_commit": base.status_name, "setup_sec": round(time.time() - t0, 1)}
    _flush(suite, spine)
    stats["setup"]["storage_bytes"] = suite._safe_storage()
    log(f"Setup: {sum(counts.values()):,} rows in {stats['setup']['setup_sec']}s, "
        f"storage {_fmt_bytes(stats['setup']['storage_bytes'])}")
    close(suite)
    save()

    todo = [("control", CONTROL, dict(BASE, scale=args.scale), CONTROL_REPS)] \
        if not (args.no_control or "control" in done) else []
    todo += [(pt.id, VERB_IMPL[pt.axis.verb], pt.params, args.reps) for pt in points]
    status = "completed"
    try:
        for idx, (pid, verb, params, reps) in enumerate(todo):
            suite = new_suite()
            point_branch = f"p{idx}"
            t0 = time.time()
            try:
                fork(suite, point_branch, spine)
                res = run_point(pid, verb, params, suite, spine, point_branch, reps=reps,
                                warmup=args.warmup, storage_reps=args.storage_reps,
                                stock_rows=args.stock_rows, log=log, new_suite=new_suite)
            except Exception as e:
                traceback.print_exc()
                res = {"id": pid, "verb": verb.name, "params": params, "status": "crashed",
                       "errors": [f"{type(e).__name__}: {e}"[:300]], "reps": [], "latency": {},
                       "storage": {}, "deferred": {}, "failed": 0}
            finally:
                drop(suite, point_branch)
                if not args.no_gc:
                    gc = suite.gc()
                    res["gc"] = {"status": gc.status_name, "latency_sec": round(gc.latency, 2)}
                close(suite)
            res["elapsed_sec"] = round(time.time() - t0, 1)
            stats["points"].append(res)
            save()
            lat, sto, dfr = res.get("latency") or {}, res.get("storage") or {}, res.get("deferred") or {}
            line = f"[{idx + 1}/{len(todo)}] {pid:28s} {res['status']:11s}"
            if lat:
                line += f" p50 {lat['p50'] * 1000:,.0f} ms (p10-p90 {lat['p10'] * 1000:,.0f}-{lat['p90'] * 1000:,.0f})"
            if sto:
                line += f"  storage {_fmt_bytes(sto['p50'])}"
            if dfr:
                line += f"  deferred {_fmt_bytes(dfr['p50'])}"
            if res.get("failed"):
                line += f"  {res['failed']} failed"
            line += f"  [{res['elapsed_sec']:.0f} s]"
            log(line)
    except KeyboardInterrupt:
        status = "interrupted"
        log("\nInterrupted: writing what was measured...")
    finally:
        stats["status"] = status
        save()
        collector.write_to_parquet(append=False)
        log(f"Stats written to {stats_path}")
    return stats
