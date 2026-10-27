"""Macrobenchmark runner: one scenario (S1-S6) over one prepared target.

    from macrobench import runner
    runner.main(["--config", "rl_env_mini.textproto", "--outdir", out],
                target=prepared_target, system_name="Doltgres")

The embedding application provisions the database and passes it as a
dblib.target.PreparedTarget. The runner creates the schema and seed data
with the generators, starts the optional TPC-C spine load, runs the
scenario, and writes the operation rows to parquet plus an e2e stats JSON
with the support summary, the invariant results, the logical work counters
and the scenario's own metrics. Deleting the database is the embedding
application's job, so the last storage point is taken after gc.
"""

import argparse
import json
import os
import signal
import sys
import threading
import time
import traceback

from google.protobuf import text_format
from google.protobuf.json_format import MessageToDict

from dblib import result_collector as rc
from dblib.target import PreparedTarget, SuiteRequest
from macrobench import schema as mschema
from macrobench import task_pb2 as tp
from macrobench.datagen.ch import CHScale, seed_ch
from macrobench.intensity import apply_intensity
from macrobench.scenarios import (  # noqa: F401 (registers the scenarios)
    SCENARIOS,
    InvariantFailed,
    InvariantRecorder,
    ScenarioContext,
    ScenarioStopped,
    create_scenario,
)
from macrobench.tpcc import SpineLoad
from dblib.db_api import native_only
from microbench.runner2 import SharedProgress


def load_config(path: str) -> tp.MacroBenchConfig:
    config = tp.MacroBenchConfig()
    with open(path, "r") as f:
        text_format.Parse(f.read(), config)
    if not config.run_id:
        raise ValueError("run_id is required")
    if config.workload.WhichOneof("scenario") is None:
        raise ValueError("workload must set one scenario (rl_env, context_mgmt, ...)")
    if (config.workload.HasField("multi_agent")
            and config.workload.multi_agent.resolution_policy == tp.ResolutionPolicy.LLM_ASSISTED):
        raise ValueError("multi_agent.resolution_policy LLM_ASSISTED is not implemented yet")
    scaled = apply_intensity(config.workload)
    if scaled:
        w = config.workload
        print(f"Intensity: branch x{w.branch_intensity or 1.0}, data x{w.data_intensity or 1.0}")
        for name, (before, after) in scaled.items():
            print(f"  {name}: {before} -> {after}")
    return config


def _flush_to_disk(suite):
    suite.flush()


DEFAULT_STORAGE_SAMPLE_SEC = 5.0
DEFAULT_OP_STALL_SEC = 600.0


class StallWatchdog(threading.Thread):
    """Cancels any single operation (a verb or an exec) that has been
    running for longer than ``threshold``: the operation fails with a
    note naming the stall, and the workflow goes on, so what follows
    (later failures, invariant results) is recorded too. Checks every
    ``interval`` seconds; the collector's ``stalls`` list what it
    cancelled and whether each failed or completed anyway."""

    def __init__(self, collector, threshold: float, interval: float = None, log=print):
        super().__init__(daemon=True, name="stall-watchdog")
        self.collector = collector
        self.threshold = threshold
        # A quarter of the threshold, between 0.5 s and 5 s.
        self.interval = interval if interval is not None else min(5.0, max(0.5, threshold / 4.0))
        self.log = log
        self._stop = threading.Event()

    def run(self):
        while not self._stop.wait(self.interval):
            for o in self.collector.stalled_ops(self.threshold):
                if o["cancelled"]:
                    continue
                desc = f"{o['op']} on {o['ref'] or '?'}" + (f" ({o['label']})" if o["label"] else "")
                note = f"stalled: cancelled after {o['elapsed_sec']:.0f}s (limit {self.threshold:.0f}s)"
                if self.collector.cancel_op(o["token"], note):
                    self.log(f"Stall watchdog: {desc} {note}; the operation fails and the run goes on")

    def stop(self):
        self._stop.set()


class StorageSampler(threading.Thread):
    """Records the backend's storage size every ``interval`` seconds while
    the scenario runs: (wall-clock seconds, bytes) pairs, cheap enough to
    stay on by default (one directory walk per sample) and attributable to
    workflow phases through the operation rows' timestamps."""

    def __init__(self, suite, interval: float):
        super().__init__(daemon=True, name="storage-sampler")
        self.suite = suite
        self.interval = interval
        self.samples: list = []
        self._stop = threading.Event()

    def run(self):
        while not self._stop.is_set():
            t = time.time()
            self.samples.append((round(t, 3), self.suite._safe_storage()))
            self._stop.wait(self.interval)

    def stop(self):
        self._stop.set()
        self.join(timeout=60)


def storage_sample_interval(config, cli_value) -> float:
    """Seconds between samples: the CLI value if given, else the config's
    (0 = the default); 0 or a negative value turns the sampler off."""
    if cli_value is not None:
        return max(0.0, float(cli_value))
    v = float(config.storage_sample_interval_sec or 0)
    if v == 0:
        return DEFAULT_STORAGE_SAMPLE_SEC
    return max(0.0, v)


def implementation_report(suite_type) -> dict:
    """How a suite realises each verb (native, simulated or unsupported)."""
    return {
        "verbs": suite_type.implementation(),
        "notes": dict(suite_type.IMPLEMENTATION_NOTES),
    }


def setup_data(config, scenario, suite, log=print) -> dict:
    """Create the schema and seed data on the spine; returns stats."""
    spine = scenario.ctx.spine
    stats = {}
    t0 = time.time()
    schema_stats = mschema.apply_schema(suite, spine, config.schema,
                                        scenario.key, log=log)
    stats["schema_statements"] = schema_stats["statements"]
    stats["schema_failed"] = schema_stats["failed"]
    log(f"Schema: {schema_stats['statements']} statements "
        f"({len(schema_stats['failed'])} failed) in {time.time() - t0:.1f}s")

    if config.schema.base == tp.BaseSchema.CH_BENCH:
        t1 = time.time()
        scale = CHScale.from_config(config.schema)
        log(f"Seeding CH-benCHmark at W={scale.warehouses}, items={scale.items}, "
            f"customers/district={scale.customers_per_district}, "
            f"orders/district={scale.orders_per_district}...")
        counts = seed_ch(suite, spine, scale, seed=config.seed or 42,
                         batch_rows=config.schema.seed_batch_rows or 500, log=log)
        stats["ch_rows"] = counts
        stats["ch_seed_sec"] = round(time.time() - t1, 2)
        log(f"CH seed: {sum(counts.values()):,} rows in {stats['ch_seed_sec']}s")

    t2 = time.time()
    ext = scenario.seed(suite)
    stats["extension_rows"] = ext
    stats["extension_seed_sec"] = round(time.time() - t2, 2)

    res = suite.commit(spine, "seed", timed=False)
    stats["seed_commit"] = res.status_name
    stats["setup_sec"] = round(time.time() - t0, 2)
    return stats


def main(argv: list, *, target: PreparedTarget, system_name: str) -> dict:
    """Run one scenario on ``target`` and return its e2e stats."""
    parser = argparse.ArgumentParser(description="Run a macrobenchmark scenario.")
    parser.add_argument("--config", required=True, help="MacroBenchConfig textproto")
    parser.add_argument("--outdir", default="/tmp/run_stats", help="parquet/json output dir")
    parser.add_argument("--no-progress", action="store_true")
    parser.add_argument("--measure-storage", action="store_true",
                        help="Measure disk_size_before/after around each timed operation.")
    parser.add_argument("--max-runtime-sec", type=int, default=0,
                        help="Cap the scenario runtime in seconds (0 = no limit).")
    parser.add_argument("--op-stall-sec", type=float, default=DEFAULT_OP_STALL_SEC,
                        help="Cancel one operation (verb or exec) once it has run longer than "
                             "this many seconds; it fails and the run goes on "
                             f"(default {DEFAULT_OP_STALL_SEC:g}; 0 = off).")
    parser.add_argument("--storage-sample-interval", type=float, default=None,
                        help="Seconds between background storage samples during the "
                             f"scenario (default {DEFAULT_STORAGE_SAMPLE_SEC:g}; 0 = off).")
    args = parser.parse_args(argv)

    try:
        config = load_config(args.config)
    except FileNotFoundError:
        print(f"Error: config file not found: {args.config}")
        sys.exit(1)
    except Exception as e:
        print(f"Error parsing config: {e}")
        sys.exit(1)
    if args.measure_storage:
        config.measure_storage = True

    scenario_key = config.workload.WhichOneof("scenario")
    print(f"Run ID: {config.run_id}")
    print(f"System: {system_name}")
    print(f"Scenario: {scenario_key}")
    print(f"Schema: base={tp.BaseSchema.Name(config.schema.base)} "
          f"W={config.schema.scale_factor or 1}")
    print(f"Params: {MessageToDict(getattr(config.workload, scenario_key))}")
    print(f"Branch ops: {MessageToDict(config.workload.branch_ops)}")
    print(f"Data ops: {MessageToDict(config.workload.data_ops)}")
    if config.measure_storage:
        print("Storage measurement: enabled")
    if args.max_runtime_sec:
        print(f"Runtime cap: {args.max_runtime_sec}s")
    if args.op_stall_sec > 0:
        print(f"Operation stall limit: {args.op_stall_sec:g}s")
    sample_sec = storage_sample_interval(config, args.storage_sample_interval)
    spine_only = config.spine_only_sec > 0
    if spine_only:
        has_load = getattr(SCENARIOS.get(scenario_key), "uses_spine_load", False)
        clients = config.workload.data_ops.spine_clients
        if not (has_load and clients > 0):
            print(f"Error: spine_only_sec needs a scenario with a spine load and spine_clients > 0 "
                  f"({scenario_key}: {'a' if has_load else 'no'} spine load, spine_clients {clients})")
            sys.exit(1)
        print(f"Spine-only baseline: {config.spine_only_sec}s, {clients} spine client(s), no workflow")
    print(f"Storage sampling: every {sample_sec:g}s" if sample_sec > 0 else "Storage sampling: off")

    # A long run with a spine load emits millions of rows; spill them to
    # the parquet file as they come so the run's memory stays bounded.
    collector = rc.ResultCollector(run_id=config.run_id, output_dir=args.outdir,
                                   spill_rows=int(os.environ.get("MACRO_SPILL_ROWS", "200000")))
    measure_storage = config.measure_storage
    suite_counter = 0
    suite_counter_lock = threading.Lock()

    def suite_factory():
        nonlocal suite_counter
        with suite_counter_lock:
            worker_index = suite_counter
            suite_counter += 1
        return target.open_suite(SuiteRequest(
            result_collector=collector,
            initial_ref=target.root_ref,
            worker_index=worker_index,
            measure_storage=measure_storage,
        ))

    invariants = InvariantRecorder(enabled=not config.invariants.disabled,
                                   fail_fast=config.invariants.fail_fast)
    ctx = ScenarioContext(config=config, suite_factory=suite_factory,
                          collector=collector, spine=target.root_ref,
                          invariants=invariants, log=print)
    scenario = create_scenario(ctx)
    scenario.validate()
    ctx.work.expected = int(scenario.total_units())
    collector.set_context(table_name="macrobench", table_schema=scenario.key,
                          initial_db_size=0, seed=ctx.seed)

    setup_suite = ctx.new_suite()
    seed_stats = setup_data(config, scenario, setup_suite)

    # Workflow-level storage points (always on; a directory walk each):
    # after setup, after the scenario (its branches deleted) and after the
    # backend's GC/flush.
    storage_points = {}
    try:
        _flush_to_disk(setup_suite)
    except Exception as e:
        print(f"Warning: flush before the workflow failed: {e}")
    storage_points["after_setup"] = setup_suite._safe_storage()
    storage_before = storage_points["after_setup"]
    print(f"Storage after setup: {storage_before} bytes ({setup_suite.STORAGE_SCOPE} scope)")
    sampler = None
    if sample_sec > 0:
        sampler = StorageSampler(setup_suite, sample_sec)
        sampler.start()

    progress = SharedProgress(total=scenario.total_units(),
                              desc=f"{scenario.name} ({system_name})",
                              disable=args.no_progress)
    ctx.progress = progress

    spine_load = None
    data_ops = config.workload.data_ops
    if data_ops.spine_clients > 0 and scenario.uses_spine_load:
        spine_load = SpineLoad(
            suite_factory, ctx.spine, ctx.scale, clients=data_ops.spine_clients,
            analytical_fraction=data_ops.analytical_fraction,
            txn_limit=data_ops.spine_txn_limit, seed=ctx.seed,
            touch_c_data=scenario.spine_touches_c_data, on_error=ctx.note,
        ).start()
        scenario.spine_load = spine_load
        print(f"Spine load: {data_ops.spine_clients} client(s), "
              f"analytical fraction {data_ops.analytical_fraction:.2f}")

    deadline_timer = None
    if args.max_runtime_sec:
        deadline_timer = threading.Timer(
            args.max_runtime_sec, ctx.cancel_all, args=(f"runtime cap of {args.max_runtime_sec}s reached",))
        deadline_timer.daemon = True
        deadline_timer.start()
    watchdog = None
    if args.op_stall_sec > 0:
        watchdog = StallWatchdog(collector, args.op_stall_sec, log=ctx.note)
        watchdog.start()

    # An interrupt (Ctrl-C, SIGTERM) stops the scenario the way the runtime
    # cap does: the workers see the stop event, the run is recorded as
    # interrupted and its rows and stats are still written.
    def _on_signal(signum, frame):
        name = signal.Signals(signum).name
        print(f"\nSignal {name}: stopping the scenario...", flush=True)
        ctx.cancel_all(f"signal {name}")
    previous_handlers = {s: signal.signal(s, _on_signal) for s in (signal.SIGINT, signal.SIGTERM)}

    if spine_only:
        print(f"\nSpine-only baseline: {config.spine_only_sec}s of spine load, no workflow...")
    else:
        print(f"\nStarting scenario {scenario.name}...")
    start_time = time.time()
    status = "completed"
    try:
        if spine_only:
            # A signal or the runtime cap sets the stop event and ends the
            # wait early; the run is then recorded as interrupted.
            ctx.stop_event.wait(config.spine_only_sec)
        else:
            scenario.run()
    except ScenarioStopped:
        status = "interrupted"
    except InvariantFailed as e:
        status = f"invariant_failed: {e}"
    except Exception as e:
        status = f"crashed: {type(e).__name__}: {e}"
        traceback.print_exc()
    finally:
        for s, h in previous_handlers.items():
            signal.signal(s, h)
        if deadline_timer is not None:
            deadline_timer.cancel()
        if watchdog is not None:
            watchdog.stop()
        if spine_load is not None:
            spine_load.stop()
        progress.close()
        elapsed = time.time() - start_time
        stopped = ctx.stop_event.is_set()
        timed_out = bool(args.max_runtime_sec) and stopped and elapsed >= args.max_runtime_sec
        if status == "completed" and stopped:
            status = "interrupted"
        if ctx.unsupported_abort:
            # The first unsupported verb ended the run (see scenarios/base.py).
            status = f"unsupported: {ctx.unsupported_abort}"
        # The scenario ran to its end, but an operation the watchdog
        # cancelled never finished: the workflow is incomplete.
        stalled_failed = [st for st in collector.stalls if st["outcome"] == "failed"]
        if status == "completed" and stalled_failed:
            status = "incomplete"
        print(f"\nScenario {status} in {elapsed:.1f}s")
        if status == "incomplete":
            print(f"{len(stalled_failed)} operation(s) stalled and were cancelled (see 'stalls' in the e2e stats)")
        if stopped:
            print(f"Run stopped early: {ctx.stop_reason or 'stop requested'}")

        if sampler is not None:
            sampler.stop()
        try:
            _flush_to_disk(setup_suite)
        except Exception as e:
            print(f"Warning: flush after the workflow failed: {e}")
        storage_after = storage_points["after_workflow"] = setup_suite._safe_storage()
        print(f"Storage after workflow: {storage_after} bytes "
              f"(delta {storage_after - storage_before})")
        gc_result = setup_suite.gc()
        if gc_result.ok:
            os.sync()
        storage_points["after_gc"] = setup_suite._safe_storage()
        print(f"Storage after gc ({gc_result.status_name}, {gc_result.latency:.1f}s): "
              f"{storage_points['after_gc']} bytes")
        ctx.close_suite(setup_suite)

        inv = invariants.summary()

        e2e = {
            "run_id": config.run_id,
            "backend": system_name,
            "scenario": scenario.key,
            "status": status,
            "elapsed_sec": round(elapsed, 2),
            "max_runtime_sec": args.max_runtime_sec,
            "timed_out": timed_out,
            "stop_reason": ctx.stop_reason if stopped else "",
            "op_stall_sec": args.op_stall_sec,
            "stalls": list(collector.stalls),
            "seed": ctx.seed,
            "schema": MessageToDict(config.schema),
            "workload": MessageToDict(config.workload),
            "setup": seed_stats,
            "capabilities": target.suite_type.capabilities(),
            "implementation": implementation_report(target.suite_type),
            "native_only": native_only(),
            "spine_only_sec": config.spine_only_sec,
            "invariants": inv,
            "metrics": ctx.metrics,
            "work": ctx.work.summary(),
            "worker_errors": list(ctx.work.worker_errors),
        }
        e2e.update(collector.support_summary())
        observed = getattr(type(setup_suite), "OBSERVED", None)
        if observed:
            e2e["backend_observations"] = dict(observed)
        if spine_load is not None:
            e2e["spine_load"] = {"transactions": spine_load.counts,
                                 "failures": spine_load.failures}
        e2e["storage_before_bytes"] = storage_before
        e2e["storage_after_bytes"] = storage_after
        e2e["storage_delta_bytes"] = storage_after - storage_before
        e2e["storage"] = {
            "unit": "bytes",
            "scope": setup_suite.STORAGE_SCOPE,
            "points": storage_points,
            "gc": {"status": gc_result.status_name, "latency_sec": round(gc_result.latency, 3),
                   "error": gc_result.error},
            "per_operation": measure_storage,
            "sample_interval_sec": sample_sec,
            "run_start_time": round(start_time, 3),
            "samples": sampler.samples if sampler is not None else [],
        }
        os.makedirs(args.outdir, exist_ok=True)
        path = os.path.join(args.outdir, f"{config.run_id}_e2e_stats.json")
        with open(path, "w") as f:
            json.dump(e2e, f, indent=2, default=str)
        print(f"E2E stats written to {path}")
        print(f"Workflow supported: {e2e['workflow_supported']}; "
              f"unsupported ops: {e2e['unsupported_ops']}")
        print(f"Invariants: {inv['passed']} passed, {inv['failed']} failed, "
              f"{inv['not_applicable']} not applicable")
        for r in inv["results"]:
            if r["passed"] is False:
                print(f"  FAILED {r['name']}: {r['detail']}")
        if ctx.metrics:
            print(f"Metrics: {json.dumps(ctx.metrics, default=str)}")
        if collector.stalls:
            print(f"Stalled operations cancelled: {len(collector.stalls)}")
            for st in collector.stalls:
                print(f"  {st['op']} on {st['ref'] or '?'} ({st['label']}) after {st['elapsed_sec']}s: {st['outcome']}")

        collector.write_to_parquet(append=False)
        return e2e
