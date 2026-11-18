"""Report and compare macrobenchmark runs across backends.

Reads the ``<run_id>_e2e_stats.json`` + ``<run_id>.parquet`` pairs that
``macrobench.runner`` writes, groups them by (scenario, backend) and
produces:

  1. ``data/summary.md`` / ``data/summary.csv``: one row per run with elapsed time,
     setup time, operation counts, per-operation median latency, support,
     invariants and the scenario's own metrics (time to recovery, merge
     counts, ...).
  2. ``progress.png``: completed agent steps against elapsed time.
  3. ``time_breakdown.png``: where the workload time went (branch ops vs
     data ops), stacked per scenario and backend.
  4. ``latency_by_op_branch.png`` / ``latency_by_op_data.png``: one panel
     per operation (data statements split into point/range/scan/bulk by
     rows touched), grouped by scenario, median with 95% CI, colour = backend;
     ``data/latency_by_op.md/.csv`` hold the table and a support matrix.
  5. ``data/data_ops.md``: data statements by role
     (label) and statement type; ``latency_exec_by_label.png``,
     ``exec_time_by_op.png`` and ``data/exec_by_label.md`` break exec() down by
     script label and by statement type.
  6. ``cdf/latency_cdf_<scenario>.png``: latency CDF of every agent op with a
     backend per line.
  7. ``storage.png``: database size over the run when the runs were made
     with ``--measure-storage``.

Backends are discovered from the stats files, so one directory holding
dolt and neon runs compares them directly. Several directories can be
given; filters narrow the set.

Usage:
    uv run python scripts/plotting/plot_macrobench.py \
        --data-dir run_stats --outdir figures/macro

    uv run python scripts/plotting/plot_macrobench.py \
        --data-dir run_stats/dolt --data-dir run_stats/neon \
        --backends DOLT NEON --scenarios ops_agent data_agent \
        --outdir figures/macro
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import re
from dataclasses import dataclass, field

import numpy as np
import pandas as pd
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

SCENARIO_ORDER = ["rl_env", "context_mgmt", "multi_agent", "dev_agent", "ops_agent", "data_agent"]
SCENARIO_TITLES = {
    "rl_env": "S1 RL env",
    "context_mgmt": "S2 Context mgmt",
    "multi_agent": "S3 Multi-agent",
    "dev_agent": "S4 Dev agent",
    "ops_agent": "S5 Ops agent",
    "data_agent": "S6 Data agent",
}
BRANCH_OPS = ["BRANCH", "COMMIT", "DIFF", "SCHEMA_DIFF", "LOG", "MERGE", "REBASE", "REVERT", "RESET", "DELETE"]
DATA_OPS = ["READ", "INSERT", "UPDATE", "DELETE_ROWS", "DDL"]
# EXEC rows summarise the statements they contain, so they are excluded from
# time sums to avoid double counting.
# Two groups: branch ops are the branch operations plus everything spent reaching a
# branch (the connection switch exec() does for a ref, which on Neon is a
# new connection per branch, and the wait on Neon's branch API); data ops
# are the statements themselves.
TIME_GROUPS = {
    "branch ops": BRANCH_OPS + ["CONNECT", "API_RETRY_WAIT"],
    "data ops": DATA_OPS,
}
# Data statements are further split by how many rows they touch
# (``num_keys_touched``: rows affected for writes, rows returned for reads,
# recorded by Session.sql). READ:scan is an aggregate/join read, which
# returns few rows but reads many. Runs recorded before the count existed
# are classified from the SQL shape instead.
DATA_CLASSES = ["READ:point", "READ:range", "READ:scan", "INSERT:point", "INSERT:bulk",
                "UPDATE:point", "UPDATE:range", "DELETE_ROWS:point", "DELETE_ROWS:range", "DDL"]
OP_ORDER = BRANCH_OPS + ["CONNECT"] + DATA_OPS + ["EXEC"]
CLASS_ORDER = BRANCH_OPS + ["CONNECT"] + DATA_CLASSES + ["EXEC"]
_SCAN_RE = re.compile(r"\bJOIN\b|GROUP BY|\b(?:COUNT|SUM|AVG|MIN|MAX)\s*\(|\bDISTINCT\b", re.I)
_RANGE_RE = re.compile(r"\bBETWEEN\b|<=|>=|<|>|\bIN\s*\(|\bLIKE\b", re.I)
_MULTI_INSERT_RE = re.compile(r"\bSELECT\b|\)\s*,\s*\(", re.I)


def _classify_statements(df: pd.DataFrame) -> pd.DataFrame:
    """Add ``stmt_class``: op_name for non-statements, else the op split
    into point/range (or scan, bulk) per DATA_CLASSES."""
    if df.empty or "op_name" not in df.columns:
        return df
    df = df.copy()
    cls = df["op_name"].astype(str).copy()
    q = df["sql_query"].fillna("").astype(str).str.split(" -- args:").str[0].str.upper() \
        if "sql_query" in df.columns else pd.Series("", index=df.index)
    counted = ("num_keys_touched" in df.columns) and bool((df["num_keys_touched"] > 0).any())
    if counted:
        multi = df["num_keys_touched"] > 1
    else:
        multi = q.str.contains(_RANGE_RE) | ~q.str.contains(r"\bWHERE\b", regex=True)
    is_read = df["op_name"] == "READ"
    cls[is_read] = "READ:point"
    cls[is_read & multi] = "READ:range"
    cls[is_read & q.str.contains(_SCAN_RE)] = "READ:scan"
    for op in ("UPDATE", "DELETE_ROWS"):
        m = df["op_name"] == op
        cls[m] = f"{op}:point"
        cls[m & multi] = f"{op}:range"
    is_ins = df["op_name"] == "INSERT"
    cls[is_ins] = "INSERT:point"
    cls[is_ins & (multi if counted else q.str.contains(_MULTI_INSERT_RE))] = "INSERT:bulk"
    df["stmt_class"] = cls
    df.attrs["rows_counted"] = counted
    return df

# Label of the background TPC-C/CH traffic; it runs until the scenario ends,
# so its volume scales with run time and backend speed. The headline
# figures and counts exclude it; the spine columns in summary.md report it.
SPINE_LABEL = "spine"
# S3's background task updater is time-based like the spine and is kept
# out of the agent figures for the same reason.
BACKGROUND_LABELS = (SPINE_LABEL, "spine_update")
# Invariant probes are recorded (their rows document the checks) but are
# harness work: neither agent steps nor background load in the figures.
HARNESS_LABELS = ("invariant",)
SPINE_OPS = [o for o in DATA_OPS if o != "DDL"] + ["EXEC"]
PALETTE = plt.get_cmap("tab10")


def _reclassify_deletes(df: pd.DataFrame) -> pd.DataFrame:
    """Runs recorded before DELETE_ROWS existed filed DELETE statements
    under UPDATE; split them out by their SQL text."""
    if df.empty or "sql_query" not in df.columns:
        return df
    mask = (df["op_name"] == "UPDATE") & df["sql_query"].fillna("").str.lstrip().str.upper().str.startswith("DELETE")
    if mask.any():
        df = df.copy()
        df.loc[mask, "op_name"] = "DELETE_ROWS"
    return df


@dataclass
class Run:
    run_id: str
    backend: str
    scenario: str
    stats: dict
    ops: pd.DataFrame
    path: str = ""
    extra: dict = field(default_factory=dict)

    @property
    def label(self) -> str:
        return self.backend


# ── Loading ───────────────────────────────────────────────────────────


def _data_path(outdir: str, name: str) -> str:
    """Tables (.md/.csv) go under <outdir>/data/."""
    d = os.path.join(outdir, "data")
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, name)


def load_runs(data_dirs: list[str], backends: list[str] | None,
              scenarios: list[str] | None, run_glob: str) -> list[Run]:
    runs: list[Run] = []
    for d in data_dirs:
        for stats_path in sorted(glob.glob(os.path.join(d, "**", f"{run_glob}_e2e_stats.json"),
                                           recursive=True)):
            with open(stats_path) as f:
                stats = json.load(f)
            scenario = stats.get("scenario")
            backend = str(stats.get("backend", "?"))
            if not scenario:
                continue  # not a macrobench stats file
            if backends and backend not in backends:
                continue
            if scenarios and scenario not in scenarios:
                continue
            parquet = stats_path.replace("_e2e_stats.json", ".parquet")
            ops = pd.read_parquet(parquet) if os.path.exists(parquet) else pd.DataFrame()
            ops = _classify_statements(_reclassify_deletes(ops))
            if not ops.empty:
                ops = ops[ops["run_id"] == stats["run_id"]] if "run_id" in ops else ops
            runs.append(Run(stats["run_id"], backend, scenario, stats, ops, stats_path))
    if not runs:
        raise SystemExit(f"no *_e2e_stats.json macrobench files under {data_dirs}")
    runs.sort(key=lambda r: (SCENARIO_ORDER.index(r.scenario) if r.scenario in SCENARIO_ORDER else 99,
                             r.backend, r.run_id))
    return runs


def dedupe_latest(runs: list[Run]) -> list[Run]:
    """Keep the newest run per (scenario, backend) unless --all-runs."""
    latest: dict[tuple[str, str], Run] = {}
    for r in runs:
        key = (r.scenario, r.backend)
        if key not in latest or os.path.getmtime(r.path) > os.path.getmtime(latest[key].path):
            latest[key] = r
    return sorted(latest.values(), key=lambda r: (SCENARIO_ORDER.index(r.scenario)
                                                  if r.scenario in SCENARIO_ORDER else 99, r.backend))


# ── Per-run aggregates ────────────────────────────────────────────────


def agent_ops(run: Run) -> pd.DataFrame:
    """The scenario's own rows: everything but the spine load."""
    if run.ops.empty or "label" not in run.ops.columns:
        return run.ops
    return run.ops[~run.ops["label"].isin(BACKGROUND_LABELS + HARNESS_LABELS)]


def spine_ops(run: Run) -> pd.DataFrame:
    if run.ops.empty or "label" not in run.ops.columns:
        return run.ops.iloc[0:0]
    return run.ops[run.ops["label"].isin(BACKGROUND_LABELS)]


def timed_ops(run: Run) -> pd.DataFrame:
    """Agent rows that carry a latency and are not the EXEC summary."""
    df = agent_ops(run)
    if df.empty:
        return df
    return df[(df["status"] == "OK") & (df["op_name"] != "EXEC")]


def time_by_group(run: Run) -> dict[str, float]:
    df = timed_ops(run)
    out = {}
    for group, names in TIME_GROUPS.items():
        out[group] = float(df[df["op_name"].isin(names)]["latency"].sum()) if not df.empty else 0.0
    return out


def latency_stats(run: Run, spine: bool = False) -> pd.DataFrame:
    """median/p90/count per op_name (OK rows only) of the agent's rows, or
    of the spine load's rows with ``spine=True``."""
    df = spine_ops(run) if spine else agent_ops(run)
    if df.empty:
        return pd.DataFrame(columns=["op_name", "median", "p90", "count"])
    ok = df[df["status"] == "OK"]
    g = ok.groupby("op_name")["latency"]
    return pd.DataFrame({"median": g.median(), "p90": g.quantile(0.9), "count": g.size()}).reset_index()


def exec_by_label_stats(run: Run) -> pd.DataFrame:
    """One row per exec() label: count, median/p90 latency and the median
    number of statements one exec issued."""
    df = run.ops
    cols = ["label", "count", "median", "p90", "stmts_per_exec"]
    if df.empty or "label" not in df.columns:
        return pd.DataFrame(columns=cols)
    ex = df[(df["op_name"] == "EXEC") & (df["status"] == "OK")].copy()
    if ex.empty:
        return pd.DataFrame(columns=cols)
    ex["label"] = ex["label"].fillna("").replace("", "(none)")
    stmts = df[df["op_name"].isin(DATA_OPS)].groupby("exec_id").size().rename("stmts")
    ex = ex.join(stmts, on="exec_id")
    g = ex.groupby("label")
    out = pd.DataFrame({"count": g.size(), "median": g["latency"].median(),
                        "p90": g["latency"].quantile(0.9),
                        "stmts_per_exec": g["stmts"].median().fillna(0)}).reset_index()
    return out.sort_values("count", ascending=False)


EXEC_PARTS = DATA_OPS + ["CONNECT", "other"]


def exec_time_breakdown(run: Run) -> pd.DataFrame:
    """Per exec() label: mean time per exec spent in each statement type.

    Every statement a script issues is its own row sharing the EXEC row's
    ``exec_id``; CONNECT rows are the connection switches; ``other`` is the
    EXEC latency not covered by any recorded row: the BEGIN/COMMIT/ROLLBACK
    round trips of ``Session.transaction()`` (recorded untimed), the Python
    between statements, result handling and pool wait.  Means are used because the
    parts of a mean add up to the mean total, which medians do not."""
    df = run.ops
    cols = ["label", "count", "mean"] + EXEC_PARTS
    if df.empty or "label" not in df.columns:
        return pd.DataFrame(columns=cols)
    ex = df[(df["op_name"] == "EXEC") & (df["status"] == "OK")].copy()
    if ex.empty:
        return pd.DataFrame(columns=cols)
    ex["label"] = ex["label"].fillna("").replace("", "(none)")
    parts = df[df["op_name"].isin(DATA_OPS + ["CONNECT"]) & df["exec_id"].isin(ex["exec_id"])]
    per_exec = parts.pivot_table(index="exec_id", columns="op_name", values="latency",
                                 aggfunc="sum", fill_value=0.0)
    for c in DATA_OPS + ["CONNECT"]:
        if c not in per_exec.columns:
            per_exec[c] = 0.0
    ex = ex.join(per_exec[DATA_OPS + ["CONNECT"]], on="exec_id").fillna(0.0)
    ex["other"] = (ex["latency"] - ex[DATA_OPS + ["CONNECT"]].sum(axis=1)).clip(lower=0.0)
    g = ex.groupby("label")
    out = g[EXEC_PARTS].mean()
    out.insert(0, "mean", g["latency"].mean())
    out.insert(0, "count", g.size())
    return out.reset_index().sort_values("count", ascending=False)


def summary_row(run: Run) -> dict:
    s = run.stats
    inv = s.get("invariants", {})
    row = {
        "scenario": run.scenario,
        "backend": run.backend,
        "run_id": run.run_id,
        "status": s.get("status"),
        "timed_out": s.get("timed_out"),
        "elapsed_sec": s.get("elapsed_sec"),
        "setup_sec": (s.get("setup") or {}).get("setup_sec"),
        "scale_factor": (s.get("schema") or {}).get("scaleFactor"),
        "branch_intensity": (s.get("workload") or {}).get("branchIntensity", 1.0),
        "data_intensity": (s.get("workload") or {}).get("dataIntensity", 1.0),
        "workflow_supported": s.get("workflow_supported"),
        "unsupported_ops": ",".join(s.get("unsupported_ops", [])),
        "invariants_passed": inv.get("passed"),
        "invariants_failed": inv.get("failed"),
        "invariants_na": inv.get("not_applicable"),
    }
    # Counts and latencies of the agent's own operations (spine excluded).
    ag = agent_ops(run)
    if not ag.empty:
        for name in OP_ORDER:
            sub = ag[ag["op_name"] == name]
            if len(sub):
                row[f"n_{name}"] = int((sub["status"] == "OK").sum())
                failed = int((sub["status"] == "FAILED").sum())
                if failed:
                    row[f"failed_{name}"] = failed
    else:
        counts = s.get("op_status_counts", {})
        for name in OP_ORDER:
            c = counts.get(name)
            if c:
                row[f"n_{name}"] = c.get("ok", 0)
                if c.get("failed"):
                    row[f"failed_{name}"] = c["failed"]
    lat = latency_stats(run).set_index("op_name")
    for name in OP_ORDER:
        if name in lat.index:
            row[f"p50_{name}_ms"] = round(lat.loc[name, "median"] * 1000, 2)
    for group, secs in time_by_group(run).items():
        row[f"time_{group.replace(' ', '_')}_sec"] = round(secs, 2)
    spine = s.get("spine_load") or {}
    if spine:
        txns = spine.get("transactions")
        total = sum(txns.values()) if isinstance(txns, dict) else txns
        row["spine_clients"] = ((s.get("workload") or {}).get("dataOps") or {}).get("spineClients")
        row["spine_txns"] = total
        row["spine_failures"] = spine.get("failures")
        if total and s.get("elapsed_sec"):
            row["spine_txn_per_sec"] = round(total / float(s["elapsed_sec"]), 2)
        slat = latency_stats(run, spine=True).set_index("op_name")
        for name in SPINE_OPS:
            if name in slat.index:
                row[f"spine_p50_{name}_ms"] = round(slat.loc[name, "median"] * 1000, 2)
    for k, v in (s.get("metrics") or {}).items():
        if isinstance(v, (int, float, str, bool)):
            row[f"m_{k}"] = v
    st = s.get("storage") or {}
    points = st.get("points") or {}
    if points:
        row["storage_scope"] = st.get("scope")
        for name in ("after_setup", "after_workflow", "after_gc", "after_cleanup"):
            if points.get(name) is not None:
                row[f"storage_{name}_mb"] = round(points[name] / 1e6, 1)
        row["storage_gc"] = (st.get("gc") or {}).get("status")
    elif s.get("storage_before_bytes") is not None:
        row["storage_before_bytes"] = s.get("storage_before_bytes")
        row["storage_after_bytes"] = s.get("storage_after_bytes")
    return row


# ── Figures ───────────────────────────────────────────────────────────


def _grid(runs: list[Run]):
    scenarios = [s for s in SCENARIO_ORDER if any(r.scenario == s for r in runs)]
    scenarios += sorted({r.scenario for r in runs} - set(scenarios))
    backends = sorted({r.backend for r in runs})
    return scenarios, backends


def _find(runs, scenario, backend):
    for r in runs:
        if r.scenario == scenario and r.backend == backend:
            return r
    return None


STEP_OPS = BRANCH_OPS + ["EXEC"]


def progress_curve(run: Run) -> tuple:
    """(elapsed seconds, cumulative completed agent steps). A step is one
    branch operation or one exec() script run by the scenario; the statements
    inside an exec, CONNECT rows and the background load are not steps."""
    df = agent_ops(run)
    if df.empty or "end_time" not in df.columns:
        return np.array([]), np.array([])
    steps = df[df["op_name"].isin(STEP_OPS) & (df["status"] != "UNSUPPORTED")]
    if steps.empty:
        return np.array([]), np.array([])
    t0 = float(run.ops["start_time"].min())
    t = np.sort(steps["end_time"].to_numpy(dtype=float) - t0)
    return t, np.arange(1, len(t) + 1)


def plot_progress(runs: list[Run], outdir: str) -> None:
    """One panel per scenario: completed agent steps against elapsed time,
    one curve per backend."""
    scenarios, backends = _grid(runs)
    ncols = min(3, len(scenarios))
    nrows = int(np.ceil(len(scenarios) / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(6 * ncols, 3.8 * nrows), squeeze=False)
    for idx, s in enumerate(scenarios):
        ax = axes[idx // ncols][idx % ncols]
        for i, b in enumerate(backends):
            r = _find(runs, s, b)
            if r is None:
                continue
            t, n = progress_curve(r)
            if len(t) == 0:
                continue
            ax.step(np.concatenate(([0.0], t)), np.concatenate(([0], n)), where="post",
                    color=PALETTE(i), label=f"{b} ({len(t)} steps, {t[-1]:.0f} s)")
        ax.set_xlabel("elapsed seconds since run start")
        ax.set_ylabel("completed agent steps")
        ax.set_title(SCENARIO_TITLES.get(s, s))
        ax.grid(alpha=0.3)
        ax.legend(fontsize=8, loc="lower right")
    for idx in range(len(scenarios), nrows * ncols):
        axes[idx // ncols][idx % ncols].axis("off")
    fig.suptitle("Agent progress: branch operations and exec() runs completed over time (spine excluded)")
    fig.tight_layout()
    fig.savefig(os.path.join(outdir, "progress.png"), dpi=150)
    plt.close(fig)


def plot_time_breakdown(runs: list[Run], outdir: str) -> None:
    scenarios, backends = _grid(runs)
    groups = list(TIME_GROUPS)
    x = np.arange(len(scenarios))
    width = 0.8 / max(1, len(backends))
    fig, ax = plt.subplots(figsize=(1.6 * len(scenarios) + 3, 4.5))
    hatches = ["", "//"]
    for i, b in enumerate(backends):
        pos = x + (i - (len(backends) - 1) / 2) * width
        bottom = np.zeros(len(scenarios))
        for gi, g in enumerate(groups):
            vals = np.array([time_by_group(_find(runs, s, b))[g] if _find(runs, s, b) else 0.0
                             for s in scenarios])
            ax.bar(pos, vals, width, bottom=bottom, color=PALETTE(i), hatch=hatches[gi % len(hatches)],
                   edgecolor="white", label=f"{b}: {g}" if vals.sum() > 0 else None)
            bottom += vals
    ax.set_xticks(x)
    ax.set_xticklabels([SCENARIO_TITLES.get(s, s) for s in scenarios])
    ax.set_ylabel("summed operation latency (s)")
    ax.set_title("Workload time: branch ops vs data ops (sum over all threads; "
                 "branch ops include exec() connection switches)")
    ax.legend(fontsize=7, ncol=2)
    fig.tight_layout()
    fig.savefig(os.path.join(outdir, "time_breakdown.png"), dpi=150)
    plt.close(fig)


OP_TITLES = {**{o: o for o in BRANCH_OPS},
             **{o: f"{o} (statement inside exec)" for o in DATA_OPS},
             **{o: f"{o} (statement inside exec)" for o in DATA_CLASSES},
             "READ:scan": "READ:scan (aggregate/join inside exec)",
             "EXEC": "EXEC (whole script)", "CONNECT": "CONNECT (connection switch)"}


def _median_ci(x: np.ndarray, z: float = 1.96) -> tuple:
    """Distribution-free 95% CI of the median from order statistics
    (binomial argument: ranks n/2 -/+ z*sqrt(n)/2); (nan, nan) below 2
    samples."""
    n = len(x)
    if n < 2:
        return np.nan, np.nan
    xs = np.sort(x)
    half = z * np.sqrt(n) / 2
    lo = int(np.clip(np.floor(n / 2 - half), 0, n - 1))
    hi = int(np.clip(np.ceil(n / 2 + half), 0, n)) - 1
    return float(xs[lo]), float(xs[max(hi, lo)])


def op_latency_table(runs: list[Run]) -> pd.DataFrame:
    """One row per (scenario, backend, op): OK count, median and its 95%
    order-statistic CI, plus how many calls came back UNSUPPORTED or FAILED.
    Agent rows only; statements inside exec() count under their own
    class (see DATA_CLASSES)."""
    rows = []
    for r in runs:
        df = agent_ops(r)
        if df.empty:
            continue
        key = "stmt_class" if "stmt_class" in df.columns else "op_name"
        for op, g in df.groupby(key):
            ok = g[g["status"] == "OK"]["latency"].to_numpy(dtype=float)
            lo, hi = _median_ci(ok)
            rows.append({"scenario": r.scenario, "backend": r.backend, "op": op, "n": len(ok),
                         "median": float(np.median(ok)) if len(ok) else np.nan,
                         "ci_lo": lo, "ci_hi": hi,
                         "unsupported": int((g["status"] == "UNSUPPORTED").sum()),
                         "failed": int((g["status"] == "FAILED").sum())})
    return pd.DataFrame(rows, columns=["scenario", "backend", "op", "n", "median", "ci_lo", "ci_hi",
                                       "unsupported", "failed"])


def _split_ops(table: pd.DataFrame, backends: list) -> tuple:
    """Ops every backend has OK samples for, and ops some backends lack."""
    present = [o for o in CLASS_ORDER if (table[(table["op"] == o) & (table["n"] > 0)]).shape[0]]
    present += sorted(set(table[table["n"] > 0]["op"]) - set(present))
    common, partial = [], []
    for o in present:
        have = set(table[(table["op"] == o) & (table["n"] > 0)]["backend"])
        (common if have >= set(backends) else partial).append(o)
    return common, partial


def _plot_op_panels(runs: list[Run], ops_wanted: list, suptitle: str, filename: str,
                    outdir: str) -> None:
    """One panel per op in ``ops_wanted``: x = scenario (each scenario a
    shaded band), one marker per backend (colour) with the median and its
    95% CI on a log scale. Ops every backend has samples for come first;
    ops some backends lack follow with the missing backends named. Hollow
    marker = single sample (no CI)."""
    table = op_latency_table(runs)
    scenarios, backends = _grid(runs)
    common, partial = _split_ops(table, backends)
    panels = [(o, True) for o in common if o in ops_wanted] + \
             [(o, False) for o in partial if o in ops_wanted]
    if not panels:
        return
    ncols = min(4, len(panels))
    nrows = int(np.ceil(len(panels) / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(4.6 * ncols, 3.3 * nrows), squeeze=False)
    x = np.arange(len(scenarios))
    width = 0.8 / max(1, len(backends))
    for idx, (op, is_common) in enumerate(panels):
        ax = axes[idx // ncols][idx % ncols]
        t = table[table["op"] == op].set_index(["scenario", "backend"])
        for i, b in enumerate(backends):
            pos = x + (i - (len(backends) - 1) / 2) * width
            med = np.full(len(scenarios), np.nan)
            lo = np.full(len(scenarios), np.nan)
            hi = np.full(len(scenarios), np.nan)
            for j, sc in enumerate(scenarios):
                if (sc, b) in t.index and t.loc[(sc, b), "n"] > 0:
                    med[j] = t.loc[(sc, b), "median"] * 1000
                    lo[j] = t.loc[(sc, b), "ci_lo"] * 1000
                    hi[j] = t.loc[(sc, b), "ci_hi"] * 1000
            if np.all(np.isnan(med)):
                continue
            err = np.vstack([np.nan_to_num(med - lo), np.nan_to_num(hi - med)])
            single = np.isnan(lo) & ~np.isnan(med)
            ax.errorbar(pos, med, yerr=err, fmt="o", ms=4, color=PALETTE(i), capsize=2,
                        lw=1, label=b if idx == 0 else None)
            if single.any():
                ax.plot(pos[single], med[single], "o", ms=5, mfc="white", mec=PALETTE(i), zorder=5)
        # one shaded band per scenario so each cluster reads as a group
        for j in range(len(scenarios)):
            ax.axvspan(j - 0.5, j + 0.5, color="0.92" if j % 2 else "0.97", zorder=0)
        missing = sorted(set(backends) - set(table[(table["op"] == op) & (table["n"] > 0)]["backend"]))
        title = OP_TITLES.get(op, op)
        if not is_common:
            title += f"\n(no samples: {', '.join(missing)})"
        ax.set_title(title, fontsize=9, color="black" if is_common else "dimgray")
        ax.set_yscale("log")
        ax.set_xticks(x)
        ax.set_xticklabels([f"S{SCENARIO_ORDER.index(sc) + 1}" if sc in SCENARIO_ORDER else sc
                            for sc in scenarios], fontsize=8)
        ax.set_xlim(-0.5, len(scenarios) - 0.5)
        ax.set_ylabel("ms", fontsize=8)
        ax.tick_params(axis="y", labelsize=7)
        ax.grid(axis="y", alpha=0.3)
    for idx in range(len(panels), nrows * ncols):
        axes[idx // ncols][idx % ncols].axis("off")
    handles, labels = axes[0][0].get_legend_handles_labels()
    names = "   ".join(f"S{i + 1} = {SCENARIO_TITLES.get(sc, sc).split(' ', 1)[-1]}"
                       for i, sc in enumerate(SCENARIO_ORDER) if sc in scenarios)
    fig.legend(handles, labels, loc="upper center", ncol=min(len(backends), 8), fontsize=8,
               bbox_to_anchor=(0.5, 0.985), frameon=False, title=names, title_fontsize=7)
    fig.suptitle(suptitle, y=1.0)
    fig.tight_layout(rect=(0, 0, 1, 0.94))
    fig.savefig(os.path.join(outdir, filename), dpi=150)
    plt.close(fig)


def plot_latency_by_op(runs: list[Run], outdir: str) -> None:
    """latency_by_op_branch.png (branch operations + CONNECT) and latency_by_op_data.png
    (statements inside exec() + EXEC)."""
    _plot_op_panels(runs, BRANCH_OPS + ["CONNECT"],
                    "Branch operation latency per scenario: median with 95% CI, colour = backend "
                    "(spine excluded; hollow marker = single sample)",
                    "latency_by_op_branch.png", outdir)
    _plot_op_panels(runs, DATA_CLASSES + ["EXEC"],
                    "Data operation latency per scenario (statements inside exec(), split by rows "
                    "touched, and whole scripts): median with 95% CI, colour = backend "
                    "(spine excluded; hollow = single sample)",
                    "latency_by_op_data.png", outdir)


def write_latency_by_op(runs: list[Run], outdir: str) -> None:
    """latency_by_op.csv/.md: the per-(scenario, backend, op) table behind
    the figure, plus an op support matrix per backend."""
    table = op_latency_table(runs)
    if table.empty:
        return
    scenarios, backends = _grid(runs)
    out = table.copy()
    for c in ("median", "ci_lo", "ci_hi"):
        out[c + "_ms"] = (out.pop(c) * 1000).round(2)
    out.to_csv(_data_path(outdir, "latency_by_op.csv"), index=False)
    common, partial = _split_ops(table, backends)
    with open(_data_path(outdir, "latency_by_op.md"), "w") as f:
        f.write("# Agent operation latency per backend\n\n")
        heuristic = [r.backend + "/" + r.scenario for r in runs
                     if not r.ops.empty and not r.ops.attrs.get("rows_counted")]
        f.write("Agent rows only (spine excluded). Statements issued inside exec() are "
                "listed under their own class: point (one row), range (several rows, by "
                "`num_keys_touched` = rows affected or returned), scan (aggregate/join read), "
                "bulk (multi-row INSERT); EXEC is the whole script. CI is the distribution-free "
                "95% CI of the median (order statistics).\n\n")
        if heuristic:
            f.write("Runs recorded before rows were counted, classified from the SQL shape "
                    "instead: " + ", ".join(heuristic) + ".\n\n")
        f.write("## Support matrix\n\n")
        f.write("Cell: scenarios with OK samples; `unsupported:` scenarios where the backend "
                "returned UNSUPPORTED; `-` not exercised.\n\n")
        ops = common + partial
        f.write("| backend | " + " | ".join(ops) + " |\n|" + "---|" * (len(ops) + 1) + "\n")
        short = {s: f"S{i + 1}" for i, s in enumerate(SCENARIO_ORDER)}
        for b in backends:
            cells = []
            for o in ops:
                t = table[(table["backend"] == b) & (table["op"] == o)]
                ok = [short.get(s, s) for s in t[t["n"] > 0]["scenario"]]
                un = [short.get(s, s) for s in t[(t["n"] == 0) & (t["unsupported"] > 0)]["scenario"]]
                cell = ",".join(ok) if ok else "-"
                if un:
                    cell += f" unsupported:{','.join(un)}"
                cells.append(cell)
            f.write(f"| {b} | " + " | ".join(cells) + " |\n")
        f.write(f"\nOps with samples on every backend: {', '.join(common) or 'none'}. "
                f"Ops some backends lack: {', '.join(partial) or 'none'}.\n\n")
        f.write("## Latency table\n\n")
        cols = ["scenario", "backend", "op", "n", "median_ms", "ci_lo_ms", "ci_hi_ms", "unsupported", "failed"]
        f.write("| " + " | ".join(cols) + " |\n|" + "---|" * len(cols) + "\n")
        for row in out[cols].itertuples(index=False):
            f.write("| " + " | ".join("" if (isinstance(v, float) and np.isnan(v)) else str(v) for v in row) + " |\n")


def data_op_stats(run: Run) -> pd.DataFrame:
    """median/p90/count per (label, op_name) for data statements (OK rows only).

    The label is the workload role the statement was issued under (spine,
    ingest, backfill, rollout_step, ...), so this separates the concurrent
    TPC-C spine traffic from the statements the agent itself runs.
    """
    df = run.ops
    if df.empty or "label" not in df.columns:
        return pd.DataFrame(columns=["label", "op_name", "median", "p90", "count"])
    ok = df[(df["status"] == "OK") & df["op_name"].isin(DATA_OPS)].copy()
    ok["label"] = ok["label"].fillna("").replace("", "(none)")
    g = ok.groupby(["label", "op_name"])["latency"]
    return pd.DataFrame({"median": g.median(), "p90": g.quantile(0.9), "count": g.size()}).reset_index()


def write_data_ops_table(runs: list[Run], outdir: str) -> None:
    rows = []
    for r in runs:
        st = data_op_stats(r)
        if st.empty:
            continue
        st.insert(0, "backend", r.backend)
        st.insert(0, "scenario", r.scenario)
        st["median_ms"] = (st.pop("median") * 1000).round(2)
        st["p90_ms"] = (st.pop("p90") * 1000).round(2)
        rows.append(st)
    if not rows:
        return
    df = pd.concat(rows, ignore_index=True)
    df.to_csv(_data_path(outdir, "data_ops.csv"), index=False)
    with open(_data_path(outdir, "data_ops.md"), "w") as f:
        f.write("# Data statement latency by role\n\n")
        f.write("| " + " | ".join(df.columns) + " |\n")
        f.write("|" + "---|" * len(df.columns) + "\n")
        for row in df.itertuples(index=False):
            f.write("| " + " | ".join(str(v) for v in row) + " |\n")


def plot_latency_cdf(runs: list[Run], outdir: str) -> None:
    scenarios, backends = _grid(runs)
    for s in scenarios:
        present = [r for r in runs if r.scenario == s and not r.ops.empty]
        agent = {r.run_id: agent_ops(r) for r in present}
        ops_present = [v for v in BRANCH_OPS + DATA_OPS
                 if any(((a["op_name"] == v) & (a["status"] == "OK")).any() for a in agent.values())]
        if not ops_present:
            continue
        ncols = min(3, len(ops_present))
        nrows = int(np.ceil(len(ops_present) / ncols))
        fig, axes = plt.subplots(nrows, ncols, figsize=(4.5 * ncols, 3.2 * nrows), squeeze=False)
        for vi, v in enumerate(ops_present):
            ax = axes[vi // ncols][vi % ncols]
            for i, b in enumerate(backends):
                r = _find(present, s, b)
                if r is None:
                    continue
                a = agent[r.run_id]
                lat = np.sort(a[(a["op_name"] == v) & (a["status"] == "OK")]["latency"].values * 1000)
                if len(lat) == 0:
                    continue
                # Start the curve at 0 so a single sample still draws a visible jump.
                xs = np.concatenate(([lat[0]], lat))
                ys = np.concatenate(([0.0], np.arange(1, len(lat) + 1) / len(lat)))
                ax.step(xs, ys, where="post", color=PALETTE(i), label=f"{b} (n={len(lat)})")
                if len(lat) == 1:
                    ax.plot(lat, [1.0], "o", color=PALETTE(i), ms=5)
            ax.set_xscale("log")
            ax.set_ylim(0, 1.05)
            ax.set_xlabel("ms")
            ax.set_ylabel("CDF")
            ax.set_title(v, fontsize=10)
            ax.grid(alpha=0.3)
            ax.legend(fontsize=7)
        for vi in range(len(ops_present), nrows * ncols):
            axes[vi // ncols][vi % ncols].axis("off")
        fig.suptitle(f"{SCENARIO_TITLES.get(s, s)}: agent operation latency (spine excluded)")
        fig.tight_layout()
        os.makedirs(os.path.join(outdir, "cdf"), exist_ok=True)
        fig.savefig(os.path.join(outdir, "cdf", f"latency_cdf_{s}.png"), dpi=150)
        plt.close(fig)


def _bar_panel(ax, runs, scenario, backends, keys, stats_for, title):
    """Grouped log-scale bars (median, whisker to p90) of ``keys`` for each
    backend; ``stats_for(run)`` returns a frame indexed by key."""
    x = np.arange(len(keys))
    width = 0.8 / max(1, len(backends))
    for i, b in enumerate(backends):
        r = _find(runs, scenario, b)
        if r is None:
            continue
        st = stats_for(r)
        med = np.array([st.loc[k, "median"] * 1000 if k in st.index else np.nan for k in keys])
        p90 = np.array([st.loc[k, "p90"] * 1000 if k in st.index else np.nan for k in keys])
        pos = x + (i - (len(backends) - 1) / 2) * width
        err = np.nan_to_num(p90 - med, nan=0.0)
        ax.bar(pos, np.nan_to_num(med), width, yerr=[np.zeros_like(err), err],
               color=PALETTE(i), label=b, capsize=2, error_kw={"lw": 0.8})
    ax.set_yscale("log")
    ax.set_xticks(x)
    ax.set_xticklabels(keys, rotation=45, ha="right", fontsize=8)
    ax.set_ylabel("median ms (whisker to p90)")
    ax.set_title(title)
    ax.grid(axis="y", alpha=0.3)


# ── Cross-branch reads as one logical step ────────────────────────────

# Script labels the scenarios issue through ScenarioContext.cross_branch_exec:
# one multi-ref exec() on a backend with multi-branch query semantics, else
# the same read once per ref (recorded as cross_branch_mode per_ref).
CROSS_BRANCH_LABELS = ["score", "policy_check"]


def cross_branch_steps(run: Run, label: str) -> pd.DataFrame:
    """One row per logical cross-branch read: wall-clock span (first row
    to last row of everything the step issued: EXEC, CONNECT, statements),
    summed exec latency, refs addressed, physical execs, and the mode.

    multi: the one EXEC row with several refs is the step.
    per_ref: the consecutive EXEC rows with this label on one thread form
    the step; any other agent operation of that thread in between (a
    diff, a commit, another script) starts the next step. CONNECT and
    statement rows join through exec_id."""
    df = run.ops
    if df.empty or "exec_id" not in df:
        return pd.DataFrame(columns=["span", "exec_latency", "refs", "execs", "mode"])
    ex = df[(df["op_name"] == "EXEC") & (df["label"] == label)].sort_values(["thread_id", "start_time"])
    if ex.empty:
        return pd.DataFrame(columns=["span", "exec_latency", "refs", "execs", "mode"])
    groups: list[list] = []
    nrefs = ex["refs"].map(lambda r: len(r) if r is not None else 0)
    if (nrefs > 1).any():
        mode = "multi"
        for _, row in ex[nrefs > 1].iterrows():
            groups.append([row])
    else:
        mode = "per_ref"
        # Start times of the thread's other agent operations (branch operations and
        # scripts under another label); one of them between two reads
        # separates two logical steps.
        others = df[df["op_name"].isin(BRANCH_OPS + ["EXEC"]) & (df["label"] != label)]
        other_starts = {t: np.sort(g["start_time"].to_numpy(dtype=float))
                        for t, g in others.groupby("thread_id")}
        cur, thread, prev_end = [], None, None
        for _, row in ex.iterrows():
            boundary = row["thread_id"] != thread
            if not boundary and prev_end is not None:
                st = other_starts.get(row["thread_id"])
                if st is not None:
                    lo = np.searchsorted(st, prev_end, side="right")
                    hi = np.searchsorted(st, row["start_time"], side="left")
                    boundary = hi > lo
            if boundary and cur:
                groups.append(cur)
                cur = []
            cur.append(row)
            thread, prev_end = row["thread_id"], row["end_time"]
        if cur:
            groups.append(cur)
    by_exec = df.groupby("exec_id").agg(start=("start_time", "min"), end=("end_time", "max"))
    out = []
    for g in groups:
        ids = [r["exec_id"] for r in g]
        w = by_exec.loc[[i for i in ids if i in by_exec.index]]
        span = float(w["end"].max() - w["start"].min()) if not w.empty else sum(r["latency"] for r in g)
        refs = sum(len(r["refs"]) if r["refs"] is not None and len(r["refs"]) > 1 else 1 for r in g)
        out.append({"span": span, "exec_latency": float(sum(r["latency"] for r in g)),
                    "refs": refs, "execs": len(g), "mode": mode})
    return pd.DataFrame(out)


def cross_branch_table(runs: list[Run]) -> pd.DataFrame:
    rows = []
    for r in runs:
        for lab in CROSS_BRANCH_LABELS:
            st = cross_branch_steps(r, lab)
            if st.empty:
                continue
            rows.append({"scenario": r.scenario, "backend": r.backend, "label": lab,
                         "mode": st["mode"].iloc[0], "steps": len(st),
                         "refs_per_step": float(st["refs"].median()),
                         "execs_per_step": float(st["execs"].median()),
                         "median_ms": st["span"].median() * 1000, "p90_ms": st["span"].quantile(0.9) * 1000,
                         "p95_ms": st["span"].quantile(0.95) * 1000, "max_ms": st["span"].max() * 1000,
                         "exec_latency_median_ms": st["exec_latency"].median() * 1000,
                         "per_ref_ms": st["span"].median() * 1000 / max(1.0, float(st["refs"].median()))})
    return pd.DataFrame(rows)


def plot_cross_branch(runs: list[Run], outdir: str) -> None:
    """latency_cross_branch.png: one panel per (scenario, label), one bar
    per backend: median wall-clock latency of one logical cross-branch
    read (whisker to p90), annotated with how it ran (multi, or per_ref x
    refs). data/cross_branch.md/.csv hold the numbers."""
    table = cross_branch_table(runs)
    if table.empty:
        return
    table.to_csv(_data_path(outdir, "cross_branch.csv"), index=False)
    with open(_data_path(outdir, "cross_branch.md"), "w") as f:
        f.write("# Cross-branch reads as one logical step\n\n")
        f.write("A step is one scenario-level read over several branches: one multi-ref "
                "exec() where the backend has multi-branch query semantics (mode multi), "
                "else the same read once per branch (mode per_ref, execs_per_step physical "
                "execs, each with its connection switch). Latency is the wall-clock span of "
                "the whole step on its thread; per_ref_ms divides it by the refs read.\n\n")
        cols = ["scenario", "backend", "label", "mode", "steps", "refs_per_step", "execs_per_step",
                "median_ms", "p90_ms", "p95_ms", "max_ms", "exec_latency_median_ms", "per_ref_ms"]
        f.write("| " + " | ".join(cols) + " |\n|" + "---|" * len(cols) + "\n")
        for _, r in table.iterrows():
            f.write("| " + " | ".join(f"{r[c]:.1f}" if isinstance(r[c], float) else str(r[c]) for c in cols) + " |\n")
    panels = sorted({(r["scenario"], r["label"]) for _, r in table.iterrows()},
                    key=lambda k: (SCENARIO_ORDER.index(k[0]) if k[0] in SCENARIO_ORDER else 99, k[1]))
    _, backends = _grid(runs)
    fig, axes = plt.subplots(1, len(panels), figsize=(5.5 * len(panels), 4.5), squeeze=False)
    for ax, (sc, lab) in zip(axes[0], panels):
        sub = table[(table["scenario"] == sc) & (table["label"] == lab)].set_index("backend")
        xs = [b for b in backends if b in sub.index]
        med = np.array([sub.loc[b, "median_ms"] for b in xs])
        p90 = np.array([sub.loc[b, "p90_ms"] for b in xs])
        ax.bar(np.arange(len(xs)), med, 0.6, yerr=[np.zeros_like(med), np.maximum(p90 - med, 0)],
               color=[PALETTE(backends.index(b)) for b in xs], capsize=3, error_kw={"lw": 0.8})
        for i, b in enumerate(xs):
            m = sub.loc[b, "mode"]
            note = "multi" if m == "multi" else f"per_ref x{sub.loc[b, 'refs_per_step']:.0f}"
            ax.annotate(f"{note}\n{med[i]:.0f} ms", (i, max(med[i], 1e-3)), textcoords="offset points",
                        xytext=(0, 4), ha="center", va="bottom", fontsize=7)
        ax.set_yscale("log")
        ax.set_xticks(np.arange(len(xs)))
        ax.set_xticklabels(xs, rotation=30, ha="right", fontsize=8)
        ax.set_ylabel("median ms per logical read (whisker to p90)")
        refs = sub["refs_per_step"].median()
        ax.set_title(f"{SCENARIO_TITLES.get(sc, sc)}: {lab} over {refs:.0f} branches")
        ax.grid(axis="y", alpha=0.3)
    fig.suptitle("One cross-branch read as a single logical step: one multi-ref exec() vs the same read once per branch")
    fig.tight_layout()
    fig.savefig(os.path.join(outdir, "latency_cross_branch.png"), dpi=150)
    plt.close(fig)


def plot_exec_by_label(runs: list[Run], outdir: str) -> None:
    """One panel per scenario: exec() latency per script label."""
    scenarios, backends = _grid(runs)
    panels = []
    for s in scenarios:
        labels = []
        for r in runs:
            if r.scenario == s:
                for lab in exec_by_label_stats(r)["label"]:
                    if lab not in labels:
                        labels.append(lab)
        if labels:
            panels.append((s, labels))
    if not panels:
        return
    ncols = min(3, len(panels))
    nrows = int(np.ceil(len(panels) / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(6 * ncols, 4 * nrows), squeeze=False)
    for idx, (s, labels) in enumerate(panels):
        ax = axes[idx // ncols][idx % ncols]
        _bar_panel(ax, runs, s, backends, labels,
                   lambda r: exec_by_label_stats(r).set_index("label"), SCENARIO_TITLES.get(s, s))
        if idx == 0:
            ax.legend(fontsize=8)
    for idx in range(len(panels), nrows * ncols):
        axes[idx // ncols][idx % ncols].axis("off")
    fig.suptitle("exec() latency per script label (one exec = one script run on one ref)")
    fig.tight_layout()
    fig.savefig(os.path.join(outdir, "latency_exec_by_label.png"), dpi=150)
    plt.close(fig)


def plot_exec_time_by_op(runs: list[Run], outdir: str) -> None:
    """One panel per scenario. Each script label gets one horizontal 100%
    bar per backend (adjacent rows, so backends compare directly), split
    into where the exec's time went: statement types, CONNECT and other
    (untimed BEGIN/COMMIT/ROLLBACK round trips plus the Python between
    statements). The mean exec time and count are printed at the end of
    each bar. Background labels (spine) come last."""
    scenarios, backends = _grid(runs)
    colors = {p: PALETTE(i) for i, p in enumerate(EXEC_PARTS)}
    panels = []
    for sc in scenarios:
        tables = {b: exec_time_breakdown(r).set_index("label")
                  for b in backends if (r := _find(runs, sc, b)) is not None}
        tables = {b: t for b, t in tables.items() if not t.empty}
        if not tables:
            continue
        labels = []
        for t in tables.values():
            for lab in t.index:
                if lab not in labels:
                    labels.append(lab)
        bg = [l for l in labels if l in BACKGROUND_LABELS]
        labels = [l for l in labels if l not in BACKGROUND_LABELS] + bg
        rows = [(lab, b) for lab in labels for b in backends if b in tables and lab in tables[b].index]
        panels.append((sc, tables, rows))
    if not panels:
        return
    heights = [len(rows) * 0.32 + 0.9 for _, _, rows in panels]
    fig, axes = plt.subplots(len(panels), 1, figsize=(11, sum(heights) + 0.8),
                             gridspec_kw={"height_ratios": heights}, squeeze=False)
    for ax, (sc, tables, rows) in zip(axes[:, 0], panels):
        y = np.arange(len(rows))[::-1]
        left = np.zeros(len(rows))
        for part in EXEC_PARTS:
            share = np.array([tables[b].loc[lab, part] / max(tables[b].loc[lab, EXEC_PARTS].sum(), 1e-12) * 100
                              for lab, b in rows])
            ax.barh(y, share, 0.72, left=left, color=colors[part], label=part)
            left += share
        for yi, (lab, b) in zip(y, rows):
            t = tables[b].loc[lab]
            ax.text(101, yi, f"{t['mean'] * 1000:,.0f} ms  n={int(t['count'])}", va="center", fontsize=7)
        ax.set_yticks(y)
        ax.set_yticklabels([f"{lab if lab not in BACKGROUND_LABELS else lab + ' (bg)'}  ·  {b}"
                            for lab, b in rows], fontsize=7.5)
        # tint the tick label by backend and draw a separator between labels
        for tick, (lab, b) in zip(ax.get_yticklabels(), rows):
            tick.set_color(PALETTE(backends.index(b)))
        prev = None
        for yi, (lab, b) in zip(y, rows):
            if prev is not None and lab != prev:
                ax.axhline(yi + 0.5, color="0.75", lw=0.6)
            prev = lab
        ax.set_xlim(0, 128)
        ax.set_xticks([0, 25, 50, 75, 100])
        ax.set_xlabel("% of exec time", fontsize=8)
        ax.set_title(SCENARIO_TITLES.get(sc, sc), fontsize=10, loc="left")
        ax.tick_params(axis="x", labelsize=7)
        ax.grid(axis="x", alpha=0.3)
        ax.set_axisbelow(True)
    handles, labels_ = axes[0][0].get_legend_handles_labels()
    fig.legend(handles, labels_, loc="upper center", ncol=len(EXEC_PARTS), fontsize=8,
               bbox_to_anchor=(0.5, 0.995), frameon=False)
    fig.suptitle("Where exec() time goes per script label (mean per exec; rows per backend; "
                 "'other' = untimed BEGIN/COMMIT round trips + Python between statements)",
                 fontsize=10, y=1.0)
    fig.tight_layout(rect=(0, 0, 1, 0.975))
    fig.savefig(os.path.join(outdir, "exec_time_by_op.png"), dpi=150)
    plt.close(fig)


def write_exec_by_label(runs: list[Run], outdir: str) -> None:
    rows = []
    for r in runs:
        st = exec_by_label_stats(r)
        if st.empty:
            continue
        st.insert(0, "backend", r.backend)
        st.insert(0, "scenario", r.scenario)
        st["median_ms"] = (st.pop("median") * 1000).round(2)
        st["p90_ms"] = (st.pop("p90") * 1000).round(2)
        bd = exec_time_breakdown(r).set_index("label")
        st["mean_ms"] = (st["label"].map(bd["mean"]) * 1000).round(2)
        for p in EXEC_PARTS:
            st[f"{p.lower()}_ms"] = (st["label"].map(bd[p]) * 1000).round(2)
        rows.append(st)
    if not rows:
        return
    df = pd.concat(rows, ignore_index=True)
    df.to_csv(_data_path(outdir, "exec_by_label.csv"), index=False)
    with open(_data_path(outdir, "exec_by_label.md"), "w") as f:
        f.write("# exec() by script label\n\n")
        f.write("One EXEC row is one script run on one ref; its latency covers every "
                "statement in the script plus the driver logic between them. "
                "`stmts_per_exec` is the median number of statements one exec issued. "
                "The `*_ms` columns after `mean_ms` split the mean exec time by what the "
                "script spent it on: each statement type, CONNECT (connection switches) "
                "and `other` (the untimed BEGIN/COMMIT/ROLLBACK round trips of "
                "`Session.transaction()`, the Python between statements, result handling, "
                "pool wait). "
                "They add up to `mean_ms`.\n\n")
        f.write("| " + " | ".join(df.columns) + " |\n")
        f.write("|" + "---|" * len(df.columns) + "\n")
        for row in df.itertuples(index=False):
            f.write("| " + " | ".join(str(v) for v in row) + " |\n")


def _storage_series(run: Run):
    """(seconds into the run, MB) from the background sampler, else from
    per-operation measurements, else None."""
    st = run.stats.get("storage") or {}
    samples = st.get("samples") or []
    if samples:
        t0 = st.get("run_start_time") or samples[0][0]
        return (np.array([p[0] for p in samples]) - t0,
                np.array([p[1] for p in samples]) / 1e6)
    if not run.ops.empty and "disk_size_after" in run.ops and (run.ops["disk_size_after"] > 0).any():
        df = run.ops[run.ops["disk_size_after"] > 0].sort_values("end_time")
        t0 = df["end_time"].iloc[0]
        return (df["end_time"] - t0).to_numpy(), (df["disk_size_after"] / 1e6).to_numpy()
    return None


def plot_storage(runs: list[Run], outdir: str) -> None:
    """Two figures. ``storage.png``: storage over the run as growth since the
    after-setup point (so database-scope and server-scope backends share one
    linear axis), with the after-setup point at t=0 (o) and the
    after-workflow point at the end of the series (s). ``storage_points.png``:
    the workflow-level points per backend as bars of growth over the
    after-setup size: after the workflow, after GC, after cleanup. Server-
    scope backends (SeekDB, MatrixOne) report the whole data directory, so
    their numbers include compaction and other databases' activity."""
    with_storage = [r for r in runs if _storage_series(r) is not None]
    if not with_storage:
        return
    scenarios, backends = _grid(with_storage)
    ncols = min(3, len(scenarios))
    nrows = int(np.ceil(len(scenarios) / ncols))

    def base_mb(r):
        points = (r.stats.get("storage") or {}).get("points") or {}
        b = points.get("after_setup")
        return (b / 1e6) if b else None

    fig, axes = plt.subplots(nrows, ncols, figsize=(5.5 * ncols, 3.6 * nrows), squeeze=False)
    for idx, s in enumerate(scenarios):
        ax = axes[idx // ncols][idx % ncols]
        for i, b in enumerate(backends):
            r = _find(with_storage, s, b)
            if r is None:
                continue
            t, mb = _storage_series(r)
            scope = (r.stats.get("storage") or {}).get("scope") or ""
            base = base_mb(r)
            if base is None:
                base = float(mb[0]) if len(mb) else 0.0
            points = (r.stats.get("storage") or {}).get("points") or {}
            label = f"{b}" + (f" ({scope})" if scope else "") + f", setup {base:,.0f} MB"
            ax.plot(t, mb - base, color=PALETTE(i), label=label, linewidth=1.2)
            ax.plot([0], [0], marker="o", color=PALETTE(i), linestyle="none", markersize=5)
            if points.get("after_workflow") is not None and len(t):
                ax.plot([t[-1]], [points["after_workflow"] / 1e6 - base], marker="s",
                        color=PALETTE(i), linestyle="none", markersize=5)
        ax.axhline(0, color="grey", linewidth=0.6)
        ax.set_xlabel("seconds into run")
        # Server-scope compaction makes swings of gigabytes; symlog keeps
        # the database-scope series readable next to them.
        ys = np.concatenate([np.abs(l.get_ydata()) for l in ax.get_lines() if len(l.get_ydata())] or [np.zeros(1)])
        if np.nanmax(ys) > 300:
            ax.set_yscale("symlog", linthresh=100)
            ax.set_ylabel("MB over the after-setup size (symlog)")
        else:
            ax.set_ylabel("MB over the after-setup size")
        ax.set_title(SCENARIO_TITLES.get(s, s))
        ax.grid(alpha=0.3)
        ax.legend(fontsize=7)
    for idx in range(len(scenarios), nrows * ncols):
        axes[idx // ncols][idx % ncols].axis("off")
    fig.suptitle("Storage growth over the run (o after setup at t=0, s after the workflow)")
    fig.text(0.5, 0.005, "growth since the after-setup point; server-scope backends report the whole data "
             "directory, so compaction shows as negative growth", ha="center", fontsize=8)
    fig.tight_layout(rect=(0, 0.03, 1, 0.97))
    fig.savefig(os.path.join(outdir, "storage.png"), dpi=150)
    plt.close(fig)

    # Workflow-level points as bars of growth over the after-setup size.
    names = [("after_workflow", "after workflow"), ("after_gc", "after GC"), ("after_cleanup", "after cleanup")]
    fig, axes = plt.subplots(nrows, ncols, figsize=(5.5 * ncols, 3.6 * nrows), squeeze=False)
    width = 0.8 / len(names)
    for idx, s in enumerate(scenarios):
        ax = axes[idx // ncols][idx % ncols]
        xs = np.arange(len(backends))
        for j, (key, title) in enumerate(names):
            vals, notes = [], []
            for b in backends:
                r = _find(with_storage, s, b)
                points = ((r.stats.get("storage") or {}).get("points") or {}) if r else {}
                base = points.get("after_setup")
                v = points.get(key)
                if r is None or base is None or v is None:
                    vals.append(np.nan); notes.append("")
                    continue
                gc = (r.stats.get("storage") or {}).get("gc") or {}
                if key == "after_gc" and gc.get("status") and gc["status"] != "OK":
                    notes.append("no gc")
                else:
                    notes.append("")
                vals.append((v - base) / 1e6)
            bars = ax.bar(xs + (j - (len(names) - 1) / 2) * width, vals, width, label=title,
                          color=plt.cm.Greys(0.35 + 0.25 * j), edgecolor="black", linewidth=0.4)
            for bar, v, n in zip(bars, vals, notes):
                if np.isnan(v):
                    continue
                txt = f"{v:+,.0f}" + (f"\n{n}" if n else "")
                ax.annotate(txt, (bar.get_x() + bar.get_width() / 2, v), textcoords="offset points",
                            xytext=(0, 3 if v >= 0 else -10), ha="center", fontsize=6)
        ax.set_xticks(xs)
        ax.set_xticklabels([f"{b}\n{(( _find(with_storage, s, b).stats.get('storage') or {}).get('scope') or '') if _find(with_storage, s, b) else ''}"
                            for b in backends], fontsize=7)
        ax.axhline(0, color="grey", linewidth=0.6)
        ax.set_yscale("symlog", linthresh=10)
        ax.set_ylabel("MB over the after-setup size (symlog)")
        ax.set_title(SCENARIO_TITLES.get(s, s))
        ax.grid(alpha=0.3, axis="y")
        if idx == 0:
            ax.legend(fontsize=7)
    for idx in range(len(scenarios), nrows * ncols):
        axes[idx // ncols][idx % ncols].axis("off")
    fig.suptitle("Storage at the workflow-level points, as growth over the after-setup size")
    fig.text(0.5, 0.005, "after cleanup = the run's database dropped (and purged on Dolt); a server-scope backend "
             "keeps its directory, so its cleanup bar is not a leak", ha="center", fontsize=8)
    fig.tight_layout(rect=(0, 0.03, 1, 0.97))
    fig.savefig(os.path.join(outdir, "storage_points.png"), dpi=150)
    plt.close(fig)


# ── Summary table ─────────────────────────────────────────────────────


def write_summary(runs: list[Run], outdir: str) -> pd.DataFrame:
    df = pd.DataFrame([summary_row(r) for r in runs])
    df.to_csv(_data_path(outdir, "summary.csv"), index=False)
    lines = ["# Macrobench summary", ""]
    head = ["scenario", "backend", "status", "elapsed_sec", "setup_sec", "workflow_supported",
            "unsupported_ops", "invariants_passed", "invariants_failed", "invariants_na"]
    lines.append("| " + " | ".join(head) + " |")
    lines.append("|" + "---|" * len(head))
    for _, row in df.iterrows():
        lines.append("| " + " | ".join("" if pd.isna(row.get(c)) else str(row.get(c)) for c in head) + " |")
    lines += ["", "## Median latency per agent operation (ms, spine load excluded)", ""]
    lat_cols = [c for c in df.columns if c.startswith("p50_")]
    head2 = ["scenario", "backend"] + [c[4:-3] for c in lat_cols]
    lines.append("| " + " | ".join(head2) + " |")
    lines.append("|" + "---|" * len(head2))
    for _, row in df.iterrows():
        vals = [row["scenario"], row["backend"]] + ["" if pd.isna(row.get(c)) else f"{row[c]:.2f}" for c in lat_cols]
        lines.append("| " + " | ".join(str(v) for v in vals) + " |")
    spine_cols = ["scenario", "backend", "spine_clients", "spine_txns", "spine_txn_per_sec", "spine_failures"] \
        + [c for c in df.columns if c.startswith("spine_p50_")]
    if "spine_txns" in df.columns:
        lines += ["", "## Spine load (background TPC-C/CH traffic, runs until the scenario ends)", ""]
        lines.append("| " + " | ".join(c.replace("spine_", "") for c in spine_cols) + " |")
        lines.append("|" + "---|" * len(spine_cols))
        for _, row in df[df["spine_txns"].notna()].iterrows():
            lines.append("| " + " | ".join("" if pd.isna(row.get(c)) else str(row.get(c)) for c in spine_cols) + " |")
    lines += ["", "exec() latency per script label, split by statement type, is in "
              "exec_by_label.md and exec_time_by_op.png.", ""]
    lines += ["", "## Scenario metrics", ""]
    for r in runs:
        metrics = r.stats.get("metrics") or {}
        if metrics:
            items = ", ".join(f"{k}={v}" for k, v in metrics.items() if isinstance(v, (int, float, str, bool)))
            lines.append(f"- **{r.scenario} / {r.backend}**: {items}")
    inv_fail = [(r, x) for r in runs for x in (r.stats.get("invariants", {}).get("results") or [])
                if x.get("passed") is False]
    if inv_fail:
        lines += ["", "## Failed invariants", ""]
        for r, x in inv_fail:
            lines.append(f"- {r.scenario} / {r.backend}: {x.get('name')}: {x.get('detail', '')}")
    with open(_data_path(outdir, "summary.md"), "w") as f:
        f.write("\n".join(lines) + "\n")
    return df


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data-dir", action="append", required=True,
                    help="directory with <run_id>_e2e_stats.json/<run_id>.parquet (repeatable, searched recursively)")
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--backends", nargs="*", help="only these backends (names as in the stats, e.g. DOLT NEON)")
    ap.add_argument("--scenarios", nargs="*", help="only these scenarios (rl_env, context_mgmt, ...)")
    ap.add_argument("--run-glob", default="*", help="glob on the run_id, e.g. 'macro_*_mini_*'")
    ap.add_argument("--all-runs", action="store_true",
                    help="keep every run instead of the newest per (scenario, backend)")
    args = ap.parse_args()

    runs = load_runs(args.data_dir, [b.upper() for b in args.backends] if args.backends else None,
                     args.scenarios, args.run_glob)
    if not args.all_runs:
        runs = dedupe_latest(runs)
    os.makedirs(args.outdir, exist_ok=True)

    df = write_summary(runs, args.outdir)
    plot_progress(runs, args.outdir)
    plot_time_breakdown(runs, args.outdir)
    plot_latency_by_op(runs, args.outdir)
    write_latency_by_op(runs, args.outdir)
    write_data_ops_table(runs, args.outdir)
    plot_exec_by_label(runs, args.outdir)
    plot_cross_branch(runs, args.outdir)
    plot_exec_time_by_op(runs, args.outdir)
    write_exec_by_label(runs, args.outdir)
    plot_latency_cdf(runs, args.outdir)
    plot_storage(runs, args.outdir)

    cols = ["scenario", "backend", "status", "elapsed_sec", "workflow_supported",
            "invariants_passed", "invariants_failed"]
    print(df[[c for c in cols if c in df.columns]].to_string(index=False))
    print(f"\nwrote {len(runs)} run(s) to {args.outdir}: progress.png, time_breakdown.png, "
          f"latency_by_op_branch.png, latency_by_op_data.png, latency_exec_by_label.png, "
          f"exec_time_by_op.png, cdf/latency_cdf_<scenario>.png, "
          f"data/{{summary,latency_by_op,data_ops,exec_by_label}}.md/.csv"
          + (", storage.png" if os.path.exists(os.path.join(args.outdir, 'storage.png')) else ""))


if __name__ == "__main__":
    main()
