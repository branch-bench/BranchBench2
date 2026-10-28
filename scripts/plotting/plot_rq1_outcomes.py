"""RQ1: does each scenario finish correctly, with the adapters' simulated
operations on and with native operations only?

A cell fails when the run failed an invariant check, reported an
operation as unsupported, or did not complete. Passing cells show the
number of checks; failing cells show what failed and the run's dominant
error, so the figure carries a short explanation of every failure.

Panel (b) uses the native-only runs in --native-dir. Without them it
still has two kinds of cells: a backend whose every verb is native runs
the same code in both modes, so its default run stands in; for the
others, the verbs the scenario used that the engine does not provide are
listed as the expected first unsupported operations (hatched, "expected")
until the native-only sweep replaces them.

    python scripts/plotting/plot_rq1_outcomes.py --run-dir run_stats/macro_20261014_1615 \\
        --outdir figures/macro_20261014_1615 [--native-dir run_stats/<native-only sweep>]
"""

import argparse
import glob
import json
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

import matplotlib  # noqa: E402
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

from plot_macro_cost import NAMES, SCENARIOS, _systems, load  # noqa: E402

VERBS = ["BRANCH", "COMMIT", "DIFF", "SCHEMA_DIFF", "LOG", "MERGE", "REBASE", "RESET", "REVERT", "DELETE"]
# In native-only mode these adapters switch to the engine's own primitive
# instead of reporting the verb unsupported (dblib/systems/seekdb/adapter.py,
# dblib/systems/matrixone/adapter.py).
NATIVE_SWITCH = {("SEEKDB", "merge"), ("MATRIXONE", "reset")}
PASS, FAIL, NONE = "#d4ecd9", "#f6cdc8", "#f2f2f2"
INK, MUTED = "#1f1f1f", "#6b6b6b"
# Short names for the errors behind failed operations.
ERROR_KEYS = [("duplicate key", "duplicate key"), ("deadlock", "deadlock"), ("cast from type", "cast error"),
              ("serialization", "serialization failure"), ("LSN is newer", "LSN error"), ("timeout", "timeout")]
# Short names for the reasons an adapter gives for an unsupported operation.
UNSUPPORTED_KEYS = [("dropped since the target", "schema changed since\nthe target commit")]


def _runs(run_dir):
    out = {}
    for f in sorted(glob.glob(os.path.join(run_dir, "*", "*_e2e_stats.json"))):
        d = json.load(open(f))
        d["_parquet"] = f.replace("_e2e_stats.json", ".parquet")
        out[(d["backend"], d["scenario"])] = d
    return out


def _dominant_error(d):
    """Most common error among the run's failed agent-side operations."""
    df = pd.read_parquet(d["_parquet"], columns=["label", "status", "error_message"])
    df = df[(df.status == "FAILED") & ~df.label.isin(["spine", "spine_update", "invariant"])]
    if df.empty:
        return ""
    msg = df.error_message.str[:300].value_counts().index[0]
    for key, short in ERROR_KEYS:
        if key.lower() in msg.lower():
            return short
    return msg.split(":")[0].rstrip("_")


def _measured(d):
    """(text, colour) for one run."""
    inv = d.get("invariants") or {}
    passed, failed, na = inv.get("passed", 0), inv.get("failed", 0), inv.get("not_applicable", 0)
    uns = d.get("unsupported_ops") or []
    incomplete = not str(d.get("status", "")).startswith("completed")
    if not (failed or uns or incomplete or d.get("workflow_supported") is False):
        return f"✓ {passed}" + (f" (+{na} n/a)" if na else ""), PASS
    lines = []
    if uns:
        verbs = sorted({str(u).split(":")[0].lower() for u in uns})
        lines.append("unsupported: " + ", ".join(verbs[:2]))
        reason = " ".join(str(u) for u in uns)
        lines += [short for key, short in UNSUPPORTED_KEYS if key in reason][:1]
    if failed:
        lines.append(f"✗ {failed} of {passed + failed} checks")
    if incomplete:
        lines.append(str(d["status"]).split(":")[0])
    err = _dominant_error(d)
    if err and (failed or incomplete):
        lines.append(err)
    return "\n".join(lines), FAIL


def _expected(d):
    """Verbs the scenario used that the engine does not provide natively."""
    impl = (d.get("implementation") or {}).get("verbs") or {}
    counts = d.get("op_status_counts") or {}
    used = [v.lower() for v in VERBS if sum(counts.get(v, {}).values()) > 0]
    missing = [v for v in used if not str(impl.get(v, "native")).startswith("native")
               and (d["backend"], v) not in NATIVE_SWITCH]
    return missing


def _panel(ax, systems, cell, title):
    cols = [s for s, _ in SCENARIOS]
    for i, b in enumerate(systems):
        for j, s in enumerate(cols):
            txt, col, hatch = cell.get((b, s), ("no run", NONE, False))
            ax.add_patch(plt.Rectangle((j, i), 1, 1, color=col, ec="white", lw=2))
            if hatch:
                ax.add_patch(plt.Rectangle((j, i), 1, 1, fill=False, hatch="////", ec=(1, 1, 1, 0.9), lw=0))
            ax.text(j + 0.5, i + 0.5, txt, ha="center", va="center", fontsize=6.6, color=INK,
                    style="italic" if hatch else "normal", linespacing=1.15)
    ax.set_xlim(0, len(cols)); ax.set_ylim(len(systems), 0)
    ax.set_xticks([j + 0.5 for j in range(len(cols))])
    ax.set_xticklabels([lab.replace("\n", " ") for _, lab in SCENARIOS], fontsize=7.5)
    ax.xaxis.tick_top()
    ax.tick_params(length=0)
    for sp in ax.spines.values():
        sp.set_visible(False)
    ax.set_title(title, fontsize=9.5, loc="left", pad=18)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--run-dir", required=True, help="default (simulated) sweep")
    ap.add_argument("--native-dir", help="native-only sweep (run_macro_all.sh --native-only)")
    ap.add_argument("--outdir", required=True)
    a = ap.parse_args()
    os.makedirs(a.outdir, exist_ok=True)

    _, create_ms = load(a.run_dir)
    systems = _systems(create_ms)
    sim, nat = _runs(a.run_dir), (_runs(a.native_dir) if a.native_dir else {})

    left, right = {}, {}
    for (be, sc), d in sim.items():
        b = NAMES.get(be, be)
        txt, col = _measured(d)
        left[(b, sc)] = (txt, col, False)
        if (be, sc) in nat:
            txt, col = _measured(nat[(be, sc)])
            right[(b, sc)] = (txt, col, False)
        elif not _expected(d) and all(str(v).startswith("native")
                                      for v in ((d.get("implementation") or {}).get("verbs") or {}).values()):
            right[(b, sc)] = (txt + "\n(same run)", col, False)
        else:
            missing = _expected(d)
            if "branch" in missing:
                right[(b, sc)] = ("expected\nunsupported:\nno native branch", FAIL, True)
            elif missing:
                verbs = [", ".join(missing[k:k + 2]) for k in range(0, min(len(missing), 4), 2)]
                right[(b, sc)] = ("expected\nunsupported:\n" + "\n".join(verbs), FAIL, True)
            else:
                right[(b, sc)] = ("expected to run\n(not yet run)", NONE, True)

    fig, axes = plt.subplots(1, 2, figsize=(13.5, 0.62 * len(systems) + 1.4), gridspec_kw={"wspace": 0.04})
    _panel(axes[0], systems, left, "(a) With the adapters' simulated operations")
    _panel(axes[1], systems, right, "(b) Native operations only")
    axes[0].set_yticks([i + 0.5 for i in range(len(systems))])
    axes[0].set_yticklabels(systems, fontsize=8.5, color=INK)
    axes[1].set_yticks([])
    handles = [plt.Rectangle((0, 0), 1, 1, color=PASS), plt.Rectangle((0, 0), 1, 1, color=FAIL),
               plt.Rectangle((0, 0), 1, 1, fc=FAIL, hatch="////", ec="white")]
    fig.legend(handles, ["finished, every check passed (count)", "failed: a check failed or an operation was unsupported",
                         "expected from the verb coverage; native-only run pending"],
               ncol=3, fontsize=7.5, frameon=False, loc="lower center", bbox_to_anchor=(0.5, -0.03))
    out = os.path.join(a.outdir, "rq1_outcomes.png")
    fig.savefig(out, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print("wrote", out)


if __name__ == "__main__":
    main()
