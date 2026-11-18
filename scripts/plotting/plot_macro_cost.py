"""Cost figures of a macrobench sweep, all from one pass over the run
directory: per run, the summed latency of every agent-side operation by
category, the operation counts, the peak worker concurrency, the elapsed
time and the invariant failures; per backend, the pooled median
branch-creation latency that orders the systems in every figure.

    python scripts/plotting/plot_macro_cost.py --run-dir run_stats/macro_20261014_curated \
        --outdir figures/macro_20261014 [--figures rank bars panels] [--log]

Figures (all by default):

* ``rank``   rank_inversion.png: (a) rank by branch-creation latency against
             rank by end-to-end elapsed per scenario, (b) share of operation
             time per category.
* ``bars``   cost_bars.png (S1, S3, S4, S6) and cost_bars_all.png (S1 to S6):
             per scenario, worker-seconds in branch operations, in data
             operations and the elapsed, as bars with the operation counts.
             A run that ended at an unsupported operation, failed an invariant
             check or did not complete gets no bar: an X and the reason
             stand in its row, since its cost is not that of the workflow.
* ``panels`` cost_panels.png: the same three quantities as one marker per
             scenario on a shared row axis.

Spine load and invariant probes are excluded everywhere; EXEC rows are
excluded because their statements are rows of their own.
"""

import argparse
import glob
import json
import os

import matplotlib
matplotlib.use("Agg")
matplotlib.rcParams["pdf.fonttype"] = 42  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from macrobench.scenarios import SCENARIOS as _REGISTRY, listed_keys  # noqa: E402

# Scenario labels, markers and groups come from the scenario classes
# (macrobench/scenarios), in registry order; examples are left out.
_KEYS = listed_keys()
SCENARIOS = [(k, _REGISTRY[k].plot_label) for k in _KEYS]
TITLES = {k: _REGISTRY[k].title for k in _KEYS}
MARKERS = {k: tuple(_REGISTRY[k].marker) for k in _KEYS}
FOUR = ["rl_env", "multi_agent", "dev_agent", "data_agent"]
NAMES = {"CHRONOS": "Chronos", "DOLT": "Doltgres", "DOLT_MYSQL": "Dolt", "FILE_COPY": "file_copy",
         "MATRIXONE": "MatrixOne", "NEON": "Neon", "SEEKDB": "SeekDB"}
COLORS = {"Chronos": "#8c6d31", "Doltgres": "#1f77b4", "Dolt": "#6baed6", "file_copy": "#7f7f7f",
          "MatrixOne": "#d62728", "Neon": "#2ca02c", "SeekDB": "#9467bd"}
CATS = [("create", "branch create"), ("delete", "branch delete"), ("commit", "commit"),
        ("reconcile", "diff / merge / rebase"), ("history", "reset / revert"), ("data", "ordinary data ops")]
CAT_COLORS = {"create": "#f4a261", "delete": "#e76f51", "commit": "#e9c46a",
              "reconcile": "#264653", "history": "#2a9d8f", "data": "#d9d9d9"}
# Branch create includes the connection switch that reaches a branch
# (CONNECT), which on Neon is a new connection per branch.
OP_CAT = {"BRANCH": "create", "CONNECT": "create", "DELETE": "delete", "COMMIT": "commit",
          "DIFF": "reconcile", "SCHEMA_DIFF": "reconcile", "MERGE": "reconcile", "REBASE": "reconcile",
          "RESET": "history", "REVERT": "history", "LOG": "history"}
BRANCH_CATS = frozenset(c for c, _ in CATS if c != "data")
RED = "#b00020"


def load(run_dir):
    """One row per run: scenario, backend, elapsed, fails, status,
    unsupported (the first unsupported operation, or ""), invalid (why the run's
    cost is not comparable, or ""), the summed latency per category
    (columns named as in CATS), branch_s / data_s and branch_n / data_n
    (branch categories against the rest), and threads (98th percentile of
    distinct threads per second). Second value: the pooled median
    branch-creation latency per backend in ms."""
    rows, create = [], {}
    for f in sorted(glob.glob(os.path.join(run_dir, "*", "*_e2e_stats.json"))):
        d = json.load(open(f))
        be = NAMES.get(d["backend"], d["backend"])
        fails = sum(1 for c in d["invariants"]["results"] if c["passed"] is False)
        df = pd.read_parquet(f.replace("_e2e_stats.json", ".parquet"),
                             columns=["op_name", "status", "label", "latency", "thread_id", "start_time"])
        # The unsupported operations with the scenario step that issued them,
        # e.g. "BRANCH (investigation_branch)", in order of first appearance.
        uns = df[df.status == "UNSUPPORTED"][["op_name", "label"]].drop_duplicates()
        uns_verbs = [f"{o} ({l})" if l else o for o, l in zip(uns.op_name, uns.label)]
        spine_s = float(df.latency[df.label.isin(["spine", "spine_update"]) & (df.op_name != "EXEC")].sum())
        df = df[~df.label.isin(["spine", "spine_update", "invariant"]) & (df.op_name != "EXEC")]
        cat = df.op_name.map(lambda o: OP_CAT.get(o, "data"))
        secs = df.latency.groupby(cat).sum().to_dict()
        is_branch = cat.isin(BRANCH_CATS)
        sec = ((df.start_time - df.start_time.min()) // 1).astype(int)
        peak = int(df.assign(sec=sec).groupby("sec").thread_id.nunique().quantile(0.98))
        create.setdefault(be, []).extend(df.latency[(df.op_name == "BRANCH") & (df.status == "OK")].tolist())
        # Why the run's cost is not the workflow's: it ended at (or, in
        # older runs, skipped) an unsupported operation, failed a check, or did
        # not complete (interrupted, crashed, incomplete).
        status = str(d.get("status", "completed"))
        ops = d.get("unsupported_ops") or []
        unsupported = ""
        if ops or d.get("workflow_supported") is False or status.startswith("unsupported"):
            unsupported = ", ".join(uns_verbs) or ", ".join(sorted({o.split(":")[0] for o in ops})) \
                or status.split(":", 1)[-1].split(":")[0].strip() or "?"
        if unsupported:
            invalid = f"unsupported: {unsupported}"
        elif fails:
            invalid = f"{fails} check{'s' if fails != 1 else ''} failed"
        elif status != "completed":
            invalid = status.split(":")[0]
        else:
            invalid = ""
        row = {"scenario": d["scenario"], "backend": be, "elapsed": d["elapsed_sec"], "fails": fails,
               "status": status, "unsupported": unsupported, "invalid": invalid,
               "branch_s": float(df.latency[is_branch].sum()), "data_s": float(df.latency[~is_branch].sum()),
               "branch_n": int(is_branch.sum()), "data_n": int((~is_branch).sum()), "threads": peak,
               "spine_s": spine_s}
        for c, _ in CATS:
            row[c] = float(secs.get(c, 0.0))
        rows.append(row)
    # A backend with no successful branch creation (native-only run of a
    # simulated adapter) gets NaN and sorts last.
    return pd.DataFrame(rows), {b: 1000 * pd.Series(v).median() if v else float("nan") for b, v in create.items()}


def _hatch_color(c):
    """A darker shade of a bar colour, used as the edge colour so the hatch
    stays visible without a black outline (hatches take the edge colour)."""
    import matplotlib.colors as mc
    r, g, b = mc.to_rgb(c)
    return (r * 0.6, g * 0.6, b * 0.6)


def _systems(create_ms):
    return sorted(create_ms, key=lambda b: (np.isnan(create_ms[b]), create_ms[b]))


def _ms(v):
    return "no branch" if np.isnan(v) else f"{v:,.0f} ms"


def _tidy(ax):
    ax.grid(axis="x", color="#eee"); ax.set_axisbelow(True)
    for sp in ("top", "right"):
        ax.spines[sp].set_visible(False)


def _row_labels(ax, systems, create_ms, size=8.5, bold=False):
    ax.set_yticks(range(len(systems)))
    ax.set_yticklabels([f"{b}  {_ms(create_ms[b])}" for b in systems], fontsize=size)
    for lab, b in zip(ax.get_yticklabels(), systems):
        lab.set_color(COLORS[b])
        if bold:
            lab.set_fontweight("bold")
    ax.tick_params(axis="y", length=0)


# --- rank_inversion.png -----------------------------------------------------

def panel_rank(ax, t, create_ms):
    """Rank by branch-creation latency (left) against rank by end-to-end
    elapsed in every scenario (one column per scenario)."""
    systems = _systems(create_ms)
    cols = [s for s, _ in SCENARIOS if (t.scenario == s).any()]
    for b in systems:
        xs, ys = [0], [systems.index(b)]
        for i, s in enumerate(cols):
            d = t[t.scenario == s].sort_values("elapsed").backend.tolist()
            if b in d:
                xs.append(i + 1); ys.append(d.index(b))
        ax.plot(xs, ys, "-o", color=COLORS[b], lw=1.6, ms=5, alpha=0.9)
        ax.annotate(f"{b}  {_ms(create_ms[b])}", (0, systems.index(b)), xytext=(-6, 0), textcoords="offset points",
                    ha="right", va="center", fontsize=8, color=COLORS[b])
    for i, s in enumerate(cols):
        d = t[t.scenario == s].sort_values("elapsed")
        for k, (b, e, f) in enumerate(zip(d.backend, d.elapsed, d.fails)):
            lab = f"{e:,.0f}s" + (f" ✗{int(f)}" if f else "")
            ax.text(i + 1, k + 0.32, lab, ha="center", va="top", fontsize=6.5, color=RED if f else "#444")
    ax.set_xticks(range(len(cols) + 1))
    ax.set_xticklabels(["branch\ncreate"] + [dict(SCENARIOS)[s] for s in cols], fontsize=7)
    ax.set_yticks([])
    ax.set_ylabel("rank (fastest at top)", fontsize=8)
    ax.set_ylim(len(systems) - 0.4, -0.6)
    ax.set_xlim(-0.6, len(cols) + 0.5)
    ax.set_title("(a) Rank by branch-creation latency vs. rank by end-to-end elapsed", fontsize=9.5, loc="left")
    for sp in ("top", "right", "left"):
        ax.spines[sp].set_visible(False)
    ax.grid(axis="x", color="#eee")


def panel_breakdown(ax, t):
    """Stacked share of agent-side operation time per category, one bar per
    (scenario, system); elapsed and invariant failures printed at right."""
    systems = list(COLORS)
    labels, y = [], 0
    for s, title in SCENARIOS:
        d = t[t.scenario == s].set_index("backend")
        if d.empty:
            continue
        present = [x for x in systems if x in d.index]
        for b in present:
            r = d.loc[b]
            total = sum(r[c] for c, _ in CATS) or 1.0
            left = 0.0
            for c, _ in CATS:
                w = r[c] / total
                ax.barh(y, w, left=left, color=CAT_COLORS[c], edgecolor="white", lw=0.4, height=0.8)
                left += w
            lab = f"{r.elapsed:,.0f} s" + (f"  ✗{int(r.fails)}" if r.fails else "")
            ax.text(1.01, y, lab, va="center", fontsize=7, color=RED if r.fails else "#444")
            labels.append((y, b))
            y += 1
        ax.text(-0.16, y - 0.5 - (len(present) - 1) / 2, title.replace("\n", " "),
                ha="right", va="center", fontsize=8, fontweight="bold", transform=ax.get_yaxis_transform())
        y += 0.8
    ax.set_yticks([p for p, _ in labels])
    ax.set_yticklabels([b for _, b in labels], fontsize=7)
    for lab, (_, b) in zip(ax.get_yticklabels(), labels):
        lab.set_color(COLORS[b])
    ax.tick_params(axis="y", length=0, pad=2)
    ax.set_ylim(y - 0.6, -0.6)
    ax.set_xlim(0, 1)
    ax.set_xticks([0, 0.25, 0.5, 0.75, 1])
    ax.set_xticklabels(["0", "25", "50", "75", "100 %"], fontsize=7)
    ax.set_title("(b) Share of agent-side operation time; elapsed and invariant failures (✗) at right", fontsize=9.5, loc="left")
    for sp in ("top", "right", "left"):
        ax.spines[sp].set_visible(False)
    handles = [plt.Rectangle((0, 0), 1, 1, color=CAT_COLORS[c]) for c, _ in CATS]
    ax.legend(handles, [n for _, n in CATS], ncol=3, fontsize=7, loc="lower center", bbox_to_anchor=(0.5, 1.02), frameon=False)


def fig_rank(t, create_ms, out):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 7.5), gridspec_kw={"width_ratios": [1, 1.15], "wspace": 0.55})
    panel_rank(ax1, t, create_ms)
    panel_breakdown(ax2, t)
    fig.suptitle("Branch creation predicts neither the cost nor the correctness of an agent workflow", fontsize=12, x=0.02, ha="left")
    fig.text(0.02, 0.01, "Shares are of summed operation latency (workers run concurrently), spine load excluded; one run per cell.",
             fontsize=7.5, color="#555")
    fig.savefig(out, dpi=150, bbox_inches="tight"); plt.close(fig)
    print("wrote", out)


# --- cost_bars.png ----------------------------------------------------------

BAR_PANELS = [("branch_s", "branch ops: worker-seconds (ops)", "#e8853a"),
              ("data_s", "data ops: worker-seconds (ops)", "#9a9a9a"),
              ("elapsed", "end-to-end elapsed: wall-clock s", "#3b5b7a")]


def _k(n):
    return f"{n/1000:.0f}k" if n >= 10000 else f"{n:,}"


def fig_bars(t, create_ms, scenarios, out, log=False):
    systems = _systems(create_ms)
    n, m = len(systems), len(scenarios)
    fig, axes = plt.subplots(m, 3, figsize=(13.5, 2.05 * m + 0.8), sharey=True,
                             gridspec_kw={"wspace": 0.08, "hspace": 0.55}, squeeze=False)
    for i, sc in enumerate(scenarios):
        d = t[t.scenario == sc].set_index("backend").reindex(systems)
        invalid = d["invalid"].fillna("").astype(str).values
        for j, (col, title, color) in enumerate(BAR_PANELS):
            ax = axes[i, j]
            raw = d[col].values.astype(float)
            missing = np.isnan(raw)
            # No bar for an invalid run: an X and the reason take its place.
            vals = np.where(missing | (invalid != ""), 0.0, np.nan_to_num(raw, nan=0.0))
            ax.barh(range(n), vals, color=color, height=0.68)
            xmax = vals.max() if vals.max() > 0 else 1.0
            counts = d["branch_n"].values if col == "branch_s" else d["data_n"].values if col == "data_s" else None
            for k, v in enumerate(vals):
                if missing[k]:
                    ax.text(xmax * 0.015, k, "no run", va="center", fontsize=7.5, color="#999", style="italic")
                    continue
                if invalid[k]:
                    ax.text(xmax * 0.015, k, f"✗ {invalid[k]}", va="center", fontsize=7.5, color=RED, fontweight="bold")
                    continue
                lab = f"{v:,.0f}"
                if counts is not None:
                    lab += f"  ({_k(counts[k])} ops)"
                ax.text(v * 1.06 if log else v + xmax * 0.015, k, lab, va="center", fontsize=7.5, color="#333")
            if log:
                ax.set_xscale("log"); ax.set_xlim(max(1, vals.min() / 3), xmax * 6)
            else:
                ax.set_xlim(0, xmax * (1.5 if col != "elapsed" else 1.32))
            ax.tick_params(axis="x", labelsize=7); _tidy(ax)
            if i == 0:
                ax.set_title(title, fontsize=10, loc="left")
            if j == 0:
                th = int(d.threads.median())
                ax.set_ylabel(f"{TITLES.get(sc, sc)}\n{th} worker{'s' if th != 1 else ''} at peak", fontsize=9.5,
                              rotation=0, ha="right", va="center", labelpad=8, fontweight="bold")
        axes[i, 0].set_ylim(n - 0.5, -0.5)
    for ax_row in axes:
        _row_labels(ax_row[0], systems, create_ms)
    fig.text(0.01, 0.005, "Systems ordered by branch-creation latency (pooled median beside the name). Worker-seconds = sum of every agent-side operation's "
             "latency across the scenario's concurrent workers, so a column can exceed the wall-clock elapsed; the spine's background load is excluded. "
             "✗ = no comparable cost: the run ended at an unsupported operation, failed invariant checks or did not complete. One run per cell.", fontsize=7.5, color="#555")
    fig.savefig(out, dpi=150, bbox_inches="tight"); plt.close(fig)
    print("wrote", out)


# --- cost_bars_grouped.png --------------------------------------------------

GROUPS = [("Branch Intensive", [k for k in _KEYS if _REGISTRY[k].group == "branch"]),
          ("Data Intensive", [k for k in _KEYS if _REGISTRY[k].group == "data"])]
# One hatch per system so the bars stay apart in black and white.
HATCHES = {"Doltgres": "", "Dolt": "//", "Chronos": "..", "MatrixOne": "xx", "file_copy": "\\\\",
           "SeekDB": "--", "Neon": "++"}
SHORT = {k: _REGISTRY[k].label for k in _KEYS}
# Scenarios whose spine traffic is counted with the data ops (stacked as a
# lighter segment): S5's production phase is the workload itself (the
# scenario drives the spine commits and waits for the load between them),
# whereas S4's and S6's spine load is background traffic.
SPINE_IN_DATA = {k for k in _KEYS if _REGISTRY[k].spine_in_data}
ROWS = [("branch_s", "branch ops\n(worker-sec)"), ("data_s", "data ops\n(worker-sec)"),
        ("elapsed", "elapsed\n(wall-clock s)")]


def _tick_fmt(v, _pos):
    return f"{v/1000:g}k" if v >= 10000 else f"{v:,.0f}"


def _log_fmt(v, _pos):
    return f"{v/1000:g}k" if v >= 1000 else f"{v:g}"


def fig_bars_grouped(t, create_ms, out, log=False, horizontal=False):
    """Scenarios as columns in two groups, metrics as rows, one bar per
    system coloured and hatched as in the legend; a failed run is a bare
    X. Each panel has its own linear y axis; with ``log``, a log axis
    shared along the row instead. ``horizontal`` lays the bars sideways,
    one row per system (fastest brancher on top) with the system names
    down the first panel of each group instead of a legend."""
    from matplotlib.patches import Patch
    from matplotlib.ticker import FuncFormatter, LogLocator, MaxNLocator, NullFormatter
    systems = _systems(create_ms)
    groups = [(name, [sc for sc in g if (t.scenario == sc).any()]) for name, g in GROUPS]
    groups = [(name, g) for name, g in groups if g]
    n = len(systems)
    plt.rcParams["hatch.linewidth"] = 0.4
    # One grid column per scenario plus a narrow spacer between groups.
    widths, cols = [], {}
    for gi, (_, g) in enumerate(groups):
        if gi:
            widths.append(0.35)
        for sc in g:
            cols[sc] = len(widths); widths.append(1.0)
    # Sized and styled like the micro stories figures (plot_micro_stories.py):
    # a 7.2 in wide page figure with small type, so the panels fill the
    # column when the figure is scaled to the text width.
    fig = plt.figure(figsize=(7.4, 3.1 if horizontal else 3.3))
    gs = fig.add_gridspec(len(ROWS), len(widths), width_ratios=widths, wspace=0.14 if log else (0.3 if horizontal else 0.55),
                          hspace=0.38 if horizontal else 0.38, top=0.83 if horizontal else 0.78, bottom=0.07 if horizontal else 0.025,
                          left=0.14 if horizontal else 0.07, right=0.995)
    first_of_group = {g[0] for _, g in groups}
    axes = {}
    for i, (col, lab) in enumerate(ROWS):
        row_max, first = 1.0, None
        for name, g in groups:
            for sc in g:
                ax = fig.add_subplot(gs[i, cols[sc]], sharey=first if log else None)
                first = first or ax
                axes[i, sc] = ax
                d = t[t.scenario == sc].set_index("backend").reindex(systems)
                invalid = d["invalid"].fillna("").astype(str).values
                raw = np.nan_to_num(d[col].values.astype(float), nan=0.0)
                vals = np.where(invalid != "", 0.0, raw)
                spine = np.zeros(n)
                if col == "data_s" and sc in SPINE_IN_DATA and "spine_s" in d:
                    spine = np.where(invalid != "", 0.0, np.nan_to_num(d["spine_s"].values.astype(float), nan=0.0))
                vals = vals + spine          # S5: one bar of agent plus spine traffic
                row_max = max(row_max, vals.max())
                top = vals.max() if vals.max() > 0 else 1.0
                if horizontal:
                    bars = ax.barh(range(n), vals, color=[COLORS[b] for b in systems], height=0.72,
                                   edgecolor="#222", linewidth=0.5)
                    for k in np.flatnonzero(invalid != ""):
                        ax.text(top * 0.03, k, "\u2717", ha="left", va="center", fontsize=7, color=RED, fontweight="bold")
                    ax.set_xlim(0, top * 1.12)
                    ax.xaxis.set_major_locator(MaxNLocator(3)); ax.xaxis.set_major_formatter(FuncFormatter(_tick_fmt))
                    ax.tick_params(axis="x", labelsize=6.5, length=2.5, width=0.5, pad=2)
                    ax.set_ylim(n - 0.4, -0.6)
                    if sc in first_of_group:
                        ax.set_yticks(range(n)); ax.set_yticklabels(systems, fontsize=5.5)
                        for tl, b in zip(ax.get_yticklabels(), systems):
                            tl.set_color(COLORS[b])
                        ax.tick_params(axis="y", length=0, pad=2)
                    else:
                        ax.set_yticks([])
                    ax.grid(axis="x", color="#eee", lw=0.5); ax.set_axisbelow(True)
                else:
                    bars = ax.bar(range(n), vals, bottom=1.0 if log else 0.0, color=[COLORS[b] for b in systems],
                                  width=0.72, edgecolor="#222", linewidth=0.5)
                    for k in np.flatnonzero(invalid != ""):
                        ax.text(k, 1.3 if log else top * 0.02, "\u2717", ha="center", va="bottom", fontsize=7,
                                color=RED, fontweight="bold")
                    if not log:
                        ax.set_ylim(0, top * 1.12)
                        ax.yaxis.set_major_locator(MaxNLocator(4)); ax.yaxis.set_major_formatter(FuncFormatter(_tick_fmt))
                    ax.tick_params(axis="y", labelsize=6.5, length=2.5, width=0.5, pad=2)
                    ax.set_xlim(-0.6, n - 0.4); ax.set_xticks([])
                    ax.grid(axis="y", color="#eee", lw=0.5); ax.set_axisbelow(True)
                for name_sp, sp in ax.spines.items():
                    sp.set_visible(name_sp in ("left", "bottom"))
                    sp.set_linewidth(0.6); sp.set_color("#444")
                if i == 0:
                    ax.set_title(SHORT.get(sc, sc), fontsize=7.5, fontweight="bold", color="#1f1f1f", pad=4)
                if log:
                    ax.tick_params(axis="y", which="minor", length=0)
                    if ax is not first:
                        ax.tick_params(axis="y", which="major", labelleft=False, length=0)
        if log:
            first.set_yscale("log"); first.set_ylim(1.0, row_max * 3)
            first.yaxis.set_major_locator(LogLocator(base=10, numticks=8))
            first.yaxis.set_major_formatter(FuncFormatter(_log_fmt))
            first.yaxis.set_minor_formatter(NullFormatter())
            first.tick_params(axis="y", which="major", labelsize=6.5)
        first.set_ylabel(lab, fontsize=7, fontweight="bold")
        # Same x for every row, whatever its tick width (further out when
        # the system names sit on the axis).
        first.yaxis.set_label_coords(-0.85 if horizontal else -0.6, 0.5)
    # Group headers over their columns, with a rule under each.
    for name, g in groups:
        l, r = axes[0, g[0]].get_position(), axes[0, g[-1]].get_position()
        y = l.y1 + 0.09
        fig.text((l.x0 + r.x1) / 2, y + 0.006, name, ha="center", va="bottom", fontsize=8.5, fontweight="bold")
        fig.add_artist(plt.Line2D([l.x0, r.x1], [y, y], color="#444", lw=0.6, transform=fig.transFigure))
    if not horizontal:   # sideways bars carry the system names on the axis
        handles = [Patch(facecolor=COLORS[b], hatch=HATCHES.get(b, ""), edgecolor="#222", linewidth=0.5,
                         label=b) for b in systems]
        fig.legend(handles=handles, loc="upper center", ncol=n, fontsize=6.5, frameon=False,
                   bbox_to_anchor=(0.5, 1.005), handlelength=1.2, handleheight=1.0, columnspacing=1.0, handletextpad=0.4)
    fig.savefig(out, dpi=300, bbox_inches="tight", pad_inches=0.02)
    fig.savefig(os.path.splitext(out)[0] + ".pdf", bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)
    print("wrote", out, "and .pdf")


# --- cost_panels.png --------------------------------------------------------

def fig_panels(t, create_ms, scenarios, out):
    systems = _systems(create_ms)
    n = len(systems)
    panels = [("branch_s", "time in branch operations (s, summed)"),
              ("data_s", "time in ordinary data operations (s, summed)"),
              ("elapsed", "end-to-end elapsed (s)")]
    fig, axes = plt.subplots(1, 3, figsize=(13, 0.55 * n + 2.2), sharey=True, gridspec_kw={"wspace": 0.08})
    for ax, (col, title) in zip(axes, panels):
        for k in range(n):
            ax.axhline(k, color="#eee", lw=0.8, zorder=0)
        for sc in scenarios:
            marker, color = MARKERS[sc]
            for k, b in enumerate(systems):
                r = t[(t.scenario == sc) & (t.backend == b)]
                if r.empty:
                    continue
                v, why = float(r.iloc[0][col]), str(r.iloc[0]["invalid"] or "")
                if why:
                    # The run's cost is not the workflow's: an X in the
                    # scenario's colour, at its value, instead of a marker.
                    ax.plot(max(v, 1e-3), k, marker="x", ms=7, color=RED, markeredgewidth=1.8, ls="none", zorder=3)
                    continue
                ax.plot(max(v, 1e-3), k, marker=marker, ms=7, color=color, ls="none", zorder=3)
        ax.set_xscale("log"); _tidy(ax)
        ax.set_title(title, fontsize=9.5, loc="left")
        ax.tick_params(axis="x", labelsize=7.5)
    axes[0].set_ylim(n - 0.5, -0.5)
    _row_labels(axes[0], systems, create_ms, bold=True)
    handles = [plt.Line2D([], [], marker=MARKERS[sc][0], color=MARKERS[sc][1], ls="none", ms=7) for sc in scenarios]
    labels = [TITLES[sc].split(" (")[0] for sc in scenarios]
    handles.append(plt.Line2D([], [], marker="x", color=RED, markeredgewidth=1.8, ls="none", ms=7))
    labels.append("no comparable cost (unsupported operation, failed check or incomplete run)")
    fig.legend(handles, labels, ncol=len(labels), fontsize=8, frameon=False, loc="lower center", bbox_to_anchor=(0.5, -0.02))
    fig.text(0.01, -0.08, "Rows ordered by branch-creation latency (pooled median). Summed times add every agent-side operation's latency "
             "over all workers (workers run concurrently, so they exceed wall time); spine load excluded. One run per cell.",
             fontsize=7.5, color="#555")
    fig.savefig(out, dpi=150, bbox_inches="tight"); plt.close(fig)
    print("wrote", out)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--figures", nargs="*", default=["rank", "bars", "panels", "grouped"],
                    choices=["rank", "bars", "panels", "grouped"])
    ap.add_argument("--log", action="store_true", help="log axis on the bars (shared per row when grouped)")
    ap.add_argument("--horizontal", action="store_true", help="grouped figure with sideways bars (cost_bars_grouped_h)")
    ap.add_argument("--spine-scenarios", default=None,
                    help="comma-separated scenarios whose spine traffic is stacked onto the data-ops bar "
                         "(default: ops_agent; '' for none)")
    a = ap.parse_args()
    if a.spine_scenarios is not None:
        SPINE_IN_DATA.clear(); SPINE_IN_DATA.update(x for x in a.spine_scenarios.split(",") if x)
    os.makedirs(a.outdir, exist_ok=True)
    t, create_ms = load(a.run_dir)
    present = [s for s, _ in SCENARIOS if (t.scenario == s).any()]
    four = [s for s in FOUR if s in present]
    if "rank" in a.figures:
        fig_rank(t, create_ms, os.path.join(a.outdir, "rank_inversion.png"))
    if "bars" in a.figures:
        fig_bars(t, create_ms, four, os.path.join(a.outdir, "cost_bars.png"), a.log)
        fig_bars(t, create_ms, present, os.path.join(a.outdir, "cost_bars_all.png"), a.log)
    if "panels" in a.figures:
        fig_panels(t, create_ms, four, os.path.join(a.outdir, "cost_panels.png"))
    if "grouped" in a.figures:
        fig_bars_grouped(t, create_ms, os.path.join(a.outdir, "cost_bars_grouped.png"), a.log)
        if a.horizontal:
            fig_bars_grouped(t, create_ms, os.path.join(a.outdir, "cost_bars_grouped_h.png"), log=a.log, horizontal=True)


if __name__ == "__main__":
    main()
