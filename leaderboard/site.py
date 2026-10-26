"""Assemble the static leaderboard site for one phase into leaderboard/.site/<phase>.

From the repository root:
    python -m leaderboard.site                  # production: only publish.json's official records
    python -m leaderboard.site --phase alpha    # DAPLab AWS staging: all validated runs, no mock rows or wizard
    python -m leaderboard.site --phase local    # development: everything the builders find here
    LEADERBOARD_PHASE=alpha python -m leaderboard.site

The phase is website configuration, never measurement provenance: it decides which
records the generated site carries, and nothing a browser sends can change it.

prod  carries the micro results and the Full campaigns that publish.json names, the
      raw JSON of those records, index.html, styles.css, the page scripts and the six
      workflow diagram PNGs. No mock, quick, unvalidated or unselected record, no wizard, no README.
alpha carries what build() and build_macro.build() find (every validated micro result
      and macro campaign on disk, including mini smoke campaigns), architecture.html,
      the README, the hardware requirements and all raw records, without mock rows or
      the local macOS wizard.
local carries what build() and build_macro.build() find: the newest micro result per
      system and machine (an invalid or failed newest file is listed as excluded), every
      campaign that passes validation, the wizard, the README and the hardware
      requirements, plus the rows of leaderboard.mock, the one module that knows about
      mock data.

All phases use the same page and the same ingestion; this module only selects and
copies. Output goes only to <board>/.site/<phase>: it is prepared in a staging
directory next to it and swapped in whole, so a failed build leaves the previous
output in place, and no symlink is ever followed on the way in or out.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
import tempfile
from pathlib import Path

from leaderboard import build, build_macro, mock, verbs as verbs_lane
from leaderboard.connector import system_catalog, system_dir
from leaderboard.validate import BOARD, MACHINE_RE, validate
from leaderboard.validate_macro import MACRO_SUITE, OUTCOMES, SCENARIOS, VERBS

PHASES = ("prod", "alpha", "local")
SITE = BOARD / ".site"
# The page script, in load order: plain scripts share one global scope and, unlike ES modules, still run from a file:// preview.
SCRIPTS = tuple(f"js/{name}.js" for name in ("dom", "details", "scenes", "micro", "macro", "app"))
PAGES = {
    "prod": ("index.html", "styles.css", *SCRIPTS),
    "alpha": ("index.html", "architecture.html", "styles.css", "README.md", "requirements/README.md", *SCRIPTS),
    "local": ("index.html", "run.html", "architecture.html", "styles.css", "README.md", "requirements/README.md", *SCRIPTS),
}
# The six agent-workflow process maps, rendered from workflow-diagrams/source.html, which itself is a build input and is never published.
WORKFLOW_DIAGRAMS = tuple(f"workflow-diagrams/{key}.png" for key in ("rl_env", "context_mgmt", "multi_agent", "dev_agent", "ops_agent", "data_agent"))
MICRO_RE = re.compile(r"[a-z0-9_]+/results/\d{8}/" + MACHINE_RE.pattern + r"\.json")
MACRO_RE = re.compile(r"[a-z0-9_]+/macro/\d{8}/campaign_[0-9a-f]{8}\.json")
VERBS_RE = verbs_lane.VERBS_RE
SUITE_FIELDS = {"suite": str, "seed": int, "tries": int, "table": str, "range_size": int, "chain_length": int, "dataset": dict, "rows": list}


class SiteError(ValueError):
    """The site cannot be assembled as asked; the message says what to change."""


def phase(value: str | None = None, env: dict = os.environ) -> str:
    """The phase from the CLI value, else $LEADERBOARD_PHASE, else prod."""
    chosen = value or env.get("LEADERBOARD_PHASE") or "prod"
    if chosen not in PHASES:
        raise SiteError(f"LEADERBOARD_PHASE (or --phase) must be prod, alpha or local, not {chosen!r}: "
                        "prod publishes only the official records in leaderboard/publish.json; "
                        "alpha stages every validated run on this machine without mock rows or the local wizard; "
                        "local builds the development preview from everything on this machine")
    return chosen


def plain_file(board: Path, rel: str) -> Path:
    """<board>/<rel> as a regular file reached without any symlink below the board."""
    path = board / rel
    for part in [path, *path.parents]:
        if part == board:
            break
        if part.is_symlink():
            raise SiteError(f"{rel}: {part.relative_to(board)} is a symlink; only regular files under {board} are used")
    if not path.is_file() or path.resolve() != board / rel:
        raise SiteError(f"{rel}: not a regular file under {board}")
    return path


def read_json(board: Path, rel: str, what: str) -> dict:
    path = plain_file(board, rel)
    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError) as e:
        raise SiteError(f"{rel}: cannot read the {what} as JSON: {e}") from None
    if not isinstance(data, dict):
        raise SiteError(f"{rel}: the {what} must be a JSON object")
    return data


def read_suite(board: Path) -> dict:
    """suite.json as the page needs it; the microbench validator checks results against it."""
    suite = read_json(board, "suite.json", "suite")
    if (any(not isinstance(suite.get(k), t) or isinstance(suite.get(k), bool) for k, t in SUITE_FIELDS.items())
            or not isinstance(suite["dataset"].get("row_counts"), dict) or not isinstance(suite["dataset"].get("sha256"), str)
            or not isinstance(suite["dataset"].get("path"), str) or suite["table"] not in suite["dataset"]["row_counts"]
            or not all(isinstance(r, dict) and isinstance(r.get("op"), str) and isinstance(r.get("batch"), int) for r in suite["rows"])):
        raise SiteError("suite.json must hold suite, seed, tries, table, range_size, chain_length, rows of {op, batch} "
                        "and a dataset with path, sha256 and row_counts that include the table")
    return suite


def read_manifest(board: Path) -> dict[str, list[str]]:
    """publish.json: exact relative paths of official micro results, macro campaign
    records, and optional verbs scaling records. Shape and path syntax are checked here;
    what the paths point at is checked by published()."""
    data = read_json(board, "publish.json", "publication manifest")
    keys = set(data)
    if not ({"micro", "macro"} <= keys <= {"micro", "macro", "verbs"}) or not all(
        isinstance(v, list) and all(isinstance(s, str) for s in v) for v in data.values()
    ):
        raise SiteError("publish.json must be an object with lists of strings for micro and macro (and optionally verbs)")
    data = {**data, "verbs": list(data.get("verbs", []))}
    for kind, pattern, shape in (("micro", MICRO_RE, "<system>/results/<YYYYMMDD>/<machine>.json"),
                                 ("macro", MACRO_RE, "<system>/macro/<YYYYMMDD>/campaign_<id>.json"),
                                 ("verbs", VERBS_RE, "<system>/verbs/<YYYYMMDD>/bb_<id>.json")):
        for entry in data[kind]:
            if not pattern.fullmatch(entry):
                raise SiteError(f"publish.json: {kind} entry {entry!r} is not a relative {shape} path")
        if len(set(data[kind])) != len(data[kind]):
            raise SiteError(f"publish.json: {kind} lists a record twice")
    return data


def published_verbs(board: Path, manifest: dict[str, list[str]]) -> tuple[list[dict], list[dict]]:
    selected = list(manifest.get("verbs", []))
    for rel in selected:
        plain_file(board, rel)
    for rel in selected:
        if problems := verbs_lane.validate_verbs(board / rel, board):
            raise SiteError(f"{rel}: invalid: {'; '.join(problems)}; an invalid verbs record is never published")
    if not selected:
        return [], []
    entries, excluded = verbs_lane.build_verbs(board, sources=set(selected))
    if len({e.get("execution_profile", "local_single_host") for e in entries}) > 1:
        raise SiteError(
            "publish.json: verbs records mix different execution_profile values; official verbs results must share one execution profile"
        )
    return entries, excluded


def published(board: Path, manifest: dict[str, list[str]]) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    """The manifest's records out of the shared builders. Every selected micro file
    must validate; among the selected files of a system and machine the newest is
    shown, so an unpublished newer run never supersedes an official one, while a
    newer selected error record still supersedes an older selected success. Only
    selected campaign records are heads, each named once, and each must pass the
    builder's validation and be a full run."""
    for rel in manifest["micro"] + manifest["macro"] + manifest.get("verbs", []):
        plain_file(board, rel)
    for rel in manifest["micro"]:
        if problems := validate(board / rel, board):
            raise SiteError(f"{rel}: invalid: {'; '.join(problems)}; an invalid record is never published")
    micro, excluded = build.build(board, sources=set(manifest["micro"]))
    for field in ("commit", "lock_sha256", "python"):
        if len({e[field] for e in micro}) > 1:
            raise SiteError(f"publish.json: micro records mix different {field} values; official micro results must share one revision and runtime")
    if len({e.get("execution_profile", "local_single_host") for e in micro}) > 1:
        raise SiteError(
            "publish.json: micro records mix different execution_profile values; official micro results must share one execution profile"
        )
    if len({e["machine"] for e in micro if e.get("hosted") == "no"}) > 1:
        raise SiteError("publish.json: self-hosted micro records mix different machine labels; official self-hosted micro results must share one machine")
    campaigns, macro_excluded = build_macro.build(board, sources=set(manifest["macro"]))
    records = {c["source"]: c for c in campaigns}
    macro = []
    for rel in manifest["macro"]:
        if rel not in records:
            reason = next((x["reason"] for x in macro_excluded if x["source"] == rel), "no valid campaign record")
            raise SiteError(f"{rel}: {reason}; a campaign that fails validation is never published")
        if records[rel]["preset"] != "full":
            raise SiteError(f"{rel}: a {records[rel]['preset']} campaign stays on the machine that ran it and is never published")
        macro.append(records[rel])
    if len({c["group"] for c in macro if c.get("hosted") == "no"}) > 1:
        raise SiteError("publish.json: macro campaigns mix different comparison groups; official macro campaigns must share one comparison group")
    return micro, excluded, macro, []


def everything(board: Path) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    entries, excluded = build.build(board)
    campaigns, macro_excluded = build_macro.build(board)
    return entries, excluded, campaigns, macro_excluded


def raw_files(
    entries: list[dict],
    excluded: list[dict],
    campaigns: list[dict],
    macro_excluded: list[dict],
    phase: str,
    verbs_entries: list[dict] = (),
    verbs_excluded: list[dict] = (),
) -> set[str]:
    """The record files the page links to: in prod only validated records and error
    records, in alpha and local every file the builders saw."""
    files = (
        {e["source"] for e in entries}
        | {x["source"] for x in excluded}
        | {c["source"] for c in campaigns}
        | {v["source"] for v in verbs_entries}
    )
    files |= {a["source"] for c in campaigns for a in c["attempts"].values()}
    if phase != "prod":
        files |= {x["source"] for x in macro_excluded} | {x["source"] for x in verbs_excluded}
    return files


def sidecars(board: Path) -> dict[str, dict]:
    """Every system integration's system.json is a regular file reached without a symlink,
    checked before any shared validator or builder reads it."""
    for path in sorted((board / "systems").glob("*/system.json")):
        plain_file(board, path.relative_to(board).as_posix())
    try:
        return system_catalog(board)
    except ValueError as error:
        raise SiteError(str(error)) from None


def output_dir(board: Path, phase: str) -> Path:
    """<board>/.site/<phase>, refusing a site directory or target that is a symlink or not a directory."""
    site = board / ".site"
    for part in (site, site / phase):
        if part.is_symlink():
            raise SiteError(f"{part} is a symlink; the generated site is written only to a real {site} directory")
        if part.exists() and not part.is_dir():
            raise SiteError(f"{part} exists and is not a directory")
    return site / phase


def assemble(phase: str, board: Path = BOARD) -> Path:
    """Write <board>/.site/<phase> from a complete staging copy and return it. The
    previous output survives any failure; nothing outside .site is ever removed."""
    if phase not in PHASES:
        raise SiteError(f"unknown phase {phase!r}")
    board = board.resolve()
    if not board.is_dir():
        raise SiteError(f"{board} is not a directory")
    target = output_dir(board, phase)
    suite = read_suite(board)
    catalog = sidecars(board)
    runnable = {name: meta for name, meta in catalog.items() if (system_dir(name, board) / "connector.py").is_file()}
    if phase == "prod":
        manifest = read_manifest(board)
        entries, excluded, campaigns, macro_excluded = published(board, manifest)
        verbs_entries, verbs_excluded = published_verbs(board, manifest)
    else:
        entries, excluded, campaigns, macro_excluded = everything(board)
        verbs_entries, verbs_excluded = verbs_lane.build_verbs(board)
    target.parent.mkdir(exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{phase}-", dir=target.parent))
    try:
        macro_suite = {"suite": MACRO_SUITE, "scenarios": SCENARIOS, "outcomes": list(OUTCOMES), "verbs": list(VERBS)}
        js = lambda pairs: "".join(f"const {name} = {json.dumps(value, indent=1)};\n" for name, value in pairs)
        rows = entries + (mock.entries(board) if phase == "local" else [])
        (staging / "data.js").write_text(
            js((
                ("site", {"phase": phase}),
                ("suite", suite),
                ("systems", runnable),
                ("data", rows),
                ("excluded", excluded),
                ("verbs", verbs_entries),
                ("verbsExcluded", verbs_excluded),
            ))
        )
        (staging / "macro-data.js").write_text(js((("macroSuite", macro_suite), ("macroCampaigns", campaigns), ("macroExcluded", macro_excluded))))
        for rel in [*PAGES[phase], *WORKFLOW_DIAGRAMS, *sorted(raw_files(entries, excluded, campaigns, macro_excluded, phase, verbs_entries, verbs_excluded))]:
            (staging / rel).parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(plain_file(board, rel), staging / rel)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    previous = None
    if target.exists():
        previous = Path(tempfile.mkdtemp(prefix=f".old-{phase}-", dir=target.parent))  # named unlike the staging dir, so the rollback rename is distinct from the install rename
        previous.rmdir()
        target.rename(previous)
    try:
        staging.rename(target)
    except BaseException:
        if previous:
            previous.rename(target)  # the previous site returns to its path
        shutil.rmtree(staging, ignore_errors=True)
        raise
    if previous:
        shutil.rmtree(previous)
    return target


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Assemble the static leaderboard site for one phase.")
    parser.add_argument("--phase", choices=PHASES, help="prod (default), alpha or local; $LEADERBOARD_PHASE otherwise")
    args = parser.parse_args(argv)
    try:
        chosen = phase(args.phase)
        target = assemble(chosen)
    except SiteError as e:
        print(f"site: {e}", file=sys.stderr)
        return 2
    data = json.loads(re.search(r"^const data = (.*?);\n^const", (target / "data.js").read_text(), re.S | re.M).group(1))
    campaigns = (target / "macro-data.js").read_text().count('"campaign_id":')
    print(f"{target}: {chosen} site, {len(data)} microbench entries, {campaigns} campaigns")
    return 0


if __name__ == "__main__":
    sys.exit(main())
