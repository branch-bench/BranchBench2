"""The site assembler: phase selection, the publication manifest and the isolated
generated directories. Run with python -m unittest leaderboard.test_site."""

from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from leaderboard import build, build_macro, site
from leaderboard import run as micro
from leaderboard.connector import connector_names, load_system, system_dir, system_identity
from leaderboard.test_macro import CAMPAIGN, SOURCE, attempt_payload, campaign_record, evidence_summary, good_evidence, outcome
from leaderboard.validate import BOARD, SCHEMA_VERSION, sha256
from leaderboard.validate_macro import SCENARIOS

FULL = {**CAMPAIGN, "preset": "full", "id": "0000f0ff", "started": "2026-10-11T10:00:00+00:00"}


class Phase(unittest.TestCase):
    def test_default_env_cli_and_invalid(self):
        self.assertEqual(site.phase(env={}), "prod", "production is the default")
        self.assertEqual(site.phase(env={"LEADERBOARD_PHASE": "local"}), "local")
        self.assertEqual(site.phase(env={"LEADERBOARD_PHASE": "alpha"}), "alpha")
        self.assertEqual(site.phase("local", env={"LEADERBOARD_PHASE": "prod"}), "local", "the CLI wins over the environment")
        self.assertEqual(site.phase("alpha", env={"LEADERBOARD_PHASE": "local"}), "alpha", "the CLI wins over the environment")
        self.assertEqual(site.phase(None, env={"LEADERBOARD_PHASE": ""}), "prod", "an empty variable is the default")
        for bad in ("production", "dev", "LOCAL", "ALPHA", "staging"):
            with self.subTest(bad=bad), self.assertRaisesRegex(site.SiteError, "must be prod, alpha or local, not"):
                site.phase(env={"LEADERBOARD_PHASE": bad})
        with self.assertRaisesRegex(site.SiteError, "'staging'"):
            site.phase("staging")

    def test_site_metadata_never_dirties_a_run(self):
        """The generated directory, the legacy generated data files and the manifest are
        website outputs or configuration: none is in the source snapshot, so promoting
        a run cannot change its identity or comparison group."""
        outputs = ("/.site/", "/publish.json", "leaderboard/data.js", "leaderboard/macro-data.js")
        snapshot = micro.source_snapshot(micro.CODE_PATHS)
        self.assertFalse(any(o in f for f in snapshot["files"] for o in outputs), [f for f in snapshot["files"] if any(o in f for o in outputs)])
        self.assertFalse(any(o.strip("/") in line for line in snapshot["status"].splitlines() for o in outputs))
        self.assertTrue(any(f.endswith("leaderboard/site.py") for f in snapshot["files"]), "the assembler's code is still an input")


class Board(unittest.TestCase):
    """A scratch board with every system folder, suite.json, the mocks and helpers for both lanes."""

    def setUp(self):
        self.board = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.board)
        shutil.copy(BOARD / "suite.json", self.board)
        (self.board / "systems").mkdir()
        for system in ("dolt", "dolt_mysql", "neon", "tiger", "xata"):
            shutil.copytree(system_dir(system), system_dir(system, self.board), ignore=shutil.ignore_patterns("__pycache__"))
        shutil.copytree(BOARD / "mock", self.board / "mock")
        self.suite = json.loads((BOARD / "suite.json").read_text())
        for name in site.PAGES["local"]:
            (self.board / name).parent.mkdir(exist_ok=True)
            (self.board / name).write_text(f"<!-- {name} -->")
        (self.board / "workflow-diagrams").mkdir()
        for rel in site.WORKFLOW_DIAGRAMS:
            shutil.copy(BOARD / rel, self.board / rel)  # the real rendered maps: binary assets the site must carry byte for byte
        self.site = self.board / ".site"
        self.manifest = self.board / "publish.json"
        self.publish({"micro": [], "macro": []})
        self.day = "20261011"

    def publish(self, manifest) -> None:
        self.manifest.write_text(json.dumps(manifest))

    def payload(self, date: str = "2026-09-30", system: str = "dolt", **changes) -> dict:
        meta = system_identity(load_system(system))
        return {"schema_version": SCHEMA_VERSION, "run_id": "0123abcd", **meta, "date": date, "machine": "box",
                "version": "0.54.10", "suite": self.suite["suite"], "suite_sha256": sha256(BOARD / "suite.json"),
                "seed": self.suite["seed"], "commit": "0" * 40, "dirty": False,
                "dataset_sha256": self.suite["dataset"]["sha256"], "python": "3.13.14", "lock_sha256": "0" * 64,
                "provision_time": 0.07, "load_time": 2.3,
                "result": [[0.01] * self.suite["tries"] for _ in self.suite["rows"]], **changes}

    def write(self, payload: dict, day_dir: str = "20260930", system: str = "dolt") -> Path:
        path = self.board / system / "results" / day_dir / "box.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload))
        return path

    def write_macro(self, payload: dict, system: str = "dolt_mysql", day: str = "20261011") -> str:
        stem = f"campaign_{payload['id']}" if "id" in payload else payload["attempt_id"]
        path = self.board / system / "macro" / day / f"{stem}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload))
        return path.relative_to(self.board).as_posix()

    def write_campaign(self, campaign: dict, ids: str = "1", broken: dict | None = None, **record) -> str:
        """A campaign with one attempt per scenario; `broken` maps a scenario to a non-verified outcome."""
        attempts, day = {}, campaign["started"][:10].replace("-", "")
        for i, key in enumerate(campaign["scenarios"]):
            attempt_id = f"bb_{ids}000000{i}"
            attempts[key] = attempt_id
            changes = {}
            if key in (broken or {}):
                changes = {"outcome": outcome("failed", ["2 of 8 work units failed"], 3, 1.5),
                           "evidence": evidence_summary(good_evidence(run_id=attempt_id, scenario=key, elapsed_sec=3, work={"expected": 8, "succeeded": 6, "failed": 2, "skipped": 0}))}
            self.write_macro(attempt_payload(attempt_id=attempt_id, scenario=key, campaign=campaign, date=campaign["started"][:10], **changes), day=day)
        return self.write_macro(campaign_record(**campaign, attempts=attempts, **record), day=day)

    def assemble(self, phase: str) -> Path:
        return site.assemble(phase, self.board)

    @staticmethod
    def read(target: Path) -> dict:
        """Every const of both data files as JSON."""
        out = {}
        for name in ("data.js", "macro-data.js"):
            for chunk in (target / name).read_text().split(";\nconst ")[0:]:
                key, _, value = chunk.removeprefix("const ").partition(" = ")
                out[key.strip()] = json.loads(value.rstrip(";\n"))
        return out

    @staticmethod
    def files(target: Path) -> list[str]:
        return sorted(p.relative_to(target).as_posix() for p in target.rglob("*") if p.is_file())


class Local(Board):
    def test_local_carries_everything_and_prod_carries_nothing_unpublished(self):
        self.write(self.payload())
        self.write_campaign(dict(CAMPAIGN))
        local = self.assemble("local")
        data = self.read(local)
        self.assertEqual(data["site"], {"phase": "local"})
        self.assertEqual(sorted(data["systems"]), connector_names(self.board))
        self.assertEqual([(e["system"], "source" in e) for e in data["data"]], [("Doltgres", True), ("Neon", False), ("Tiger Cloud", False), ("Xata", False)], "mock rows follow the real ones and link to no record")
        self.assertFalse([k for e in data["data"] for k in ("mock", "basis", "note") if k in e], "a mock row looks like any other row")
        self.assertEqual([c["preset"] for c in data["macroCampaigns"]], ["mini"], "a quick campaign is a result on the machine that ran it")
        self.assertIn("run.html", self.files(local)); self.assertIn("README.md", self.files(local)); self.assertIn("requirements/README.md", self.files(local)); self.assertNotIn("mock/neon.json", self.files(local))
        self.assertIn("dolt/results/20260930/box.json", self.files(local))
        self.assertIn("dolt_mysql/macro/20261010/bb_10000000.json", self.files(local))
        prod = self.assemble("prod")
        data = self.read(prod)
        self.assertEqual(data["site"], {"phase": "prod"})
        self.assertEqual((data["data"], data["excluded"], data["macroCampaigns"], data["macroExcluded"]), ([], [], [], []), "an empty manifest publishes nothing")
        self.assertEqual(self.files(prod), sorted(["data.js", "index.html", "macro-data.js", "styles.css", *site.SCRIPTS, *site.WORKFLOW_DIAGRAMS]), "no wizard, README, mock or raw record")
        self.assertEqual(sorted(p.name for p in self.site.iterdir()), ["local", "prod"], "the two phases are isolated directories")
        self.assertTrue((local / "run.html").exists(), "building prod did not touch local")
        alpha = self.assemble("alpha")
        alpha_data = self.read(alpha)
        self.assertEqual(alpha_data["site"], {"phase": "alpha"})
        self.assertEqual([(e["system"], "source" in e) for e in alpha_data["data"]], [("Doltgres", True)], "alpha carries real validated runs without mock rows")
        self.assertEqual([c["preset"] for c in alpha_data["macroCampaigns"]], ["mini"], "alpha carries validated mini and full campaigns without requiring publish.json")
        self.assertNotIn("run.html", self.files(alpha), "alpha omits the macOS-only Terminal launcher")
        self.assertIn("architecture.html", self.files(alpha))
        self.assertIn("README.md", self.files(alpha))
        self.assertIn("requirements/README.md", self.files(alpha))
        self.assertIn("dolt/results/20260930/box.json", self.files(alpha))
        self.assertIn("dolt_mysql/macro/20261010/bb_10000000.json", self.files(alpha))
        self.assertEqual(sorted(p.name for p in self.site.iterdir()), ["alpha", "local", "prod"], "all three phases are isolated directories")

    def test_generated_directory_is_ignored_by_git(self):
        self.assertIn(".site/", (BOARD / ".gitignore").read_text().split())

    def test_the_page_loads_exactly_the_published_scripts_in_order(self):
        """A script the page loads but the site leaves out would 404 in production; the order is the load order."""
        self.assertEqual(tuple(re.findall(r'<script src="(js/[^"]+)"></script>', (BOARD / "index.html").read_text())), site.SCRIPTS)

    def test_both_phases_carry_exactly_the_six_workflow_diagrams(self):
        """The PNGs are copied byte for byte; the HTML they are rendered from and anything
        else dropped into the folder stay out, in both phases."""
        self.assertEqual(len(site.WORKFLOW_DIAGRAMS), 6)
        self.assertEqual([rel.rsplit("/", 1)[1] for rel in site.WORKFLOW_DIAGRAMS], [f"{key}.png" for key in SCENARIOS], "one map per scenario, in suite order")
        (self.board / "workflow-diagrams" / "source.html").write_text("<!doctype html>")
        (self.board / "workflow-diagrams" / "draft.png").write_bytes(b"\x89PNG stray")
        (self.board / "workflow-diagrams" / "rl_env.svg").write_text("<svg/>")
        self.write(self.payload())
        for phase in ("prod", "alpha", "local"):
            target = self.assemble(phase)
            files = self.files(target)
            for rel in site.WORKFLOW_DIAGRAMS:
                self.assertIn(rel, files, phase)
                self.assertEqual((target / rel).read_bytes(), (BOARD / rel).read_bytes(), f"{phase}: {rel} is the real rendering")
                self.assertEqual((target / rel).read_bytes()[:8], b"\x89PNG\r\n\x1a\n")
            self.assertEqual([f for f in files if f.startswith("workflow-diagrams/")], sorted(site.WORKFLOW_DIAGRAMS), f"{phase}: nothing else from the folder is published")

    def test_missing_or_symlinked_workflow_diagram_fails_closed(self):
        self.write(self.payload())
        self.publish({"micro": ["dolt/results/20260930/box.json"], "macro": []})
        prod = self.assemble("prod")
        before = self.files(prod)
        png = self.board / "workflow-diagrams" / "ops_agent.png"
        aside = self.board / "ops_agent-copy.png"; shutil.copy(png, aside); self.addCleanup(aside.unlink)
        png.unlink()
        with self.assertRaisesRegex(site.SiteError, "workflow-diagrams/ops_agent.png: not a regular file"):
            self.assemble("prod")
        png.symlink_to(aside)
        with self.assertRaisesRegex(site.SiteError, "ops_agent.png is a symlink"):
            self.assemble("local")
        png.unlink(); shutil.copy(aside, png)
        folder = self.board / "workflow-diagrams"; real = self.board / "maps-elsewhere"
        folder.rename(real); folder.symlink_to(real, target_is_directory=True)
        with self.assertRaisesRegex(site.SiteError, "workflow-diagrams is a symlink"):
            self.assemble("prod")
        self.assertEqual(self.files(prod), before, "the previous site is intact")
        self.assertEqual([p.name for p in self.site.iterdir()], ["prod"], "no staging directory or local site is left behind")

    def test_output_is_only_the_canonical_generated_directory(self):
        self.write(self.payload())
        local = self.assemble("local")
        self.assertEqual(local, self.board.resolve() / ".site" / "local")
        with self.assertRaisesRegex(TypeError, "positional"):
            site.assemble("local", self.board, self.board / "elsewhere")  # no other destination can be named
        stale = local / "dolt" / "results" / "20200101" / "old.json"
        stale.parent.mkdir(parents=True); stale.write_text("{}")
        self.assemble("local")
        self.assertFalse(stale.exists(), "a stale generated file never survives a rebuild")
        with self.assertRaisesRegex(site.SiteError, "unknown phase"):
            self.assemble("staging")

    def test_symlinked_site_or_target_is_refused_and_nothing_behind_it_removed(self):
        self.write(self.payload())
        elsewhere = self.board / "valuable"; (elsewhere / "prod").mkdir(parents=True)
        precious = elsewhere / "prod" / "precious.csv"; precious.write_text("KEEP_ME")
        self.site.symlink_to(elsewhere, target_is_directory=True)
        with self.assertRaisesRegex(site.SiteError, "is a symlink"):
            self.assemble("prod")
        self.assertEqual(precious.read_text(), "KEEP_ME")
        self.site.unlink(); self.site.mkdir()
        (self.site / "prod").symlink_to(elsewhere / "prod", target_is_directory=True)
        with self.assertRaisesRegex(site.SiteError, "is a symlink"):
            self.assemble("prod")
        self.assertEqual(precious.read_text(), "KEEP_ME")
        (self.site / "prod").unlink(); (self.site / "prod").write_text("not a directory")
        with self.assertRaisesRegex(site.SiteError, "not a directory"):
            self.assemble("prod")

    def test_previous_output_survives_a_failed_build(self):
        self.write(self.payload())
        self.publish({"micro": ["dolt/results/20260930/box.json"], "macro": []})
        prod = self.assemble("prod")
        before = self.files(prod)
        self.assertIn("dolt/results/20260930/box.json", before)
        self.publish({"micro": ["dolt/results/20260930/missing.json"], "macro": []})
        with self.assertRaisesRegex(site.SiteError, "not a regular file"):
            self.assemble("prod")
        self.assertEqual(self.files(prod), before, "the complete previous site is still there")
        self.assertEqual([p.name for p in self.site.iterdir()], ["prod"], "no staging directory is left behind")
        (self.board / "index.html").unlink()
        with self.assertRaisesRegex(site.SiteError, "index.html: not a regular file"):
            self.assemble("local")
        self.assertFalse((self.site / "local").exists())
        self.assertEqual([p.name for p in self.site.iterdir()], ["prod"])
        (self.board / "index.html").write_text("<!-- index.html -->")
        self.publish({"micro": ["dolt/results/20260930/box.json"], "macro": []})
        real, injected = Path.rename, []
        def failing_swap(path, destination):
            # Fail exactly the install of the staging directory, once; the rollback rename of the backup must succeed.
            if not injected and destination == prod and path.name.startswith(".prod-") and not path.name.startswith(".old-"):
                injected.append(path)
                raise OSError("injected swap failure")
            return real(path, destination)
        with mock.patch.object(Path, "rename", failing_swap), self.assertRaisesRegex(OSError, "injected"):
            self.assemble("prod")
        self.assertEqual(len(injected), 1, "the injected failure hit the staging install, not the rollback")
        self.assertEqual(self.files(prod), before, "a failed installation puts the previous site back at its path")
        self.assertEqual([p.name for p in self.site.iterdir()], ["prod"], "no staging or backup directory is left behind")

    def test_system_sidecars_must_be_regular_files(self):
        self.write(self.payload())
        self.publish({"micro": ["dolt/results/20260930/box.json"], "macro": []})
        meta = system_dir("dolt", self.board) / "system.json"; meta.unlink(); meta.symlink_to(system_dir("dolt") / "system.json")
        for phase in ("prod", "alpha", "local"):
            with self.assertRaisesRegex(site.SiteError, "systems/dolt/system.json is a symlink"):
                self.assemble(phase)
        self.assertFalse(self.site.exists() and any(self.site.iterdir()))

    def test_pages_suite_and_raw_files_must_be_regular_files(self):
        secret = self.board.parent / "secret.env"; secret.write_text("PRIVATE"); self.addCleanup(secret.unlink)
        (self.board / "index.html").unlink(); (self.board / "index.html").symlink_to(secret)
        with self.assertRaisesRegex(site.SiteError, "index.html is a symlink"):
            self.assemble("prod")
        (self.board / "index.html").unlink(); (self.board / "index.html").write_text("<!-- index.html -->")
        (self.board / "suite.json").unlink(); (self.board / "suite.json").symlink_to(secret)
        with self.assertRaisesRegex(site.SiteError, "suite.json is a symlink"):
            self.assemble("local")
        (self.board / "suite.json").unlink(); (self.board / "suite.json").write_text(json.dumps({"suite": "x"}))
        with self.assertRaisesRegex(site.SiteError, "suite.json must hold"):
            self.assemble("local")
        (self.board / "suite.json").write_text("[]")
        with self.assertRaisesRegex(site.SiteError, "must be a JSON object"):
            self.assemble("local")
        shutil.copy(BOARD / "suite.json", self.board)
        real = self.write(self.payload())
        link = self.board / "dolt" / "results" / "20261001" / "box.json"; link.parent.mkdir(); link.symlink_to(real)
        with self.assertRaisesRegex(site.SiteError, "box.json is a symlink"):
            self.assemble("local")
        link.unlink(); link.parent.rmdir()
        campaign = dict(FULL)
        rel = self.write_campaign(campaign)
        attempt = self.board / "dolt_mysql" / "macro" / self.day / "bb_10000000.json"
        alias = self.board / "dolt_mysql" / "macro" / "20261012" / "bb_10000000.json"; alias.parent.mkdir()
        alias.symlink_to(attempt)  # resolves to the valid file, so the builder sees it as a second, conflicting attempt
        self.publish({"micro": [], "macro": [rel]})
        with self.assertRaisesRegex(site.SiteError, "2 attempts in one campaign"):
            self.assemble("prod")
        self.assertFalse((self.site / "prod").exists(), "nothing is published when a selected campaign's file is a symlink")
        local = self.assemble("local")
        self.assertIn("2 attempts in one campaign", self.read(local)["macroExcluded"][0]["reason"])
        self.assertNotIn("dolt_mysql/macro/20261012/bb_10000000.json", self.files(local), "a file behind a symlink is never copied")


class Manifest(Board):
    def test_shape_and_path_syntax(self):
        for bad in ("[]", "{}", '{"micro": [], "macro": [], "extra": []}', '{"micro": {}, "macro": []}', '{"micro": [1], "macro": []}', "not json"):
            with self.subTest(bad=bad):
                self.manifest.write_text(bad)
                with self.assertRaises(site.SiteError):
                    self.assemble("prod")
        self.manifest.unlink()
        with self.assertRaisesRegex(site.SiteError, "publish.json: not a regular file"):
            self.assemble("prod")
        aside = self.board.parent / "selected.json"; aside.write_text('{"micro": [], "macro": []}'); self.addCleanup(aside.unlink)
        self.manifest.symlink_to(aside)
        with self.assertRaisesRegex(site.SiteError, "publish.json is a symlink"):
            self.assemble("prod")
        self.manifest.unlink(); self.publish({"micro": [], "macro": []})
        for kind, bad in (("micro", "../suite.json"), ("micro", "/etc/passwd"), ("micro", "dolt/results/20260930/../../../x.json"),
                          ("micro", "dolt/macro/20261010/campaign_0000f0ff.json"), ("micro", "mock/neon.json"), ("micro", "dolt/results/20260930/box.txt"),
                          ("macro", "dolt_mysql/macro/20261011/bb_10000000.json"), ("macro", "dolt_mysql/results/20260930/box.json"),
                          ("macro", "dolt_mysql\\macro\\20261011\\campaign_0000f0ff.json"), ("macro", "Dolt/macro/20261011/campaign_0000f0ff.json")):
            with self.subTest(kind=kind, bad=bad):
                self.publish({"micro": [], "macro": [], kind: [bad]})
                with self.assertRaisesRegex(site.SiteError, "is not a relative"):
                    self.assemble("prod")
        self.publish({"micro": ["dolt/results/20260930/box.json", "dolt/results/20260930/box.json"], "macro": []})
        with self.assertRaisesRegex(site.SiteError, "twice"):
            self.assemble("prod")

    def test_entries_must_be_regular_files_under_the_board(self):
        self.publish({"micro": ["dolt/results/20260930/box.json"], "macro": []})
        with self.assertRaisesRegex(site.SiteError, "not a regular file"):
            self.assemble("prod")
        outside = Path(tempfile.mkdtemp()); self.addCleanup(shutil.rmtree, outside)
        (outside / "box.json").write_text(json.dumps(self.payload()))
        target = self.board / "dolt" / "results" / "20260930" / "box.json"; target.parent.mkdir(parents=True)
        target.symlink_to(outside / "box.json")
        with self.assertRaisesRegex(site.SiteError, "box.json is a symlink"):
            self.assemble("prod")
        target.unlink(); target.parent.rmdir()
        (self.board / "dolt" / "results" / "20260930").symlink_to(outside, target_is_directory=True)
        with self.assertRaisesRegex(site.SiteError, "20260930 is a symlink"):
            self.assemble("prod")

    def test_micro_selection_competes_only_among_selected_records(self):
        """An unpublished newer run never supersedes the official record; among selected
        records the newest wins, so a newer selected error record is shown and the older
        selected success is omitted; an invalid selected record fails closed."""
        official = "dolt/results/20260930/box.json"
        self.write(self.payload())
        self.publish({"micro": [official], "macro": []})
        data = self.read(self.assemble("prod"))
        self.assertEqual([e["source"] for e in data["data"]], [official])
        self.assertTrue(all("source" in e for e in data["data"]), "no mock row stands in for an unpublished system")
        self.assertEqual(self.files(self.site / "prod"), sorted(["data.js", official, "index.html", "macro-data.js", "styles.css", *site.SCRIPTS, *site.WORKFLOW_DIAGRAMS]))
        for newer in ({"error": "newer unpublished failure"}, self.payload(date="2026-10-01")):
            self.write(newer, day_dir="20261001")
            data = self.read(self.assemble("prod"))
            self.assertEqual(([e["source"] for e in data["data"]], data["excluded"]), ([official], []), "an unselected newer file changes nothing")
            self.assertNotIn("dolt/results/20261001/box.json", self.files(self.site / "prod"))
        self.assertEqual(len(site.everything(self.board)[0]), 1, "development still shows the newest file")
        self.write({"error": "server did not start"}, day_dir="20261002")
        self.publish({"micro": [official, "dolt/results/20261002/box.json"], "macro": []})
        data = self.read(self.assemble("prod"))
        self.assertEqual(data["data"], [], "the older selected success is superseded by the newer selected error record")
        self.assertEqual([(x["source"], x["reason"]) for x in data["excluded"]], [("dolt/results/20261002/box.json", "server did not start")], "a published failure stays visible")
        self.assertNotIn(official, self.files(self.site / "prod")); self.assertIn("dolt/results/20261002/box.json", self.files(self.site / "prod"))
        self.write(self.payload(date="2026-10-03", dirty=True), day_dir="20261003")
        self.publish({"micro": ["dolt/results/20261003/box.json"], "macro": []})
        with self.assertRaisesRegex(site.SiteError, "uncommitted changes; an invalid record is never published"):
            self.assemble("prod")
        self.publish({"micro": [official, "dolt/results/20261003/box.json"], "macro": []})
        with self.assertRaisesRegex(site.SiteError, "never published"):
            self.assemble("prod")
        (self.board / "dolt" / "results" / "20260930" / "box.json").write_text("PRIVATE_INVALID_OLDER")
        self.publish({"micro": [official, "dolt/results/20261002/box.json"], "macro": []})
        with self.assertRaisesRegex(site.SiteError, "dolt/results/20260930/box.json: invalid: .*never published"):
            self.assemble("prod")
        self.assertFalse((self.site / "prod" / official).exists(), "an invalid older selected file is validated too and never copied")

    def test_build_source_filter_is_opt_in(self):
        self.write(self.payload())
        self.write({"error": "later"}, day_dir="20261001")
        entries, excluded = build.build(self.board)
        self.assertEqual(([e["source"] for e in entries], [x["source"] for x in excluded]), ([], ["dolt/results/20261001/box.json"]), "the build carries real results only")
        entries, excluded = build.build(self.board, sources={"dolt/results/20260930/box.json"})
        self.assertEqual(([e["source"] for e in entries], excluded), (["dolt/results/20260930/box.json"], []))

    def test_macro_rejects_development_campaigns(self):
        mini = self.write_campaign(dict(CAMPAIGN))
        self.publish({"micro": [], "macro": [mini]})
        with self.assertRaisesRegex(site.SiteError, "a mini campaign stays on the machine that ran it"):
            self.assemble("prod")
        dirty = {**FULL, "id": "0000d1ff", "started": "2026-10-12T10:00:00+00:00"}
        rel = self.write_campaign(dirty, "2", source={**SOURCE, "dirty": True})
        self.publish({"micro": [], "macro": [rel]})
        with self.assertRaisesRegex(site.SiteError, "run from uncommitted sources; a campaign that fails validation is never published"):
            self.assemble("prod")
        broken = self.write_macro({"id": "0000bad0", "record": "campaign"}, day="20261013")
        self.publish({"micro": [], "macro": [broken]})
        with self.assertRaisesRegex(site.SiteError, "invalid campaign record"):
            self.assemble("prod")
        self.publish({"micro": [], "macro": ["dolt_mysql/macro/20261014/campaign_00000000.json"]})
        with self.assertRaisesRegex(site.SiteError, "not a regular file"):
            self.assemble("prod")
        self.assertFalse((self.site / "prod").exists(), "nothing was published")

    def test_published_campaign_keeps_failed_workflows_and_a_campaign_failing_validation_is_refused(self):
        """A failed workflow is a result and is published as one; a campaign whose record fails
        validation (an abort, a duplicate attempt, an invalid named attempt) is never published."""
        campaign = dict(FULL)
        rel = self.write_campaign(campaign, broken={"ops_agent": True})
        day = "20261011"
        record = json.loads((self.board / rel).read_text())
        self.publish({"micro": [], "macro": [rel]})
        data = self.read(self.assemble("prod"))
        [c] = data["macroCampaigns"]
        self.assertEqual(c["preset"], "full")
        self.assertEqual(c["attempts"]["ops_agent"]["outcome"]["status"], "failed", "a failed attempt is published as failed")
        self.assertEqual(c["attempts"]["ops_agent"]["outcome"]["completion_sec"], None)
        self.assertEqual(data["macroExcluded"], [])
        files = self.files(self.site / "prod")
        self.assertIn(rel, files)
        self.assertIn(f"dolt_mysql/macro/{day}/{record['attempts']['ops_agent']}.json", files)
        # An unpublished campaign of the same system stays out of production; development shows the newest first.
        other = self.write_campaign({**FULL, "id": "0000aaaa", "started": "2026-10-12T10:00:00+00:00"}, "3")
        data = self.read(self.assemble("prod"))
        self.assertEqual([c["campaign_id"] for c in data["macroCampaigns"]], ["0000f0ff"])
        self.assertNotIn(other, self.files(self.site / "prod"))
        self.assertEqual(self.read(self.assemble("local"))["macroCampaigns"][0]["campaign_id"], "0000aaaa")
        # Each validation failure alone refuses the publication and names its reason.
        (self.board / "dolt_mysql" / "macro" / day / f"{record['attempts']['data_agent']}.json").write_text('{"password": "hunter2"}')
        with self.assertRaisesRegex(site.SiteError, "evidence missing or unreadable: S6 Data agent"):
            self.assemble("prod")
        self.write_macro(attempt_payload(attempt_id="bb_1000ffff", scenario="multi_agent", campaign=campaign, date="2026-10-11"), day=day)
        with self.assertRaisesRegex(site.SiteError, "2 attempts in one campaign"):
            self.assemble("prod")
        self.write_macro(campaign_record(**campaign, attempts=record["attempts"], aborted="stopped after S6 Data agent: cleanup failed"), day=day)
        with self.assertRaisesRegex(site.SiteError, "campaign aborted"):
            self.assemble("prod")
        self.assertFalse(any(b"hunter2" in (self.site / "prod" / f).read_bytes() for f in self.files(self.site / "prod")), "a refused build leaves the previous site")

    def test_only_selected_campaign_records_are_heads(self):
        """An unselected record of the same campaign at another date neither blocks nor
        replaces the selected head; its attempts are still ingested; selecting a
        campaign twice fails clearly."""
        campaign = dict(FULL)
        rel = self.write_campaign(campaign, broken={"ops_agent": True})
        self.publish({"micro": [], "macro": [rel]})
        [c] = self.read(self.assemble("prod"))["macroCampaigns"]
        self.assertEqual(c["attempts"]["ops_agent"]["outcome"]["status"], "failed")
        twin = self.write_macro(json.loads((self.board / rel).read_text()), day="20261012")  # identical head, unselected
        [c] = self.read(self.assemble("prod"))["macroCampaigns"]
        self.assertEqual((c["source"], sorted(c["attempts"])), (rel, sorted(SCENARIOS)), "the selected head is published with every attempt")
        self.assertNotIn(twin, self.files(self.site / "prod"))
        self.publish({"micro": [], "macro": [rel, twin]})
        with self.assertRaisesRegex(site.SiteError, "selected more than once|also selected at"):
            self.assemble("prod")
        self.assertEqual(len(self.read(self.assemble("local"))["macroCampaigns"]), 1, "the default build is unchanged: the newest record of a campaign wins")
        self.publish({"micro": [], "macro": [rel]})
        self.write_macro(attempt_payload(attempt_id="bb_1000ffff", scenario="rl_env", campaign=campaign, date="2026-10-11"), day=self.day)
        with self.assertRaisesRegex(site.SiteError, "2 attempts in one campaign"):
            self.assemble("prod")  # every attempt file is still ingested, so a conflict is still a conflict

    def test_macro_source_filter_is_opt_in(self):
        campaign = dict(FULL)
        rel = self.write_campaign(campaign)
        twin = self.write_macro(json.loads((self.board / rel).read_text()), day="20261012")
        campaigns, excluded = build_macro.build(self.board)
        self.assertEqual(([c["source"] for c in campaigns], excluded), ([twin], []), "by default the newest record of a campaign is its head")
        campaigns, excluded = build_macro.build(self.board, sources={rel})
        self.assertEqual(([c["source"] for c in campaigns], excluded), ([rel], []))
        campaigns, excluded = build_macro.build(self.board, sources={rel, twin})
        self.assertEqual(campaigns, [])
        self.assertTrue({rel, twin} <= {x["source"] for x in excluded}, "both selected heads are reported, and their attempt is then an orphan")
        self.assertTrue(all("selected" in x["reason"] for x in excluded if x["source"] in (rel, twin)))

    def test_changed_during_run_is_never_published(self):
        rel = self.write_campaign(dict(FULL), source={**SOURCE, "changed_during_run": True})
        self.publish({"micro": [], "macro": [rel]})
        with self.assertRaisesRegex(site.SiteError, "sources changed during the run"):
            self.assemble("prod")

    def test_mixed_micro_or_macro_provenance_is_rejected(self):
        self.write(self.payload(), system="dolt")
        for field, value in (("commit", "f" * 40), ("lock_sha256", "e" * 64), ("python", "3.12.0")):
            with self.subTest(field=field):
                self.write(self.payload(system="dolt_mysql", **{field: value}), system="dolt_mysql")
                self.publish({"micro": ["dolt/results/20260930/box.json", "dolt_mysql/results/20260930/box.json"], "macro": []})
                with self.assertRaisesRegex(site.SiteError, f"mix different {field}"):
                    self.assemble("prod")
        other_machine = self.board / "dolt_mysql" / "results" / "20260930" / "other.json"
        other_machine.write_text(json.dumps(self.payload(system="dolt_mysql", machine="other")))
        self.publish({"micro": ["dolt/results/20260930/box.json", "dolt_mysql/results/20260930/other.json"], "macro": []})
        with self.assertRaisesRegex(site.SiteError, "mix different machine labels"):
            self.assemble("prod")
        c1 = self.write_campaign(dict(FULL), "1")
        c2_meta = {**FULL, "id": "0000f0fe"}
        c2_attempts = {}
        for i, key in enumerate(c2_meta["scenarios"]):
            aid = f"bb_2000000{i}"
            c2_attempts[key] = aid
            self.write_macro(attempt_payload(attempt_id=aid, scenario=key, campaign=c2_meta, date="2026-10-11", python="3.12.0"), day="20261011")
        c2 = self.write_macro(campaign_record(**c2_meta, attempts=c2_attempts, python="3.12.0"), day="20261011")
        self.publish({"micro": [], "macro": [c1, c2]})
        with self.assertRaisesRegex(site.SiteError, "mix different comparison groups"):
            self.assemble("prod")


class Cli(unittest.TestCase):
    def test_invalid_phase_fails_with_an_actionable_message(self):
        import subprocess, sys
        env = {**os.environ, "LEADERBOARD_PHASE": "staging"}
        process = subprocess.run([sys.executable, "-m", "leaderboard.site"], capture_output=True, text=True, env=env, cwd=BOARD.parent)
        self.assertEqual(process.returncode, 2)
        self.assertIn("must be prod, alpha or local, not 'staging'", process.stderr)
        process = subprocess.run([sys.executable, "-m", "leaderboard.site", "--phase", "dev"], capture_output=True, text=True, cwd=BOARD.parent)
        self.assertEqual(process.returncode, 2)
        self.assertIn("invalid choice", process.stderr)


class AwsAlphaLifecycle(Board):
    """Verify the collapsible DAPLab AWS alpha lifecycle."""

    def setUp(self):
        super().setUp()
        from leaderboard import aws_alpha
        self.aws_alpha = aws_alpha
        self.identity = aws_alpha.DaplabIdentity(
            "ai-forking",
            "ik2619",
            "123456789012",
        )
        self.runs = self.board / ".runs"
        self.calls: list[list[str]] = []
        self.cloud = {
            "account_id": "123456789012",
            "instances": [],
            "volumes": [],
            "sgs": [],
            "rules": [],
            "keys": [],
            "buckets": set(),
            "bucket_objects": {},
        }

    @staticmethod
    def _tag_spec(tokens: list[str], resource_type: str) -> list[dict[str, str]]:
        prefix = f"ResourceType={resource_type},"
        spec = next((token for token in tokens if token.startswith(prefix)), "")
        return [
            {"Key": key, "Value": value}
            for key, value in re.findall(r"\{Key=([^,{}]+),Value=([^{}]*)\}", spec)
        ]

    @staticmethod
    def _filtered(items: list[dict], tokens: list[str]) -> list[dict]:
        filters = [token for token in tokens if token.startswith("Name=")]

        def matches(item: dict) -> bool:
            tags = {
                tag["Key"]: tag["Value"]
                for tag in item.get("Tags", [])
            }
            fields = {
                "instance-id": "InstanceId",
                "volume-id": "VolumeId",
                "group-id": "GroupId",
                "group-name": "GroupName",
                "vpc-id": "VpcId",
                "key-name": "KeyName",
            }
            for item_filter in filters:
                name, values = item_filter.removeprefix("Name=").split(",Values=", 1)
                allowed = values.split(",")
                actual = tags.get(name.removeprefix("tag:")) if name.startswith("tag:") else item.get(fields.get(name, ""))
                if str(actual or "") not in allowed:
                    return False
            return True

        return [item for item in items if matches(item)]

    def fake_aws_runner(self, cmd: list[str], **_kwargs):
        import subprocess
        self.calls.append(cmd)
        # Strip 'aws', '--region <r>', '--profile <p>', '--output <o>'
        tokens = []
        i = 1
        while i < len(cmd):
            if cmd[i] in ("--region", "--profile", "--output"):
                i += 2
            else:
                tokens.append(cmd[i])
                i += 1
        svc, op = tokens[0], tokens[1]
        out = ""
        err = ""
        rc = 0
        if (
            svc == "s3api"
            and "--bucket" in tokens
            and "--expected-bucket-owner" in tokens
        ):
            bucket = tokens[tokens.index("--bucket") + 1]
            expected_owner = tokens[
                tokens.index("--expected-bucket-owner") + 1
            ]
            actual_owner = self.cloud.get("bucket_owners", {}).get(
                bucket,
                self.cloud["account_id"],
            )
            if expected_owner != actual_owner:
                return subprocess.CompletedProcess(
                    cmd,
                    1,
                    stdout="",
                    stderr=(
                        "An error occurred (403) because the bucket owner does not "
                        "match the expected owner"
                    ),
                )
        if (svc, op) == ("sts", "get-caller-identity"):
            out = json.dumps(
                {
                    "Account": self.cloud["account_id"],
                    "Arn": self.cloud.get(
                        "caller_arn",
                        "arn:aws:iam::123456789012:user/ik2619",
                    ),
                }
            )
        elif (svc, op) == ("ssm", "get-parameter"):
            out = "ami-0ubuntu2404amd64"
        elif (svc, op) == ("ec2", "describe-instances"):
            instances = list(self.cloud["instances"])
            if "--instance-ids" in tokens:
                instance_id = tokens[tokens.index("--instance-ids") + 1]
                instances = [
                    item for item in instances if item["InstanceId"] == instance_id
                ]
            else:
                instances = self._filtered(instances, tokens)
            out = json.dumps({"Reservations": [{"Instances": instances}]})
        elif (svc, op) == ("ec2", "describe-volumes"):
            out = json.dumps(
                {"Volumes": self._filtered(list(self.cloud["volumes"]), tokens)}
            )
        elif (svc, op) == ("ec2", "describe-security-groups"):
            out = json.dumps(
                {"SecurityGroups": self._filtered(list(self.cloud["sgs"]), tokens)}
            )
        elif (svc, op) == ("ec2", "describe-key-pairs"):
            out = json.dumps(
                {"KeyPairs": self._filtered(list(self.cloud["keys"]), tokens)}
            )
        elif (svc, op) == ("ec2", "describe-security-group-rules"):
            out = json.dumps(
                {
                    "SecurityGroupRules": self._filtered(
                        list(self.cloud["rules"]),
                        tokens,
                    )
                }
            )
        elif (svc, op) == ("ec2", "create-key-pair"):
            kname = tokens[tokens.index("--key-name") + 1]
            self.cloud["keys"].append(
                {
                    "KeyName": kname,
                    "Tags": self._tag_spec(tokens, "key-pair"),
                }
            )
            out = json.dumps({"KeyName": kname, "KeyMaterial": "-----BEGIN OPENSSH PRIVATE KEY-----\nalpha\n"})
        elif (svc, op) == ("ec2", "create-security-group"):
            gname = tokens[tokens.index("--group-name") + 1]
            gid = "sg-0alpha123"
            group = {
                "GroupId": gid,
                "GroupName": gname,
                "Tags": self._tag_spec(tokens, "security-group"),
            }
            if "--vpc-id" in tokens:
                group["VpcId"] = tokens[tokens.index("--vpc-id") + 1]
            self.cloud["sgs"].append(group)
            self.cloud["rules"].append(
                {
                    "SecurityGroupRuleId": "sgr-0egress",
                    "GroupId": gid,
                    "IsEgress": True,
                    "IpProtocol": "-1",
                    "CidrIpv4": "0.0.0.0/0",
                }
            )
            out = json.dumps({"GroupId": gid})
        elif (svc, op) == ("ec2", "authorize-security-group-ingress"):
            gid = tokens[tokens.index("--group-id") + 1]
            protocol = tokens[tokens.index("--protocol") + 1]
            port = int(tokens[tokens.index("--port") + 1])
            cidr = tokens[tokens.index("--cidr") + 1]
            duplicate = any(
                not rule["IsEgress"]
                and rule["GroupId"] == gid
                and rule.get("IpProtocol") == protocol
                and rule.get("FromPort") == port
                and rule.get("ToPort") == port
                and rule.get("CidrIpv4") == cidr
                for rule in self.cloud["rules"]
            )
            if duplicate:
                rc = 1
                err = "An error occurred (InvalidPermission.Duplicate)"
            else:
                self.cloud["rules"].append(
                    {
                        "SecurityGroupRuleId": "sgr-0ingress",
                        "GroupId": gid,
                        "IsEgress": False,
                        "IpProtocol": protocol,
                        "FromPort": port,
                        "ToPort": port,
                        "CidrIpv4": cidr,
                        "Tags": self._tag_spec(tokens, "security-group-rule"),
                    }
                )
        elif (svc, op) == ("ec2", "revoke-security-group-ingress"):
            rule_id = tokens[tokens.index("--security-group-rule-ids") + 1]
            self.cloud["rules"] = [
                rule
                for rule in self.cloud["rules"]
                if rule["SecurityGroupRuleId"] != rule_id
            ]
        elif (svc, op) == ("ec2", "create-tags"):
            if self.cloud.get("fail_rule_tagging"):
                rc = 1
        elif (svc, op) == ("ec2", "run-instances"):
            iid = "i-0alpha999"
            itype = tokens[tokens.index("--instance-type") + 1]
            self.cloud["instances"].append(
                {
                    "InstanceId": iid,
                    "State": {"Name": "running"},
                    "InstanceType": itype,
                    "PublicIpAddress": "198.51.100.42",
                    "Tags": self._tag_spec(tokens, "instance"),
                }
            )
            self.cloud["volumes"].append(
                {
                    "VolumeId": "vol-0alpha1",
                    "State": "available",
                    "Tags": self._tag_spec(tokens, "volume"),
                }
            )
            out = json.dumps({"Instances": [{"InstanceId": iid}]})
        elif (svc, op) == ("ec2", "terminate-instances"):
            ids = set(tokens[tokens.index("--instance-ids") + 1 :])
            for inst in self.cloud["instances"]:
                if inst["InstanceId"] in ids:
                    inst["State"] = {"Name": "terminated"}
            out = "terminated"
        elif (svc, op) == ("ec2", "delete-volume"):
            vid = tokens[tokens.index("--volume-id") + 1]
            if vid in self.cloud.get("fail_delete_volumes", set()):
                rc = 1
            else:
                self.cloud["volumes"] = [
                    v for v in self.cloud["volumes"] if v["VolumeId"] != vid
                ]
        elif (svc, op) == ("ec2", "delete-security-group"):
            gid = tokens[tokens.index("--group-id") + 1]
            self.cloud["sgs"] = [g for g in self.cloud["sgs"] if g["GroupId"] != gid]
            self.cloud["rules"] = [
                rule for rule in self.cloud["rules"] if rule["GroupId"] != gid
            ]
        elif (svc, op) == ("ec2", "delete-key-pair"):
            kname = tokens[tokens.index("--key-name") + 1]
            self.cloud["keys"] = [k for k in self.cloud["keys"] if k["KeyName"] != kname]
        elif (svc, op) == ("s3api", "head-bucket"):
            bname = tokens[tokens.index("--bucket") + 1]
            if bname in self.cloud.get("forbidden_buckets", set()):
                return subprocess.CompletedProcess(
                    cmd,
                    1,
                    stdout="",
                    stderr="An error occurred (403) when calling the HeadBucket operation: Forbidden",
                )
            if bname not in self.cloud["buckets"]:
                return subprocess.CompletedProcess(
                    cmd,
                    1,
                    stdout="",
                    stderr="An error occurred (404) when calling the HeadBucket operation: Not Found",
                )
            rc = 0
        elif (svc, op) == ("s3api", "list-buckets"):
            if self.cloud.get("fail_list_buckets"):
                rc = 1
                err = "list buckets unavailable"
            else:
                prefix = (
                    tokens[tokens.index("--prefix") + 1]
                    if "--prefix" in tokens
                    else ""
                )
                out = json.dumps(
                    {
                        "Buckets": [
                            {"Name": bucket}
                            for bucket in sorted(self.cloud["buckets"])
                            if bucket.startswith(prefix)
                        ]
                    }
                )
        elif (svc, op) == ("s3api", "get-bucket-location"):
            bname = tokens[tokens.index("--bucket") + 1]
            if bname not in self.cloud["buckets"]:
                rc = 1
            else:
                region = self.cloud.get("bucket_regions", {}).get(
                    bname,
                    "us-east-1",
                )
                out = json.dumps(
                    {
                        "LocationConstraint": (
                            None if region == "us-east-1" else region
                        )
                    }
                )
        elif (svc, op) == ("s3api", "create-bucket"):
            bname = tokens[tokens.index("--bucket") + 1]
            self.cloud["buckets"].add(bname)
            self.cloud.setdefault("bucket_owners", {})[bname] = self.cloud[
                "account_id"
            ]
            self.cloud["bucket_objects"].setdefault(bname, set())
            out = json.dumps({"Location": f"/{bname}"})
        elif (svc, op) == ("s3api", "put-bucket-tagging"):
            bname = tokens[tokens.index("--bucket") + 1]
            if bname in self.cloud.get("fail_bucket_tagging", set()):
                rc = 1
            else:
                tagging = tokens[tokens.index("--tagging") + 1]
                self.cloud.setdefault("bucket_tags", {})[bname] = json.loads(tagging)[
                    "TagSet"
                ]
        elif (svc, op) == ("s3api", "get-bucket-tagging"):
            bname = tokens[tokens.index("--bucket") + 1]
            tags = self.cloud.get("bucket_tags", {}).get(bname)
            if tags is None:
                rc = 1
            else:
                out = json.dumps({"TagSet": tags})
        elif (svc, op) == ("s3api", "list-objects-v2"):
            bname = tokens[tokens.index("--bucket") + 1]
            out = json.dumps(
                {
                    "Contents": [
                        {"Key": key}
                        for key in sorted(
                            self.cloud["bucket_objects"].get(bname, set())
                        )
                    ]
                }
            )
        elif (svc, op) == ("s3api", "put-object"):
            bname = tokens[tokens.index("--bucket") + 1]
            key = tokens[tokens.index("--key") + 1]
            self.cloud["bucket_objects"].setdefault(bname, set()).add(key)
            out = json.dumps({"ETag": '"fake"'})
        elif (svc, op) == ("s3api", "list-object-versions"):
            bname = tokens[tokens.index("--bucket") + 1]
            out = json.dumps(
                {
                    "Versions": [
                        {"Key": key, "VersionId": "null"}
                        for key in sorted(
                            self.cloud["bucket_objects"].get(bname, set())
                        )
                    ],
                    "DeleteMarkers": [],
                }
            )
        elif (svc, op) == ("s3api", "delete-objects"):
            bname = tokens[tokens.index("--bucket") + 1]
            payload = json.loads(tokens[tokens.index("--delete") + 1])
            for item in payload.get("Objects", []):
                self.cloud["bucket_objects"].setdefault(bname, set()).discard(
                    item["Key"]
                )
            out = json.dumps({"Deleted": payload.get("Objects", [])})
        elif (svc, op) == ("s3api", "delete-bucket"):
            bname = tokens[tokens.index("--bucket") + 1]
            if bname in self.cloud.get("fail_delete_buckets", set()):
                rc = 1
            elif self.cloud["bucket_objects"].get(bname):
                rc = 1
            else:
                self.cloud["buckets"].discard(bname)
                self.cloud["bucket_objects"].pop(bname, None)
                self.cloud.get("bucket_owners", {}).pop(bname, None)
        elif (svc, op) == ("s3", "presign"):
            out = "https://s3.amazonaws.com/presigned/index.html"
        return subprocess.CompletedProcess(cmd, rc, stdout=out, stderr=err)

    def test_up_deploy_status_and_collapse_tear_down_all_resources(self):
        self.write(self.payload())
        aws = self.aws_alpha.AwsCli(region="us-east-1", runner=self.fake_aws_runner)
        state = self.aws_alpha.up_alpha(
            aws=aws,
            identity=self.identity,
            stack="branchbench-alpha",
            spot=True,
            runs_dir=self.runs,
            http_getter=lambda _url: "203.0.113.9\n",
        )
        self.assertEqual(state["phase"], "alpha")
        self.assertEqual(state["instance_id"], "i-0alpha999")
        self.assertEqual(state["ssh_cidr"], "203.0.113.9/32")
        self.assertEqual(
            state["bucket"],
            "ai-forking-ik2619-branchbench-alpha-site-123456789012-us-east-1",
        )
        self.assertEqual(state["instance_type"], "t3a.xlarge")
        self.assertEqual(state["daplab_project"], "ai-forking")
        self.assertEqual(state["billing_tag1"], "ik2619")
        self.assertTrue((self.runs / "aws_alpha_state.json").is_file())
        self.assertTrue((self.runs / "branchbench-alpha.pem").is_file())
        run_call = next(call for call in self.calls if "run-instances" in call)
        self.assertEqual(
            run_call[run_call.index("--instance-type") + 1], "t3a.xlarge"
        )
        self.assertEqual(
            run_call[run_call.index("--credit-specification") + 1],
            "CpuCredits=standard",
        )
        self.assertEqual(
            run_call[
                run_call.index("--instance-initiated-shutdown-behavior") + 1
            ],
            "terminate",
        )
        block_devices = json.loads(
            run_call[run_call.index("--block-device-mappings") + 1]
        )
        self.assertEqual(block_devices[0]["Ebs"]["VolumeSize"], 64)
        self.assertTrue(block_devices[0]["Ebs"]["Encrypted"])
        tag_specs = [part for part in run_call if part.startswith("ResourceType=")]
        self.assertEqual(
            {spec.split(",", 1)[0] for spec in tag_specs},
            {
                "ResourceType=instance",
                "ResourceType=volume",
                "ResourceType=network-interface",
                "ResourceType=spot-instances-request",
            },
        )
        for call in self.calls:
            for spec in (part for part in call if part.startswith("ResourceType=")):
                self.assertIn("{Key=project,Value=ai-forking}", spec)
                self.assertIn("{Key=billing-tag1,Value=ik2619}", spec)
                self.assertIn("Value=ai-forking-ik2619-", spec)
        self.assertIn(
            {"Key": "project", "Value": "ai-forking"},
            self.cloud["bucket_tags"][state["bucket"]],
        )
        self.assertIn(
            {"Key": "billing-tag1", "Value": "ik2619"},
            self.cloud["bucket_tags"][state["bucket"]],
        )
        self.assertTrue(
            any("put-bucket-encryption" in call for call in self.calls),
            "alpha buckets must explicitly enable encryption at rest",
        )
        ingress_call = next(
            call for call in self.calls if "authorize-security-group-ingress" in call
        )
        ingress_tags = ingress_call[
            ingress_call.index("--tag-specifications") + 1
        ]
        self.assertIn("ResourceType=security-group-rule", ingress_tags)
        self.assertIn("{Key=project,Value=ai-forking}", ingress_tags)
        self.assertIn("{Key=billing-tag1,Value=ik2619}", ingress_tags)
        rule_tag_calls = [call for call in self.calls if "create-tags" in call]
        self.assertEqual(len(rule_tag_calls), 2)
        for call in rule_tag_calls:
            self.assertIn("Key=project,Value=ai-forking", call)
            self.assertIn("Key=billing-tag1,Value=ik2619", call)
        filtered_describes = [
            call
            for call in self.calls
            if "--filters" in call
            and any(
                operation in call
                for operation in (
                    "describe-instances",
                    "describe-volumes",
                    "describe-security-groups",
                    "describe-key-pairs",
                )
            )
        ]
        self.assertTrue(filtered_describes)
        for call in filtered_describes:
            self.assertIn("Name=tag:project,Values=ai-forking", call)
            self.assertIn("Name=tag:billing-tag1,Values=ik2619", call)

        # Duplicate `up` while active refuses to leak a second instance
        with self.assertRaisesRegex(self.aws_alpha.AlphaError, "already has an EC2 instance"):
            self.aws_alpha.up_alpha(
                aws=aws,
                identity=self.identity,
                stack="branchbench-alpha",
                runs_dir=self.runs,
                http_getter=lambda _url: "203.0.113.9",
            )

        self.cloud["bucket_objects"][state["bucket"]].add("stale.txt")
        deployed = self.aws_alpha.deploy_alpha_site(
            aws=aws,
            identity=self.identity,
            stack="branchbench-alpha",
            board=self.board,
            runs_dir=self.runs,
        )
        self.assertTrue((self.site / "alpha" / "index.html").is_file())
        self.assertEqual(deployed["bucket"], state["bucket"])
        self.assertIn("index.html", self.cloud["bucket_objects"][state["bucket"]])
        self.assertNotIn("stale.txt", self.cloud["bucket_objects"][state["bucket"]])
        self.assertFalse(any("sync" in call for call in self.calls))

        active_status = self.aws_alpha.status_alpha(
            aws=aws,
            identity=self.identity,
            stack="branchbench-alpha",
            board=self.board,
            runs_dir=self.runs,
        )
        self.assertFalse(active_status["collapsed"])

        # Even if state file is completely lost before collapse, tag discovery (including list-buckets + tag check) tears everything down
        (self.runs / "aws_alpha_state.json").unlink()
        collapsed = self.aws_alpha.collapse_alpha(
            aws=aws,
            identity=self.identity,
            stack="branchbench-alpha",
            board=self.board,
            runs_dir=self.runs,
        )
        self.assertTrue(collapsed["collapsed"])
        self.assertEqual(collapsed["terminated_instances"], ["i-0alpha999"])
        self.assertEqual(collapsed["deleted_volumes"], ["vol-0alpha1"])
        self.assertEqual(collapsed["deleted_security_groups"], ["sg-0alpha123"])
        self.assertEqual(collapsed["deleted_bucket"], state["bucket"])
        self.assertEqual(collapsed["ongoing_aws_cost"], "$0.00/hr")
        self.assertFalse((self.site / "alpha").exists())
        self.assertFalse((self.runs / "aws_alpha_state.json").exists())
        self.assertFalse((self.runs / "branchbench-alpha.pem").exists())
        for call in self.calls:
            if (
                "s3api" in call
                and "--bucket" in call
                and "create-bucket" not in call
            ):
                self.assertIn("--expected-bucket-owner", call)
                self.assertEqual(
                    call[call.index("--expected-bucket-owner") + 1],
                    self.identity.account_id,
                )

        final_status = self.aws_alpha.status_alpha(
            aws=aws,
            identity=self.identity,
            stack="branchbench-alpha",
            board=self.board,
            runs_dir=self.runs,
        )
        self.assertTrue(final_status["collapsed"])

    def test_daplab_identity_region_state_and_bucket_creation_fail_closed(self):
        with self.assertRaisesRegex(self.aws_alpha.AlphaError, "--daplab-project"):
            self.aws_alpha.DaplabIdentity(
                "bad project",
                "ik2619",
                "123456789012",
            )
        with self.assertRaisesRegex(self.aws_alpha.AlphaError, "--daplab-account-id"):
            self.aws_alpha.DaplabIdentity("ai-forking", "ik2619", "bad")
        with self.assertRaisesRegex(self.aws_alpha.AlphaError, "IAM user"):
            self.aws_alpha.verify_caller_owner(
                {
                    "account_id": "123456789012",
                    "arn": "arn:aws:iam::123456789012:user/another-user",
                },
                self.identity,
            )

        east = self.aws_alpha.AwsCli(
            region="us-east-1",
            runner=self.fake_aws_runner,
        )
        self.cloud["account_id"] = "999999999999"
        self.cloud["caller_arn"] = "arn:aws:iam::999999999999:user/ik2619"
        with self.assertRaisesRegex(self.aws_alpha.AlphaError, "daplab-account-id"):
            self.aws_alpha.up_alpha(
                aws=east,
                identity=self.identity,
                ssh_cidr="203.0.113.9/32",
                runs_dir=self.runs,
            )
        self.assertFalse(
            any(
                operation in call
                for call in self.calls
                for operation in (
                    "create-key-pair",
                    "create-security-group",
                    "create-bucket",
                    "run-instances",
                )
            )
        )
        self.cloud["account_id"] = self.identity.account_id
        self.cloud["caller_arn"] = (
            "arn:aws:iam::123456789012:user/ik2619"
        )
        self.calls.clear()

        west = self.aws_alpha.AwsCli(
            region="us-west-2",
            dry_run=True,
            runner=self.fake_aws_runner,
        )
        with self.assertRaisesRegex(self.aws_alpha.AlphaError, "locked to us-east-1"):
            self.aws_alpha.up_alpha(
                aws=west,
                identity=self.identity,
                ssh_cidr="203.0.113.9/32",
                runs_dir=self.runs,
            )

        bad_state = {
            "daplab_project": "another-project",
            "billing_tag1": "ik2619",
        }
        self.aws_alpha.save_state(bad_state, self.runs)
        with self.assertRaisesRegex(self.aws_alpha.AlphaError, "does not match"):
            self.aws_alpha.status_alpha(
                aws=east,
                identity=self.identity,
                runs_dir=self.runs,
                board=self.board,
            )
        self.aws_alpha.state_file_path(self.runs).unlink()

        aws = east
        bucket = self.aws_alpha.default_bucket_name(
            "branchbench-alpha",
            "123456789012",
            "us-east-1",
            self.identity,
        )
        self.cloud.setdefault("fail_bucket_tagging", set()).add(bucket)
        with self.assertRaises(self.aws_alpha.AlphaError):
            self.aws_alpha.up_alpha(
                aws=aws,
                identity=self.identity,
                runs_dir=self.runs,
                http_getter=lambda _url: "203.0.113.9",
            )
        self.assertNotIn(bucket, self.cloud["buckets"])

    def test_live_commands_reject_a_caller_other_than_owner(self):
        self.aws_alpha.save_state(
            {
                "phase": "alpha",
                "stack": "branchbench-alpha",
                "daplab_project": self.identity.project,
                "billing_tag1": self.identity.owner,
                "region": "us-east-1",
                "account_id": "123456789012",
            },
            self.runs,
        )
        self.cloud["caller_arn"] = (
            "arn:aws:iam::123456789012:user/another-user"
        )
        aws = self.aws_alpha.AwsCli(
            region="us-east-1",
            runner=self.fake_aws_runner,
        )
        commands = {
            "run": lambda: self.aws_alpha.run_alpha_benchmarks(
                aws=aws,
                identity=self.identity,
                board=self.board,
                runs_dir=self.runs,
            ),
            "deploy-site": lambda: self.aws_alpha.deploy_alpha_site(
                aws=aws,
                identity=self.identity,
                board=self.board,
                runs_dir=self.runs,
            ),
            "status": lambda: self.aws_alpha.status_alpha(
                aws=aws,
                identity=self.identity,
                board=self.board,
                runs_dir=self.runs,
            ),
            "collapse": lambda: self.aws_alpha.collapse_alpha(
                aws=aws,
                identity=self.identity,
                board=self.board,
                runs_dir=self.runs,
            ),
        }
        for command, invoke in commands.items():
            with self.subTest(command=command):
                self.calls.clear()
                with self.assertRaisesRegex(self.aws_alpha.AlphaError, "IAM user"):
                    invoke()
                self.assertTrue(
                    any("get-caller-identity" in call for call in self.calls)
                )
                self.assertFalse(
                    any(
                        operation in call
                        for call in self.calls
                        for operation in (
                            "terminate-instances",
                            "delete-security-group",
                            "delete-key-pair",
                            "delete-bucket",
                            "sync",
                        )
                    )
                )

    def test_collapse_ignores_unverified_state_resource_references(self):
        foreign_file = self.board / "must-not-delete.pem"
        foreign_file.write_text("foreign", encoding="utf-8")
        foreign_bucket = "ai-forking-ik2619-foreign-production"
        foreign_tags = [
            {"Key": "project", "Value": "another-project"},
            {"Key": "billing-tag1", "Value": "another-user"},
            {"Key": "Project", "Value": "OtherApp"},
            {"Key": "Phase", "Value": "prod"},
        ]
        self.cloud.update(
            {
                "instances": [
                    {
                        "InstanceId": "i-foreign",
                        "State": {"Name": "running"},
                        "InstanceType": "m7a.2xlarge",
                        "PublicIpAddress": "198.51.100.200",
                        "Tags": foreign_tags,
                    }
                ],
                "volumes": [],
                "sgs": [
                    {
                        "GroupId": "sg-foreign",
                        "GroupName": "foreign-production-sg",
                        "Tags": foreign_tags,
                    }
                ],
                "keys": [
                    {
                        "KeyName": "foreign-production-key",
                        "Tags": foreign_tags,
                    }
                ],
                "buckets": {foreign_bucket},
            }
        )
        self.cloud.setdefault("bucket_tags", {})[foreign_bucket] = foreign_tags
        self.aws_alpha.save_state(
            {
                "phase": "alpha",
                "stack": "branchbench-alpha",
                "daplab_project": self.identity.project,
                "billing_tag1": self.identity.owner,
                "region": "us-east-1",
                "account_id": "123456789012",
                "instance_id": "i-foreign",
                "security_group_id": "sg-foreign",
                "key_name": "foreign-production-key",
                "key_path": str(foreign_file),
                "bucket": foreign_bucket,
            },
            self.runs,
        )
        aws = self.aws_alpha.AwsCli(
            region="us-east-1",
            runner=self.fake_aws_runner,
        )

        collapsed = self.aws_alpha.collapse_alpha(
            aws=aws,
            identity=self.identity,
            board=self.board,
            runs_dir=self.runs,
        )

        self.assertFalse(collapsed["collapsed"])
        self.assertEqual(collapsed["terminated_instances"], [])
        self.assertEqual(collapsed["deleted_security_groups"], [])
        self.assertEqual(collapsed["deleted_key_pairs"], [])
        self.assertEqual(collapsed["deleted_buckets"], [])
        self.assertEqual(
            collapsed["unverified_resources"],
            [
                {"type": "instance", "id": "i-foreign"},
                {"type": "security_group", "id": "sg-foreign"},
                {"type": "key_pair", "id": "foreign-production-key"},
            ],
        )
        self.assertIn(foreign_bucket, collapsed["remaining_buckets"])
        self.assertTrue(foreign_file.is_file())
        self.assertIn(foreign_bucket, self.cloud["buckets"])
        retained = self.aws_alpha.load_state(self.runs)
        self.assertEqual(retained["instance_id"], "i-foreign")
        self.assertEqual(retained["security_group_id"], "sg-foreign")
        self.assertEqual(retained["key_name"], "foreign-production-key")
        for operation in (
            "terminate-instances",
            "delete-security-group",
            "delete-key-pair",
            "delete-bucket",
        ):
            self.assertFalse(any(operation in call for call in self.calls))

    def test_collapse_rejects_wrong_region_and_account_before_deletion(self):
        state = {
            "phase": "alpha",
            "stack": "branchbench-alpha",
            "daplab_project": self.identity.project,
            "billing_tag1": self.identity.owner,
            "region": "us-west-2",
            "account_id": "123456789012",
        }
        self.aws_alpha.save_state(state, self.runs)
        aws = self.aws_alpha.AwsCli(
            region="us-east-1",
            runner=self.fake_aws_runner,
        )

        with self.assertRaisesRegex(self.aws_alpha.AlphaError, "us-west-2"):
            self.aws_alpha.collapse_alpha(
                aws=aws,
                identity=self.identity,
                board=self.board,
                runs_dir=self.runs,
            )
        self.assertFalse(any("delete-" in part for call in self.calls for part in call))

        state["region"] = "us-east-1"
        state["account_id"] = self.identity.account_id
        self.aws_alpha.save_state(state, self.runs)
        self.cloud["account_id"] = "999999999999"
        self.cloud["caller_arn"] = "arn:aws:iam::999999999999:user/ik2619"
        self.calls.clear()
        with self.assertRaisesRegex(self.aws_alpha.AlphaError, "daplab-account-id"):
            self.aws_alpha.collapse_alpha(
                aws=aws,
                identity=self.identity,
                board=self.board,
                runs_dir=self.runs,
            )
        self.assertTrue(any("get-caller-identity" in call for call in self.calls))
        self.assertFalse(any("delete-" in part for call in self.calls for part in call))

    def test_state_loss_partial_collapse_can_retry(self):
        self.cloud["volumes"] = [
            {
                "VolumeId": "vol-0alpha1",
                "State": "available",
                "Tags": self.aws_alpha.resource_tags(
                    "branchbench-alpha",
                    self.identity,
                    self.identity.resource_name("branchbench-alpha-root"),
                ),
            }
        ]
        volume_id = self.cloud["volumes"][0]["VolumeId"]
        self.cloud["fail_delete_volumes"] = {volume_id}
        aws = self.aws_alpha.AwsCli(
            region="us-east-1",
            runner=self.fake_aws_runner,
        )

        partial = self.aws_alpha.collapse_alpha(
            aws=aws,
            identity=self.identity,
            board=self.board,
            runs_dir=self.runs,
        )

        self.assertFalse(partial["collapsed"])
        recovered = self.aws_alpha.load_state(self.runs)
        self.assertEqual(recovered["daplab_project"], self.identity.project)
        self.assertEqual(recovered["billing_tag1"], self.identity.owner)
        self.cloud["fail_delete_volumes"].clear()

        retried = self.aws_alpha.collapse_alpha(
            aws=aws,
            identity=self.identity,
            board=self.board,
            runs_dir=self.runs,
        )
        self.assertTrue(retried["collapsed"])
        self.assertFalse(self.aws_alpha.state_file_path(self.runs).exists())

    def test_state_loss_does_not_delete_bucket_from_another_region(self):
        bucket = "ai-forking-ik2619-branchbench-alpha-site-west"
        self.cloud.update(
            {
                "instances": [],
                "volumes": [],
                "sgs": [],
                "keys": [],
                "buckets": {bucket},
                "bucket_regions": {bucket: "us-west-2"},
            }
        )
        self.cloud.setdefault("bucket_tags", {})[bucket] = [
            {"Key": "project", "Value": self.identity.project},
            {"Key": "billing-tag1", "Value": self.identity.owner},
            {"Key": "Project", "Value": "BranchBenchLeaderboard"},
            {"Key": "Phase", "Value": "alpha"},
            {"Key": "Stack", "Value": "branchbench-alpha"},
        ]
        aws = self.aws_alpha.AwsCli(
            region="us-east-1",
            runner=self.fake_aws_runner,
        )

        result = self.aws_alpha.collapse_alpha(
            aws=aws,
            identity=self.identity,
            board=self.board,
            runs_dir=self.runs,
        )

        self.assertFalse(result["collapsed"])
        self.assertEqual(
            result["wrong_region_buckets"],
            [{"bucket": bucket, "region": "us-west-2"}],
        )
        self.assertIn(bucket, self.cloud["buckets"])
        self.assertFalse(any("delete-bucket" in call for call in self.calls))

    def test_state_loss_ignores_unrelated_shared_account_resources(self):
        unrelated_bucket = "another-project-someone-production-data"
        foreign_tags = [
            {"Key": "project", "Value": "another-project"},
            {"Key": "billing-tag1", "Value": "another-user"},
            {"Key": "Project", "Value": "OtherApp"},
            {"Key": "Phase", "Value": "prod"},
            {"Key": "Stack", "Value": "production"},
        ]
        self.cloud.update(
            {
                "instances": [
                    {
                        "InstanceId": "i-foreign",
                        "State": {"Name": "running"},
                        "InstanceType": "m7a.2xlarge",
                        "PublicIpAddress": "198.51.100.200",
                        "Tags": foreign_tags,
                    }
                ],
                "volumes": [
                    {
                        "VolumeId": "vol-foreign",
                        "State": "available",
                        "Tags": foreign_tags,
                    }
                ],
                "sgs": [
                    {
                        "GroupId": "sg-foreign",
                        "GroupName": "another-project-production",
                        "Tags": foreign_tags,
                    }
                ],
                "keys": [
                    {
                        "KeyName": "another-project-production",
                        "Tags": foreign_tags,
                    }
                ],
                "buckets": {unrelated_bucket},
            }
        )
        aws = self.aws_alpha.AwsCli(
            region="us-east-1",
            runner=self.fake_aws_runner,
        )

        result = self.aws_alpha.collapse_alpha(
            aws=aws,
            identity=self.identity,
            board=self.board,
            runs_dir=self.runs,
        )

        self.assertTrue(result["collapsed"])
        self.assertEqual(self.cloud["instances"][0]["State"]["Name"], "running")
        self.assertEqual(self.cloud["volumes"][0]["VolumeId"], "vol-foreign")
        self.assertEqual(self.cloud["sgs"][0]["GroupId"], "sg-foreign")
        self.assertEqual(self.cloud["keys"][0]["KeyName"], "another-project-production")
        self.assertIn(unrelated_bucket, self.cloud["buckets"])
        list_call = next(call for call in self.calls if "list-buckets" in call)
        self.assertEqual(
            list_call[list_call.index("--prefix") + 1],
            f"{self.identity.name_prefix}-",
        )
        self.assertFalse(
            any(
                unrelated_bucket in call
                and (
                    "get-bucket-location" in call
                    or "get-bucket-tagging" in call
                    or "delete-bucket" in call
                )
                for call in self.calls
            )
        )

    def test_bucket_discovery_uses_the_owner_prefix_and_fails_closed(self):
        bucket = "ai-forking-ik2619-orphan-alpha-site"
        self.cloud["buckets"] = {bucket}
        self.cloud.setdefault("bucket_tags", {})[bucket] = (
            self.aws_alpha.resource_tags(
                "branchbench-alpha",
                self.identity,
                bucket,
            )
        )
        self.aws_alpha.save_state(
            {
                "phase": "alpha",
                "stack": "branchbench-alpha",
                "daplab_project": self.identity.project,
                "billing_tag1": self.identity.owner,
                "account_id": self.identity.account_id,
                "region": "us-east-1",
                "bucket": "",
            },
            self.runs,
        )
        aws = self.aws_alpha.AwsCli(
            region="us-east-1",
            runner=self.fake_aws_runner,
        )

        recovered = self.aws_alpha.collapse_alpha(
            aws=aws,
            identity=self.identity,
            board=self.board,
            runs_dir=self.runs,
        )

        self.assertTrue(recovered["collapsed"])
        self.assertEqual(recovered["deleted_buckets"], [bucket])
        list_call = next(call for call in self.calls if "list-buckets" in call)
        self.assertEqual(
            list_call[list_call.index("--prefix") + 1],
            f"{self.identity.name_prefix}-",
        )

        self.calls.clear()
        self.cloud["fail_list_buckets"] = True
        unresolved = self.aws_alpha.collapse_alpha(
            aws=aws,
            identity=self.identity,
            keep_bucket=True,
            board=self.board,
            runs_dir=self.runs,
        )
        self.assertFalse(unresolved["collapsed"])
        self.assertEqual(
            unresolved["ongoing_aws_cost"],
            "partial (see remaining resources)",
        )
        self.assertTrue(self.aws_alpha.state_file_path(self.runs).is_file())

    def test_reused_security_group_replaces_stale_ssh_cidr(self):
        stack = "branchbench-alpha"
        group_id = "sg-0alpha123"
        self.cloud["sgs"] = [
            {
                "GroupId": group_id,
                "GroupName": self.identity.resource_name(f"{stack}-sg"),
                "Tags": self.aws_alpha.resource_tags(
                    stack,
                    self.identity,
                    self.identity.resource_name(f"{stack}-sg"),
                ),
            }
        ]
        self.cloud["rules"] = [
            {
                "SecurityGroupRuleId": "sgr-egress",
                "GroupId": group_id,
                "IsEgress": True,
                "IpProtocol": "-1",
                "CidrIpv4": "0.0.0.0/0",
            },
            {
                "SecurityGroupRuleId": "sgr-stale",
                "GroupId": group_id,
                "IsEgress": False,
                "IpProtocol": "tcp",
                "FromPort": 22,
                "ToPort": 22,
                "CidrIpv4": "198.51.100.1/32",
            },
        ]
        aws = self.aws_alpha.AwsCli(
            region="us-east-1",
            runner=self.fake_aws_runner,
        )

        returned_id = self.aws_alpha.ensure_security_group(
            aws,
            stack,
            self.identity,
            "203.0.113.9/32",
        )

        self.assertEqual(returned_id, group_id)
        ingress = [rule for rule in self.cloud["rules"] if not rule["IsEgress"]]
        self.assertEqual(len(ingress), 1)
        self.assertEqual(ingress[0]["CidrIpv4"], "203.0.113.9/32")
        revoke = next(
            call for call in self.calls if "revoke-security-group-ingress" in call
        )
        self.assertIn("sgr-stale", revoke)

    def test_run_with_collapse_after_pulls_results_builds_alpha_site_and_collapses_aws(self):
        import subprocess
        self.write(self.payload())
        aws = self.aws_alpha.AwsCli(region="us-east-1", runner=self.fake_aws_runner)
        self.aws_alpha.up_alpha(
            aws=aws,
            identity=self.identity,
            stack="branchbench-alpha",
            runs_dir=self.runs,
            http_getter=lambda _url: "203.0.113.9",
        )
        unrelated_key = self.board / "must-not-use.pem"
        unrelated_key.write_text("foreign", encoding="utf-8")
        state = self.aws_alpha.load_state(self.runs)
        state["public_ip"] = "203.0.113.250"
        state["key_path"] = str(unrelated_key)
        self.aws_alpha.save_state(state, self.runs)
        ssh_rsync_calls = []

        def fake_cmd_runner(cmd: list[str], **_kwargs):
            ssh_rsync_calls.append(cmd)
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

        rc = self.aws_alpha.run_alpha_benchmarks(
            aws=aws,
            identity=self.identity,
            stack="branchbench-alpha",
            preset="mini",
            systems="dolt dolt_mysql",
            collapse_after=True,
            board=self.board,
            runs_dir=self.runs,
            cmd_runner=fake_cmd_runner,
        )
        self.assertEqual(rc, 0)
        self.assertEqual(len(ssh_rsync_calls), 5, "wait for bootstrap, rsync up, ssh worker run, rsync pull, ssh teardown ownership verify+refresh")
        self.assertTrue((self.site / "alpha" / "data.js").is_file(), "--collapse-after keeps local .site/alpha while tearing down AWS")
        self.assertFalse((self.runs / "aws_alpha_state.json").exists(), "AWS state is collapsed after the run")
        self.assertEqual(self.cloud["sgs"], [])
        self.assertEqual(self.cloud["buckets"], set())
        rendered_calls = "\n".join(" ".join(call) for call in ssh_rsync_calls)
        self.assertIn("198.51.100.42", rendered_calls)
        self.assertNotIn("203.0.113.250", rendered_calls)
        self.assertNotIn(str(unrelated_key), rendered_calls)
        self.assertTrue(unrelated_key.is_file())

    def test_run_rejects_a_state_instance_without_stack_ownership(self):
        import subprocess

        aws = self.aws_alpha.AwsCli(
            region="us-east-1",
            runner=self.fake_aws_runner,
        )
        self.aws_alpha.up_alpha(
            aws=aws,
            identity=self.identity,
            stack="branchbench-alpha",
            runs_dir=self.runs,
            http_getter=lambda _url: "203.0.113.9",
        )
        self.cloud["instances"].append(
            {
                "InstanceId": "i-foreign",
                "State": {"Name": "running"},
                "InstanceType": "t3a.xlarge",
                "PublicIpAddress": "198.51.100.200",
                "Tags": [{"Key": "Project", "Value": "OtherApp"}],
            }
        )
        state = self.aws_alpha.load_state(self.runs)
        state["instance_id"] = "i-foreign"
        self.aws_alpha.save_state(state, self.runs)
        commands: list[list[str]] = []

        def record_command(cmd: list[str], **_kwargs):
            commands.append(cmd)
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

        with self.assertRaisesRegex(self.aws_alpha.AlphaError, "not owned"):
            self.aws_alpha.run_alpha_benchmarks(
                aws=aws,
                identity=self.identity,
                board=self.board,
                runs_dir=self.runs,
                cmd_runner=record_command,
            )
        self.assertEqual(commands, [])

    def test_foreign_or_untagged_bucket_is_refused(self):
        aws = self.aws_alpha.AwsCli(region="us-east-1", runner=self.fake_aws_runner)
        foreign_bucket = "ai-forking-ik2619-foreign-prod-bucket"
        self.cloud["buckets"].add(foreign_bucket)
        self.cloud.setdefault("bucket_tags", {})[foreign_bucket] = [
            {"Key": "Project", "Value": "OtherApp"},
            {"Key": "Phase", "Value": "prod"},
        ]
        with self.assertRaisesRegex(self.aws_alpha.AlphaError, "refusing to modify or delete pre-existing S3 bucket"):
            self.aws_alpha.up_alpha(
                aws=aws,
                identity=self.identity,
                stack="branchbench-alpha",
                bucket=foreign_bucket,
                runs_dir=self.runs,
                http_getter=lambda _url: "203.0.113.9",
            )
        self.assertIn(foreign_bucket, self.cloud["buckets"])

    def test_matching_bucket_tags_do_not_bypass_account_ownership(self):
        bucket = "ai-forking-ik2619-cross-account-alpha-site"
        self.cloud["buckets"].add(bucket)
        self.cloud.setdefault("bucket_owners", {})[bucket] = "999999999999"
        self.cloud.setdefault("bucket_tags", {})[bucket] = (
            self.aws_alpha.resource_tags(
                "branchbench-alpha",
                self.identity,
                bucket,
            )
        )
        aws = self.aws_alpha.AwsCli(
            region="us-east-1",
            runner=self.fake_aws_runner,
        )

        with self.assertRaisesRegex(self.aws_alpha.AlphaError, "403|owner"):
            self.aws_alpha.ensure_bucket(
                aws,
                "branchbench-alpha",
                bucket,
                self.identity,
            )
        self.assertFalse(
            any(
                operation in call
                for call in self.calls
                for operation in (
                    "put-bucket-tagging",
                    "put-bucket-encryption",
                    "put-object",
                    "delete-objects",
                    "delete-bucket",
                )
            )
        )

        self.aws_alpha.save_state(
            {
                "phase": "alpha",
                "stack": "branchbench-alpha",
                "daplab_project": self.identity.project,
                "billing_tag1": self.identity.owner,
                "account_id": self.identity.account_id,
                "region": "us-east-1",
                "bucket": bucket,
            },
            self.runs,
        )
        self.calls.clear()
        result = self.aws_alpha.collapse_alpha(
            aws=aws,
            identity=self.identity,
            board=self.board,
            runs_dir=self.runs,
        )
        self.assertFalse(result["collapsed"])
        self.assertEqual(result["unverified_buckets"], [bucket])
        self.assertIn(bucket, self.cloud["buckets"])
        self.assertFalse(any("delete-bucket" in call for call in self.calls))

    def test_pull_failure_preserves_worker_and_stdin_carries_secrets(self):
        import os
        import subprocess
        from unittest import mock

        self.write(self.payload())
        aws = self.aws_alpha.AwsCli(region="us-east-1", runner=self.fake_aws_runner)
        self.aws_alpha.up_alpha(
            aws=aws,
            identity=self.identity,
            stack="branchbench-alpha",
            runs_dir=self.runs,
            http_getter=lambda _url: "203.0.113.9",
        )
        captured_calls = []

        def failing_on_pull(cmd: list[str], **kwargs):
            captured_calls.append((cmd, kwargs))
            if len(captured_calls) == 4:
                return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="pull failed")
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

        with mock.patch.dict(os.environ, {"NEON_API_KEY_ORG": "super-secret-neon-key"}):
            with self.assertRaisesRegex(
                self.aws_alpha.AlphaError,
                "failed to pull results.*dead-man shutdown remains armed",
            ):
                self.aws_alpha.run_alpha_benchmarks(
                    aws=aws,
                    identity=self.identity,
                    stack="branchbench-alpha",
                    preset="mini",
                    lane="macro",
                    systems="dolt",
                    collapse_after=True,
                    board=self.board,
                    runs_dir=self.runs,
                    cmd_runner=failing_on_pull,
                )
        ssh_cmd, ssh_kwargs = captured_calls[2]
        self.assertIn("--lane macro", " ".join(ssh_cmd))
        self.assertNotIn("super-secret-neon-key", " ".join(ssh_cmd), "secret must not appear in ssh argv")
        self.assertIn("super-secret-neon-key", ssh_kwargs.get("input", ""), "secret is passed via ssh stdin")
        self.assertTrue(
            (self.runs / "aws_alpha_state.json").exists(),
            "failed artifact download must retain state for recovery",
        )
        self.assertEqual(self.cloud["instances"][0]["State"]["Name"], "running")
        self.assertNotEqual(self.cloud["sgs"], [])
        self.assertNotEqual(self.cloud["buckets"], set())
        self.assertFalse(
            any("terminate-instances" in call for call in self.calls),
            "failed artifact download must not destroy the only artifact copy",
        )

    def test_head_bucket_403_fails_closed_and_partial_collapse_retains_state_and_Sync_excludes_untracked(self):
        import subprocess
        aws = self.aws_alpha.AwsCli(region="us-east-1", runner=self.fake_aws_runner)
        forbidden_bucket = "ai-forking-ik2619-forbidden-bucket"
        self.cloud.setdefault("forbidden_buckets", set()).add(forbidden_bucket)
        with self.assertRaisesRegex(self.aws_alpha.AlphaError, "403|Forbidden"):
            self.aws_alpha.up_alpha(
                aws=aws,
                identity=self.identity,
                stack="branchbench-alpha",
                bucket=forbidden_bucket,
                runs_dir=self.runs,
                http_getter=lambda _url: "203.0.113.9",
            )
        self.cloud["forbidden_buckets"].remove(forbidden_bucket)
        recovered = self.aws_alpha.collapse_alpha(
            aws=aws,
            identity=self.identity,
            stack="branchbench-alpha",
            board=self.board,
            runs_dir=self.runs,
        )
        self.assertTrue(recovered["collapsed"])

        # Partial collapse keeps state file and returns collapsed=False
        state = self.aws_alpha.up_alpha(
            aws=aws,
            identity=self.identity,
            stack="branchbench-alpha",
            runs_dir=self.runs,
            http_getter=lambda _url: "203.0.113.9",
        )
        self.cloud.setdefault("fail_delete_buckets", set()).add(state["bucket"])
        partial = self.aws_alpha.collapse_alpha(
            aws=aws,
            identity=self.identity,
            stack="branchbench-alpha",
            board=self.board,
            runs_dir=self.runs,
        )
        self.assertFalse(partial["collapsed"])
        self.assertEqual(partial["remaining_bucket"], state["bucket"])
        self.assertTrue((self.runs / "aws_alpha_state.json").is_file(), "state file retained when bucket delete fails")

        # write_sync_filelist never includes untracked files, .git/config, or directories
        untracked = self.board / "untracked_secret_notes.txt"
        untracked.write_text("TOP_SECRET")
        flist = self.aws_alpha.write_sync_filelist(self.runs, "branchbench-alpha", self.aws_alpha.ROOT)
        lines = set(flist.read_text().splitlines())
        self.assertNotIn(".git/config", lines)
        self.assertNotIn(".git/refs", lines)
        self.assertNotIn(".git/objects", lines)
        self.assertFalse(any("untracked_secret_notes.txt" in ln for ln in lines))
        self.assertTrue(all((self.aws_alpha.ROOT / ln).is_file() for ln in lines))

        # _classify_head_bucket fails closed on empty stderr, network error (rc=255), or bucket named *404* with 403
        for bad_rc, bad_err in ((2, ""), (255, "Could not connect to the endpoint URL: https://s3.us-east-1.amazonaws.com/not-found")):
            bad_aws = self.aws_alpha.AwsCli(
                region="us-east-1",
                runner=lambda c, r=bad_rc, e=bad_err, **_kw: subprocess.CompletedProcess(c, r, stdout="", stderr=e),
            )
            with self.assertRaises(self.aws_alpha.AlphaError):
                self.aws_alpha._classify_head_bucket(
                    bad_aws,
                    "my-404-bucket",
                    self.identity.account_id,
                )

        # Custom bucket name (not starting with stack prefix) is still discovered by tags when state file is missing
        self.cloud["fail_delete_buckets"].clear()
        custom_bucket = "ai-forking-ik2619-custom-alpha-site-bucket"
        self.cloud["buckets"].add(custom_bucket)
        self.cloud.setdefault("bucket_tags", {})[custom_bucket] = [
            {"Key": "project", "Value": "ai-forking"},
            {"Key": "billing-tag1", "Value": "ik2619"},
            {"Key": "Project", "Value": "BranchBenchLeaderboard"},
            {"Key": "Phase", "Value": "alpha"},
            {"Key": "Stack", "Value": "branchbench-alpha"},
        ]
        (self.runs / "aws_alpha_state.json").unlink(missing_ok=True)
        col_all = self.aws_alpha.collapse_alpha(
            aws=aws,
            identity=self.identity,
            stack="branchbench-alpha",
            board=self.board,
            runs_dir=self.runs,
        )
        self.assertTrue(col_all["collapsed"])
        self.assertIn(custom_bucket, col_all["deleted_buckets"])
        self.assertEqual(self.cloud["buckets"], set())

    def test_sync_git_index_reconstruction_elf_check_and_bucket_tag_fail_closed(self):
        import subprocess

        # 1. Real temp git repo: write_sync_filelist + rsync --files-from + reconstruct_synced_git_index leaves clean git status
        src_repo = self.board / "src_repo"
        dst_repo = self.board / "dst_repo"
        for rel in (
            "leaderboard/x.py",
            "microbench/x.py",
            "macrobench/x.py",
            "dblib/x.py",
            "util/x.py",
            "pyproject.toml",
            "uv.lock",
            "build_protos.py",
        ):
            p = src_repo / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text("# tracked\n")
        subprocess.run(["git", "init", str(src_repo)], check=True, capture_output=True)
        subprocess.run(["git", "-C", str(src_repo), "config", "user.email", "test@example.com"], check=True)
        subprocess.run(["git", "-C", str(src_repo), "config", "user.name", "Test"], check=True)
        subprocess.run(["git", "-C", str(src_repo), "add", "."], check=True)
        subprocess.run(["git", "-C", str(src_repo), "commit", "-m", "init"], check=True, capture_output=True)
        (src_repo / "leaderboard" / "secret_untracked.txt").write_text("do not sync")
        stale_dataset = src_repo / "leaderboard" / "data" / "tpcc_10w.sql"
        stale_dataset.parent.mkdir()
        stale_dataset.write_text("do not sync")

        flist = self.aws_alpha.write_sync_filelist(self.runs, "branchbench-alpha", src_repo)
        self.assertNotIn("leaderboard/data/tpcc_10w.sql", flist.read_text().splitlines())
        dst_repo.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            ["rsync", "-a", f"--files-from={flist}", f"{src_repo}/", f"{dst_repo}/"],
            check=True,
        )
        self.assertFalse((dst_repo / ".git" / "index").exists(), "index is reconstructed on the destination, not copied")
        self.assertFalse((dst_repo / "leaderboard" / "data" / "tpcc_10w.sql").exists())
        self.aws_alpha.reconstruct_synced_git_index(dst_repo)
        self.assertTrue((dst_repo / ".git" / "index").is_file())
        st = subprocess.run(
            [
                "git",
                "-C",
                str(dst_repo),
                "status",
                "--porcelain",
                "--",
                "leaderboard",
                "microbench",
                "macrobench",
                "dblib",
                "util",
                "pyproject.toml",
                "uv.lock",
                "build_protos.py",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        self.assertEqual(st.stdout.strip(), "")

        # 2. _is_linux_x86_64_elf distinguishes Linux x86_64 ELF from macOS Mach-O
        macho = self.board / "mo-macho"
        macho.write_bytes(b"\xcf\xfa\xed\xfe" + b"\x00" * 32)
        self.assertFalse(self.aws_alpha._is_linux_x86_64_elf(macho))
        elf_x64 = self.board / "mo-elf"
        elf_x64.write_bytes(b"\x7fELF\x02\x01\x01" + b"\x00" * 9 + b"\x02\x00\x3e\x00" + b"\x00" * 16)
        self.assertTrue(self.aws_alpha._is_linux_x86_64_elf(elf_x64))

        # 3. run_alpha_benchmarks rejects --profile-mode paper and fails closed on remote lock (42) or journal (43)
        aws = self.aws_alpha.AwsCli(region="us-east-1", runner=self.fake_aws_runner)
        self.aws_alpha.up_alpha(
            aws=aws,
            identity=self.identity,
            stack="branchbench-alpha",
            runs_dir=self.runs,
            http_getter=lambda _url: "203.0.113.9",
        )
        with self.assertRaisesRegex(self.aws_alpha.AlphaError, "paper_separated_vms|economic"):
            self.aws_alpha.run_alpha_benchmarks(
                aws=aws,
                identity=self.identity,
                stack="branchbench-alpha",
                preset="full",
                profile_mode="paper",
                board=self.board,
                runs_dir=self.runs,
            )
        for code, pat in ((42, "active benchmark lock"), (43, "unrecovered journal")):
            with self.assertRaisesRegex(self.aws_alpha.AlphaError, pat):
                self.aws_alpha.run_alpha_benchmarks(
                    aws=aws,
                    identity=self.identity,
                    stack="branchbench-alpha",
                    collapse_after=True,
                    board=self.board,
                    runs_dir=self.runs,
                    cmd_runner=lambda cmd, rc=code, **_kw: subprocess.CompletedProcess(cmd, rc, stdout="", stderr=""),
                )
            self.assertTrue(
                (self.runs / "aws_alpha_state.json").is_file(),
                "rejected duplicate invocation must NOT collapse the active worker even when collapse_after=True",
            )
            self.assertEqual(self.cloud["instances"][0]["State"]["Name"], "running")

        # 3b. Activation script uses portable lock_fd (no external flock binary required), preserves run.lock inode,
        # rejects when run.lock is held (exit 42), preserves original results on backup cp failure (exit 1),
        # and restores ~/db-fork on checkout update failure (exit 1)
        import fcntl

        fake_home = self.board / "fake_home"
        old_repo = fake_home / "db-fork"
        runs_sub = old_repo / "leaderboard" / ".runs"
        runs_sub.mkdir(parents=True, exist_ok=True)
        run_lock_file = runs_sub / "run.lock"
        run_lock_file.write_text("")
        orig_lock_inode = run_lock_file.stat().st_ino
        (runs_sub / "file_copy.prev_method").write_text("reflink\n")
        (old_repo / "leaderboard" / "dolt" / "results" / "20261020").mkdir(parents=True, exist_ok=True)
        sole_result = old_repo / "leaderboard" / "dolt" / "results" / "20261020" / "box.json"
        sole_result.write_text('{"ok": true}\n')
        staged_repo = fake_home / "db-fork.sync.t1"
        shutil.copytree(dst_repo, staged_repo)

        # Holding batch.lock or run.lock causes build_activate_sync_script to exit 42 without truncating batch.lock or replacing run.lock inode
        batch_lock_file = runs_sub / "batch.lock"
        batch_lock_file.write_text("active-batch-token-xyz\n")
        with batch_lock_file.open("a+") as bfh:
            fcntl.flock(bfh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            batch_locked_res = subprocess.run(
                ["bash", "-c", self.aws_alpha.build_activate_sync_script("~/db-fork.sync.t1")],
                env={**os.environ, "HOME": str(fake_home)},
                capture_output=True,
                text=True,
            )
        self.assertEqual(batch_locked_res.returncode, 42)
        self.assertEqual(batch_lock_file.read_text(), "active-batch-token-xyz\n", "rejected invocation must not truncate active batch token")

        with run_lock_file.open("a+") as lfh:
            fcntl.flock(lfh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            locked_res = subprocess.run(
                ["bash", "-c", self.aws_alpha.build_activate_sync_script("~/db-fork.sync.t1")],
                env={**os.environ, "HOME": str(fake_home)},
                capture_output=True,
                text=True,
            )
        self.assertEqual(locked_res.returncode, 42)
        self.assertEqual(run_lock_file.stat().st_ino, orig_lock_inode)

        # Failed shutdown scheduling (sudo returning 1) before build_activate_sync_script prevents activation and worker execution
        worker_marker = fake_home / "worker_ran"
        chain_res = subprocess.run(
            [
                "bash",
                "-c",
                f"false && {self.aws_alpha.build_activate_sync_script('~/db-fork.sync.t1')} && touch {worker_marker}",
            ],
            env={**os.environ, "HOME": str(fake_home)},
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(chain_res.returncode, 0)
        self.assertFalse(worker_marker.exists(), "failed shutdown scheduling must prevent activation and worker execution")
        self.assertTrue(staged_repo.is_dir(), "activation did not run after failed shutdown scheduling")

        # Injected backup copy failure: unreadable result file causes cp -a to fail with exit 1 (not 42), keeping sole_result intact
        sole_result.chmod(0o000)
        try:
            cp_fail_res = subprocess.run(
                ["bash", "-c", self.aws_alpha.build_activate_sync_script("~/db-fork.sync.t1")],
                env={**os.environ, "HOME": str(fake_home)},
                capture_output=True,
                text=True,
            )
            self.assertEqual(cp_fail_res.returncode, 1, f"expected exit 1 on cp failure, got {cp_fail_res.returncode}: {cp_fail_res.stderr}")
            self.assertTrue((runs_sub / "file_copy.prev_method").is_file())
            self.assertEqual(run_lock_file.stat().st_ino, orig_lock_inode)
        finally:
            sole_result.chmod(0o644)
        self.assertTrue(sole_result.is_file(), "sole old result in ~/db-fork survives failed preservation copy")

        # Injected final checkout update failure: read-only subdirectory with an untracked file blocks rsync --delete with exit 1
        ro_sub = old_repo / "microbench" / "locked_subdir"
        ro_sub.mkdir(parents=True, exist_ok=True)
        (ro_sub / "f.txt").write_text("x")
        ro_sub.chmod(0o500)
        try:
            up_fail_res = subprocess.run(
                ["bash", "-c", self.aws_alpha.build_activate_sync_script("~/db-fork.sync.t1")],
                env={**os.environ, "HOME": str(fake_home)},
                capture_output=True,
                text=True,
            )
            self.assertEqual(up_fail_res.returncode, 1)
            self.assertTrue(sole_result.is_file())
            self.assertTrue((runs_sub / "file_copy.prev_method").is_file())
            self.assertEqual(run_lock_file.stat().st_ino, orig_lock_inode)
        finally:
            ro_sub.chmod(0o755)
            shutil.rmtree(ro_sub, ignore_errors=True)

        # Successful activation updates ~/db-fork in-place while preserving sole_result, file_copy.prev_method, and run.lock inode
        ok_res = subprocess.run(
            ["bash", "-c", self.aws_alpha.build_activate_sync_script("~/db-fork.sync.t1")],
            env={**os.environ, "HOME": str(fake_home)},
            capture_output=True,
            text=True,
        )
        self.assertEqual(ok_res.returncode, 0, ok_res.stderr)
        self.assertTrue(sole_result.is_file())
        self.assertTrue((runs_sub / "file_copy.prev_method").is_file())
        self.assertEqual(run_lock_file.stat().st_ino, orig_lock_inode)

        # 3c. Upload failure releases the remote lease in finally; newer micro error record supersedes older economic_single_vm success
        upload_calls = []

        def fail_on_rsync_up(cmd: list[str], **_kw):
            upload_calls.append(cmd)
            if len(upload_calls) == 2:
                return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="rsync failed")
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

        with self.assertRaisesRegex(self.aws_alpha.AlphaError, "failed to rsync"):
            self.aws_alpha.run_alpha_benchmarks(
                aws=aws,
                identity=self.identity,
                stack="branchbench-alpha",
                systems="dolt",
                board=self.board,
                runs_dir=self.runs,
                cmd_runner=fail_on_rsync_up,
            )
        self.assertEqual(len(upload_calls), 3, "wait_cmd, failed sync_up, and finally lease release ssh")
        self.assertIn("alpha.owner", " ".join(upload_calls[2]))

        # Lease release failure when collapse_after=False returns nonzero (42) even when benchmark run succeeded
        rel_fail_calls = []

        def fail_on_lease_release(cmd: list[str], **_kw):
            rel_fail_calls.append(cmd)
            if len(rel_fail_calls) == 5:
                return subprocess.CompletedProcess(cmd, 42, stdout="", stderr="lease release failed")
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

        rc_rel_fail = self.aws_alpha.run_alpha_benchmarks(
            aws=aws,
            identity=self.identity,
            stack="branchbench-alpha",
            systems="dolt",
            deploy_site_after=False,
            collapse_after=False,
            board=self.board,
            runs_dir=self.runs,
            cmd_runner=fail_on_lease_release,
        )
        self.assertEqual(rc_rel_fail, 42, "failed lease release when retaining instance must return nonzero")

        # Lease aged 21,601s (>6h, within 12h batch deadline / 24h TTL) still blocks check_alpha_lease and wait_script;
        # and if teardown ownership check fails (exit 42) when collapse_after=True, collapse_alpha is skipped
        import time as _time

        six_hr_plus_ts = int(_time.time()) - 21601
        wait_script_under_test = rel_fail_calls[0][-1].replace(
            "/var/lib/branchbench-alpha-ready", str(fake_home / "ready_marker")
        )
        (fake_home / "ready_marker").write_text("ok\n")
        for bad_or_active_owner in (
            f"foreign_token:{six_hr_plus_ts}:86400\n",
            f"foreign_token:{six_hr_plus_ts}\n",
            f"foreign_token:{six_hr_plus_ts}:0\n",
            f"foreign_token:{six_hr_plus_ts}:-5\n",
            f"foreign_token:{six_hr_plus_ts}:\n",
            f"foreign_token:{six_hr_plus_ts}:NaN\n",
            "foreign_token:NaN:86400\n",
            "foreign_token:0:86400\n",
            f": {six_hr_plus_ts}:86400\n",
            f"foreign_token:{six_hr_plus_ts}:86400:extra\n",
            f"foreign:1:1\nforeign:{int(_time.time())}:86400",
        ):
            (fake_home / "db-fork.alpha.owner").write_text(bad_or_active_owner)
            (runs_sub / "alpha.owner").write_text(bad_or_active_owner)
            with self.assertRaisesRegex(micro.RunError, "another alpha sync/benchmark session owns this worker"):
                micro.check_alpha_lease(runs_sub)
            ws_res = subprocess.run(
                ["bash", "-c", wait_script_under_test],
                env={**os.environ, "HOME": str(fake_home)},
                capture_output=True,
                text=True,
            )
            self.assertEqual(ws_res.returncode, 42, f"wait_script must exit 42 for {bad_or_active_owner!r}")
            self.assertEqual((fake_home / "db-fork.alpha.owner").read_text(), bad_or_active_owner)
            self.assertEqual((runs_sub / "alpha.owner").read_text(), bad_or_active_owner)
        (fake_home / "db-fork.alpha.owner").unlink(missing_ok=True)
        (runs_sub / "alpha.owner").unlink(missing_ok=True)

        collapse_verify_fail_calls = []

        def fail_on_teardown_verify(cmd: list[str], **_kw):
            collapse_verify_fail_calls.append(cmd)
            if len(collapse_verify_fail_calls) == 5:
                return subprocess.CompletedProcess(cmd, 42, stdout="", stderr="ownership lost")
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

        rc_col_verify_fail = self.aws_alpha.run_alpha_benchmarks(
            aws=aws,
            identity=self.identity,
            stack="branchbench-alpha",
            systems="dolt",
            deploy_site_after=False,
            collapse_after=True,
            board=self.board,
            runs_dir=self.runs,
            cmd_runner=fail_on_teardown_verify,
        )
        self.assertEqual(rc_col_verify_fail, 42, "lost ownership before teardown must return nonzero and skip collapse")
        self.assertTrue((self.runs / "aws_alpha_state.json").is_file(), "worker must not be collapsed when ownership check fails")
        self.assertNotIn("rm -f ~/db-fork.alpha.owner ", " ".join(collapse_verify_fail_calls[4]), "collapse path refreshes owner rather than deleting it")

        # Injected write failure in teardown finalizer preserves both owner files intact and exits nonzero
        col_teardown_script = collapse_verify_fail_calls[4][-1]
        col_token_match = re.search(r"\^([0-9_]+):", col_teardown_script)
        self.assertIsNotNone(col_token_match)
        col_token = col_token_match.group(1)
        orig_owner_line = f"{col_token}:9999999999:86400\n"
        (fake_home / "db-fork.alpha.owner").write_text(orig_owner_line)
        (runs_sub / "alpha.owner").write_text(orig_owner_line)
        printf_fail_res = subprocess.run(
            ["bash", "-c", f"printf() {{ return 1; }}; {col_teardown_script}"],
            env={**os.environ, "HOME": str(fake_home)},
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(printf_fail_res.returncode, 0, "failed owner write in teardown finalizer must exit nonzero")
        self.assertEqual((fake_home / "db-fork.alpha.owner").read_text(), orig_owner_line)
        self.assertEqual((runs_sub / "alpha.owner").read_text(), orig_owner_line)

        # Execute the complete generated remote_script with a failing sudo stub and assert worker does not execute
        import shlex as _shlex

        remote_bash_arg = _shlex.split(rel_fail_calls[2][-1])[-1]
        token_match = re.search(r"DBFORK_ALPHA_TOKEN=([^\s&]+)", remote_bash_arg)
        self.assertIsNotNone(token_match)
        captured_token = _shlex.split(token_match.group(1))[0]
        (fake_home / "db-fork.alpha.owner").write_text(f"{captured_token}:9999999999\n")
        fake_bin = fake_home / ".local" / "bin"
        fake_bin.mkdir(parents=True, exist_ok=True)
        sudo_stub = fake_bin / "sudo"
        sudo_stub.write_text("#!/bin/sh\nexit 1\n")
        sudo_stub.chmod(0o755)
        worker_script = old_repo / "leaderboard" / "aws_worker.sh"
        worker_script.parent.mkdir(parents=True, exist_ok=True)
        worker_ran_marker = fake_home / "dummy_worker_executed"
        worker_script.write_text(f"#!/bin/sh\ntouch {worker_ran_marker}\nexit 0\n")
        worker_script.chmod(0o755)
        try:
            full_remote_res = subprocess.run(
                ["bash", "-c", remote_bash_arg],
                env={**os.environ, "HOME": str(fake_home)},
                capture_output=True,
                text=True,
            )
        finally:
            worker_script.unlink(missing_ok=True)
        self.assertNotEqual(full_remote_res.returncode, 0, "failing sudo in complete remote_script must exit nonzero")
        self.assertFalse(worker_ran_marker.exists(), "failing sudo in complete remote_script must prevent worker execution")

        # Newer error record (with or without execution_profile) or invalid execution_profile supersedes older economic_single_vm success
        from leaderboard import build as micro_build

        self.write({**self.payload(), "execution_profile": "economic_single_vm"}, day_dir="20260930")
        err_path = self.board / "dolt" / "results" / "20261001" / "box.json"
        err_path.parent.mkdir(parents=True, exist_ok=True)
        err_path.write_text(json.dumps({"error": "RunError: failed"}))
        entries_after_err, excluded_after_err = micro_build.build(self.board)
        self.assertEqual(entries_after_err, [], "newer error record must supersede older economic_single_vm success")
        self.assertEqual(len(excluded_after_err), 1)
        err_path.write_text(json.dumps({**self.payload(date="2026-10-01"), "execution_profile": "typo_profile"}))
        entries_after_typo, excluded_after_typo = micro_build.build(self.board)
        self.assertEqual(entries_after_typo, [], "newer invalid execution_profile must supersede older economic_single_vm success")
        self.assertEqual(len(excluded_after_typo), 1)
        err_path.unlink()

        # 4. Bucket tag inspection failure (get-bucket-tagging rc=1) causes collapse_alpha to report collapsed=False
        self.aws_alpha.state_file_path(self.runs).unlink(missing_ok=True)
        unreadable_bucket = "ai-forking-ik2619-unreadable-tags"
        self.cloud["buckets"].add(unreadable_bucket)
        self.cloud.get("bucket_tags", {}).pop(unreadable_bucket, None)
        col = self.aws_alpha.collapse_alpha(
            aws=aws,
            identity=self.identity,
            stack="branchbench-alpha",
            board=self.board,
            runs_dir=self.runs,
        )
        self.assertFalse(col["collapsed"], "unreadable bucket tags must fail closed during collapse")
        self.assertIn(unreadable_bucket, col["remaining_buckets"])


if __name__ == "__main__":
    unittest.main()
