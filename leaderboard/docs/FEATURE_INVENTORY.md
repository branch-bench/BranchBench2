# Leaderboard Feature Inventory

This document is the behavior contract for `leaderboard/`. The inventory describes the current code and tests. The counts below preserve the pre-change baseline used to bound the completed reorganization. Proposed production automation is not listed as existing behavior.

## Pre-change scope and baseline

- Scope: `leaderboard/` only.
- Tracked files: 91.
- Tracked text: 17,615 lines.
- Python: 27 files and 11,381 lines.
- Runtime Python: 21 files and 7,276 lines.
- Python tests: 6 files and 4,105 lines.
- Browser assets and page tests: 18 files and 4,785 text lines.
- Maximum tracked directory nesting: three directories below `leaderboard/`.
- Direct Python dependency roots outside `leaderboard`: 7. The repository dependencies are `dblib`, `macrobench`, `microbench`, and `util`. The installed dependencies are `google.protobuf`, `psycopg2`, and `pyarrow`.
- No leaderboard-specific formatter, linter, or type checker is configured.

The main verification commands are:

```bash
uv run --frozen python -m pytest -q leaderboard
node leaderboard/test_page.cjs
uv run --frozen python -m leaderboard.site --phase local
```

Runtime and generated paths are outside the structural refactor. These paths include `.runs/`, `.site/`, `__pycache__/`, and generated `results/`, `macro/`, and `verbs/` artifacts. Existing published artifacts are inputs to validation and publication, so a later plan must not rewrite them silently.

## Entry points

The package exposes these current command contracts:

- `python -m leaderboard.run`
- `python -m leaderboard.run_macro`
- `python -m leaderboard.verbs`
- `python -m leaderboard.validate`
- `python -m leaderboard.validate_macro`
- `python -m leaderboard.build`
- `python -m leaderboard.build_macro`
- `python -m leaderboard.site`
- `python -m leaderboard.serve`
- `python -m leaderboard.aws_worker`
- `python -m leaderboard.aws_alpha`
- `leaderboard/aws_worker.sh`
- `leaderboard/aws_alpha.sh`

## Behavior inventory

| ID | Feature | User-visible behavior | Entry point | Implementing files | Covering tests | Edge cases and business rules |
|---|---|---|---|---|---|---|
| F01 | Database system discovery | A database system becomes runnable when its directory has valid `system.json` metadata and a `connector.py`. The runner loads its exported `CONNECTOR`. | All benchmark CLIs | `connector.py` and `systems/*/` | `Connectors.test_every_connector_in_the_repository_fills_the_contract`<br>`Connectors.test_adding_a_system_is_adding_its_folder`<br>`Connectors.test_system_metadata_rejects_unsafe_or_ambiguous_fields`<br>`Launcher.test_missing_connectors_cannot_generate_scripts` | The public integration location is `leaderboard/systems/<system>/`.<br>A metadata-only directory is not runnable.<br>The current `tiger` directory is metadata-only.<br>The shared catalog validates every manifest.<br>A change outside `leaderboard/` fails the contribution boundary test. |
| F02 | Bundled database integrations | BranchBench includes connectors for Chronos, Doltgres, Dolt, file-copy PostgreSQL, MatrixOne, Neon, SeekDB, and Xata. Self-hosted systems also supply lifecycle scripts. | Connector discovery and benchmark CLIs | `systems/chronos/`<br>`systems/dolt/`<br>`systems/dolt_mysql/`<br>`systems/file_copy/`<br>`systems/matrixone/`<br>`systems/neon/`<br>`systems/seekdb/`<br>`systems/xata/` | Connector contract tests<br>File-copy method restoration test<br>MatrixOne data-directory relocation test<br>Dolt cleanup tests<br>Connector scope tests | Self-hosted and hosted systems have different lifecycle rules.<br>Connection scope must not expose secrets.<br>File-copy PostgreSQL restores its prior setting.<br>Dolt integrations remove dropped-database trash.<br>MatrixOne relocates its data directory while retaining its launch configuration.<br>⚠ UNPROTECTED: Real vendor installation, startup, and live service calls are not exercised by the unit suite. |
| F03 | Safe server lifecycle and recovery | A runner starts only the selected self-hosted server. It records recoverable cleanup state before database creation. It cleans the database and stops the server after success or failure. | `leaderboard.run`<br>`leaderboard.run_macro`<br>`leaderboard.verbs` | `run.py`<br>`run_macro.py`<br>`verbs.py`<br>System lifecycle scripts | `Runner` in `test_leaderboard.py`<br>`Recovery`, `RunLifecycle`, and `Deadlines` in `test_runner_lifecycle.py`<br>`Runner` and `ChildProcess` in `test_macro.py` | One system lock prevents concurrent runs of the same target.<br>A private journal is written before creation.<br>Unreadable, legacy, or scope-mismatched journals remain for operator review.<br>The runner never claims an unrelated PID or server port.<br>Cleanup and stop have deadlines.<br>Signals terminate child process groups and still run required cleanup.<br>A failed cleanup keeps the journal. |
| F04 | Managed service execution | Hosted connectors read credentials from the launched process environment. They run a preflight before creating resources and record service location data. | `leaderboard.run`<br>`leaderboard.run_macro --allow-hosted`<br>`leaderboard.verbs --allow-hosted` | `run.py`<br>`run_macro.py`<br>`verbs.py`<br>`systems/neon/connector.py`<br>`systems/xata/connector.py` | Hosted result validation tests<br>Connector scope tests<br>Hosted lifecycle tests<br>Launcher credential tests | Credential variable names come from validated metadata. Microbench requires a client location.<br>Macrobench and verb scaling reject hosted systems unless the caller opts in.<br>Hosted systems do not use local start or stop scripts.<br>Network statements receive a timeout.<br>Secrets must not enter artifacts or error text.<br>⚠ UNPROTECTED: Unit tests do not call real managed service APIs. |
| F05 | Operations microbenchmark | The runner loads the fixed dataset and measures eight configured branch, read, and write workloads for three tries. It writes one result or one error record for the system, date, and machine. | `python -m leaderboard.run` | `run.py`<br>`suite.json` | `Workload` in `test_workload.py`<br>`ResultFiles` and `Runner` in `test_leaderboard.py`<br>`RunLifecycle` in `test_runner_lifecycle.py` | The runner executes configured workload rows in order. It does not run all eight rows concurrently.<br>Each data workload uses an isolated branch.<br>A failed try does not change later tries or rows.<br>Non-OK operation results do not become timings.<br>Batch totals omit wrapper calls and reject failed operations.<br>A newer error hides an older success for the same identity.<br>Success requires unchanged source and successful cleanup. |
| F06 | Branch-verb scaling benchmark | The runner measures the branch-verb grid across selected verbs and scale axes. It records repetitions, warmups, latency quantiles, cleanup, and provenance in a separate artifact. | `python -m leaderboard.verbs` | `verbs.py` | Verb wrapper and child termination tests in `test_runner_lifecycle.py`<br>Artifact validation in `test_leaderboard.py` | Scale and repetition counts must be valid.<br>Verb and axis names must be known and unique.<br>All expected grid points must be present.<br>Latency values must be finite, non-negative, and ordered by quantile.<br>Hosted execution requires explicit opt-in.<br>A timeout or failed point cannot become a verified result. |
| F07 | Agent workflow campaign | One campaign runs the six S1 to S6 scenarios in order. Each scenario uses a separate child process and temporary database. The scenario can run concurrent workers according to its workload configuration. | `python -m leaderboard.run_macro` | `run_macro.py`<br>`validate_macro.py`<br>Upstream scenario configurations | `Classifier`, `Contract`, `Runner`, and `ChildProcess` in `test_macro.py` | The campaign record exists before the server starts.<br>The outer runner executes scenarios sequentially.<br>A cleanup failure stops the remaining scenarios.<br>Each attempt is classified as verified, incomplete, failed, timed out, or unverified.<br>Only verified attempts receive completion time.<br>A valid campaign contains every scenario and never cherry-picks successful attempts.<br>Mini is for smoke testing. Production accepts full campaigns only. |
| F08 | Artifact validation and secret scrubbing | Validators reject malformed, inconsistent, incomparable, or secret-bearing records before the site consumes them. They report validation problems instead of raising on malformed artifacts. | `leaderboard.validate`<br>`leaderboard.validate_macro`<br>Site assembly | `validate.py`<br>`validate_macro.py`<br>`verbs.py` | `ResultFiles` in `test_leaderboard.py`<br>`Classifier` and `Contract` in `test_macro.py`<br>Manifest tests in `test_site.py` | Paths must follow the system, artifact kind, date, and filename contracts.<br>Numeric evidence must be finite and non-negative.<br>Macro evidence is allowlisted and bounded.<br>Credential-looking keys and values are rejected.<br>Source, lock, workload, runtime, machine, and environment identities are checked.<br>Changed or dirty sources cannot silently become official results. |
| F09 | Result selection and comparison cohorts | The builders select current valid records and keep incomparable environments in separate groups. Failed and excluded records retain an explanation. | `leaderboard.build`<br>`leaderboard.build_macro`<br>Site assembly | `build.py`<br>`build_macro.py` | Result selection tests in `test_leaderboard.py`<br>Campaign build and group-key tests in `test_macro.py`<br>Source filter and provenance tests in `test_site.py` | The newest micro record wins per system, machine, and execution profile.<br>A newer invalid record does not expose an older success.<br>Macro groups bind workload, source, lock, runtime, machine, environment, execution profile, and native-only mode.<br>Failed workflow outcomes remain visible inside a valid campaign.<br>Invalid campaigns are excluded as a unit. |
| F10 | Controlled publication and site phases | The assembler creates local, alpha, or production static sites. Production includes only artifacts named in `publish.json`. | `python -m leaderboard.site`<br>`python -m leaderboard.build`<br>`python -m leaderboard.build_macro` | `site.py`<br>`build.py`<br>`build_macro.py`<br>`publish.json`<br>`mock.py` | `Phase`, `Local`, `Manifest`, and `Cli` in `test_site.py` | Local includes validated disk results, mock rows, documentation, and the run wizard.<br>Alpha includes real local artifacts but excludes mocks and the run wizard.<br>Production requires an explicit manifest and comparable provenance.<br>Site output is atomic.<br>Failed assembly preserves the previous output.<br>Symlinked outputs, inputs, and sidecars fail closed.<br>Mocks disappear when a real result exists. |
| F11 | Leaderboard interface and scoring | The static page ranks microbenchmark categories and one selected macro workflow. It provides filters, URL state, workload evidence, exclusions, and responsive accessible controls. | `leaderboard/index.html` | `index.html`<br>`styles.css`<br>`js/*.js`<br>`workflow-diagrams/` | `test_page.cjs`<br>Static asset and script-order tests in `test_site.py` | Micro scores compare only compatible groups and use best or selected tries.<br>Macro ranks one workflow at a time and has no composite score.<br>The branch-operation table reports mean successful call time.<br>Time in branch operations equals successful seconds for nine branch verbs divided by those seconds plus successful `EXEC` seconds.<br>The ratio adds concurrent worker call time, so it can exceed wall time before division.<br>Incomplete comparisons remain visible but unranked.<br>⚠ UNPROTECTED: Automated tests do not provide full real-browser visual and assistive-technology coverage. |
| F12 | Local command review and launch | The local wizard builds a shell script for selected systems. The server launches only the exact reviewed script from a local origin. | `python -m leaderboard.serve`<br>`leaderboard/run.html` | `serve.py`<br>`run.html` | `Launcher` and `Served` in `test_serve.py`<br>Wizard tests in `test_page.cjs` | Inputs are validated and shell-quoted.<br>Micro runs stop at the first failure.<br>Macro runs retain the first failure, attempt each selected system, and rebuild the site.<br>Unknown systems and hosted macro selections are rejected.<br>Missing credentials are requested without echo and exist only in the launched process environment.<br>The published site has no launcher. |
| F13 | Economic AWS worker | A Linux worker runs selected systems under one batch lock. It performs host checks, runs microbench and macrobench sequentially, validates new artifacts, and can power off after the batch. | `python -m leaderboard.aws_worker`<br>`leaderboard/aws_worker.sh` | `aws_worker.py`<br>`aws_worker.sh` | `AwsWorker` in `test_leaderboard.py` | The worker supports the economic single-VM execution profile only.<br>It rejects active conflicting services, occupied database ports, dirty runtime state, and unsuitable reflink storage.<br>The batch has one deadline.<br>Interruption or dirty host state stops later systems.<br>Poweroff happens only after its preflight succeeds.<br>Child process groups are terminated on timeout.<br>⚠ UNPROTECTED: Full-scale resource capacity and real EC2 storage behavior require the planned hardware calibration. |
| F14 | DAPLab AWS alpha environment | The alpha command creates one low-cost smoke worker and S3 site, syncs tracked code, runs benchmarks, pulls artifacts, deploys the alpha site, reports status, and removes owned resources. | `python -m leaderboard.aws_alpha`<br>`leaderboard/aws_alpha.sh` | `aws_alpha.py`<br>`aws_alpha.sh` | `AwsAlphaLifecycle` in `test_site.py` | The registered `project` and owner `billing-tag1` are mandatory on every created resource.<br>Names follow `{project}-{owner}-{purpose}`.<br>Every live command verifies the expected shared account ID and named IAM user.<br>The alpha environment is locked to `us-east-1`.<br>The smoke profile locks the compute size to `t3a.xlarge` and uses standard CPU credits. It defaults to a 64 GiB encrypted `gp3` root volume.<br>Foreign, untagged, wrong-region, or cross-account resources fail closed.<br>S3 data calls bind the expected bucket owner.<br>Sync excludes untracked files and reconstructs the tracked Git index.<br>Collapse removes only resources with matching ownership tags and retains state when a reference is unresolved.<br>Collapse-after runs after a benchmark failure only when artifact recovery succeeded.<br>A failed artifact download retains the worker only until its dead-man shutdown.<br>⚠ UNPROTECTED: Tests mock AWS CLI calls and do not provision a live AWS account. |

## Current structural contracts

1. External contributors add `leaderboard/systems/<system>/`.
2. Connector discovery imports `leaderboard.systems.<system>.connector` dynamically.
3. Result, macro, and verb artifact paths remain under `leaderboard/<system>/`.
4. Tests import root modules such as `leaderboard.run`, `leaderboard.site`, and `leaderboard.aws_worker` directly.
5. Shell scripts and documentation invoke the current module names.
6. Static site files use a fixed script order and global browser scope.

## Known gaps and conflicts

These items are not current features. They must not be hidden inside a behavior-preserving reorganization.

- No command watches a merged pull request.
- No production launcher creates one EC2 worker for each database system.
- No production command uploads artifacts, edits `publish.json`, builds the production site, deploys it, and records a release as one workflow.
- No Fable review integration exists.
- The fixed `m7a.2xlarge` production profile is documented but is not a production launcher configuration.
- Per-database EC2 profile selection does not exist.
- No AcmeDB integration exists. AcmeDB is documentation sample text only.

The production automation is new behavior. It needs a separate change after the structural refactor or before it in a separate commit.
