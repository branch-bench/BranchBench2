# BranchBench Database Developer Guide

This document explains how to integrate a database system into the BranchBench leaderboard and verify it locally before submitting a pull request. For the technical architecture of the local runner and official cloud VM infrastructure, see [`architecture.html`](architecture.html).

---

## Core Isolation Rule

All leaderboard code, database lifecycle scripts, and connector adapters must live inside **`leaderboard/`**.

- **Never modify files outside `leaderboard/`** (`dblib/`, `microbench/`, `macrobench/`, `util/`, `tests/` are upstream read-only packages).
- If an upstream adapter in `dblib/` needs leaderboard-specific wiring or a compatibility wrapper, place that logic inside `leaderboard/systems/<system>/connector.py`.
- The automated test `Connectors.test_no_files_outside_leaderboard_differ_from_main_merge_base` in `leaderboard/test_leaderboard.py` fails if any file outside `leaderboard/` differs from `origin/main`.

---

## How to Submit a Database System (`leaderboard/systems/<system>/`)

Contributors submit one integration folder at `leaderboard/systems/<system>/` (where `<system>` matches `^[a-z][a-z0-9_]*$`). You never submit pre-recorded benchmark timing JSON from a personal workstation. Official production rankings come from a dedicated ephemeral cloud VM (`1 Self-Hosted DB = 1 Cloud VM`). Generated evidence remains at `leaderboard/<system>/results/`, `leaderboard/<system>/macro/`, and `leaderboard/<system>/verbs/`.

An integration directory contains:

```text
leaderboard/systems/<system>/
├── system.json      # System metadata, dialect, hosting mode, capability tags
├── connector.py     # Exports CONNECTOR (subclass of leaderboard.connector.Connector)
├── install          # Executable: installs binaries/containers (required when hosted="no")
├── start            # Executable: starts local server on 127.0.0.1 (required when hosted="no")
├── check            # Executable: exits 0 when ready for SQL (required when hosted="no")
└── stop             # Executable: stops server cleanly (required when hosted="no")
```

You can also generate a starter scaffold interactively at `http://127.0.0.1:8000/run.html` (under **Submit a new database to the leaderboard**).

### Step 1: Create `leaderboard/systems/<system>/system.json`

Every system folder requires a `system.json` manifest matching the validator contract:

```json
{
  "system": "AcmeDB",
  "proprietary": "no",
  "hosted": "no",
  "tuned": "no",
  "tags": ["SQL", "Postgres protocol", "Copy-on-write branches"],
  "requirements": "AcmeDB and psql (libpq) on PATH.",
  "credentials": []
}
```

- `system`: Human-readable display name shown on the leaderboard.
- `proprietary`: `"yes"` or `"no"`.
- `hosted`: `"no"` for self-hosted servers managed via local lifecycle scripts, or `"yes"` for managed cloud services accessed over the network.
- `tuned`: `"yes"` or `"no"`.
- `tags`: Capability and architecture tags used by the leaderboard's **Capabilities** filter.
- `requirements`: One printable line that lists the local tools needed to run the integration.
- `credentials`: Public environment variable names required by a hosted service. Use an empty list for self-hosted systems. The manifest never stores secret values.

### Step 2: Create Lifecycle Scripts (`install`, `start`, `check`, `stop`)

Self-hosted systems (`"hosted": "no"`) must provide executable lifecycle scripts (`chmod +x leaderboard/systems/<system>/{start,check,stop}` and optional `install`):

1. `install`: Optional script that installs or verifies the database binary locally before `start`.
2. `start`: Launches the database server bound to `127.0.0.1` on its local port and writes the server PID to `leaderboard/.runs/<system>.pid`. Must be idempotent if already running.
3. `check`: Verifies that the PID in `leaderboard/.runs/<system>.pid` is alive and the port answers as the expected database engine, exiting `0` only when ready to accept SQL.
4. `stop`: Stops only the process recorded in `leaderboard/.runs/<system>.pid` and removes the PID file once the process has exited.

See `leaderboard/systems/dolt/` (`Doltgres`), `leaderboard/systems/dolt_mysql/` (`Dolt`), `leaderboard/systems/matrixone/`, `leaderboard/systems/seekdb/`, or `leaderboard/systems/file_copy/` for working reference scripts.

### Step 3: Implement `leaderboard/systems/<system>/connector.py`

Create `leaderboard/systems/<system>/connector.py` exporting a module-level `CONNECTOR` instance that subclasses `leaderboard.connector.Connector`:

```python
from __future__ import annotations

from contextlib import closing
from pathlib import Path

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from leaderboard.connector import Connector
from microbench import task2_pb2 as tp
from microbench.runner2 import BackendInfo
from util.import_db import load_sql_file


class AcmeConnector(Connector):
    backend = tp.Backend.DOLT
    needs_psql = True
    version_sql = "SELECT version()"
    data_dir = Path("~/acmedb/data").expanduser()

    def scope(self) -> dict:
        return {"host": "127.0.0.1", "port": 5432, "user": "postgres", "data_dir": str(self.data_dir.resolve())}

    def _admin(self) -> psycopg2.extensions.connection:
        conn = psycopg2.connect("postgresql://postgres@127.0.0.1:5432/postgres")
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return conn

    def provision(self, db: str) -> BackendInfo:
        with closing(self._admin()) as conn, conn.cursor() as cur:
            cur.execute(f'CREATE DATABASE "{db}"')
        return BackendInfo(default_branch_name="main")

    def load(self, info: BackendInfo, db: str, dump: Path) -> None:
        load_sql_file(f"postgresql://postgres@127.0.0.1:5432/{db}", dump)

    def delete(self, db: str) -> None:
        with closing(self._admin()) as conn, conn.cursor() as cur:
            cur.execute(f'DROP DATABASE IF EXISTS "{db}"')
        if self.exists(db):
            raise RuntimeError(f"AcmeDB database '{db}' still exists after deletion")

    def exists(self, db: str) -> bool:
        with closing(self._admin()) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (db,))
            return cur.fetchone() is not None

    def version(self) -> str:
        with closing(self._admin()) as conn, conn.cursor() as cur:
            cur.execute(self.version_sql)
            return str(cur.fetchone()[0])


CONNECTOR = AcmeConnector()
```

#### Supporting Macrobench Agent Workflows

Both `python -m leaderboard.run` (microbench) and `python -m leaderboard.run_macro` (macrobench) dispatch database operations using `CONNECTOR.backend` (an enum value from `microbench/task2_pb2.py` and `macrobench/task_pb2.py` backed by a `DBToolSuite` subclass in `dblib/`). For each scenario (`S1`–`S6`), `run_macro` records a recovery journal before spawning `python -m macrobench.runner` with a unique `bb_<id>` database and `cleanup: false`, then verifies database deletion through `CONNECTOR.delete(db)` and `CONNECTOR.exists(db)`. If a backend reports a branching verb as unsupported (`UnsupportedOperation`), the scenario stops cleanly with `status = "unsupported: <VERB>: <reason>"` and `validate_macro` classifies the attempt as `incomplete`.

---

## Quick start

Run all commands from the repository root (`/path/to/db-fork`):

### 1. Install Dependencies & Build Protobufs

```bash
uv sync --frozen
uv run --frozen python build_protos.py
```

### 2. Run a Single Database Benchmark Locally

Run the microbenchmark suite. It starts the server through `leaderboard/systems/<system>/{install,start,check,stop}`, runs 3 tries of all 8 operations, stops the server, and writes `leaderboard/<system>/results/<YYYYMMDD>/<machine>.json`:

```bash
uv run python -m leaderboard.run dolt --machine dev-box
```

Run the macrobenchmark agent workflow suite (`--preset mini` for local verification or `--preset full` for full scale):

```bash
uv run python -m leaderboard.run_macro dolt --preset mini --machine dev-box
```

### 3. Build and Serve the Local Leaderboard UI

Assemble the local site into `leaderboard/.site/local/` and start the development server on `http://127.0.0.1:8000`:

```bash
uv run python -m leaderboard.site --phase local
uv run python -m leaderboard.serve --phase local --port 8000
```

- Open `http://127.0.0.1:8000/` to inspect the leaderboard.
- Open `http://127.0.0.1:8000/run.html` to use the interactive benchmark wizard or generate a new adapter scaffold.
- Open `http://127.0.0.1:8000/architecture.html` for the technical architecture of local vs. cloud runs.

### 4. Run the Verification Suite

Before opening a pull request, verify that all Python unit tests and page regression checks pass:

```bash
uv run python -m pytest
node leaderboard/test_page.cjs
```

---

## Hosted connectors

Managed cloud database folders (`"hosted": "yes"`, such as `leaderboard/systems/neon/`, `leaderboard/systems/xata/`, and `leaderboard/systems/tiger/`) represent remote serverless or cloud-hosted services:

- Hosted connectors do **not** use local `install`, `start`, `check`, or `stop` scripts.
- `leaderboard/systems/neon/system.json` declares `NEON_API_KEY_ORG`. `leaderboard/systems/xata/system.json` declares `XATA_API_KEY` and `XATA_ORGANIZATION_ID`. Their connectors read those variables at runtime, run `preflight()` before creating a project, require `--client-location <location>` for microbench, and record `region` and measured SQL `rtt_ms`.
- If a hosted system folder has no `connector.py` yet (such as `leaderboard/systems/tiger/`) or has no result file on disk, illustrative reference rows are supplied in `--phase local` by `leaderboard/mock.py` (never included in `--phase prod`).

---

## Running on AWS (Economic 1-VM Model & Collapsible `alpha` Phase)

The site assembler (`python -m leaderboard.site`) and local preview server (`python -m leaderboard.serve`) support three isolated website phases under `leaderboard/.site/<phase>`:

- **`local`** (`--phase local`): Local developer preview (`leaderboard/.site/local`). Includes all validated micro and macro results on disk, illustrative mock rows (`leaderboard/mock.py`), the macOS Terminal launcher (`run.html`), `architecture.html`, and `README.md`.
- **`alpha`** (`--phase alpha`): DAPLab AWS development and staging phase (`leaderboard/.site/alpha`). Includes all validated real micro and macro runs on disk (including `mini` smoke campaigns without requiring `publish.json`), `architecture.html`, `README.md`, and all raw records, while excluding mock rows and the macOS-only `run.html` wizard.
- **`prod`** (`--phase prod`, default): Official production release (`leaderboard/.site/prod`). Publishes only the records listed in `leaderboard/publish.json` (`full` macro campaigns and comparable micro results).

### Collapsible DAPLab AWS Deployment (`alpha` Phase)

Use `./leaderboard/aws_alpha.sh` (`python -m leaderboard.aws_alpha`) to provision, benchmark, deploy, inspect, and completely collapse one DAPLab AWS `alpha` environment. Every AWS resource receives the exact registered `project` tag and the operator's `billing-tag1`. Internal ownership tags remain `Project=BranchBenchLeaderboard`, `Phase=alpha`, and `Stack=branchbench-alpha`.

The CLI requires `--daplab-project`, `--owner`, and `--daplab-account-id` on every command. The account ID is the expected 12-digit shared DAPLab AWS account ID. It is not a secret. Every live command verifies both the account ID and the IAM username before it inspects or changes resources. The alpha phase is locked to `us-east-1`.

The CLI names AWS resources as `{project}-{owner}-{purpose}`. Authentication remains operator-local. The CLI uses the standard AWS CLI credential chain. It does not call a password manager or store AWS credentials. Load approved credentials into the AWS CLI environment before using this command. Never put credentials in this repository.

The alpha compute size is locked to one `t3a.xlarge` with 4 vCPUs and 16 GiB of memory. It defaults to On-Demand capacity and a 64 GiB encrypted `gp3` root volume. T3 CPU credits use `standard` mode to prevent surplus credit charges. Non-NVMe instances use a 40 GiB XFS loopback data mount. This profile is for smoke testing one database at a time. Its burstable CPU and loopback storage make its timings ineligible for production ranking. Use `--spot` only when interruption and rerun are acceptable. Add per-database instance selection later in the production launcher, with separate ranking cohorts for different hardware.

```bash
DAPLAB_PROJECT='registered-project-tag'
DAPLAB_OWNER='your-uni'
DAPLAB_ACCOUNT_ID='replace-with-12-digit-account-id'

# 1. Review the low-cost plan without contacting AWS
./leaderboard/aws_alpha.sh up --daplab-project "$DAPLAB_PROJECT" --owner "$DAPLAB_OWNER" --daplab-account-id "$DAPLAB_ACCOUNT_ID" --dry-run --ssh-cidr 203.0.113.10/32

# 2. Provision one Ubuntu 24.04 t3a.xlarge worker and one private encrypted S3 bucket
./leaderboard/aws_alpha.sh up --daplab-project "$DAPLAB_PROJECT" --owner "$DAPLAB_OWNER" --daplab-account-id "$DAPLAB_ACCOUNT_ID"

# 3. Run the smallest smoke test, pull its artifacts, build the alpha site, and remove all AWS resources
./leaderboard/aws_alpha.sh run --daplab-project "$DAPLAB_PROJECT" --owner "$DAPLAB_OWNER" --daplab-account-id "$DAPLAB_ACCOUNT_ID" --preset mini --lane micro --systems dolt_mysql --collapse-after

# 4. Preview the retained local alpha site
uv run --frozen python -m leaderboard.serve --phase alpha
```

To inspect the resources before teardown, omit `--collapse-after`. Run `status`, `deploy-site`, and `collapse` with the same project, owner, and account ID. `collapse` discovers resources by all ownership tags before deletion. It waits for EC2 termination, deletes leftover owned EBS volumes, deletes the owned security group and key pair, and empties the owned S3 bucket. It preserves state and reports a partial collapse when ownership or bucket-region checks remain unresolved. It preserves local benchmark artifacts. `--keep-bucket` retains S3, and `--keep-local-site` retains `leaderboard/.site/alpha`.

If artifact download fails, `--collapse-after` retains the worker and state instead of deleting the only artifact copy. The worker's dead-man shutdown remains armed. Retry the run or recover the artifacts promptly before EC2 terminates the worker.

### Production Target (`prod` Phase, One Fixed Worker per Database)

The provisional production profile uses one On-Demand **`m7a.2xlarge`** worker for each self-hosted database. Each worker has 8 physical CPU cores and 32 GiB of memory. It also has a 40 GiB `gp3` root volume and a dedicated 200 GiB `gp3` data volume. All workers in one ranked cohort use this fixed profile. When implemented, the launcher may start database workers concurrently. Each worker will run Microbench and Macrobench sequentially for one database.

The full suite configures at most 16 concurrent database clients, which is two clients per physical core on this profile. The 32 GiB memory allocation is twice MatrixOne's 16 GiB standalone test recommendation. The 200 GiB data volume is the initial allowance for the dataset, branch history, write-ahead logs, and temporary data. These inputs do not prove that the profile has sufficient capacity. Complete the [required full calibration](requirements/README.md#official-aws-compute) before the first ranked release, and change the fixed cohort profile if calibration finds memory exhaustion, disk exhaustion, or sustained EBS saturation.

No production fleet launcher or merge-trigger workflow exists yet. A GitHub merge trigger must live under `.github/workflows/`, outside the requested `leaderboard/`-only scope. Until that automation exists, maintainers must perform the production steps below.

The production launcher will own EC2 placement. A later version can select an instance type for each database without changing `aws_worker.sh`. Before that version mixes instance types, result provenance and ranking groups must separate runs by instance type.

1. Provision one clean **Ubuntu 24.04 LTS (`x86_64`)** On-Demand `m7a.2xlarge` for each selected self-hosted database. Use a 40 GiB `gp3` root volume. Attach a 200 GiB `gp3` data volume at the 3,000 IOPS and 125 MiB/s baseline. Format the data volume as `mkfs.xfs -m crc=1,reflink=1`. Mount it at `/mnt/branchbench` so PostgreSQL 18 `file_copy_method=clone` uses real copy-on-write reflinks.
2. Stop and disable conflicting OS and benchmark services (`postgresql`, `pg18-bench`, `mysql`, `mariadb`, `dolt`, `doltgres`, `matrixone`, `seekdb`, `unattended-upgrades`).
3. On each worker, run one database from the same clean commit using `leaderboard/aws_worker.sh`. The worker enforces a batch-wide lock and `--batch-deadline-min`, verifies server stop and cleanup, and validates every artifact that it produces:

```bash
./leaderboard/aws_worker.sh --systems dolt --machine aws-m7a-2xlarge-gp3-200-dolt --preset full --data-mount /mnt/branchbench --batch-deadline-min 720
```

4. Upload the validated artifacts before stopping or terminating each worker. Then list the validated result and campaign paths in `leaderboard/publish.json`, build the static production bundle with `uv run --frozen python -m leaderboard.site --phase prod`, and sync `leaderboard/.site/prod/` to S3 and CloudFront or GitHub Pages.

---

## What is measured

The **Microbench (`Operations`)** lane times eight branch, read, and write primitives on a deterministic CH-benCHmark seed (`leaderboard/suite.json`, `seed: 20260929`, `tries: 3`):

1. **Time to first query** (`time_to_first_query`, batch of 1): Create a branch from `main`, open a connection to it, and run `SELECT 1`.
2. **Branch create** (`branch_create`, batch of 1): Create a child branch from `main` without connecting.
3. **Branch connect** (`branch_connect`, batch of 100): Build a chain of 4 branches and reconnect across them for 100 `SELECT 1` queries.
4. **Point read** (`read`, batch of 1,000): Single-row primary-key lookups on `item` inside a fresh branch.
5. **Range read** (`range_read`, batch of 200): 100-row range scans on `item` inside a fresh branch.
6. **Point insert** (`insert`, batch of 1,000): Single-row inserts into `item` on an isolated branch.
7. **Point update** (`update`, batch of 1,000): Single-row primary-key updates on `item` on an isolated branch.
8. **Range update** (`range_update`, batch of 200): 100-row range updates on `item` on an isolated branch.

Alongside the eight operations, each run records **`load_time`** (creating the database and loading the SQL seed) and **`provision_time`** (connecting and creating/deleting a warm-up database).

**Scoring**: Each category (**First query**, **Reads & writes**, **Load**) is scored relative to the fastest comparable system in that workload (`×1.00` is fastest, lower is better) using a shifted geometric mean (`s = 0.01s` shift for operation categories, no shift for **Load**).

---

## Agent workflows

The **Macrobench (`Agent workflows`)** lane (`leaderboard/macro_suite.json`) runs six end-to-end stateful AI agent scenarios in isolated child processes:

1. **Evaluate agent rollouts (`rl_env`)**: Parallel repair rollouts branch from a task checkpoint, execute multi-turn episodes, get scored, and are deleted without merging into `main`.
2. **Manage context history (`context_mgmt`)**: Candidate plans branch from `main`; the winning candidate fast-forwards `main`, bad commits are reverted, and later work is rebased.
3. **Coordinate multiple agents (`multi_agent`)**: Concurrent specialist branches modify shared tables and merge serially into `main` under a deterministic conflict policy.
4. **Develop against live data (`dev_agent`)**: A feature branch develops alongside background production writes, rebases onto the latest `production` head, and passes a quiet review window.
5. **Recover from a bad deployment (`ops_agent`)**: Investigation branches inspect a faulty rollout while `production` resets to the last known-good commit checkpoint.
6. **Ingest and merge data (`data_agent`)**: An ingestion batch branch rewinds and retries a failed validation step, rebases onto `warehouse`, and fast-forward merges.

**Outcomes & Ranking**: Every scenario attempt is classified from its execution evidence and teardown check into one of four outcomes:

- **`verified`**: All expected work units succeeded, all applicable correctness invariants passed, and temporary databases were deleted. Only `verified` attempts have a **Completion time**.
- **`incomplete`**: Work units were skipped because the backend reported one or more branching verbs as unsupported (`NotImplementedError`).
- **`failed`**: A work unit, setup step, cleanup verification, or correctness invariant failed.
- **`timed_out`**: The scenario subprocess exceeded its deadline before completing.

Systems are ranked by one selected workflow's completion time at a time (`verified` attempts first, fastest first). There is no composite score across workflows.
