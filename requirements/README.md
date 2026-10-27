# Engine ownership requirements

BranchBench2 must run benchmarks without a leaderboard checkout.

| Component | Owner |
|---|---|
| Database API, workload protobufs, scenarios, measurement and result files | Engine |
| Concrete adapters and reusable database connectors | Engine |
| Standalone microbench, macrobench and verb-grid commands | Engine |
| Server installation/start/check/stop scripts and system presentation metadata | Leaderboard |
| Benchmark campaigns, recovery journals, publication checks and website | Leaderboard |
| Maintainer-merge workflow, AWS workers and S3 publication | Leaderboard |

## Execution boundary

1. The CLI or application selects a connector.
2. The connector creates or opens a database and returns a `PreparedTarget`.
3. The runner executes workloads through `DBToolSuite` and records evidence.
4. The caller applies the database cleanup policy through the connector.

The abstract runners must not import concrete providers or leaderboard code.
Concrete providers may live in the same package without reversing that dependency.
The CLI must honor configured database sources and cleanup settings.

The engine must ship its protobuf modules, workload presets, SQL schemas,
and datasets. A build must fail if protobuf generation fails.

## Contribution and verification

Database makers add adapters, connectors, registration, and tests to this
repository. They then register the published engine revision in the leaderboard.
The leaderboard must use one immutable engine revision and retain its provenance.

Server-free tests verify execution and lifecycle behavior with fake databases.
They do not prove compatibility with a real provider. Live tests must report
which provider and configuration they exercised.

See [CONTRIBUTING.md](../CONTRIBUTING.md) for the contribution steps.
