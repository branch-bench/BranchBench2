# inkeun-demo-db

`inkeun-demo-db` is an alpha-only database-maker demonstration. It has its own engine identity and leaderboard records. Dolt supplies the mock MySQL-compatible implementation for this test; the leaderboard does not present the system as Dolt.

The engine adapter is `dblib/systems/inkeun_demo_db/adapter.py`. Its connector is `dblib/systems/inkeun_demo_db/connector.py`. This folder provides the metadata and complete self-hosted lifecycle used by the temporary alpha benchmark EC2 instance.
