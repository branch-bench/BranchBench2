-- Commits on SeekDB are flashback snapshots (AS OF SNAPSHOT <scn>), so undo
-- must be kept long enough to read every commit of a run. The default (1800 s)
-- is shorter than a full scenario.
ALTER SYSTEM SET undo_retention = 86400;
