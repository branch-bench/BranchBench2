"""dblib.sql_guard: what agent SQL may run, per dialect."""

import pytest

from dblib.sql_guard import READ, WRITE, classify

PG, MY = "postgres", "mysql"

ALLOWED = [
    (PG, "SELECT * FROM t WHERE id = 1", READ),
    (PG, "select count(*) from orders o join customer c on c.c_id = o.o_c_id", READ),
    (PG, "SELECT * FROM t;", READ),
    (PG, "SELECT 1 UNION SELECT 2", READ),
    (PG, "WITH x AS (SELECT * FROM t) SELECT * FROM x", READ),
    (PG, "SELECT * FROM public.t", READ),                       # schema, not another database
    (PG, "SELECT * FROM information_schema.tables", READ),
    (PG, "SELECT * FROM dolt_log", READ),                       # Doltgres system tables are reads
    (PG, "SELECT * FROM dolt_diff('main', 'b', 't')", READ),
    (PG, "SELECT * FROM t FOR UPDATE", READ),
    (PG, "EXPLAIN SELECT * FROM t", READ),
    (PG, "EXPLAIN UPDATE t SET v = 1", READ),                   # plans only
    (PG, "VALUES (1), (2)", READ),
    (MY, "SHOW TABLES", READ),
    (MY, "DESCRIBE t", READ),
    (MY, "SELECT * FROM information_schema.columns WHERE table_name = 't'", READ),
    (PG, "INSERT INTO t VALUES (1, 'a')", WRITE),
    (PG, "INSERT INTO t SELECT * FROM u", WRITE),
    (PG, "UPDATE t SET v = 'a;b' WHERE id = 1", WRITE),         # ';' inside a literal
    (PG, "DELETE FROM t WHERE id = 1", WRITE),
    (PG, "WITH x AS (DELETE FROM t RETURNING *) SELECT * FROM x", WRITE),
    (PG, "SELECT * INTO t2 FROM t", WRITE),
    (PG, "SELECT nextval('s')", WRITE),
    (PG, "EXPLAIN ANALYZE DELETE FROM t", WRITE),               # EXPLAIN ANALYZE runs it
    (PG, "CREATE TABLE a (x int PRIMARY KEY)", WRITE),
    (PG, "CREATE INDEX i ON t (v)", WRITE),
    (PG, "CREATE VIEW v AS SELECT * FROM t", WRITE),
    (PG, "ALTER TABLE t ADD COLUMN y int", WRITE),
    (PG, "DROP TABLE t", WRITE),
    (PG, "TRUNCATE t", WRITE),
    (MY, "REPLACE INTO t VALUES (1, 'a')", WRITE),
    (MY, "INSERT INTO t VALUES (1) ON DUPLICATE KEY UPDATE v = 2", WRITE),
]


@pytest.mark.parametrize("dialect,sql,kind", ALLOWED, ids=[s for _, s, _ in ALLOWED])
def test_allowed(dialect, sql, kind):
    v = classify(sql, dialect)
    assert not v.refused, v.reason
    assert v.kind == kind


REFUSED = [
    # engine branching SQL that would go around the branch tools
    (PG, "SELECT dolt_checkout('b')", "dolt_checkout"),
    (PG, "select DOLT_COMMIT('-am', 'x')", "dolt_commit"),
    (PG, "SELECT dolt_merge('b')", "dolt_merge"),
    (PG, "SELECT * FROM t WHERE id = (SELECT dolt_reset('--hard'))", "dolt_reset"),
    (MY, "CALL DOLT_CHECKOUT('b')", "CALL"),
    (MY, "CALL dolt_branch('x')", "CALL"),
    (MY, "DATA BRANCH CREATE DATABASE a FROM b", "not a recognised"),   # MatrixOne
    (MY, "CREATE SNAPSHOT s FOR DATABASE d", "CREATE"),                 # MatrixOne
    (MY, "RESTORE DATABASE d FROM SNAPSHOT s", ""),
    (MY, "FORK DATABASE a TO b", "not a recognised"),                   # SeekDB
    (PG, "CREATE DATABASE x TEMPLATE y", "CREATE"),                     # file_copy
    (MY, "SELECT * FROM t AS OF TIMESTAMP '2020-01-01'", "not a recognised"),
    # other databases / branches
    (PG, 'INSERT INTO "db/main".public.t VALUES (1)', "another database"),
    (PG, 'SELECT * FROM "db/feature".public.t', "another database"),
    (MY, "INSERT INTO `db/main`.t VALUES (1)", "another database"),
    (MY, "SELECT * FROM otherdb.t", "another database"),
    (MY, "UPDATE t JOIN otherdb.u ON t.id = u.id SET t.v = u.v", "another database"),
    (PG, "SELECT * FROM dblink('host=x', 'select 1') AS t(a int)", "dblink"),
    (MY, "USE other", "USE"),
    (MY, "DROP DATABASE x", "DROP DATABASE"),
    (PG, "CREATE SCHEMA s", "CREATE SCHEMA"),
    (PG, "CREATE EXTENSION postgres_fdw", "CREATE"),
    # session and server state
    (PG, "SET search_path = other", "SET"),
    (MY, "SET @@dolt_branch = 'b'", "SET"),
    (PG, "SELECT set_config('search_path', 'x', false)", "set_config"),
    (PG, "SELECT pg_terminate_backend(1)", "pg_terminate_backend"),
    (PG, "BEGIN", "atomic"),
    (PG, "COMMIT", "atomic"),
    (PG, "ROLLBACK", "atomic"),
    (PG, "COPY t FROM PROGRAM 'id'", "COPY"),
    (MY, "LOAD DATA INFILE '/etc/passwd' INTO TABLE t", ""),
    (MY, "KILL 5", ""),
    (PG, "GRANT ALL ON t TO bob", ""),
    # code the guard can't see into
    (PG, "CREATE FUNCTION f() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql", "CREATE FUNCTION"),
    (MY, "CREATE TRIGGER tr AFTER INSERT ON t FOR EACH ROW SET @x = 1", "CREATE"),
    (MY, "CREATE PROCEDURE p() BEGIN SELECT 1; END", ""),
    (PG, "DO $$ BEGIN PERFORM 1; END $$", ""),
    # shape
    (PG, "SELECT 1; DROP TABLE t", "one statement"),
    (PG, "", "empty"),
    (PG, "   ;  ", "empty"),
    (PG, "EXPLAIN SELECT dolt_checkout('x')", "dolt_checkout"),
]


@pytest.mark.parametrize("dialect,sql,why", REFUSED, ids=[s or "<empty>" for _, s, _ in REFUSED])
def test_refused(dialect, sql, why):
    v = classify(sql, dialect)
    assert v.refused, f"{sql!r} was allowed as {v.kind}"
    assert why.lower() in v.reason.lower(), v.reason
