-- Example scenario (macrobench/scenarios/template.py): rows written on
-- feature branches. Write DDL in the subset PostgreSQL and MySQL share.

CREATE TABLE feature_row (
    feature INT NOT NULL,
    n       INT NOT NULL,
    note    VARCHAR(64),
    PRIMARY KEY (feature, n)
);
