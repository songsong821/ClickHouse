-- A subquery over the same table as the enclosing query, with the inner table expression aliased: SQL says
-- the alias replaces the table's name, so a qualifier that spells the name refers to the enclosing query's
-- table and the subquery is correlated. ClickHouse binds it to the aliased inner table instead, which turns
-- the correlated predicate into a comparison of the inner row with itself and answers 0.
--
-- `analyzer_alias_hides_table_name` selects the standard reading. It is off by default, because the
-- previous one is what the `distributed_product_mode = 'local'` rewrite of an `IN` subquery relies on
-- (`00858_issue_4756`, `01103_distributed_product_mode_local_column_renames`) and what
-- `03148_asof_join_ddb_subquery` is written in.

-- Correlated subqueries are a feature of the analyzer, so this test needs it.
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_qualifier_alias;
DROP TABLE IF EXISTS t_qualifier_other;

CREATE TABLE t_qualifier_alias (id UInt64, grp UInt8, val UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_qualifier_alias VALUES (1, 1, 10), (2, 1, 20), (3, 1, 30);

CREATE TABLE t_qualifier_other (id UInt64, grp UInt8, val UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_qualifier_other VALUES (1, 1, 5);

SELECT 'EXISTS over the same table, the qualifier read as the enclosing table';
SELECT count() FROM t_qualifier_alias WHERE EXISTS (
    SELECT 1 FROM t_qualifier_alias AS c WHERE c.grp = t_qualifier_alias.grp AND c.val > t_qualifier_alias.val)
SETTINGS analyzer_alias_hides_table_name = 1;
-- The same query written with the outer table aliased, which has always been read this way.
SELECT count() FROM t_qualifier_alias AS o WHERE EXISTS (
    SELECT 1 FROM t_qualifier_alias AS c WHERE c.grp = o.grp AND c.val > o.val);
-- And qualified with the database name, which addresses the table in the same way.
SELECT count() FROM {CLICKHOUSE_DATABASE:Identifier}.t_qualifier_alias WHERE EXISTS (
    SELECT 1 FROM t_qualifier_alias AS c
    WHERE c.grp = {CLICKHOUSE_DATABASE:Identifier}.t_qualifier_alias.grp
      AND c.val > {CLICKHOUSE_DATABASE:Identifier}.t_qualifier_alias.val)
SETTINGS analyzer_alias_hides_table_name = 1;

SELECT 'the same qualifier read as the aliased inner table, which is the default';
SELECT count() FROM t_qualifier_alias WHERE EXISTS (
    SELECT 1 FROM t_qualifier_alias AS c WHERE c.grp = t_qualifier_alias.grp AND c.val > t_qualifier_alias.val);

SELECT 'NOT EXISTS over the same table';
SELECT count() FROM t_qualifier_alias WHERE NOT EXISTS (
    SELECT 1 FROM t_qualifier_alias AS c WHERE c.grp = t_qualifier_alias.grp AND c.val > t_qualifier_alias.val)
SETTINGS analyzer_alias_hides_table_name = 1;

SELECT 'a correlated scalar subquery';
SELECT id, (SELECT max(c.val) FROM t_qualifier_alias AS c
    WHERE c.grp = t_qualifier_alias.grp AND c.val > t_qualifier_alias.val) AS next_val
FROM t_qualifier_alias ORDER BY id
SETTINGS analyzer_alias_hides_table_name = 1;

SELECT 'the name of an aliased table still qualifies it where nothing else carries it';
SELECT count() FROM t_qualifier_alias AS c WHERE t_qualifier_alias.grp = 1
SETTINGS analyzer_alias_hides_table_name = 1;
SELECT count() FROM t_qualifier_other WHERE EXISTS (
    SELECT 1 FROM t_qualifier_alias AS c WHERE t_qualifier_alias.val > 15)
SETTINGS analyzer_alias_hides_table_name = 1;
SELECT count() FROM t_qualifier_other AS t_qualifier_alias WHERE EXISTS (
    SELECT 1 FROM t_qualifier_alias AS c WHERE c.val > t_qualifier_alias.val)
SETTINGS analyzer_alias_hides_table_name = 1;

DROP TABLE t_qualifier_alias;
DROP TABLE t_qualifier_other;
