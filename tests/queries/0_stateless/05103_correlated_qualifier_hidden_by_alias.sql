-- A subquery over the same table as the enclosing query, with the inner table expression aliased: SQL
-- says the alias replaces the table's name, so a qualifier that spells the name refers to the enclosing
-- query's table and the subquery is correlated. It used to bind to the aliased inner table instead, which
-- turned the correlated predicate into a comparison of the inner row with itself and silently answered 0.

DROP TABLE IF EXISTS t_qualifier_alias;
DROP TABLE IF EXISTS t_qualifier_other;

CREATE TABLE t_qualifier_alias (id UInt64, grp UInt8, val UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_qualifier_alias VALUES (1, 1, 10), (2, 1, 20), (3, 1, 30);

CREATE TABLE t_qualifier_other (id UInt64, grp UInt8, val UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_qualifier_other VALUES (1, 1, 5);

SELECT 'EXISTS over the same table';
SELECT count() FROM t_qualifier_alias WHERE EXISTS (
    SELECT 1 FROM t_qualifier_alias AS c WHERE c.grp = t_qualifier_alias.grp AND c.val > t_qualifier_alias.val);
-- The same query written with the outer table aliased, which has always been read this way.
SELECT count() FROM t_qualifier_alias AS o WHERE EXISTS (
    SELECT 1 FROM t_qualifier_alias AS c WHERE c.grp = o.grp AND c.val > o.val);
-- And qualified with the database name, which addresses the table in the same way.
SELECT count() FROM {CLICKHOUSE_DATABASE:Identifier}.t_qualifier_alias WHERE EXISTS (
    SELECT 1 FROM t_qualifier_alias AS c
    WHERE c.grp = {CLICKHOUSE_DATABASE:Identifier}.t_qualifier_alias.grp
      AND c.val > {CLICKHOUSE_DATABASE:Identifier}.t_qualifier_alias.val);

SELECT 'NOT EXISTS over the same table';
SELECT count() FROM t_qualifier_alias WHERE NOT EXISTS (
    SELECT 1 FROM t_qualifier_alias AS c WHERE c.grp = t_qualifier_alias.grp AND c.val > t_qualifier_alias.val);

SELECT 'a correlated scalar subquery';
SELECT id, (SELECT max(c.val) FROM t_qualifier_alias AS c
    WHERE c.grp = t_qualifier_alias.grp AND c.val > t_qualifier_alias.val) AS next_val
FROM t_qualifier_alias ORDER BY id;

SELECT 'the previous resolution, under the compatibility setting';
SELECT count() FROM t_qualifier_alias WHERE EXISTS (
    SELECT 1 FROM t_qualifier_alias AS c WHERE c.grp = t_qualifier_alias.grp AND c.val > t_qualifier_alias.val)
SETTINGS analyzer_compatibility_qualify_aliased_table_by_name = 1;

SELECT 'the name of an aliased table still qualifies it where nothing else carries it';
SELECT count() FROM t_qualifier_alias AS c WHERE t_qualifier_alias.grp = 1;
SELECT count() FROM t_qualifier_other WHERE EXISTS (
    SELECT 1 FROM t_qualifier_alias AS c WHERE t_qualifier_alias.val > 15);
SELECT count() FROM t_qualifier_other AS t_qualifier_alias WHERE EXISTS (
    SELECT 1 FROM t_qualifier_alias AS c WHERE c.val > t_qualifier_alias.val);

DROP TABLE t_qualifier_alias;
DROP TABLE t_qualifier_other;
