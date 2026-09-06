-- `query_plan_join_shard_by_pk_ranges = 1` reads both sides of a `full_sorting_merge` join in primary-key
-- layers, and each layer computes the sorting key on top of the read. `PREWHERE` runs before that and may
-- drop an input column that only the prewhere condition needs, so a column of the sorting key could be
-- missing there and the query failed at execution with 10 `NOT_FOUND_COLUMN_IN_BLOCK`.

DROP TABLE IF EXISTS t_shard_pk_prewhere;
CREATE TABLE t_shard_pk_prewhere (s String, n UInt32, v Int64) ENGINE = MergeTree ORDER BY (s, n);
INSERT INTO t_shard_pk_prewhere SELECT toString(number % 10), number, number % 7 FROM numbers(1000);

SELECT 'a correlated EXISTS under OR, whose decorrelated join is sharded';
SELECT count() FROM (
    SELECT o.n FROM t_shard_pk_prewhere AS o
    WHERE (EXISTS (SELECT 1 FROM t_shard_pk_prewhere AS i WHERE i.s = o.s AND v >= n)) OR o.n = 435)
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_shard_by_pk_ranges = 1;
SELECT count() FROM (
    SELECT o.n FROM t_shard_pk_prewhere AS o
    WHERE (EXISTS (SELECT 1 FROM t_shard_pk_prewhere AS i WHERE i.s = o.s AND v >= n)) OR o.n = 435)
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_shard_by_pk_ranges = 0;
SELECT count() FROM (
    SELECT o.n FROM t_shard_pk_prewhere AS o
    WHERE (EXISTS (SELECT 1 FROM t_shard_pk_prewhere AS i WHERE i.s = o.s AND v >= n)) OR o.n = 435)
SETTINGS join_algorithm = 'hash';

SELECT 'a plain join with a filter that only PREWHERE reads';
SELECT count() FROM t_shard_pk_prewhere AS a INNER JOIN t_shard_pk_prewhere AS b ON a.s = b.s PREWHERE a.v > 3
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_shard_by_pk_ranges = 1;
SELECT count() FROM t_shard_pk_prewhere AS a INNER JOIN t_shard_pk_prewhere AS b ON a.s = b.s PREWHERE a.v > 3
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_shard_by_pk_ranges = 0;

SELECT 'and one that reads both key columns of the join';
SELECT sum(a.n) FROM t_shard_pk_prewhere AS a INNER JOIN t_shard_pk_prewhere AS b ON a.s = b.s AND a.n = b.n PREWHERE a.v = 2
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_shard_by_pk_ranges = 1;
SELECT sum(a.n) FROM t_shard_pk_prewhere AS a INNER JOIN t_shard_pk_prewhere AS b ON a.s = b.s AND a.n = b.n PREWHERE a.v = 2
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_shard_by_pk_ranges = 0;

DROP TABLE t_shard_pk_prewhere;
