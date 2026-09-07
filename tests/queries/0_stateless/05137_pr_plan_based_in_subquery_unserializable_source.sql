-- An `IN (subquery)` set is shipped as its subquery plan, so that plan must be serializable - not merely
-- present. `numbers()` is the case where the two differ: `ReadFromSystemNumbers` declares `clone` but not
-- `isSerializable`, so the source is clonable, `buildOrderedSetInplace` keeps the plan, and shipping the
-- fragment threw `Method serialize is not implemented for ReadFromSystemNumbers`. Such a fragment has to
-- stay local instead.

DROP TABLE IF EXISTS t_unserializable_in;

CREATE TABLE t_unserializable_in (k Int64, v Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_unserializable_in SELECT number, number FROM numbers(100);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;

-- Correct results for both local-plan modes, on a non-primary-key and on a primary-key `IN` (the latter is
-- also built in place for index analysis). Previously a NOT_IMPLEMENTED exception.
SET parallel_replicas_local_plan = 0;
SELECT count() FROM t_unserializable_in WHERE v IN (SELECT number FROM numbers(10));
SELECT count() FROM t_unserializable_in WHERE k IN (SELECT number FROM numbers(10));
SET parallel_replicas_local_plan = 1;
SELECT count() FROM t_unserializable_in WHERE v IN (SELECT number FROM numbers(10));
SELECT count() FROM t_unserializable_in WHERE k IN (SELECT number FROM numbers(10));

-- The fragment stays local: the split marker is left unconverted.
SET parallel_replicas_local_plan = 0;
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') = 0 AS stayed_local
FROM (EXPLAIN optimize = 1, description = 0 SELECT count() FROM t_unserializable_in WHERE v IN (SELECT number FROM numbers(10)));

-- Regression guard: a subquery over a MergeTree table is serializable and is still distributed.
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS distributed
FROM (EXPLAIN optimize = 1, description = 0 SELECT count() FROM t_unserializable_in WHERE v IN (SELECT v FROM t_unserializable_in WHERE v % 2 = 0));

DROP TABLE t_unserializable_in;
