DROP DATABASE IF EXISTS probe SYNC;
CREATE DATABASE probe;
USE probe;

CREATE TABLE t (k UInt32, v UInt64, s String, d Date, arr Array(UInt32), n Nullable(UInt32), lc LowCardinality(String), INDEX idx_v v TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64;
INSERT INTO t SELECT number % 20, number, concat('s', toString(number % 7)), toDate('2024-01-01') + number % 90, [number % 3, number % 5], if(number % 4 = 0, NULL, number % 11), concat('lc', toString(number % 3)) FROM numbers(3000);
INSERT INTO t SELECT number % 20, number, concat('s', toString(number % 7)), toDate('2024-01-01') + number % 90, [number % 3, number % 5], if(number % 4 = 0, NULL, number % 11), concat('lc', toString(number % 3)) FROM numbers(3000, 3000);
INSERT INTO t SELECT number % 20, number, concat('s', toString(number % 7)), toDate('2024-01-01') + number % 90, [number % 3, number % 5], if(number % 4 = 0, NULL, number % 11), concat('lc', toString(number % 3)) FROM numbers(6000, 3000);

CREATE TABLE t2 (k UInt32, w UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t2 SELECT number % 25, number * 10 FROM numbers(500);
INSERT INTO t2 SELECT number % 25, number * 10 FROM numbers(500, 500);

CREATE TABLE tr (k UInt32, v UInt64, ver UInt32) ENGINE = ReplacingMergeTree(ver) ORDER BY k PARTITION BY k % 3;
INSERT INTO tr SELECT number, number, 1 FROM numbers(2000);
INSERT INTO tr SELECT number, number * 100, 2 FROM numbers(0, 1000);

CREATE TABLE tp (k UInt32, v UInt64, PROJECTION p_agg (SELECT k, sum(v) GROUP BY k), PROJECTION p_ord (SELECT k, v ORDER BY v)) ENGINE = MergeTree ORDER BY k;
INSERT INTO tp SELECT number % 10, number FROM numbers(5000);

CREATE TABLE tpart (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k PARTITION BY k % 3;
INSERT INTO tpart SELECT number, number FROM numbers(3000);

CREATE TABLE tsample (k UInt32, h UInt32, v UInt64) ENGINE = MergeTree ORDER BY (k, h) SAMPLE BY h;
INSERT INTO tsample SELECT number, intHash32(number), number FROM numbers(3000);

CREATE TABLE tlwd (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO tlwd SELECT number, number FROM numbers(3000);
DELETE FROM tlwd WHERE k % 10 = 0;

CREATE TABLE trp (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO trp SELECT number, number FROM numbers(3000);
CREATE ROW POLICY IF NOT EXISTS probe_rp ON probe.trp FOR SELECT USING k % 2 = 0 TO default;

CREATE TABLE tm (k UInt32, v UInt64) ENGINE = Memory;
INSERT INTO tm SELECT number, number FROM numbers(100);

CREATE TABLE tlog (k UInt32, v UInt64) ENGINE = Log;
INSERT INTO tlog SELECT number, number FROM numbers(100);

CREATE TABLE tj (k UInt32, w UInt64) ENGINE = Join(ANY, LEFT, k);
INSERT INTO tj SELECT number, number * 7 FROM numbers(30);

CREATE TABLE tset (k UInt32) ENGINE = Set;
INSERT INTO tset SELECT number * 3 FROM numbers(10);

CREATE TABLE tnull (k UInt32, v UInt64) ENGINE = Null;

CREATE TABLE tbuf (k UInt32, v UInt64) ENGINE = Buffer(probe, tlog, 1, 10, 100, 10000, 1000000, 10000000, 100000000);

CREATE VIEW tv AS SELECT k, sum(v) AS sv FROM t GROUP BY k;
CREATE VIEW tv_settings AS SELECT k, v FROM t WHERE k < 3 SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1;

CREATE TABLE tmv_target (k UInt32, sv UInt64) ENGINE = SummingMergeTree ORDER BY k;
CREATE MATERIALIZED VIEW tmv TO tmv_target AS SELECT k, sum(v) AS sv FROM t GROUP BY k;
INSERT INTO t SELECT number % 20, number, concat('s', toString(number % 7)), toDate('2024-01-01') + number % 90, [number % 3, number % 5], if(number % 4 = 0, NULL, number % 11), concat('lc', toString(number % 3)) FROM numbers(9000, 1000);

CREATE TABLE tdist AS t ENGINE = Distributed(test_shard_localhost, probe, t);
CREATE TABLE tdist2 AS t ENGINE = Distributed(test_cluster_two_shards, probe, t, rand());

CREATE TABLE tmerge AS t ENGINE = Merge(probe, '^t$');

CREATE TABLE tjson (k UInt32, j JSON, dyn Dynamic, var Variant(UInt32, String), m Map(String, UInt32), tup Tuple(a UInt32, b String)) ENGINE = MergeTree ORDER BY k;
INSERT INTO tjson SELECT number, concat('{"a":', toString(number), ',"b":{"c":"x', toString(number % 3), '"}}')::JSON, if(number % 2 = 0, number::Dynamic, toString(number)::Dynamic), if(number % 2 = 0, toUInt32(number)::Variant(UInt32, String), toString(number)::Variant(UInt32, String)), map('k', toUInt32(number % 5)), tuple(number, toString(number)) FROM numbers(1000);

CREATE TABLE tagg (k UInt32, st AggregateFunction(sum, UInt64), us AggregateFunction(uniq, UInt32)) ENGINE = AggregatingMergeTree ORDER BY k;
INSERT INTO tagg SELECT k, sumState(v), uniqState(k) FROM t GROUP BY k;

SET allow_experimental_full_text_index = 1;
CREATE TABLE ttext (k UInt32, s String, INDEX idx_text s TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY k;
INSERT INTO ttext SELECT number, if(number % 5 = 0, 'foo bar', 'baz qux') FROM numbers(3000);

SET allow_experimental_vector_similarity_index = 1;
CREATE TABLE tvec (k UInt32, vec Array(Float32), INDEX idx_vec vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY k;
INSERT INTO tvec SELECT number, [toFloat32(number % 17), toFloat32(number % 13)] FROM numbers(3000);

CREATE TABLE tnested (k UInt32, nest Nested(a UInt32, b String)) ENGINE = MergeTree ORDER BY k;
INSERT INTO tnested SELECT number, [number, number + 1], ['x', 'y'] FROM numbers(500);

CREATE DICTIONARY dict (k UInt32, w UInt64) PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 't2' DB 'probe')) LAYOUT(FLAT()) LIFETIME(0);
SYSTEM RELOAD DICTIONARY probe.dict;

CREATE TABLE ins_target (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE ins_target_proj (k UInt32, v UInt64, PROJECTION p (SELECT k, sum(v) GROUP BY k)) ENGINE = MergeTree ORDER BY k;
CREATE TABLE ins_target_mem (k UInt32, v UInt64) ENGINE = Memory;
CREATE TABLE tmut (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO tmut SELECT number, number FROM numbers(1000);
