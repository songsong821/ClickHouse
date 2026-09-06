-- `optimize_read_in_order` serves `ORDER BY negate(x)` over an ascending `Float` key with a backward read,
-- which surfaces the `NaN`s first while `ASC NULLS LAST` wants them last. The guard that keeps floats with
-- `NaN` out of the optimization tested `nulls_direction` before the direction of the match was known, so it
-- passed and the elided sort never repaired the placement.

DROP TABLE IF EXISTS t_read_in_order_nan;
CREATE TABLE t_read_in_order_nan (x Float64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_read_in_order_nan VALUES (1), (2), (nan), (3);

SELECT 'a monotonically decreasing function of the key';
SELECT negate(x) AS n FROM t_read_in_order_nan ORDER BY n;
SELECT negate(x) AS n FROM t_read_in_order_nan ORDER BY n SETTINGS optimize_read_in_order = 0;

SELECT 'under LIMIT, where the wrong order also returns wrong rows';
SELECT negate(x) AS n FROM t_read_in_order_nan ORDER BY n LIMIT 2;
SELECT negate(x) AS n FROM t_read_in_order_nan ORDER BY n LIMIT 2 SETTINGS optimize_read_in_order = 0;

SELECT 'the mirror order, which a backward read satisfies exactly';
SELECT negate(x) AS n FROM t_read_in_order_nan ORDER BY n DESC NULLS FIRST;
SELECT negate(x) AS n FROM t_read_in_order_nan ORDER BY n DESC NULLS FIRST SETTINGS optimize_read_in_order = 0;

SELECT 'the key itself, which was always read in order';
SELECT x FROM t_read_in_order_nan ORDER BY x;
SELECT x FROM t_read_in_order_nan ORDER BY x DESC;
SELECT x FROM t_read_in_order_nan ORDER BY x SETTINGS optimize_read_in_order = 0;

SELECT 'an increasing function of the key';
SELECT x + 1 AS n FROM t_read_in_order_nan ORDER BY n;
SELECT x + 1 AS n FROM t_read_in_order_nan ORDER BY n SETTINGS optimize_read_in_order = 0;

SELECT 'and a key column declared DESC';
DROP TABLE IF EXISTS t_read_in_order_nan_desc;
CREATE TABLE t_read_in_order_nan_desc (x Float64) ENGINE = MergeTree ORDER BY x DESC;
INSERT INTO t_read_in_order_nan_desc VALUES (1), (2), (nan), (3);
SELECT negate(x) AS n FROM t_read_in_order_nan_desc ORDER BY n;
SELECT negate(x) AS n FROM t_read_in_order_nan_desc ORDER BY n SETTINGS optimize_read_in_order = 0;
SELECT x FROM t_read_in_order_nan_desc ORDER BY x DESC;
SELECT x FROM t_read_in_order_nan_desc ORDER BY x DESC SETTINGS optimize_read_in_order = 0;

DROP TABLE t_read_in_order_nan_desc;
DROP TABLE t_read_in_order_nan;
