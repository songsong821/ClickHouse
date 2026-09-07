SET session_timezone = 'UTC';

DROP TABLE IF EXISTS t_week_tz;

-- The week functions are executed in the time zone of their argument, so their monotonicity must be
-- decided in that time zone too: a key range inside one week of the session time zone can straddle a
-- week boundary of the column's time zone, and primary-key pruning then dropped matching rows.

CREATE TABLE t_week_tz (dt DateTime('Asia/Tokyo')) ENGINE = MergeTree ORDER BY dt;
INSERT INTO t_week_tz SELECT toDateTime('2026-08-09 10:00:00', 'UTC') + (3600 * number) FROM numbers(11);

SELECT countIf(toDayOfWeek(dt) = 7), count() FROM t_week_tz WHERE toDayOfWeek(dt) = 7;
SELECT countIf(toDayOfWeek(dt) = 1), count() FROM t_week_tz WHERE toDayOfWeek(dt) = 1;
SELECT countIf(toWeek(dt) = 32), count() FROM t_week_tz WHERE toWeek(dt) = 32;
SELECT countIf(toStartOfWeek(dt) = toDate('2026-08-09')), count() FROM t_week_tz WHERE toStartOfWeek(dt) = toDate('2026-08-09');

DROP TABLE t_week_tz;

-- The same data in a column whose time zone is the session one is unaffected.

CREATE TABLE t_week_tz (dt DateTime('UTC')) ENGINE = MergeTree ORDER BY dt;
INSERT INTO t_week_tz SELECT toDateTime('2026-08-09 10:00:00', 'UTC') + (3600 * number) FROM numbers(11);

SELECT countIf(toDayOfWeek(dt) = 7), count() FROM t_week_tz WHERE toDayOfWeek(dt) = 7;
SELECT countIf(toWeek(dt) = 32), count() FROM t_week_tz WHERE toWeek(dt) = 32;

DROP TABLE t_week_tz;
