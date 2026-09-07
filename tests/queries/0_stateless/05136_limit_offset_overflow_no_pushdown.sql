-- `LimitStep::getLimitForSorting` returns 0 when `limit + offset` overflows `UInt64`, which means
-- "the number of rows to read is unknown", while a source step reads it as an exact upper bound on
-- the number of rows it may produce. Propagating that sentinel made the sources below return
-- nothing at all instead of every row after the offset.

SELECT 'numbers';
SELECT count() FROM (SELECT * FROM numbers(10) LIMIT 18446744073709551615 OFFSET 1);
SELECT count() FROM (SELECT * FROM numbers(10) LIMIT 18446744073709551614 OFFSET 2);
SELECT count() FROM (SELECT * FROM numbers(10) WHERE number > 3 LIMIT 18446744073709551615 OFFSET 1);

SELECT 'generate_series';
SELECT count() FROM (SELECT * FROM generate_series(1, 10) LIMIT 18446744073709551615 OFFSET 1);
SELECT count() FROM (SELECT * FROM generate_series(10, 0, -1) LIMIT 18446744073709551615 OFFSET 1);

SELECT 'no overflow';
SELECT count() FROM (SELECT * FROM numbers(10) LIMIT 18446744073709551615);
SELECT count() FROM (SELECT * FROM numbers(10) LIMIT 3 OFFSET 1);

-- A real `LIMIT 0` still reads nothing.
SELECT 'limit 0';
SELECT count() FROM (SELECT * FROM numbers(10) LIMIT 0);
SELECT count() FROM (SELECT * FROM numbers(10) LIMIT 0 OFFSET 3);
