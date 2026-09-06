DROP TABLE IF EXISTS events;
CREATE TABLE events (begin Float64, value Int32) ENGINE = MergeTree() ORDER BY begin;

INSERT INTO events VALUES (1, 0), (3, 1), (6, 2), (8, 3);

SET enable_analyzer = 1;
SET join_algorithm = 'full_sorting_merge';
SET joined_subquery_requires_alias = 0;

-- `events e1` is aliased, so by default `events.value` refers to the enclosing query and the subquery is
-- correlated, which a JOIN does not support yet. This query keeps the previous resolution, where the name
-- of an aliased table expression qualifies it even inside a query that selects from the same table, so
-- `events.value = e1.value` compares the inner row with itself.
SELECT
    begin,
    value IN (
        SELECT e1.value
        FROM (
            SELECT *
            FROM events e1
            WHERE e1.value = events.value
        ) AS e1
        ASOF JOIN (
            SELECT number :: Float64 AS begin
            FROM numbers(10)
            WHERE number >= 1 AND number < 10
        )
        USING (begin)
    )
FROM events
ORDER BY begin ASC
SETTINGS analyzer_compatibility_qualify_aliased_table_by_name = 1;

DROP TABLE IF EXISTS events;
