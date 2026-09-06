-- `TTL ... GROUP BY (a, b, c)` must mean the same list of keys as `TTL ... GROUP BY a, b, c`,
-- the same way `ORDER BY (a, b, c)` means the same key as `ORDER BY a, b, c`.

DROP TABLE IF EXISTS ttl_group_by_parens;
DROP TABLE IF EXISTS ttl_group_by_no_parens;
DROP TABLE IF EXISTS ttl_group_by_single_parens;

CREATE TABLE ttl_group_by_parens (a String, b UInt64, c String, d Date)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(d)
ORDER BY (a, c, toStartOfMonth(d), d)
TTL d + INTERVAL 8 DAY GROUP BY (a, c, toStartOfMonth(d)) SET b = sum(b), d = min(toStartOfMonth(d));

CREATE TABLE ttl_group_by_no_parens (a String, b UInt64, c String, d Date)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(d)
ORDER BY (a, c, toStartOfMonth(d), d)
TTL d + INTERVAL 8 DAY GROUP BY a, c, toStartOfMonth(d) SET b = sum(b), d = min(toStartOfMonth(d));

-- Each spelling is preserved as written, like `ORDER BY` is, so that formatting an AST and parsing
-- it back is idempotent.
SELECT name, extract(create_table_query, 'GROUP BY .*? SET')
FROM system.tables
WHERE database = currentDatabase() AND name IN ('ttl_group_by_parens', 'ttl_group_by_no_parens')
ORDER BY name;

-- Both must actually aggregate.
INSERT INTO ttl_group_by_parens VALUES ('x', 1, 'y', '2000-01-01'), ('x', 2, 'y', '2000-01-02'), ('x', 4, 'z', '2000-01-03');
OPTIMIZE TABLE ttl_group_by_parens FINAL;
SELECT a, b, c, d FROM ttl_group_by_parens ORDER BY a, c;

INSERT INTO ttl_group_by_no_parens VALUES ('x', 1, 'y', '2000-01-01'), ('x', 2, 'y', '2000-01-02'), ('x', 4, 'z', '2000-01-03');
OPTIMIZE TABLE ttl_group_by_no_parens FINAL;
SELECT a, b, c, d FROM ttl_group_by_no_parens ORDER BY a, c;

-- A single parenthesized key was always accepted and must keep working.
CREATE TABLE ttl_group_by_single_parens (a String, b UInt64, d Date)
ENGINE = MergeTree
ORDER BY (a, d)
TTL d + INTERVAL 8 DAY GROUP BY (a) SET b = sum(b);

-- A key that is not a prefix of the primary key is still rejected, parenthesized or not.
CREATE TABLE ttl_group_by_bad (a String, b UInt64, c String, d Date)
ENGINE = MergeTree
ORDER BY (a, c, d)
TTL d + INTERVAL 8 DAY GROUP BY (c, a) SET b = sum(b); -- { serverError BAD_TTL_EXPRESSION }

-- An empty key is still rejected.
CREATE TABLE ttl_group_by_empty (a String, b UInt64, d Date)
ENGINE = MergeTree
ORDER BY (a, d)
TTL d + INTERVAL 8 DAY GROUP BY () SET b = sum(b); -- { serverError BAD_TTL_EXPRESSION }

-- Only one level of parentheses is a key list; a nested tuple stays a single (non-prefix) key.
-- Unwrapping this in the parser instead would make formatting non-idempotent, because the formatted
-- form would be unwrapped again on the next parse.
CREATE TABLE ttl_group_by_nested (a String, b UInt64, d Date)
ENGINE = MergeTree
ORDER BY (a, d)
TTL d + INTERVAL 8 DAY GROUP BY tuple(tuple('x')) SET b = sum(b); -- { serverError BAD_TTL_EXPRESSION }

SELECT formatQuerySingleLine($$CREATE TABLE t (a String, b UInt64, d Date) ENGINE = MergeTree ORDER BY (a, d) TTL d + INTERVAL 8 DAY GROUP BY (a, d) SET b = sum(b)$$);
SELECT formatQuerySingleLine($$CREATE TABLE t (a String, b UInt64, d Date) ENGINE = MergeTree ORDER BY (a, d) TTL d + INTERVAL 8 DAY GROUP BY a, d SET b = sum(b)$$);

DROP TABLE ttl_group_by_parens;
DROP TABLE ttl_group_by_no_parens;
DROP TABLE ttl_group_by_single_parens;
