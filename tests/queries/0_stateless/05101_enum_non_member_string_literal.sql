-- With the default `validate_enum_literals_in_operators = 0`, comparing an `Enum` column with a string
-- literal that is not a member of the enum must not throw, and must not depend on whether the column
-- happens to be in the table's sorting or partition key.

DROP TABLE IF EXISTS t_enum_key;
DROP TABLE IF EXISTS t_enum_nonkey;
DROP TABLE IF EXISTS t_enum_partition;
DROP TABLE IF EXISTS t_enum16_key;

CREATE TABLE t_enum_key (e Enum8('a' = 1, 'b' = 2)) ENGINE = MergeTree ORDER BY e;
CREATE TABLE t_enum_nonkey (e Enum8('a' = 1, 'b' = 2)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_enum_partition (e Enum8('a' = 1, 'b' = 2)) ENGINE = MergeTree PARTITION BY e ORDER BY tuple();
CREATE TABLE t_enum16_key (e Enum16('a' = 1000, 'b' = 2000)) ENGINE = MergeTree ORDER BY e;

INSERT INTO t_enum_key VALUES ('a'), ('b');
INSERT INTO t_enum_nonkey VALUES ('a'), ('b');
INSERT INTO t_enum_partition VALUES ('a'), ('b');
INSERT INTO t_enum16_key VALUES ('a'), ('b');

SELECT 'equals', count() FROM t_enum_key WHERE e = '4';
SELECT 'equals', count() FROM t_enum_nonkey WHERE e = '4';
SELECT 'equals', count() FROM t_enum_partition WHERE e = '4';
SELECT 'equals', count() FROM t_enum16_key WHERE e = '4';

SELECT 'notEquals', count() FROM t_enum_key WHERE e != '4';
SELECT 'notEquals', count() FROM t_enum_nonkey WHERE e != '4';
SELECT 'notEquals', count() FROM t_enum_partition WHERE e != '4';
SELECT 'notEquals', count() FROM t_enum16_key WHERE e != '4';

SELECT 'less', count() FROM t_enum_key WHERE e < '4';
SELECT 'less', count() FROM t_enum_nonkey WHERE e < '4';

SELECT 'greaterOrEquals', count() FROM t_enum_key WHERE e >= '4';
SELECT 'greaterOrEquals', count() FROM t_enum_nonkey WHERE e >= '4';

SELECT 'in', count() FROM t_enum_key WHERE e IN ('4', 'a');
SELECT 'in', count() FROM t_enum_nonkey WHERE e IN ('4', 'a');

SELECT 'notIn', count() FROM t_enum_key WHERE e NOT IN ('4', 'a');
SELECT 'notIn', count() FROM t_enum_nonkey WHERE e NOT IN ('4', 'a');

-- A `FixedString` literal carries the same non-member value, padded.
SELECT 'fixedString', count() FROM t_enum_key WHERE e < toFixedString('4', 3);
SELECT 'fixedString', count() FROM t_enum_nonkey WHERE e < toFixedString('4', 3);

-- Row-wise, so that the folded value of every operator is visible. A literal that does not convert to the
-- compared type is `false` for `=`, `<`, `>`, `<=`, `>=` and `true` for `!=`, which is why both `e < '4'`
-- and `e >= '4'` are `false`, exactly as for `toUInt8(1) = '257'`.
SELECT 'perRow', e, e = '4', e != '4', e < '4', e >= '4' FROM t_enum_nonkey ORDER BY e;

-- A literal that is a member is still compared, and the key is still used to prune.
SELECT 'member', count() FROM t_enum_key WHERE e = 'b';
SELECT 'member', count() FROM t_enum_partition WHERE e = 'b';
SELECT 'member', count() FROM t_enum16_key WHERE e > 'a';

-- With the validation enabled, all of them throw, for a key column and a non-key column alike.
SELECT count() FROM t_enum_key WHERE e = '4' SETTINGS validate_enum_literals_in_operators = 1; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT count() FROM t_enum_nonkey WHERE e = '4' SETTINGS validate_enum_literals_in_operators = 1; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT count() FROM t_enum_key WHERE e != '4' SETTINGS validate_enum_literals_in_operators = 1; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT count() FROM t_enum_nonkey WHERE e != '4' SETTINGS validate_enum_literals_in_operators = 1; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT count() FROM t_enum_key WHERE e IN ('4', 'a') SETTINGS validate_enum_literals_in_operators = 1; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }

DROP TABLE t_enum_key;
DROP TABLE t_enum_nonkey;
DROP TABLE t_enum_partition;
DROP TABLE t_enum16_key;
