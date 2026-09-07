#!/usr/bin/env bash
# every case has a twin table with the same projection materialized, the estimate must match what it reads

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# count() must reach the read step and the real projection must be allowed to win
PIN="optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, optimize_use_projections = 1, optimize_read_in_order = 1"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_est; DROP TABLE IF EXISTS t_real;
    CREATE TABLE t_est (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
    CREATE TABLE t_real AS t_est;
    ALTER TABLE t_real ADD PROJECTION p_b (SELECT a, b, v ORDER BY b);
    ALTER TABLE t_real ADD PROJECTION p_ba (SELECT a, b, v ORDER BY (b, a));
    ALTER TABLE t_real ADD PROJECTION p_c (SELECT a, b, v, b * 2 AS c ORDER BY c);
    ALTER TABLE t_real ADD PROJECTION p_idx INDEX b TYPE basic;
    SYSTEM STOP MERGES t_est; SYSTEM STOP MERGES t_real;
    -- three parts, b = a % 100 so every b value is one granule per part once sorted
    INSERT INTO t_est SELECT number, number % 100, number FROM numbers(3000);
    INSERT INTO t_est SELECT number, number % 100, number FROM numbers(3000, 3000);
    INSERT INTO t_est SELECT number, number % 100, number FROM numbers(6000, 4000);
    INSERT INTO t_real SELECT number, number % 100, number FROM numbers(3000);
    INSERT INTO t_real SELECT number, number % 100, number FROM numbers(3000, 3000);
    INSERT INTO t_real SELECT number, number % 100, number FROM numbers(6000, 4000);
    -- WITH SETTINGS granularity overrides need an adaptive-granularity parent
    DROP TABLE IF EXISTS t_est_g; DROP TABLE IF EXISTS t_real_g;
    CREATE TABLE t_est_g (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 100, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
    CREATE TABLE t_real_g AS t_est_g;
    ALTER TABLE t_real_g ADD PROJECTION p_g (SELECT a, b, v ORDER BY b) WITH SETTINGS (index_granularity = 50);
    INSERT INTO t_est_g SELECT number, number % 100, number FROM numbers(10000);
    INSERT INTO t_real_g SELECT number, number % 100, number FROM numbers(10000);
"

# the read-step header holds the final granule count, the per-index lines vary by build
compare()
{
    local projection_name="$1" projection_body="$2" query="$3" est="${4:-t_est}" real="${5:-t_real}"
    echo "hypothetical:"
    $CLICKHOUSE_CLIENT -q "
        CREATE HYPOTHETICAL PROJECTION ${projection_name} ON ${est} ${projection_body};
        EXPLAIN WHATIF ${query/TABLE/$est} SETTINGS ${PIN};
    " | grep -E '^\s+(status|marks|skip_ratio|verdict|source):' | awk '{$1=$1; print}'
    echo "real:"
    $CLICKHOUSE_CLIENT -q "EXPLAIN indexes = 1 ${query/TABLE/$real} SETTINGS ${PIN}, preferred_optimize_projection_name = '${projection_name}'" \
        | grep -oE 'ReadFromMergeTree \([^)]*\)|Granules: [0-9]+$'
}

echo "--- point query on the projection key, three parts ---"
compare p_b "(SELECT a, b, v ORDER BY b)" "SELECT count() FROM TABLE WHERE b = 42"

echo "--- range on the projection key ---"
compare p_b "(SELECT a, b, v ORDER BY b)" "SELECT count() FROM TABLE WHERE b >= 10 AND b < 20"

echo "--- composite key, leading column ---"
compare p_ba "(SELECT a, b, v ORDER BY (b, a))" "SELECT count() FROM TABLE WHERE b = 42"

echo "--- base PK already prunes to one granule, the projection cannot beat it ---"
compare p_b "(SELECT a, b, v ORDER BY b)" "SELECT count() FROM TABLE WHERE a < 100 AND b = 42"

echo "--- key over a computed expression ---"
compare p_c "(SELECT a, b, v, b * 2 AS c ORDER BY c)" "SELECT count() FROM TABLE WHERE b * 2 = 84"

echo "--- the projection's own index_granularity is used ---"
compare p_g "(SELECT a, b, v ORDER BY b) WITH SETTINGS (index_granularity = 50)" "SELECT count() FROM TABLE WHERE b = 42" t_est_g t_real_g

echo "--- no filter, the projection serves the ORDER BY ---"
compare p_b "(SELECT a, b, v ORDER BY b)" "SELECT a, b, v FROM TABLE ORDER BY b"
# with read-in-order off there is nothing the projection could help with, as in the optimizer
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_b ON t_est (SELECT a, b, v ORDER BY b);
    EXPLAIN WHATIF SELECT a, b, v FROM t_est ORDER BY b SETTINGS ${PIN}, optimize_read_in_order = 0;
" | grep -E '^\s+reason:' | awk '{$1=$1; print}'

echo "--- the INDEX form, which the optimizer serves the read from ---"
compare p_idx "INDEX b TYPE basic" "SELECT count() FROM TABLE WHERE b = 42"

echo "--- not applicable cases ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_key ON t_est (SELECT a, b, v ORDER BY b);
    CREATE HYPOTHETICAL PROJECTION p_agg ON t_est (SELECT b, sum(v) GROUP BY b);
    CREATE HYPOTHETICAL PROJECTION p_where ON t_est (SELECT a, b, v WHERE b < 50 ORDER BY b);
    CREATE HYPOTHETICAL PROJECTION p_nocol ON t_est (SELECT a, b ORDER BY b);
    CREATE HYPOTHETICAL PROJECTION p_skipidx ON t_est (SELECT a, b, v ORDER BY a) WITH SETTINGS (add_minmax_index_for_numeric_columns = 1);
    EXPLAIN WHATIF SELECT sum(v) FROM t_est WHERE a = 5000 SETTINGS ${PIN};
" | grep -E '^With|^\s+reason:' | awk '{$1=$1; print}'

echo "--- projections disabled by the query ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_b ON t_est (SELECT a, b, v ORDER BY b);
    EXPLAIN WHATIF SELECT count() FROM t_est WHERE b = 42 SETTINGS optimize_trivial_count_query = 0, optimize_use_projections = 0;
" | grep -E '^\s+reason:' | awk '{$1=$1; print}'

echo "--- empirical = 0 scans nothing ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_b ON t_est (SELECT a, b, v ORDER BY b);
    EXPLAIN WHATIF empirical = 0 SELECT count() FROM t_est WHERE b = 42 SETTINGS ${PIN};
" | grep -E '^\s+(status|source|empirical_status):' | awk '{$1=$1; print}'

echo "--- the scan honours max_rows_to_read ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_b ON t_est (SELECT a, b, v ORDER BY b);
    EXPLAIN WHATIF SELECT count() FROM t_est WHERE b = 42 SETTINGS ${PIN}, max_rows_to_read = 100, read_overflow_mode = 'break';
" | grep -E '^\s+(status|source|empirical_status|empirical_reason):' | awk '{$1=$1; print}'

# the scan reads the projection columns, so SELECT is checked at estimate time
echo "--- estimating needs SELECT on the projection columns ---"
user="u_estimate_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT -q "
    DROP USER IF EXISTS ${user}; CREATE USER ${user} NOT IDENTIFIED;
    GRANT ALTER ADD PROJECTION ON ${CLICKHOUSE_DATABASE}.t_est TO ${user};
    GRANT SELECT(a, b) ON ${CLICKHOUSE_DATABASE}.t_est TO ${user};
"
$CLICKHOUSE_CLIENT --user "${user}" -q "
    CREATE HYPOTHETICAL PROJECTION p_priv ON ${CLICKHOUSE_DATABASE}.t_est (SELECT a, b, v ORDER BY b);
    EXPLAIN WHATIF SELECT count() FROM ${CLICKHOUSE_DATABASE}.t_est WHERE b = 42 SETTINGS ${PIN};
" 2>&1 | grep -m1 -oE 'ACCESS_DENIED'
$CLICKHOUSE_CLIENT -q "GRANT SELECT(v) ON ${CLICKHOUSE_DATABASE}.t_est TO ${user};"
$CLICKHOUSE_CLIENT --user "${user}" -q "
    CREATE HYPOTHETICAL PROJECTION p_priv ON ${CLICKHOUSE_DATABASE}.t_est (SELECT a, b, v ORDER BY b);
    EXPLAIN WHATIF SELECT count() FROM ${CLICKHOUSE_DATABASE}.t_est WHERE b = 42 SETTINGS ${PIN};
" 2>&1 | grep -E '^\s+status:' | awk '{$1=$1; print}'
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${user}"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_est; DROP TABLE IF EXISTS t_real; DROP TABLE IF EXISTS t_est_g; DROP TABLE IF EXISTS t_real_g;"
