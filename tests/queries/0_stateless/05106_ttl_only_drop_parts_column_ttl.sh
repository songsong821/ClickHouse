#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ttl_only_drop_parts` trades the merges that delete expired rows for dropping whole parts, but a
# column TTL can only be honoured by rewriting the part. The setting must therefore not suppress the
# merges that clear an expired column, and it must not clear a column whose TTL has not expired yet.

for only_drop in 0 1
do
    $CLICKHOUSE_CLIENT --query "
        DROP TABLE IF EXISTS ttl_col_${only_drop};
        CREATE TABLE ttl_col_${only_drop}
        (
            d Date,
            keep String,
            expired String TTL d + INTERVAL 1 DAY,
            not_expired String TTL d + INTERVAL 100 YEAR
        )
        ENGINE = MergeTree ORDER BY d
        SETTINGS ttl_only_drop_parts = ${only_drop}, merge_with_ttl_timeout = 0, min_bytes_for_wide_part = 0;

        INSERT INTO ttl_col_${only_drop} VALUES ('2020-01-01', 'keep', 'expired', 'not_expired');"
done

# The background TTL merge is asynchronous; wait for it instead of forcing it with OPTIMIZE, because
# OPTIMIZE bypasses the merge selector that this test is about.
for only_drop in 0 1
do
    for _ in {1..300}
    do
        result=$($CLICKHOUSE_CLIENT --query "SELECT expired = '' FROM ttl_col_${only_drop}")
        [[ "$result" == "1" ]] && break
        sleep 0.3
    done
done

for only_drop in 0 1
do
    echo "ttl_only_drop_parts = ${only_drop}"
    $CLICKHOUSE_CLIENT --query "
        SELECT count(), keep, expired, not_expired FROM ttl_col_${only_drop} GROUP BY keep, expired, not_expired;"
done

for only_drop in 0 1
do
    $CLICKHOUSE_CLIENT --query "DROP TABLE ttl_col_${only_drop};"
done
