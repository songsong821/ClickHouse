#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings
# The exact number of streams and marks files depends on the serialization settings.

# Streams whose data file is empty (for example, `SharedVariant` of a `JSON` path whose values all have one type)
# have nothing to read, so their marks files must not be loaded. On object storage every marks file is a network
# round trip, and such streams are about 40% of the files of a part with a `JSON` column.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_json_empty_streams;
    CREATE TABLE t_json_empty_streams (t UInt32, json JSON)
    ENGINE = MergeTree ORDER BY t
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1, index_granularity = 1;

    SYSTEM STOP MERGES t_json_empty_streams;

    INSERT INTO t_json_empty_streams SELECT number, concat('{\"a\":', toString(number), ',\"b\":\"s', toString(number), '\",\"c\":[', toString(number), '],\"d\":', toString(number / 2), ',\"e\":true}')::JSON FROM numbers(5);
"

# Every path has values of a single type, so the \`SharedVariant\` streams of all paths are empty.
${CLICKHOUSE_CLIENT} -q "
    SELECT 'streams', sum(length(substreams)) FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_json_empty_streams' AND active
"

${CLICKHOUSE_CLIENT} -q "SYSTEM CLEAR MARK CACHE"

QUERY_ID="05109_$(${CLICKHOUSE_CLIENT} -q "SELECT lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")"

# Marks are loaded synchronously in the query thread, so the misses are attributed to the query.
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID}" -q "
    SELECT json FROM t_json_empty_streams WHERE t = 4 FORMAT Null
    SETTINGS max_threads = 1, load_marks_asynchronously = 0, local_filesystem_read_prefetch = 1, use_uncompressed_cache = 0, enable_parallel_replicas = 0
"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"

${CLICKHOUSE_CLIENT} -q "
    SELECT 'marks files loaded', ProfileEvents['MarkCacheMisses']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '${QUERY_ID}' AND type = 'QueryFinish'
"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_json_empty_streams"
