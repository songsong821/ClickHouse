#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings

# Metadata streams of `JSON` and `Dynamic` columns (`object_structure`, `dynamic_structure`) are read only while
# deserializing the prefix and are released right after that. The prefetch for the current mark must not create
# them again and read the files a second time: on object storage every file is a network round trip.
# So every file of the part is opened at most once: the marks file of each loaded stream and the data file of each stream.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_json_metadata_streams;
    CREATE TABLE t_json_metadata_streams (t UInt32, json JSON)
    ENGINE = MergeTree ORDER BY t
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1, index_granularity = 1;

    SYSTEM STOP MERGES t_json_metadata_streams;

    INSERT INTO t_json_metadata_streams SELECT number, concat('{\"a\":', toString(number), ',\"b\":\"s', toString(number), '\",\"c\":[', toString(number), '],\"d\":', toString(number / 2), ',\"e\":true}')::JSON FROM numbers(5);
"

STREAMS=$(${CLICKHOUSE_CLIENT} -q "
    SELECT sum(length(substreams)) FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_json_metadata_streams' AND active
")

${CLICKHOUSE_CLIENT} -q "SYSTEM CLEAR MARK CACHE"

QUERY_ID="05111_$(${CLICKHOUSE_CLIENT} -q "SELECT lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")"

# Read a granule that is not the first one, with prefetch enabled.
${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID}" -q "
    SELECT json FROM t_json_metadata_streams WHERE t = 4 FORMAT Null
    SETTINGS max_threads = 1, load_marks_asynchronously = 0, local_filesystem_read_prefetch = 1, use_uncompressed_cache = 0, enable_parallel_replicas = 0
"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"

${CLICKHOUSE_CLIENT} -q "
    SELECT 'streams', ${STREAMS}, 'every file is opened at most once', ProfileEvents['FileOpen'] <= ProfileEvents['MarkCacheMisses'] + ${STREAMS}
    FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '${QUERY_ID}' AND type = 'QueryFinish'
"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_json_metadata_streams"
