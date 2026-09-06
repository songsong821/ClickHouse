#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A pressure sweep claims staged chunks up to the part bound. A claim that reaches the bound is a
# part of its own, drained into a producer-local table and written; one that runs out of chunks
# first is a tail, drained into the session's shared table, where the tails accumulate toward a
# part instead of fragmenting per producer. That claim is sized against a fresh part, which is
# what tells the two apart, and it used to be the only sizing: a tail was drained into the shared
# table whatever the table already held, and the table was written out only afterwards, once it
# was found over the bound. A residue just under a part and a tail just under a part therefore
# built a shared table of nearly two parts on every such sweep, which is the working set the
# bound exists to keep. The sweep now writes the residue out first when the residue and the tail
# together would reach the bound, so the shared table never holds more than a part.
#
# The shape makes every tail nearly half a part: keys three hundred bytes wide in blocks of
# sixty-five thousand records stage about twenty megabytes per block, over the thirty-two-megabyte
# floor of the part bound at this threshold once the drain's own cells are counted, so each block's
# chunk is cut into three pieces of at most half a part. Two of them fill a claim, which goes to a
# producer-local table, and the third is the tail that goes to the shared table, so the residue
# there is half a part when the next tail of half a part arrives, and the residue is written out
# before it. The sweep still checks the table after a drain, because the estimate it decides on
# cannot see the arena's chunk doubling: a small tail into a residue that sits at a chunk boundary
# can take the table's allocation over the bound in one step, so the count of writes before a tail
# is asserted to be positive rather than equal to the count of all writes.
# All keys are distinct, so the thaw verdict cannot fire and the whole stream goes through the
# staging path; the thresholds are pinned because the runner randomizes them. Two threads, because
# the adaptive path does not engage on one.
#
# The query runs in its own clickhouse-local process, so the counters in `system.events` belong
# to it alone. The memory limit is a loose ceiling; the assertions that carry this test are the
# one on the shared table's writes before a tail.
$CLICKHOUSE_LOCAL --query "
SET enable_adaptive_aggregator = 1;
SET adaptive_aggregator_freeze_threshold = 1000;
SET adaptive_aggregator_freeze_threshold_bytes = 0;
SET group_by_two_level_threshold = 1000;
SET group_by_two_level_threshold_bytes = 1000000;
SET collect_hash_table_stats_during_aggregation = 0;
SET max_bytes_before_external_group_by = 20000000;
SET max_bytes_ratio_before_external_group_by = 0;
SET max_memory_usage = 300000000;
SET max_threads = 2;
SET max_block_size = 65536;

SELECT count(), sum(c) FROM (
    SELECT concat(repeat('k', 300), toString(number)) AS k, count() AS c
    FROM numbers_mt(1000000) GROUP BY k);

SELECT 'went external', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'ExternalAggregationWritePart') > 0;
SELECT 'the valve ran', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPressureSweeps') > 0;
SELECT 'the tables froze', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationLocalFreezes') > 0;
SELECT 'the chunks were cut', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationStagedChunkSplits') > 0;
SELECT 'the shared table was written', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationSharedTableSpills') > 0;
SELECT 'the residue was written before a tail would have filled the shared table', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationSharedTableSpillsBeforeTail') > 0;
SELECT 'stayed on the frozen path',
    (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationThaws') = 0
    AND (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPressureStandDowns') = 0;
"
