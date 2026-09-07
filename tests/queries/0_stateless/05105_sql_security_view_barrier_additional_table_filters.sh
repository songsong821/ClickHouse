#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An `additional_table_filters` entry keyed to a view (by its qualified name or by its alias) is a
# predicate of the invoker, applied to the view's output. For a `SQL SECURITY DEFINER` / `NONE`
# view it must stay above the view's own read: `QueryAnalyzer::inlineViewSubqueryIfNeeded` keeps
# such a view a table expression, and the filter becomes a separate `Filter` step, while the same
# predicate over the `SQL SECURITY INVOKER` twin is free to sink into the read as a PREWHERE.
#
# The same must hold under `parallel_replicas_allow_view_over_mergetree = 1`, which otherwise
# replaces a "simple" view with the `MergeTree` table below it and reads that table with parallel
# replicas under the outer query's context - the shortcut `05104` fences off for a view that hides
# rows. A matched additional filter takes the query off the parallel-replicas path for every view
# alike; the "no filter" line is the control showing the shortcut is engaged for this shape.

db=${CLICKHOUSE_DATABASE}
invoker="user05105_${CLICKHOUSE_DATABASE}_$RANDOM"
definer="definer05105_${CLICKHOUSE_DATABASE}_$RANDOM"

${CLICKHOUSE_CLIENT} <<EOSQL
CREATE TABLE $db.atf_secrets (owner String, secret String) ENGINE = MergeTree ORDER BY owner;
INSERT INTO $db.atf_secrets SELECT 'visible_owner', 'visible_' || toString(number) FROM numbers(3);
INSERT INTO $db.atf_secrets SELECT 'someone_else', 'other_' || toString(number) FROM numbers(3);

CREATE USER $invoker;
CREATE USER $definer;
GRANT SELECT ON $db.atf_secrets TO $definer;
GRANT SELECT ON $db.atf_secrets TO $invoker;
GRANT CREATE TEMPORARY TABLE ON *.* TO $invoker;

-- Projection-only views: they hide no row of the source table by themselves.
CREATE VIEW $db.atf_definer_view DEFINER = $definer SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.atf_secrets;

CREATE VIEW $db.atf_none_view SQL SECURITY NONE
AS SELECT owner, secret FROM $db.atf_secrets;

CREATE VIEW $db.atf_invoker_view SQL SECURITY INVOKER
AS SELECT owner, secret FROM $db.atf_secrets;

GRANT SELECT ON $db.atf_definer_view TO $invoker;
GRANT SELECT ON $db.atf_none_view TO $invoker;
GRANT SELECT ON $db.atf_invoker_view TO $invoker;
EOSQL

PR_SETTINGS="--enable_analyzer 1 --enable_parallel_replicas 1 --max_parallel_replicas 3 \
    --cluster_for_parallel_replicas test_cluster_one_shard_three_replicas_localhost \
    --parallel_replicas_for_non_replicated_merge_tree 1 --parallel_replicas_plan_based 0 \
    --parallel_replicas_allow_view_over_mergetree 1 --parallel_replicas_local_plan 0 \
    --parallel_replicas_min_number_of_rows_per_replica 0 --automatic_parallel_replicas_mode 0"

function plan_markers()
{
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} $PR_SETTINGS --user "$invoker" --query "
        SELECT countIf(explain LIKE '%ReadFromRemoteParallelReplicas%') AS parallel_replicas_reads,
               countIf(explain LIKE '%Prewhere filter column%') AS filters_inside_the_read
        FROM (EXPLAIN indexes = 0 $1)
        FORMAT TSV"
}

for view in atf_definer_view atf_none_view atf_invoker_view; do
    echo "--- $view (parallel replicas reads, filters inside the read) ---"
    echo -e "no filter:\t$(plan_markers "SELECT owner, secret FROM $db.$view")"
    echo -e "filter on the view:\t$(plan_markers "SELECT owner, secret FROM $db.$view
        SETTINGS additional_table_filters = {'$db.$view': 'owner = ''visible_owner'''}")"
    echo -e "filter on the alias:\t$(plan_markers "SELECT owner, secret FROM $db.$view AS atf_alias
        SETTINGS additional_table_filters = {'atf_alias': 'owner = ''visible_owner'''}")"
    # shellcheck disable=SC2086
    echo -e "rows through the filter:\t$(${CLICKHOUSE_CLIENT} $PR_SETTINGS --user "$invoker" --query "
        SELECT count() FROM $db.$view
        SETTINGS additional_table_filters = {'$db.$view': 'owner = ''visible_owner'''}")"
done

${CLICKHOUSE_CLIENT} --query "DROP VIEW $db.atf_definer_view, $db.atf_none_view, $db.atf_invoker_view"
${CLICKHOUSE_CLIENT} --query "DROP USER $invoker, $definer"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.atf_secrets"
