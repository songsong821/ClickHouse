#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: SET ast_fuzzer_runs / ast_fuzzer_oracle are EXPERIMENTAL-tier settings and
#              are not allowed when `allow_feature_tier=0` (the Fast test default).
# no-parallel: the proof event `ASTFuzzerOracleChecks` is server-global, and the assertions
#              below require it to stay put, so no other test may run oracle checks against
#              the same server meanwhile.
#
# `approx_top_k` / `approx_top_sum` keep a bounded counter table (the space-saving algorithm),
# so which elements survive - and the counts reported for them - depend on the order the values
# arrive in and on how the partial states are merged. The oracles compare results for exact row
# equality, so `QueryOracleChecker` has to reject a query naming such an aggregate rather than
# check it. `QueryFuzzer`'s swap list offers `approx_top_k` for any single-argument aggregate,
# which is how a false `TLP Aggregate oracle mismatch!` reached master CI:
# https://s3.amazonaws.com/clickhouse-test-reports/praktika.html?REF=master&sha=6c335833c268d3c52d83b47abf4c2f807251e716&name_0=MasterCI&name_1=Stateless%20tests%20%28amd_debug%2C%20sequential%29

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS oracle_approx_top;
    CREATE TABLE oracle_approx_top (v Int64) ENGINE = MergeTree ORDER BY v;
    -- All values distinct and far more numerous than the default k = 10, so every partition of
    -- a TLP rewrite contributes its own equally-frequent candidates: were the oracle to run
    -- here, the merged top-k would legitimately differ from the directly computed one.
    INSERT INTO oracle_approx_top SELECT number FROM numbers(200);
"

# The oracle rejects any query reading `system.*`, so reading the counter can never move it.
get_counter()
{
    $CLICKHOUSE_CLIENT --query "SELECT toInt64(sum(value)) FROM system.events WHERE event = 'ASTFuzzerOracleChecks'"
}

# `send_logs_level = 'fatal'` suppresses the expected error-level log lines from random
# mutations that produce valid-but-nonsense queries (see 04256_04250). No `FORMAT Null` on the
# fuzzed query: the oracle skips queries carrying an explicit FORMAT clause, which would make
# every assertion below vacuous - discard the output via redirection instead.
run_fuzzed()
{
    $CLICKHOUSE_CLIENT --query "
        SET send_logs_level = 'fatal';
        SET ast_fuzzer_runs = 1;
        SET ast_fuzzer_oracle = 1;
        $1
    " >/dev/null 2>/dev/null
}

# One fuzzer iteration per round, over four unsafe aggregates at once. The swap path replaces
# one aggregate node in 30 and most names it can pick are themselves oracle-unsafe, so a round
# that loses every `approx_top_*` is vanishingly unlikely - across all rounds the gate must
# reject the query every time and the counter must not budge.
before=$(get_counter)
for _ in $(seq 1 20)
do
    run_fuzzed "SELECT approx_top_k(v), approx_top_k(v + 1), approx_top_k(v * 2), approx_top_sum(v, 1) FROM oracle_approx_top WHERE v > 5;"
done
after=$(get_counter)

if [[ "$after" -eq "$before" ]]
then
    echo "approx_top not checked"
else
    echo "approx_top checked $((after - before)) times"
fi

# Alias spellings resolve to the same unsafe aggregates and must be rejected as well:
# `approx_top_count` is an alias of `approx_top_k`, `min_by` of `argMin`, `array_agg` of
# `groupArray`, `medianExact` of `quantileExact`, and `array_concat_agg` of `groupArrayArray`
# - which is itself `groupArray` plus an `Array` combinator, so the expansion has to be
# stripped in turn. The backstop set names none of these under its alias, so this only holds
# once the name is resolved through the aggregate function factory.
before=$after
for _ in $(seq 1 20)
do
    run_fuzzed "SELECT approx_top_count(v), min_by(v, v), array_agg(v), medianExact(v), array_concat_agg([v]) FROM oracle_approx_top WHERE v > 5;"
done
after=$(get_counter)

if [[ "$after" -eq "$before" ]]
then
    echo "alias spellings not checked"
else
    echo "alias spellings checked $((after - before)) times"
fi

# Positive control: the same query shape over exact, merge-order-independent aggregates IS
# checked. This proves the counter is live under these settings, so the two results above are
# the gate rejecting the unsafe aggregates and not the oracle ignoring this query shape
# altogether. Retried until the counter moves, because a single mutation can occasionally
# break oracle eligibility for an unrelated reason (see 04658).
before=$after
for _ in $(seq 1 100)
do
    run_fuzzed "SELECT count(), min(v), max(v) FROM oracle_approx_top WHERE v > 5;"
    after=$(get_counter)
    if [[ "$after" -gt "$before" ]]
    then
        break
    fi
done

if [[ "$after" -gt "$before" ]]
then
    echo "exact aggregates checked"
else
    echo "exact aggregates never checked: counter stayed at $after"
fi

$CLICKHOUSE_CLIENT --query "DROP TABLE oracle_approx_top"
