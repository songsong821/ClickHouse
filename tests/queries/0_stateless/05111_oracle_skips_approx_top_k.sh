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

# One probe per spelling: each query names exactly ONE of the aggregates under test, so a
# probe fails on its own the moment that spelling stops being rejected - a query mixing
# several unsafe aggregates would stay oracle-unsafe (and the test green) even if all but
# one of them were dropped from the denylist.
# The spelling is repeated three times within the query because `QueryFuzzer` replaces an
# aggregate node with a random name from its swap list once in 30 nodes: with a single
# occurrence a swap to a deterministic aggregate would let the oracle run and turn the
# assertion into a false failure, while losing all three occurrences in one round is
# vanishingly unlikely. Aliases are not swapped at all (the swap list matches on the
# canonical prefix), so for them the repetition is merely harmless.
#
# Three rounds, not more. Rounds buy only insurance against a round in which the mutation
# happens to make the query ineligible for an unrelated reason and the probe passes
# vacuously; that risk falls off geometrically, so three is already plenty. They do not add
# detection power - a missing denylist entry moves the counter on the very first round - and
# each extra round costs real time and one more chance of the false failure above. Keep this
# number small: at eight rounds the whole test took ~204s under `amd_asan_ubsan` and tripped
# the 180s per-test limit.
probe()
{
    local label="$1"
    local aggregates="$2"
    local before
    local after

    before=$(get_counter)
    for _ in $(seq 1 3)
    do
        run_fuzzed "SELECT $aggregates FROM oracle_approx_top WHERE v > 5;"
    done
    after=$(get_counter)

    if [[ "$after" -eq "$before" ]]
    then
        echo "$label not checked"
    else
        echo "$label checked $((after - before)) times"
    fi
}

probe "approx_top_k" "approx_top_k(v), approx_top_k(v + 1), approx_top_k(v * 2)"
probe "approx_top_sum" "approx_top_sum(v, 1), approx_top_sum(v + 1, 1), approx_top_sum(v * 2, 1)"

# Alias spellings resolve to the same unsafe aggregates and must be rejected as well:
# `approx_top_count` is an alias of `approx_top_k`, `min_by` of `argMin`, `array_agg` of
# `groupArray`, `medianTDigest` of the approximate `quantileTDigest`, `anova` of
# `analysisOfVariance`, and `array_concat_agg` of `groupArrayArray` - which is itself
# `groupArray` plus an `Array` combinator, so the expansion has to be stripped in turn.
# The backstop set names none of these under its alias, so this only holds once the name is
# resolved through the aggregate function factory.
probe "approx_top_count" "approx_top_count(v), approx_top_count(v + 1), approx_top_count(v * 2)"
probe "min_by" "min_by(v, v), min_by(v + 1, v), min_by(v * 2, v)"
probe "array_agg" "array_agg(v), array_agg(v + 1), array_agg(v * 2)"
probe "array_concat_agg" "array_concat_agg([v]), array_concat_agg([v + 1]), array_concat_agg([v * 2])"
probe "medianTDigest" "medianTDigest(v), medianTDigest(v + 1), medianTDigest(v * 2)"
probe "anova" "anova(v, (v % 3)::UInt8), anova(v + 1, (v % 3)::UInt8), anova(v * 2, (v % 3)::UInt8)"

# Positive controls: the same query shape over exact, merge-order-independent aggregates IS
# checked. This proves the counter is live under these settings, so every probe above is
# the gate rejecting one unsafe spelling and not the oracle ignoring this query shape
# altogether. Retried until the counter moves, because a single mutation can occasionally
# break oracle eligibility for an unrelated reason (see 04658). The retry budget is bounded
# well below the point where the loop alone could exhaust the 180s per-test limit: it
# normally succeeds on the first round, and if it genuinely never fires it is far better to
# report that below than to be killed as a timeout.
positive_control()
{
    local label="$1"
    local aggregates="$2"
    local before
    local after

    before=$(get_counter)
    after=$before
    for _ in $(seq 1 40)
    do
        run_fuzzed "SELECT $aggregates FROM oracle_approx_top WHERE v > 5;"
        after=$(get_counter)
        if [[ "$after" -gt "$before" ]]
        then
            break
        fi
    done

    if [[ "$after" -gt "$before" ]]
    then
        echo "$label checked"
    else
        echo "$label never checked: counter stayed at $after"
    fi
}

positive_control "exact aggregates" "count(), min(v), max(v)"

# The second control is about the alias path specifically: resolving through the factory must
# reject only the aliases whose canonical name is unsafe, not every alias. `BIT_AND` / `BIT_OR`
# / `BIT_XOR` are aliases of `groupBitAnd` / `groupBitOr` / `groupBitXor`, which are
# associative and commutative over integers, are therefore absent from the backstop set, and
# must stay checkable through their alias spelling too. Without this control every alias probe
# above would be satisfied just as well by a checker that rejected all aliases outright.
positive_control "safe aliases" "BIT_AND(v), BIT_OR(v), BIT_XOR(v)"

# And one alias whose canonical name is a `quantile*`: `medianDeterministic` resolves to
# `quantileDeterministic`, which is NOT on the backstop list because
# `ReservoirSamplerDeterministic` retains a sample purely by hash and re-thins on merge, so a
# merged state equals the directly accumulated one. This is the probe that catches alias
# resolution reaching a neighbouring `quantile*` entry (`median` and the approximate
# `quantile*` families ARE listed) and skipping a checkable query.
# The determinator is the constant `1` rather than `v`: with `v` in that position the oracle
# skips the query for an unrelated reason (the `State`/`Merge` rewrite of a determinator
# that is itself the aggregated column does not survive), which would make the probe
# vacuous. A constant determinator still exercises the alias path, which is what is asserted.
positive_control "medianDeterministic" "medianDeterministic(v, 1)"

$CLICKHOUSE_CLIENT --query "DROP TABLE oracle_approx_top"
