# distributed-plan-probe

Compares `make_distributed_plan = 1` against local execution over a battery of queries and reports, per query,
whether the result matches, whether the query really went through the distributed executor, and why it fell back
to local execution when it did. It found the defects tracked in https://github.com/ClickHouse/ClickHouse/issues/118534
(forced projections, vector index header order, top-K skip-index pruning vs pinned buckets, `EXPLAIN ESTIMATE`,
read limits on workers, `max_rows_in_join`, `rows_before_limit_at_least`, truncated sets).

## Files

| File | Purpose |
|------|---------|
| `probe.py` | the runner |
| `setup.sql` | creates database `probe`: MergeTree variants (skip index, FINAL, partitions, SAMPLE, projections, lightweight deletes, row policy, text and vector indexes), Memory, Log, Join, Set, Buffer, Null, views, materialized view, Distributed (1 and 2 shards), Merge, JSON/Dynamic/Variant, AggregateFunction states, a dictionary, DML targets |
| `queries.txt` | 321 queries: aggregation modifiers, sorting and limits, window functions, array joins and lambdas, every join kind and algorithm, IN/sets/correlated/scalar subqueries, CTEs, set operations, every source, MergeTree read variants, types and functions, EXPLAIN variants, settings scoping, DML |
| `queries_settings_contracts.txt` | 171 queries hunting settings whose guarantee a distributed plan may break (limits, overflow modes, `force_*`, query cache, quotas, ...) |
| `queries_two_shards.txt` | Distributed tables, `remote()`, `cluster()` over the two-shard cluster of the CI config |
| `queries_vector_index.txt` | vector similarity index queries; aborts a debug server on builds without the header-order fix, run them against a throwaway server |

## Running

The server needs `query_log` and `text_log`, and a `distributed_query.temporary_files_storage` section for
`distributed_plan_execute_locally = 1`; the CI stateless config (`tests/config/install.sh`) provides all of it, and its
`clusters.xml` provides `test_shard_localhost` and `test_cluster_two_shards` used by `setup.sql`.

```bash
clickhouse client --multiquery < setup.sql
./probe.py queries.txt > report.md
./probe.py queries.txt --url http://127.0.0.1:18123 --modes base,dist,strict   # another server, fewer modes
```

Every query runs once per mode with its own `query_id`:

| mode | settings |
|------|----------|
| `base` | `make_distributed_plan = 0` (the reference result) |
| `dist` | `make_distributed_plan = 1, distributed_plan_execute_locally = 1` (fallback enabled, the default) |
| `strict` | `dist` + `distributed_plan_fallback_to_local_execution = 0` (an undistributable plan throws `SUPPORT_IS_DISABLED`) |
| `casc` | `dist` + `enable_cascades_optimizer = 1, distributed_plan_workers_num = 2` |
| `cstrict` | `casc` + strict |

All modes also set `distributed_plan_max_rows_to_broadcast = 0` (buckets even the small probe tables and shuffles every
join, so the plan shapes are the ones large tables get), `max_rows_to_group_by = 0` (the CI profile sets it to 10G,
which alone makes every aggregation fall back) and `enable_parallel_replicas = 0`.

## Reading the report

Per query and mode: `SAME` / `SAME(reordered)` / `DIFF` / `ERR <code>` against the local result, `distributed_exec`
(1 = the query went through the distributed executor, 0 = it ran locally) from `system.query_log`, and the fallback
reason logged by the `makeDistributedPlan` logger in `system.text_log`. The `Findings` section at the end applies the
filters that matter:

1. `dist` differs from local or errors while local succeeds: real defects, plus expected nondeterminism (`any()`,
   `topK`, ties in `ORDER BY ... LIMIT`, `LIMIT` without `ORDER BY`).
2. `strict` fails with anything other than `SUPPORT_IS_DISABLED`: the plan was accepted as distributable and failed
   later, on a worker or at stage start; these are the cases the fallback decision does not see.
3. `strict` passes although `dist` logged a fallback: the decision is not deterministic or depends on the mode.
4. `dist` ran locally with no fallback reason logged: a silent local run (DDL and EXPLAIN are excluded).
5. `dist` was distributed but a nested plan fell back: usually benign (a set source, a scalar subquery, the projection
   calculation of an INSERT), worth a glance.

Raw results, including full outputs and errors, go to `results.json`.

## Notes

- A debug build aborts the server on any `LOGICAL_ERROR`; run suspicious query files last or against a scratch server.
- Cascades mode needs `distributed_plan_workers_num` when no stateless worker cluster is configured; the runner sets it.
- Worker tasks of a distributed query log their own `query_log` rows with the initiator's `query_id` and
  `log_comment`; aggregate per `query_id` when correlating.
