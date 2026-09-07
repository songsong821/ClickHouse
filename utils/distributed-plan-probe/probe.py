#!/usr/bin/env python3
"""Probe `make_distributed_plan` against local execution.

Runs every query of a query file in several modes over HTTP and reports, per query, whether the
distributed result matches the local one, whether the query really went through the distributed
executor (`ProfileEvents['DistributedPlanLocalExecution']` in `system.query_log`), and the reason
logged by the fallback decision (`makeDistributedPlan` logger in `system.text_log`).

Modes:
  base     make_distributed_plan = 0 (the reference result)
  dist     make_distributed_plan = 1, distributed_plan_execute_locally = 1 (fallback enabled, the default)
  strict   dist + distributed_plan_fallback_to_local_execution = 0 (an undistributable plan throws)
  casc     dist + enable_cascades_optimizer = 1, distributed_plan_workers_num = 2
  cstrict  casc + strict

Usage:
  clickhouse client --multiquery < setup.sql          # creates the `probe` database
  ./probe.py queries.txt > report.md                   # default: http://127.0.0.1:8123
  ./probe.py queries.txt --url http://127.0.0.1:18123 --modes base,dist,strict

The server needs `query_log` and `text_log` enabled (the CI stateless config has both) and, for
`distributed_plan_execute_locally`, a `distributed_query.temporary_files_storage` section.
"""

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

FALLBACK_PREFIX = "Cannot make a distributed query plan, falling back to local execution: "

# Applied to every mode. `distributed_plan_max_rows_to_broadcast = 0` buckets even tiny tables and shuffles
# every join, so the small probe tables exercise the same plan shapes as large production tables.
# `max_rows_to_group_by = 0` is needed because the CI stateless profile sets it to 10G, which makes every
# aggregation fall back. `param_p` serves the one parameterized query (`{p:UInt32}`) in queries.txt.
COMMON = {
    "enable_analyzer": "1",
    "enable_parallel_replicas": "0",
    "distributed_plan_max_rows_to_broadcast": "0",
    "max_rows_to_group_by": "0",
    "log_queries": "1",
    "output_format_pretty_row_numbers": "0",
    "param_p": "3",
}

MODES = {
    "base": {"make_distributed_plan": "0"},
    "dist": {"make_distributed_plan": "1", "distributed_plan_execute_locally": "1"},
    "strict": {
        "make_distributed_plan": "1",
        "distributed_plan_execute_locally": "1",
        "distributed_plan_fallback_to_local_execution": "0",
    },
    "casc": {
        "make_distributed_plan": "1",
        "distributed_plan_execute_locally": "1",
        "enable_cascades_optimizer": "1",
        "distributed_plan_workers_num": "2",
    },
    "cstrict": {
        "make_distributed_plan": "1",
        "distributed_plan_execute_locally": "1",
        "enable_cascades_optimizer": "1",
        "distributed_plan_workers_num": "2",
        "distributed_plan_fallback_to_local_execution": "0",
    },
}

SUPPORT_IS_DISABLED = 344


def run(url, query, settings, query_id=None, timeout=120):
    """Execute one query over HTTP. Returns (output, None) or (None, (error_code, first_line))."""
    params = dict(settings)
    if query_id:
        params["query_id"] = query_id
    request = urllib.request.Request(url + "/?" + urllib.parse.urlencode(params), data=query.encode("utf-8"), method="POST")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.read().decode("utf-8", "replace"), None
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")
        code = None
        for line in body.splitlines():
            if line.startswith("Code:"):
                try:
                    code = int(line.split()[1].rstrip("."))
                except (IndexError, ValueError):
                    pass
                break
        first = body.strip().splitlines()[0] if body.strip() else str(e)
        return None, (code, first[:400])
    except Exception as e:  # connection refused, timeout, server died
        return None, (-1, str(e)[:400])


def load_queries(path):
    queries = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            queries.append(line)
    return queries


def is_explain(query):
    return query.lstrip().upper().startswith("EXPLAIN")


def lines_of(output):
    return output.rstrip("\n").split("\n") if output and output.strip() else []


def compare(base, other, query):
    """Classify `other` against the local result `base`."""
    if other["err"]:
        return f"ERR {other['err'][0]}"
    if base is None or base["err"]:
        return "OK"
    a, b = lines_of(base["out"]), lines_of(other["out"])
    if a == b:
        return "SAME"
    if sorted(a) == sorted(b):
        return "SAME(reordered)"
    if is_explain(query):
        return "DIFF(explain)"
    return "DIFF"


def fetch_logs(url, database, query_ids):
    """Return (query_log rows by query_id, fallback reasons by query_id)."""
    run(url, "SYSTEM FLUSH LOGS query_log, text_log", {"database": database})
    time.sleep(1)
    id_list = ",".join("'" + x + "'" for x in query_ids)
    query_log = {}
    out, _ = run(
        url,
        "SELECT query_id, ProfileEvents['DistributedPlanLocalExecution'] AS local_exec, "
        "ProfileEvents['DistributedPlanRemoteTasks'] AS remote_tasks, exception_code "
        f"FROM system.query_log WHERE query_id IN ({id_list}) AND type != 'QueryStart' FORMAT JSONEachRow",
        {"database": database},
    )
    for line in lines_of(out):
        row = json.loads(line)
        query_log[row["query_id"]] = row
    reasons = {}
    out, _ = run(
        url,
        f"SELECT query_id, message FROM system.text_log WHERE query_id IN ({id_list}) "
        f"AND logger_name = 'makeDistributedPlan' AND message LIKE '{FALLBACK_PREFIX[:30]}%' FORMAT JSONEachRow",
        {"database": database},
    )
    for line in lines_of(out):
        row = json.loads(line)
        reasons.setdefault(row["query_id"], []).append(row["message"].replace(FALLBACK_PREFIX, ""))
    return query_log, reasons


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("queries", help="file with one query per line; lines starting with # are comments")
    parser.add_argument("--url", default="http://127.0.0.1:8123", help="HTTP endpoint of the server (default: %(default)s)")
    parser.add_argument("--database", default="probe", help="database created by setup.sql (default: %(default)s)")
    parser.add_argument("--modes", default=",".join(MODES), help="comma-separated subset of: " + ",".join(MODES))
    parser.add_argument("--results-json", default="results.json", help="where to dump the raw results (default: %(default)s)")
    args = parser.parse_args()

    modes = args.modes.split(",")
    for mode in modes:
        if mode not in MODES:
            parser.error(f"unknown mode {mode}")
    queries = load_queries(args.queries)
    run_tag = uuid.uuid4().hex[:6]

    results = []
    for index, query in enumerate(queries):
        record = {"n": index, "query": query, "modes": {}}
        for mode in modes:
            query_id = f"probe_{run_tag}_{index}_{mode}"
            settings = {"database": args.database, **COMMON, **MODES[mode]}
            out, err = run(args.url, query, settings, query_id)
            record["modes"][mode] = {"qid": query_id, "out": out, "err": err}
        results.append(record)
        sys.stderr.write(f"\r{index + 1}/{len(queries)}")
    sys.stderr.write("\n")

    query_log, reasons = fetch_logs(args.url, args.database, [m["qid"] for r in results for m in r["modes"].values()])

    def describe(record, mode):
        entry = record["modes"][mode]
        status = compare(record["modes"].get("base"), entry, record["query"])
        log_row = query_log.get(entry["qid"])
        distributed = log_row["local_exec"] if log_row else "?"
        return status, distributed, sorted(set(reasons.get(entry["qid"], [])))

    findings = {
        "dist: result differs from local or errors while local succeeds": [],
        "strict: error other than SUPPORT_IS_DISABLED while local succeeds": [],
        "strict: passes although dist logged a fallback (inconsistent decision)": [],
        "dist: ran locally without a logged fallback reason (silent local)": [],
        "dist: distributed, but a nested plan fell back (check the reason is benign)": [],
    }

    print("# Distributed plan probe report\n")
    print("Per mode: result vs local | distributed_exec = went through the distributed executor (1/0) | fallback reasons logged\n")
    for record in results:
        query = record["query"]
        base = record["modes"].get("base")
        base_failed = base is not None and base["err"] is not None
        print(f"### [{record['n']}] {query}")
        if base_failed:
            print(f"- base: ERR {base['err'][0]}: {base['err'][1][:200]}")
        dist_reasons = []
        for mode in modes:
            if mode == "base":
                continue
            status, distributed, mode_reasons = describe(record, mode)
            entry = record["modes"][mode]
            line = f"- {mode}: {status} | distributed_exec={distributed}"
            if mode_reasons:
                line += " | fallback: " + " || ".join(mode_reasons)
            if entry["err"]:
                line += f" | {entry['err'][1][:300]}"
            print(line)
            if status.startswith("DIFF") and not is_explain(query):
                print("  base: " + " / ".join(lines_of(base["out"])[:8]))
                print(f"  {mode}: " + " / ".join(lines_of(entry["out"])[:8]))

            if mode == "dist":
                dist_reasons = mode_reasons
                if not base_failed and (status == "DIFF" or status.startswith("ERR")):
                    findings["dist: result differs from local or errors while local succeeds"].append((record["n"], query, line))
                if not is_explain(query) and distributed == 0 and not mode_reasons and not entry["err"]:
                    findings["dist: ran locally without a logged fallback reason (silent local)"].append((record["n"], query, line))
                if distributed == 1 and mode_reasons:
                    findings["dist: distributed, but a nested plan fell back (check the reason is benign)"].append((record["n"], query, line))
            if mode == "strict" and not base_failed:
                if entry["err"] and entry["err"][0] != SUPPORT_IS_DISABLED:
                    findings["strict: error other than SUPPORT_IS_DISABLED while local succeeds"].append((record["n"], query, line))
                if not entry["err"] and dist_reasons:
                    findings["strict: passes although dist logged a fallback (inconsistent decision)"].append((record["n"], query, line))
        print()

    print("# Findings\n")
    print("Expected noise in the first list: nondeterministic results (`any()`, `topK`, ties in `ORDER BY ... LIMIT`, `LIMIT` without ORDER BY).")
    print("DDL and EXPLAIN never go through the distributed executor, so they are excluded from the silent-local list.\n")
    for title, items in findings.items():
        print(f"## {title}: {len(items)}\n")
        for n, query, line in items:
            print(f"- [{n}] {query}")
            print(f"  {line}")
        print()

    with open(args.results_json, "w", encoding="utf-8") as f:
        json.dump({"results": results, "query_log": query_log, "fallback_reasons": reasons}, f, indent=1)


if __name__ == "__main__":
    main()
