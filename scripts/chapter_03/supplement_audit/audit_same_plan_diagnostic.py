#!/usr/bin/env python3
"""Audit same-plan diagnostic artifacts and summarize within-block variation."""
import argparse
import collections
import csv
import hashlib
import json
import re
import statistics
from pathlib import Path


def normalized_final_plan(plan):
    final = plan.split("+- == Initial Plan ==")[0]
    ids = {}
    def replace_id(match):
        token = match.group(1)
        if token not in ids:
            ids[token] = len(ids)
        return "#ID" + str(ids[token])
    final = re.sub(r"#(\d+)", replace_id, final)
    return re.sub(r"plan_id=\d+", "plan_id=ID", final)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--event-log", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    protocol = json.loads((args.results / "protocol.json").read_text())
    rows = list(csv.DictReader((args.results / "runs.csv").open()))
    if len(rows) != protocol["measured_actions"] or len({r["job_group"] for r in rows}) != len(rows):
        raise ValueError("Missing or duplicated measured actions")
    jobs, job_end, stages, tasks = collections.defaultdict(list), {}, {}, collections.defaultdict(list)
    application_end = False
    for line in args.event_log.open():
        event = json.loads(line)
        kind = event.get("Event")
        if kind == "SparkListenerJobStart":
            props = event.get("Properties") or {}
            jobs[props.get("spark.jobGroup.id")].append((event["Job ID"], props.get("spark.sql.execution.id"), event.get("Stage IDs", [])))
        elif kind == "SparkListenerJobEnd":
            job_end[event["Job ID"]] = (event.get("Job Result") or {}).get("Result")
        elif kind == "SparkListenerStageSubmitted":
            info = event["Stage Info"]
            stages[(info["Stage ID"], info["Stage Attempt ID"])] = event
        elif kind == "SparkListenerTaskEnd":
            tasks[(event["Stage ID"], event["Stage Attempt ID"])].append(event)
        elif kind == "SparkListenerApplicationEnd":
            application_end = True
    reference_rows = json.loads((args.results / "outputs" / "duckdb_reference.json").read_text())
    reference = {(r[0], r[1]): r[2:] for r in reference_rows}
    checks, pairs = [], collections.defaultdict(dict)
    for r in rows:
        group = r["job_group"]
        plan = Path(r["plan_path"]).read_text()
        final = normalized_final_plan(plan)
        output = json.loads(Path(r["output_path"]).read_text())
        observed = {(x["PU_Borough"], x["DO_Borough"]): (x["cnt"], x["tot_miles"], x["tot_fare"]) for x in output}
        correct = len(observed) == len(output) and observed.keys() == reference.keys() and all(
            observed[k][0] == reference[k][0] and all(observed[k][i] is not None and reference[k][i] is not None and
            abs(observed[k][i] - reference[k][i]) <= 0.05 for i in (1, 2)) for k in reference)
        grouped = jobs[group]
        sql = {j[1] for j in grouped}
        stage_key = (int(r["target_stage_id"]), int(r["target_stage_attempt"]))
        stage = stages[stage_key]
        props = stage.get("Properties") or {}
        codegen = set()
        for rdd in stage["Stage Info"].get("RDD Info", []):
            if rdd.get("Scope"):
                codegen.update(re.findall(r"WholeStageCodegen \((\d+)\)", json.loads(rdd["Scope"]).get("name", "")))
        join_codes = set(re.findall(r"\*\((\d+)\)\s+SortMergeJoin", plan))
        successful = [t for t in tasks[stage_key] if (t.get("Task End Reason") or {}).get("Reason") == "Success"]
        times = [t["Task Metrics"]["Executor Run Time"] for t in successful]
        event_tsr = round(max(times) / statistics.median(times), 2) if times and statistics.median(times) else None
        cfg = {"spark.sql.adaptive.enabled": "true", "spark.sql.adaptive.coalescePartitions.enabled": "true",
               "spark.sql.adaptive.skewJoin.enabled": "false", "spark.sql.shuffle.partitions": "200",
               "spark.sql.autoBroadcastJoinThreshold": "-1", "spark.sql.adaptive.autoBroadcastJoinThreshold": "-1"}
        fields_match = all(str(props.get(k, "")).lower() == value for k, value in cfg.items())
        check = {"block": int(r["block"]), "label": r["label"], "job_group": group, "job_count": len(grouped),
                 "sql_execution_count": len(sql), "job_success": bool(grouped) and all(job_end.get(j[0]) == "JobSucceeded" for j in grouped),
                 "stage_execution_match": len(sql) == 1 and props.get("spark.sql.execution.id") in sql and any(stage_key[0] in j[2] for j in grouped),
                 "target_contains_join": bool(codegen & join_codes), "config_matches": fields_match,
                 "successful_tasks": len(successful), "all_target_tasks_success": len(successful) == len(tasks[stage_key]),
                 "tsr_matches": event_tsr == float(r["task_skew_ratio_max"]), "duckdb_matches": correct,
                 "output_sha256_matches": hashlib.sha256(Path(r["output_path"]).read_bytes()).hexdigest() == r["output_sha256"],
                 "plan_sha256_normalized": hashlib.sha256(final.encode()).hexdigest(),
                 "final_plan_markers": all(x in final.lower() for x in ("isfinalplan=true", "sortmergejoin", "aqeshuffleread coalesced")) and
                     not re.search(r"(?i)broadcast(?:hash|nestedloop)join|skew=true|skewed", final),
                 "collector_ok": r["collector_status"] == "OK"}
        checks.append(check)
        pairs[int(r["block"])][r["label"]] = (float(r["wall_clock_duration_s"]), int(r["position"]))
    if set(pairs) != set(range(1, protocol["blocks"] + 1)) or any(set(p) != {"X", "Y"} for p in pairs.values()):
        raise ValueError("Pair/block coverage failure")
    pair_rows = []
    for block, pair in sorted(pairs.items()):
        x, px = pair["X"]
        y, py = pair["Y"]
        pair_rows.append({"block": block, "order": "XY" if px < py else "YX", "x_s": x, "y_s": y,
                          "y_div_x": y / x, "abs_y_div_x_minus_1_pct": abs(y / x - 1) * 100,
                          "first_s": x if px < py else y, "second_s": y if px < py else x})
    metrics = [r["abs_y_div_x_minus_1_pct"] for r in pair_rows]
    hashes = sorted({r["plan_sha256_normalized"] for r in checks})
    check_fields = ("job_success", "stage_execution_match", "target_contains_join", "config_matches", "all_target_tasks_success", "tsr_matches", "duckdb_matches", "output_sha256_matches", "final_plan_markers", "collector_ok")
    summary = {"application_end": application_end, "actions": len(rows), "blocks": len(pair_rows),
               "checks_passed": {k: sum(bool(r[k]) for r in checks) for k in check_fields},
               "one_sql_execution": sum(r["sql_execution_count"] == 1 for r in checks),
               "normalized_final_plan_hashes": hashes, "target_task_counts": dict(collections.Counter(r["successful_tasks"] for r in checks)),
               "orders": dict(collections.Counter(r["order"] for r in pair_rows)),
               "median_abs_y_div_x_minus_1_pct": statistics.median(metrics), "min_abs_pct": min(metrics), "max_abs_pct": max(metrics),
               "blocks_abs_pct_above_5": sum(m > 5 for m in metrics),
               "first_slower_count": sum(r["first_s"] > r["second_s"] for r in pair_rows),
               "event_log": str(args.event_log), "results": str(args.results)}
    args.output.mkdir(parents=True, exist_ok=False)
    for name, values in (("action_audit.csv", checks), ("paired_differences.csv", pair_rows)):
        with (args.output / name).open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=values[0].keys()); writer.writeheader(); writer.writerows(values)
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
