#!/usr/bin/env python3
"""Audit measured SQL execution, Spark configuration and stage provenance."""
import argparse
import collections
import csv
import json
import re
import statistics
from pathlib import Path


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("results", type=Path)
    ap.add_argument("event_log", type=Path)
    args = ap.parse_args()
    with (args.results / "runs.csv").open(newline="") as f:
        runs = list(csv.DictReader(f))
    groups = {r["job_group"]: r for r in runs}
    if len(groups) != len(runs):
        raise ValueError("Duplicate job group IDs")
    jobs = collections.defaultdict(list)
    stages = {}
    durations = collections.defaultdict(list)
    with args.event_log.open() as f:
        for line in f:
            ev = json.loads(line)
            event = ev.get("Event")
            if event == "SparkListenerJobStart":
                props = ev.get("Properties") or {}
                group = props.get("spark.jobGroup.id")
                if group in groups:
                    jobs[group].append({"execution": props.get("spark.sql.execution.id"),
                                        "stages": ev.get("Stage IDs", []),
                                        "time": ev.get("Submission Time")})
            elif event == "SparkListenerStageSubmitted":
                info = ev["Stage Info"]
                stages[(info["Stage ID"], info["Stage Attempt ID"])] = {
                    "props": ev.get("Properties") or {}, "rdds": info.get("RDD Info", []),
                }
            elif event == "SparkListenerTaskEnd" and (ev.get("Task End Reason") or {}).get("Reason") == "Success":
                metrics = ev.get("Task Metrics") or {}
                value = metrics.get("Executor Run Time")
                if value is not None:
                    durations[(ev["Stage ID"], ev["Stage Attempt ID"])].append(value)

    keys = ["spark.sql.adaptive.enabled", "spark.sql.adaptive.coalescePartitions.enabled",
            "spark.sql.adaptive.skewJoin.enabled", "spark.sql.adaptive.skewJoin.skewedPartitionFactor",
            "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "spark.sql.autoBroadcastJoinThreshold"]
    output = []
    for row in runs:
        group = row["job_group"]
        group_jobs = sorted(jobs[group], key=lambda j: j["time"])
        if not group_jobs or group_jobs[0]["execution"] is None:
            raise ValueError(f"Missing measured SQL execution for {group}")
        execution = group_jobs[0]["execution"]
        measured_jobs = [j for j in group_jobs if j["execution"] == execution]
        stage_ids = {s for j in measured_jobs for s in j["stages"]}
        target = int(row["target_stage_id"])
        attempt = int(row["target_stage_attempt"])
        stage = stages.get((target, attempt))
        props = stage["props"] if stage else {}
        plan = Path(row["executed_plan_path"]).read_text()
        join_ids = set(re.findall(r"\*\((\d+)\)\s+SortMergeJoin", plan))
        stage_codegen = set()
        if stage:
            for rdd in stage["rdds"]:
                scope = rdd.get("Scope")
                if scope:
                    scope_name = json.loads(scope).get("name", "")
                    stage_codegen.update(re.findall(r"WholeStageCodegen \((\d+)\)", scope_name))
        config_mismatch = [k for k in keys if str(props.get(k, "")).lower() != row[k].lower()]
        task_times = durations.get((target, attempt), [])
        task_median = statistics.median(task_times) if task_times else None
        task_max = max(task_times) if task_times else None
        event_tsr = round(task_max / task_median, 2) if task_median else None
        csv_tsr = float(row["task_skew_ratio_max"]) if row["task_skew_ratio_max"] else None
        output.append({"workload_id": row["workload_id"], "method": row["method"], "block": row["block"],
                       "job_group": group, "measured_execution_id": execution,
                       "group_execution_count": len({j['execution'] for j in group_jobs}),
                       "measured_job_count": len(measured_jobs), "target_stage_id": target,
                       "target_in_measured_execution": target in stage_ids,
                       "stage_submission_execution_matches": props.get("spark.sql.execution.id") == execution,
                       "join_plan_codegen_ids": ",".join(sorted(join_ids)),
                       "target_stage_codegen_ids": ",".join(sorted(stage_codegen)),
                       "target_contains_join_codegen": bool(join_ids & stage_codegen),
                       "event_successful_tasks": len(task_times),
                       "csv_successful_tasks": row["total_tasks"],
                       "event_task_median_ms": task_median, "event_task_max_ms": task_max,
                       "event_tsr": event_tsr, "csv_tsr": csv_tsr,
                       "tsr_matches": event_tsr == csv_tsr,
                       "config_mismatch": ",".join(config_mismatch),
                       "adaptive_broadcast_disabled": props.get("spark.sql.adaptive.autoBroadcastJoinThreshold") == "-1",
                       "shuffle_partitions_200": props.get("spark.sql.shuffle.partitions") == "200"})
    dest = args.results / "stage_execution_audit.csv"
    with dest.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=output[0].keys()); w.writeheader(); w.writerows(output)
    for key in ["target_in_measured_execution", "stage_submission_execution_matches", "target_contains_join_codegen", "tsr_matches", "adaptive_broadcast_disabled", "shuffle_partitions_200"]:
        print(key, collections.Counter(row[key] for row in output))
    print("config_mismatch", collections.Counter(row["config_mismatch"] or "none" for row in output))
    print("group_execution_count", collections.Counter(row["group_execution_count"] for row in output))
    print("output", dest)


if __name__ == "__main__":
    main()
