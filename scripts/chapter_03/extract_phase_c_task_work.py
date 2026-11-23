#!/usr/bin/env python3
"""Summarize per-run task work from a Spark event log and ND3 run CSV."""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


METRICS = {
    "executor_run_time_ms": "Executor Run Time",
    "executor_cpu_time_ns": "Executor CPU Time",
    "jvm_gc_time_ms": "JVM GC Time",
    "memory_bytes_spilled": "Memory Bytes Spilled",
    "disk_bytes_spilled": "Disk Bytes Spilled",
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-log", type=Path, required=True)
    parser.add_argument("--runs", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    runs = pd.read_csv(args.runs)
    if runs["job_group"].duplicated().any():
        raise ValueError("Duplicate job_group in run CSV")
    groups = set(runs.job_group)
    job_group_by_id = {}
    job_stages = {}
    job_started_ms = {}
    task_rows = defaultdict(list)
    job_result = {}
    with args.event_log.open(encoding="utf-8") as stream:
        for line in stream:
            event = json.loads(line)
            kind = event.get("Event")
            if kind == "SparkListenerJobStart":
                group = (event.get("Properties") or {}).get("spark.jobGroup.id")
                if group not in groups:
                    continue
                jid = event["Job ID"]
                job_group_by_id[jid] = group
                job_stages[jid] = set(event.get("Stage IDs", []))
                job_started_ms[jid] = event.get("Submission Time", 0)
            elif kind == "SparkListenerJobEnd":
                jid = event["Job ID"]
                if jid in job_group_by_id:
                    job_result[jid] = event.get("Job Result", {}).get("Result")
            elif kind == "SparkListenerTaskEnd":
                sid, attempt = event.get("Stage ID"), event.get("Stage Attempt ID", 0)
                info = event.get("Task Info") or {}
                if (event.get("Task End Reason") or {}).get("Reason") != "Success" or info.get("Failed") or info.get("Killed"):
                    continue
                metrics = event.get("Task Metrics") or {}
                task_rows[(sid, attempt)].append({key: metrics.get(source) for key, source in METRICS.items()})
    result = []
    for _, run in runs.iterrows():
        group = run.job_group
        cutoff = int(datetime.fromisoformat(str(run.run_timestamp_utc).replace("Z", "+00:00")).timestamp() * 1000)
        measured_jobs = [jid for jid, g in job_group_by_id.items()
                         if g == group and job_started_ms[jid] <= cutoff]
        measured_stages = set().union(*(job_stages[jid] for jid in measured_jobs)) if measured_jobs else set()
        relevant = [items for (sid, _), items in task_rows.items() if sid in measured_stages]
        records = [item for items in relevant for item in items]
        row = {"job_group": group, "method": run.method, "iteration": int(run.iteration),
               "application_id": run.application_id, "successful_tasks": len(records),
               "matched_job_count": len(measured_jobs),
               "all_jobs_succeeded": bool(measured_jobs) and all(job_result.get(jid) == "JobSucceeded" for jid in measured_jobs)}
        for key in METRICS:
            values = [item[key] for item in records]
            row[key] = sum(values) if values and all(value is not None for value in values) else None
        row["executor_cpu_time_ms"] = row["executor_cpu_time_ns"] / 1e6 if row["executor_cpu_time_ns"] is not None else None
        result.append(row)
    out = pd.DataFrame(result)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.output, index=False)
    print(out[["method", "iteration", "successful_tasks", "executor_run_time_ms", "executor_cpu_time_ms", "jvm_gc_time_ms"]].to_string(index=False))


if __name__ == "__main__":
    main()
