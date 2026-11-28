#!/usr/bin/env python3
"""Reconcile ND3 collector stage totals with task-end Event Log metrics."""
import argparse
import ast
import collections
import csv
import hashlib
import json
from pathlib import Path

METRICS = {
    "memory_bytes_spilled": lambda t: t["Memory Bytes Spilled"],
    "disk_bytes_spilled": lambda t: t["Disk Bytes Spilled"],
    "shuffle_read_bytes": lambda t: t["Shuffle Read Metrics"]["Remote Bytes Read"] + t["Shuffle Read Metrics"]["Local Bytes Read"],
    "shuffle_write_bytes": lambda t: t["Shuffle Write Metrics"]["Shuffle Bytes Written"],
}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--results", type=Path, required=True)
    p.add_argument("--event-log", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    a = p.parse_args()
    a.output.mkdir(parents=True, exist_ok=False)
    with (a.results / "runs.csv").open(newline="") as f:
        runs = list(csv.DictReader(f))
    groups = {r["job_group"] for r in runs}
    jobs = collections.defaultdict(list)
    task_events = collections.defaultdict(list)
    submitted = {}
    completed = {}
    with a.event_log.open() as f:
        for line in f:
            e = json.loads(line)
            event = e.get("Event")
            if event == "SparkListenerJobStart":
                props = e.get("Properties") or {}
                group = props.get("spark.jobGroup.id")
                if group in groups:
                    jobs[group].append({"sql": props.get("spark.sql.execution.id"), "job": e["Job ID"], "stages": e.get("Stage IDs", []), "time": e["Submission Time"]})
            elif event in ("SparkListenerStageSubmitted", "SparkListenerStageCompleted"):
                info = e["Stage Info"]
                key = (info["Stage ID"], info["Stage Attempt ID"])
                if event.endswith("Submitted"):
                    submitted[key] = e.get("Properties") or {}
                else:
                    completed[key] = info
            elif event == "SparkListenerTaskEnd":
                info = e.get("Task Info") or {}
                task_events[(e["Stage ID"], e["Stage Attempt ID"])].append({
                    "reason": (e.get("Task End Reason") or {}).get("Reason"),
                    "partition": info.get("Partition ID", info.get("Index")),
                    "task_id": info.get("Task ID"), "attempt": info.get("Attempt"),
                    "speculative": info.get("Speculative"), "metrics": e.get("Task Metrics")})
    stage_rows, run_rows = [], []
    for r in runs:
        group = r["job_group"]
        ordered = sorted(jobs[group], key=lambda x: x["time"])
        if not ordered or ordered[0]["sql"] is None:
            raise RuntimeError(f"No SQL execution for {group}")
        sql = ordered[0]["sql"]
        measured = [j for j in ordered if j["sql"] == sql]
        measured_stages = {sid for j in measured for sid in j["stages"]}
        details = ast.literal_eval(r["stages_detail"])
        seen = set()
        totals = {m: 0 for m in METRICS}
        valid = True
        for d in details:
            key = (int(d["stage_id"]), int(d["attempt_id"]))
            if key in seen:
                raise ValueError(f"Duplicate stage {key} in {group}")
            seen.add(key)
            tasks = task_events.get(key, [])
            successes = [t for t in tasks if t["reason"] == "Success"]
            duplicates = len(successes) - len({t["partition"] for t in successes})
            stage_ok = (key[0] in measured_stages and submitted.get(key, {}).get("spark.sql.execution.id") == sql
                        and key in completed and not completed[key].get("Failure Reason")
                        and len(successes) == completed[key].get("Number of Tasks") and duplicates == 0)
            stage = {"workload_id": r.get("workload_id") or r.get("level"), "block": r["block"], "method": r["method"], "job_group": group,
                     "sql_execution_id": sql, "job_ids": ",".join(str(j["job"]) for j in measured),
                     "stage_id": key[0], "attempt_id": key[1], "is_target": d["is_target"],
                     "stage_scope_matches": stage_ok, "num_tasks_expected": (completed.get(key) or {}).get("Number of Tasks"),
                     "tasks_success": len(successes), "tasks_failed_or_killed": len(tasks)-len(successes),
                     "duplicate_success_partitions": duplicates, "speculative_tasks": sum(bool(t["speculative"]) for t in tasks)}
            for metric, getter in METRICS.items():
                try:
                    value = sum(getter(t["metrics"]) for t in successes)
                    if not successes or any(t["metrics"] is None for t in successes):
                        value = None
                except (KeyError, TypeError):
                    value = None
                csv_key = {"memory_bytes_spilled":"memory_bytes_spilled", "disk_bytes_spilled":"disk_bytes_spilled", "shuffle_read_bytes":"shuffle_read_bytes", "shuffle_write_bytes":"shuffle_write_bytes"}[metric]
                reference = d.get(csv_key)
                stage[metric + "_event"] = value
                stage[metric + "_collector"] = reference
                stage[metric + "_delta"] = value - int(reference) if value is not None and reference is not None else None
                if value is None:
                    stage_ok = False
                else:
                    totals[metric] += value
            stage["stage_scope_matches"] = stage_ok
            valid = valid and stage_ok
            stage_rows.append(stage)
        out = {"workload_id": r.get("workload_id") or r.get("level"), "block": r["block"], "method": r["method"], "job_group": group,
               "sql_execution_id": sql, "measured_job_ids": ",".join(str(j["job"]) for j in measured),
               "measured_stages": len(details), "status": "Verified" if valid else "Unavailable"}
        for metric in METRICS:
            collector = int(r[metric]) if r[metric] else None
            event_total = totals[metric] if valid else None
            out[metric + "_collector"] = collector
            out[metric + "_event"] = event_total
            out[metric + "_delta"] = event_total - collector if event_total is not None and collector is not None else None
            if out[metric + "_delta"] is None:
                out["status"] = "Unavailable"
            elif out[metric + "_delta"] != 0 and out["status"] == "Verified":
                out["status"] = "Needs investigation"
        run_rows.append(out)
    for filename, rows in [("audit_96_runs.csv", run_rows), ("audit_stage_task.csv", stage_rows)]:
        with (a.output / filename).open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=rows[0].keys()); w.writeheader(); w.writerows(rows)
    counts = collections.Counter(r["status"] for r in run_rows)
    digest = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    summary = {"runs": len(run_rows), "stages": len(stage_rows), "status": dict(counts),
               "nonzero_stage_metric_deltas": {m: sum(r[m+"_delta"] not in (None, 0) for r in stage_rows) for m in METRICS},
               "script_sha256": digest, "event_log": str(a.event_log), "results": str(a.results)}
    (a.output / "audit_summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
