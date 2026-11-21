#!/usr/bin/env python3
"""Summarize record-count versus runtime per selected Spark stage/run.

Example:
  python scripts/chapter_04/analyze_task_load.py \
    --runs data/benchmarks/benchmark_nd3_join_runs_official_standalone.csv \
    --events logs/spark-events/app-20261103153727-0002 \
    --output chapters/03_pipeline_skew_benchmarks/docs/phase_a/nd3_task_load.csv
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path

import pandas as pd


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=Path, required=True)
    parser.add_argument("--events", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    runs = pd.read_csv(args.runs)
    required = {"target_stage_id", "target_stage_attempt", "method", "iteration"}
    if missing := required.difference(runs.columns):
        raise ValueError(f"Run CSV lacks required columns: {sorted(missing)}")
    selected = {
        (int(row.target_stage_id), int(row.target_stage_attempt))
        for row in runs.itertuples(index=False)
    }

    task_rows: dict[tuple[int, int], list[tuple[int, int]]] = defaultdict(list)
    with args.events.open(encoding="utf-8") as stream:
        for line in stream:
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event.get("Event") != "SparkListenerTaskEnd":
                continue
            key = (event.get("Stage ID"), event.get("Stage Attempt ID"))
            if key not in selected:
                continue
            info = event.get("Task Info") or {}
            if info.get("Failed") or info.get("Killed"):
                continue
            metrics = event.get("Task Metrics") or {}
            records = (metrics.get("Shuffle Read Metrics") or {}).get("Total Records Read")
            runtime = metrics.get("Executor Run Time")
            if records is not None and runtime is not None:
                task_rows[key].append((int(records), int(runtime)))

    output = []
    for run in runs.itertuples(index=False):
        key = (int(run.target_stage_id), int(run.target_stage_attempt))
        tasks = pd.DataFrame(task_rows[key], columns=["shuffle_read_records", "executor_runtime_ms"])
        rho = None
        if len(tasks) >= 3 and tasks.nunique().min() > 1:
            rho = tasks["shuffle_read_records"].rank().corr(tasks["executor_runtime_ms"].rank())
        output.append({
            "method": run.method,
            "iteration": run.iteration,
            "stage_id": key[0],
            "stage_attempt": key[1],
            "successful_tasks_with_metrics": len(tasks),
            "spearman_records_vs_runtime": rho,
            "metric_records": "shuffle_read_records",
            "metric_runtime": "executorRunTime_ms",
            "run_csv": str(args.runs),
            "event_log": str(args.events),
        })

    result = pd.DataFrame(output)
    if result["successful_tasks_with_metrics"].eq(0).any():
        missing = result.loc[result["successful_tasks_with_metrics"].eq(0), ["method", "iteration"]]
        raise ValueError(f"No matching successful tasks for run(s): {missing.to_dict('records')}")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(args.output, index=False)
    print(result[["method", "iteration", "successful_tasks_with_metrics", "spearman_records_vs_runtime"]].to_string(index=False))


if __name__ == "__main__":
    main()
