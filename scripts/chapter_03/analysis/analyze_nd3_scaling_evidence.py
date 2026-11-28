#!/usr/bin/env python3
"""Export successful task metrics and AQE final-plan interventions for ND3 pilots."""
from __future__ import annotations

import argparse
import csv
import json
import re
import statistics
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


TASK_FIELDS = [
    "session", "application_id", "input_manifest_sha256", "method", "iteration",
    "target_stage_id", "target_stage_attempt", "task_id", "task_attempt",
    "partition_id", "host", "executor_id", "speculative", "task_duration_ms",
    "executor_runtime_ms", "executor_cpu_time_ns", "executor_cpu_to_runtime_pct",
    "executor_deserialize_time_ms", "executor_deserialize_cpu_time_ns",
    "jvm_gc_time_ms", "result_serialization_time_ms", "input_bytes_read",
    "input_records_read", "output_bytes_written", "output_records_written",
    "shuffle_read_local_bytes", "shuffle_read_remote_bytes",
    "shuffle_read_remote_bytes_to_disk", "shuffle_read_records",
    "shuffle_read_fetch_wait_ms", "shuffle_write_bytes", "shuffle_write_records",
    "shuffle_write_time_ns", "memory_bytes_spilled", "disk_bytes_spilled",
    "peak_execution_memory_bytes",
]

AQE_FIELDS = [
    "session", "application_id", "iteration", "target_stage_id",
    "target_stage_attempt", "target_stage_tasks", "is_final_plan",
    "coalesced_shuffle_reads", "skew_split_markers", "join_operators",
    "skew_split_detected", "executed_plan_path",
]

RESOURCE_FIELDS = [
    "session", "application_id", "method", "iteration", "start_utc", "end_utc",
    "container", "samples", "cpu_median_pct", "cpu_max_pct", "memory_max_sampled_raw",
    "block_io_start_raw", "block_io_end_raw", "net_io_start_raw", "net_io_end_raw",
]

STATS_LINE = re.compile(
    r"^(?P<timestamp>\S+) (?P<container>\S+) (?P<cpu>[\d.]+)% "
    r"(?P<memory>[\d.]+[A-Za-z]+) / (?P<memory_limit>[\d.]+[A-Za-z]+) "
    r"(?P<block_read>[\d.]+[A-Za-z]+) / (?P<block_write>[\d.]+[A-Za-z]+) "
    r"(?P<net_rx>[\d.]+[A-Za-z]+) / (?P<net_tx>[\d.]+[A-Za-z]+)$"
)


def value(mapping: dict[str, Any], key: str) -> Any:
    item = mapping.get(key)
    return "NA" if item is None else item


def resolve_plan_path(raw_path: str) -> Path:
    prefix = "/opt/spark/data/"
    if raw_path.startswith(prefix):
        return Path("data") / raw_path.removeprefix(prefix)
    return Path(raw_path)


def final_plan_text(plan: str) -> str:
    marker = "== Final Plan =="
    if marker not in plan:
        return plan
    result = plan.split(marker, 1)[1]
    return result.split("== Initial Plan ==", 1)[0]


def size_bytes(raw: str) -> float:
    match = re.fullmatch(r"([\d.]+)([A-Za-z]+)", raw)
    if not match:
        return 0
    scale = {
        "B": 1, "kB": 1_000, "KB": 1_000, "MB": 1_000_000,
        "GB": 1_000_000_000, "TB": 1_000_000_000_000,
    }
    return float(match.group(1)) * scale.get(match.group(2), 1)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        action="append",
        nargs=3,
        metavar=("SESSION", "RUNS_CSV", "EVENT_LOG"),
        required=True,
        help="Input triplet; repeat for each S1/S2 session.",
    )
    parser.add_argument("--task-output", type=Path, required=True)
    parser.add_argument("--aqe-output", type=Path, required=True)
    parser.add_argument("--container-stats", type=Path)
    parser.add_argument("--stats-session")
    parser.add_argument("--resource-output", type=Path)
    args = parser.parse_args()
    if any((args.container_stats, args.stats_session, args.resource_output)) and not all(
        (args.container_stats, args.stats_session, args.resource_output)
    ):
        parser.error("--container-stats, --stats-session, and --resource-output must be used together.")

    task_rows: list[dict[str, Any]] = []
    aqe_rows: list[dict[str, Any]] = []
    resource_runs: list[tuple[str, dict[str, str]]] = []

    for session, run_arg, event_arg in args.input:
        runs_path, event_path = Path(run_arg), Path(event_arg)
        if not runs_path.is_file():
            raise FileNotFoundError(f"Run CSV not found: {runs_path}")
        if not event_path.is_file():
            raise FileNotFoundError(f"Spark event log not found: {event_path}")

        with runs_path.open(newline="", encoding="utf-8") as handle:
            runs = list(csv.DictReader(handle))
        if not runs:
            raise ValueError(f"No measured runs in {runs_path}")

        app_ids = {row["application_id"] for row in runs}
        if len(app_ids) != 1:
            raise ValueError(f"Expected one application in {runs_path}, found {sorted(app_ids)}")
        for row in runs:
            if row["collector_status"] != "OK" or row["validation_status"] != "PASS":
                raise ValueError(f"Run did not pass collection/correctness: {runs_path} {row}")
            resource_runs.append((session, row))

        stage_to_run: dict[tuple[int, int], dict[str, str]] = {}
        for row in runs:
            key = (int(row["target_stage_id"]), int(row["target_stage_attempt"]))
            if key in stage_to_run:
                raise ValueError(f"Duplicate selected stage {key} in {runs_path}")
            stage_to_run[key] = row

            if row["method"] == "Spark_AQE_Skew_Join":
                plan_path = resolve_plan_path(row["executed_plan_path"])
                if not plan_path.is_file():
                    raise FileNotFoundError(f"Executed plan not found: {plan_path}")
                plan = plan_path.read_text(encoding="utf-8")
                final = final_plan_text(plan)
                split_markers = re.findall(
                    r"PartialReducerPartitionSpec|PartialMapperPartitionSpec|"
                    r"AQEShuffleRead\s+skewed|isSkew\s*=\s*true",
                    final,
                    flags=re.IGNORECASE,
                )
                joins = sorted(set(re.findall(
                    r"SortMergeJoin|BroadcastHashJoin|ShuffledHashJoin|"
                    r"BroadcastNestedLoopJoin|CartesianProduct",
                    final,
                )))
                aqe_rows.append({
                    "session": session,
                    "application_id": row["application_id"],
                    "iteration": row["iteration"],
                    "target_stage_id": row["target_stage_id"],
                    "target_stage_attempt": row["target_stage_attempt"],
                    "target_stage_tasks": row["total_tasks"],
                    "is_final_plan": "true" if "isFinalPlan=true" in plan else "false",
                    "coalesced_shuffle_reads": len(re.findall(r"AQEShuffleRead coalesced", final)),
                    "skew_split_markers": len(split_markers),
                    "join_operators": "|".join(joins) if joins else "NA",
                    "skew_split_detected": "yes" if split_markers else "no",
                    "executed_plan_path": str(plan_path),
                })

        tasks_by_stage: dict[tuple[int, int], list[dict[str, Any]]] = {
            key: [] for key in stage_to_run
        }
        with event_path.open(encoding="utf-8") as stream:
            for line in stream:
                event = json.loads(line)
                if event.get("Event") != "SparkListenerTaskEnd":
                    continue
                key = (event.get("Stage ID"), event.get("Stage Attempt ID", 0))
                if key not in stage_to_run:
                    continue
                info = event.get("Task Info") or {}
                reason = event.get("Task End Reason") or {}
                if info.get("Failed") or info.get("Killed") or reason.get("Reason") != "Success":
                    continue

                run = stage_to_run[key]
                metrics = event.get("Task Metrics") or {}
                input_metrics = metrics.get("Input Metrics") or {}
                output_metrics = metrics.get("Output Metrics") or {}
                shuffle_read = metrics.get("Shuffle Read Metrics") or {}
                shuffle_write = metrics.get("Shuffle Write Metrics") or {}
                runtime_ms = metrics.get("Executor Run Time")
                cpu_ns = metrics.get("Executor CPU Time")
                finish_ms, launch_ms = info.get("Finish Time"), info.get("Launch Time")
                cpu_ratio = (
                    round(cpu_ns / (runtime_ms * 1_000_000) * 100, 4)
                    if cpu_ns is not None and runtime_ms else "NA"
                )
                task_rows.append({
                    "session": session,
                    "application_id": run["application_id"],
                    "input_manifest_sha256": run.get("trip_dataset_manifest_sha256")
                    or run.get("dataset_manifest_sha256", "NA"),
                    "method": run["method"],
                    "iteration": run["iteration"],
                    "target_stage_id": key[0],
                    "target_stage_attempt": key[1],
                    "task_id": value(info, "Task ID"),
                    "task_attempt": value(info, "Attempt"),
                    "partition_id": value(info, "Partition ID"),
                    "host": value(info, "Host"),
                    "executor_id": value(info, "Executor ID"),
                    "speculative": value(info, "Speculative"),
                    "task_duration_ms": finish_ms - launch_ms
                    if finish_ms is not None and launch_ms is not None else "NA",
                    "executor_runtime_ms": value(metrics, "Executor Run Time"),
                    "executor_cpu_time_ns": value(metrics, "Executor CPU Time"),
                    "executor_cpu_to_runtime_pct": cpu_ratio,
                    "executor_deserialize_time_ms": value(metrics, "Executor Deserialize Time"),
                    "executor_deserialize_cpu_time_ns": value(metrics, "Executor Deserialize CPU Time"),
                    "jvm_gc_time_ms": value(metrics, "JVM GC Time"),
                    "result_serialization_time_ms": value(metrics, "Result Serialization Time"),
                    "input_bytes_read": value(input_metrics, "Bytes Read"),
                    "input_records_read": value(input_metrics, "Records Read"),
                    "output_bytes_written": value(output_metrics, "Bytes Written"),
                    "output_records_written": value(output_metrics, "Records Written"),
                    "shuffle_read_local_bytes": value(shuffle_read, "Local Bytes Read"),
                    "shuffle_read_remote_bytes": value(shuffle_read, "Remote Bytes Read"),
                    "shuffle_read_remote_bytes_to_disk": value(shuffle_read, "Remote Bytes Read To Disk"),
                    "shuffle_read_records": value(shuffle_read, "Total Records Read"),
                    "shuffle_read_fetch_wait_ms": value(shuffle_read, "Fetch Wait Time"),
                    "shuffle_write_bytes": value(shuffle_write, "Shuffle Bytes Written"),
                    "shuffle_write_records": value(shuffle_write, "Shuffle Records Written"),
                    "shuffle_write_time_ns": value(shuffle_write, "Shuffle Write Time"),
                    "memory_bytes_spilled": value(metrics, "Memory Bytes Spilled"),
                    "disk_bytes_spilled": value(metrics, "Disk Bytes Spilled"),
                    "peak_execution_memory_bytes": value(metrics, "Peak Execution Memory"),
                })
                tasks_by_stage[key].append(task_rows[-1])

        for key, row in stage_to_run.items():
            actual = len(tasks_by_stage[key])
            expected = int(row["total_tasks"])
            if actual != expected:
                raise ValueError(
                    f"Successful task count mismatch for {session} {row['method']} "
                    f"iteration {row['iteration']}: expected {expected}, found {actual}"
                )

    args.task_output.parent.mkdir(parents=True, exist_ok=True)
    with args.task_output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=TASK_FIELDS)
        writer.writeheader()
        writer.writerows(task_rows)
    with args.aqe_output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=AQE_FIELDS)
        writer.writeheader()
        writer.writerows(aqe_rows)
    if args.container_stats and args.stats_session and args.resource_output:
        if args.stats_session not in {session for session, _, _ in args.input}:
            raise ValueError(f"Stats session not present in inputs: {args.stats_session}")
        snapshots: list[dict[str, Any]] = []
        with args.container_stats.open(encoding="utf-8") as stream:
            for line in stream:
                match = STATS_LINE.match(line.strip())
                if not match:
                    continue
                item = match.groupdict()
                item["time"] = datetime.fromisoformat(item.pop("timestamp").replace("Z", "+00:00"))
                item["cpu"] = float(item.pop("cpu"))
                snapshots.append(item)
        if not snapshots:
            raise ValueError(f"No Podman stats records parsed from {args.container_stats}")

        resource_rows = []
        for session, run in resource_runs:
            if session != args.stats_session:
                continue
            end = datetime.fromisoformat(run["run_timestamp_utc"].replace("Z", "+00:00"))
            start = end - timedelta(seconds=float(run["wall_clock_duration_s"]))
            by_container: dict[str, list[dict[str, Any]]] = {}
            for sample in snapshots:
                if start <= sample["time"] <= end:
                    by_container.setdefault(sample["container"], []).append(sample)
            for container, samples in sorted(by_container.items()):
                resource_rows.append({
                    "session": session,
                    "application_id": run["application_id"],
                    "method": run["method"],
                    "iteration": run["iteration"],
                    "start_utc": start.isoformat(),
                    "end_utc": end.isoformat(),
                    "container": container,
                    "samples": len(samples),
                    "cpu_median_pct": round(statistics.median(x["cpu"] for x in samples), 3),
                    "cpu_max_pct": round(max(x["cpu"] for x in samples), 3),
                    "memory_max_sampled_raw": max(
                        samples, key=lambda x: size_bytes(x["memory"])
                    )["memory"],
                    "block_io_start_raw": f'{samples[0]["block_read"]} / {samples[0]["block_write"]}',
                    "block_io_end_raw": f'{samples[-1]["block_read"]} / {samples[-1]["block_write"]}',
                    "net_io_start_raw": f'{samples[0]["net_rx"]} / {samples[0]["net_tx"]}',
                    "net_io_end_raw": f'{samples[-1]["net_rx"]} / {samples[-1]["net_tx"]}',
                })
        if not resource_rows:
            raise ValueError(
                f"No resource samples overlap measured runs for {args.stats_session}"
            )
        args.resource_output.parent.mkdir(parents=True, exist_ok=True)
        with args.resource_output.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=RESOURCE_FIELDS)
            writer.writeheader()
            writer.writerows(resource_rows)
    print(
        f"Exported {len(task_rows)} successful task records and "
        f"{len(aqe_rows)} AQE plan records"
        + (f" and {len(resource_rows)} container resource windows." if args.container_stats else ".")
    )


if __name__ == "__main__":
    main()
