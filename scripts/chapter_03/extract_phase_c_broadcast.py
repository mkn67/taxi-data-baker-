#!/usr/bin/env python3
"""Export BroadcastExchange SQL metrics for already recorded ND3 runs."""
import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path


def broadcast_nodes(plan):
    if "BroadcastExchange" in plan.get("nodeName", ""):
        yield {metric["name"]: metric["accumulatorId"] for metric in plan.get("metrics", [])}
    for child in plan.get("children", []):
        yield from broadcast_nodes(child)


def read_event_log(path):
    groups = defaultdict(set)
    exchanges = defaultdict(dict)
    updates = defaultdict(dict)
    with path.open() as stream:
        for line in stream:
            event = json.loads(line)
            kind = event["Event"].rsplit(".", 1)[-1]
            if kind == "SparkListenerJobStart":
                props = event.get("Properties") or {}
                if "spark.jobGroup.id" in props and "spark.sql.execution.id" in props:
                    groups[props["spark.jobGroup.id"]].add(int(props["spark.sql.execution.id"]))
            elif kind in {"SparkListenerSQLExecutionStart", "SparkListenerSQLAdaptiveExecutionUpdate"}:
                for node in broadcast_nodes(event.get("sparkPlanInfo") or {}):
                    if "data size" in node:
                        exchanges[event["executionId"]][node["data size"]] = node
            elif kind == "SparkListenerDriverAccumUpdates":
                updates[event["executionId"]].update(event["accumUpdates"])
    return groups, exchanges, updates


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=Path, nargs="+", required=True)
    parser.add_argument("--logs", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    fields = ["application_id", "job_group", "workload", "execution_id",
              "accumulator_id", "exchange_data_size_bytes", "exchange_rows", "status"]
    rows = []
    for source in sorted(args.runs):
        with source.open() as stream:
            runs = [run for run in csv.DictReader(stream)
                    if run["method"] == "Spark_AutoJoin_Reference"
                    and run["collector_status"] == "OK" and run["validation_status"] == "PASS"]
        by_app = defaultdict(list)
        for run in runs:
            by_app[run["application_id"]].append(run)
        for app, app_runs in by_app.items():
            path = args.logs / app
            groups, exchanges, updates = read_event_log(path) if path.is_file() else ({}, {}, {})
            for run in app_runs:
                group = run["job_group"]
                base = {"application_id": app, "job_group": group, "workload": source.parent.name}
                found = False
                for execution in sorted(groups.get(group, ())):
                    for acc, node in exchanges.get(execution, {}).items():
                        values = updates.get(execution, {})
                        size = values.get(acc)
                        rows.append({**base, "execution_id": execution, "accumulator_id": acc,
                                     "exchange_data_size_bytes": size,
                                     "exchange_rows": values.get(node.get("number of output rows")),
                                     "status": "recorded" if size is not None else "metric_missing"})
                        found = True
                if not found:
                    status = "no_exchange" if groups.get(group) else "execution_unavailable"
                    rows.append({**base, "status": status if path.is_file() else "event_log_missing"})
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", newline="") as stream:
        writer = csv.DictWriter(stream, fields)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
