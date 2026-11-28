#!/usr/bin/env python3
"""Compare validated S1/S2 pilot run CSVs without mixing official/monthly tiers."""
from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path


def load_runs(path: Path, *, manifest_column: str) -> dict[str, list[dict]]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise ValueError(f"No measured runs: {path}")
    if any(r["collector_status"] != "OK" or r["validation_status"] != "PASS" for r in rows):
        raise ValueError(f"Non-passing run in {path}")
    if any(r["result_tier"] != "pilot_standalone" for r in rows):
        raise ValueError(f"Expected pilot_standalone tier only: {path}")
    if len({r[manifest_column] for r in rows}) != 1 or len({r["application_id"] for r in rows}) != 1:
        raise ValueError(f"Mixed input manifests or Spark applications in {path}")
    for row in rows:
        hosts = json.loads(row["worker_evidence"])["executor_hosts"]
        if len(set(hosts)) < 2:
            raise ValueError(f"Fewer than two Worker hosts in {path}")
    by_method: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        by_method[row["method"]].append(row)
    for method, group in by_method.items():
        if sorted(int(r["iteration"]) for r in group) != [1, 2, 3]:
            raise ValueError(f"Expected exactly iterations 1,2,3 for {method}: {path}")
    return dict(by_method)


def fmt(values: list[float]) -> str:
    return f"{statistics.median(values):.3f} [{min(values):.3f}–{max(values):.3f}]"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("nd2_s1", "nd2_s2", "nd3_s1", "nd3_s2"):
        parser.add_argument(f"--{name.replace('_', '-')}", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result_rows = []
    for workload in ("nd2", "nd3"):
        manifest_col = "dataset_manifest_sha256" if workload == "nd2" else "trip_dataset_manifest_sha256"
        s1 = load_runs(getattr(args, f"{workload}_s1"), manifest_column=manifest_col)
        s2 = load_runs(getattr(args, f"{workload}_s2"), manifest_column=manifest_col)
        if set(s1) != set(s2):
            raise ValueError(f"Different methods between S1 and S2 for {workload}")
        for method in sorted(s1):
            a, b = s1[method], s2[method]
            if {r["spark_version"] for r in a + b} != {"3.5.9"}:
                raise ValueError(f"Spark version mismatch for {method}")
            s1_times = [float(r["wall_clock_duration_s"]) for r in a]
            s2_times = [float(r["wall_clock_duration_s"]) for r in b]
            result_rows.append({
                "workload": workload.upper(),
                "method": method,
                "s1_wall_s_median_range": fmt(s1_times),
                "s2_wall_s_median_range": fmt(s2_times),
                "s2_over_s1_median_ratio": round(statistics.median(s2_times) / statistics.median(s1_times), 3),
                "s1_shuffle_read_mib_median": round(statistics.median(int(r["shuffle_read_bytes"]) / 1048576 for r in a), 2),
                "s2_shuffle_read_mib_median": round(statistics.median(int(r["shuffle_read_bytes"]) / 1048576 for r in b), 2),
                "s1_shuffle_write_mib_median": round(statistics.median(int(r["shuffle_write_bytes"]) / 1048576 for r in a), 2),
                "s2_shuffle_write_mib_median": round(statistics.median(int(r["shuffle_write_bytes"]) / 1048576 for r in b), 2),
                "s1_app": a[0]["application_id"],
                "s2_app": b[0]["application_id"],
                "s1_manifest": a[0][manifest_col],
                "s2_manifest": b[0][manifest_col],
            })
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(result_rows[0]))
        writer.writeheader()
        writer.writerows(result_rows)
    print(f"Compared {len(result_rows)} methods; output={args.output}")


if __name__ == "__main__":
    main()
