#!/usr/bin/env python3
"""Align ND3 measured runs with sampled host PSI and container CPU counters."""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timedelta
from pathlib import Path


COUNTERS = (
    "cpu_psi_some_total", "io_psi_some_total", "io_psi_full_total",
    *(f"{name}_{key}" for name in ("spark-worker-1", "spark-worker-2", "spark-driver")
      for key in ("usage_usec", "nr_periods", "nr_throttled", "throttled_usec")),
)


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def numeric(value: str | None) -> float | None:
    try:
        return float(value) if value else None
    except ValueError:
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=Path, required=True)
    parser.add_argument("--samples", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    runs = read_csv(args.runs)
    samples = read_csv(args.samples)
    for sample in samples:
        sample["_time"] = datetime.fromisoformat(sample["timestamp_utc"])
    samples.sort(key=lambda row: row["_time"])
    if len(samples) < 2:
        raise ValueError("At least two host samples are required")

    output = []
    for run in runs:
        end = datetime.fromisoformat(run["run_timestamp_utc"].replace("Z", "+00:00"))
        start = end - timedelta(seconds=float(run["wall_clock_duration_s"]))
        # One sample immediately outside each end brackets the entire action.
        before = [row for row in samples if row["_time"] <= start]
        after = [row for row in samples if row["_time"] >= end]
        row = {key: run.get(key, "") for key in (
            "application_id", "iteration", "method", "wall_clock_duration_s",
            "collector_status", "validation_status")}
        row.update({"run_start_utc_estimated": start.isoformat(),
                    "run_end_utc_reported": end.isoformat(),
                    "window_sample_count": 0, "sampling_coverage": "missing"})
        if before and after:
            first, last = before[-1], after[0]
            window = [sample for sample in samples if first["_time"] <= sample["_time"] <= last["_time"]]
            row["window_sample_count"] = len(window)
            row["sampling_coverage"] = "complete"
            row["sample_window_seconds"] = round((last["_time"] - first["_time"]).total_seconds(), 3)
            for key in COUNTERS:
                a, b = numeric(first.get(key)), numeric(last.get(key))
                row[f"delta_{key}"] = round(b - a, 3) if a is not None and b is not None and b >= a else ""
            for key in ("host_load_1m", "cpu_psi_some_avg10", "io_psi_some_avg10", "io_psi_full_avg10"):
                values = [v for sample in window if (v := numeric(sample.get(key))) is not None]
                row[f"max_{key}"] = max(values) if values else ""
        output.append(row)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(dict.fromkeys(key for row in output for key in row)))
        writer.writeheader()
        writer.writerows(output)
    print(f"Aligned {len(output)} runs; complete windows: {sum(row['sampling_coverage'] == 'complete' for row in output)}")


if __name__ == "__main__":
    main()
