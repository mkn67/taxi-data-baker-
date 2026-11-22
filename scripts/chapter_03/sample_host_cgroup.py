#!/usr/bin/env python3
"""Sample host PSI/load and worker cgroup counters during a Spark pilot."""
from __future__ import annotations

import argparse
import csv
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path


def pressure(path: str, kind: str) -> tuple[str, str]:
    for line in Path(path).read_text().splitlines():
        if line.startswith(kind + " "):
            parts = dict(field.split("=", 1) for field in line.split()[1:])
            return parts.get("avg10", ""), parts.get("total", "")
    return "", ""


def cgroup_stats(container: str) -> dict[str, str]:
    result = subprocess.run(
        ["podman", "exec", container, "cat", "/sys/fs/cgroup/cpu.stat"],
        capture_output=True, text=True, timeout=8, check=True,
    )
    stats = dict(line.split(maxsplit=1) for line in result.stdout.splitlines() if " " in line)
    for key in ("memory.current", "memory.peak"):
        mem = subprocess.run(
            ["podman", "exec", container, "cat", f"/sys/fs/cgroup/{key}"],
            capture_output=True, text=True, timeout=8, check=True,
        )
        stats[key.replace(".", "_")] = mem.stdout.strip()
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--duration", type=float, default=900.0)
    args = parser.parse_args()
    if args.interval <= 0 or args.duration <= 0:
        parser.error("interval and duration must be positive")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    containers = ("spark-worker-1", "spark-worker-2", "spark-driver")
    fields = ["timestamp_utc", "host_load_1m", "host_top_processes", "cpu_psi_some_avg10", "cpu_psi_some_total",
              "io_psi_some_avg10", "io_psi_some_total", "io_psi_full_avg10", "io_psi_full_total"]
    for name in containers:
        fields += [f"{name}_{key}" for key in ("usage_usec", "nr_periods", "nr_throttled", "throttled_usec", "memory_current", "memory_peak")]
    end = time.monotonic() + args.duration
    with args.output.open("w", newline="", encoding="utf-8", buffering=1) as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        while time.monotonic() < end:
            start = time.monotonic()
            row = {"timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                   "host_load_1m": Path("/proc/loadavg").read_text().split()[0]}
            ps = subprocess.run(
                ["ps", "-eo", "%cpu,comm", "--sort=-%cpu"],
                capture_output=True, text=True, timeout=8, check=True,
            )
            row["host_top_processes"] = " | ".join(ps.stdout.splitlines()[1:6])
            row["cpu_psi_some_avg10"], row["cpu_psi_some_total"] = pressure("/proc/pressure/cpu", "some")
            row["io_psi_some_avg10"], row["io_psi_some_total"] = pressure("/proc/pressure/io", "some")
            row["io_psi_full_avg10"], row["io_psi_full_total"] = pressure("/proc/pressure/io", "full")
            for name in containers:
                try:
                    stats = cgroup_stats(name)
                except (subprocess.SubprocessError, OSError):
                    stats = {}
                for key in ("usage_usec", "nr_periods", "nr_throttled", "throttled_usec", "memory_current", "memory_peak"):
                    row[f"{name}_{key}"] = stats.get(key, "")
            writer.writerow(row)
            time.sleep(max(0.0, args.interval - (time.monotonic() - start)))


if __name__ == "__main__":
    main()
