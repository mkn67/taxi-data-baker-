#!/usr/bin/env python3
"""Run fixed-tier partition and AQE-threshold sensitivity after the synthetic grid."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from pathlib import Path


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--grid-status", type=Path, required=True)
    ap.add_argument("--data-dir", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    ap.add_argument("--wait-minutes", type=int, default=120)
    args = ap.parse_args()
    deadline = time.monotonic() + args.wait_minutes * 60
    while time.monotonic() < deadline:
        if args.grid_status.exists():
            rows = json.loads(args.grid_status.read_text())
            if len(rows) == 12:
                if any(row["exit_code"] for row in rows):
                    raise SystemExit("Grid has failed cells; sensitivity run not started")
                break
        time.sleep(30)
    else:
        raise TimeoutError("Synthetic grid did not finish before sensitivity deadline")
    args.output.mkdir(parents=True, exist_ok=False)
    configs = [("partitions_64", 64, 16), ("partitions_200", 200, 16),
               ("partitions_400", 400, 16), ("aqe_threshold_4", 200, 4),
               ("aqe_threshold_64", 200, 64)]
    (args.output / "sensitivity_protocol.json").write_text(json.dumps({
        "fixed_workload": "moderate_medium", "fact_rows": 2_000_000,
        "dimension_source": "synthetic_v2/dimension_medium.parquet", "blocks": 3,
        "partition_sweep_applies_to_all_methods": True,
        "threshold_sweep_changes_only_AQE_method": True, "configs": configs}, indent=2))
    statuses = []
    for index, (label, partitions, threshold) in enumerate(configs):
        directory = args.output / label
        directory.mkdir()
        env = dict(os.environ, BENCHMARKS_DIR=str(directory),
                   SILVER_PATH=str(args.data_dir / "fact_moderate.parquet"),
                   OD_MATRIX_PATH=str(args.data_dir / "dimension_medium.parquet"),
                   HOTSPOTS_PU_PATH=str(args.data_dir / "hotspots_moderate.parquet"))
        command = ["/opt/spark/bin/spark-submit", "--master", "spark://spark-master:7077",
                   "--deploy-mode", "client", "--executor-memory", "1500m",
                   "/opt/spark/scripts/chapter_03/run_benchmark_nd3.py", "--mode", "full",
                   "--include-auto-broadcast", "--official-iterations", "3",
                   "--partitions", str(partitions), "--aqe-skew-threshold-mb", str(threshold),
                   "--order-seed", str(20261123 + index)]
        started = time.time()
        with (directory / "runner.log").open("w") as log:
            code = subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT).returncode
        status = {"configuration": label, "exit_code": code, "elapsed_seconds": time.time() - started}
        statuses.append(status)
        (args.output / "sensitivity_status.json").write_text(json.dumps(statuses, indent=2))
        print(json.dumps(status), flush=True)
    if any(row["exit_code"] for row in statuses):
        raise SystemExit("Sensitivity contains failed configurations")


if __name__ == "__main__":
    main()
