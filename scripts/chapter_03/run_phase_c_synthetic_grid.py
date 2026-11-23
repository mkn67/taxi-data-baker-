#!/usr/bin/env python3
"""Run the frozen 12-cell synthetic ND3 grid sequentially on Spark Standalone."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from pathlib import Path


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--manifest", type=Path, required=True)
    ap.add_argument("--data-dir", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    ap.add_argument("--blocks", type=int, default=5)
    args = ap.parse_args()
    manifest = json.loads(args.manifest.read_text())
    args.output.mkdir(parents=True, exist_ok=False)
    protocol = {"manifest": str(args.manifest), "seed": manifest["seed"],
                "blocks_per_cell": args.blocks, "cells": [c["cell_id"] for c in manifest["cells"]],
                "split_rule": manifest["split_rule"], "pilot_cell": "moderate_medium",
                "pilot_reason": "One-block pilot passed four-method DuckDB audit; five blocks chosen before grid execution"}
    (args.output / "grid_protocol.json").write_text(json.dumps(protocol, indent=2))
    statuses = []
    for index, cell in enumerate(manifest["cells"]):
        cell_dir = args.output / cell["cell_id"]
        cell_dir.mkdir()
        env = dict(os.environ, BENCHMARKS_DIR=str(cell_dir),
                   SILVER_PATH=str(args.data_dir / Path(cell["fact_path"]).name),
                   OD_MATRIX_PATH=str(args.data_dir / Path(cell["dimension_path"]).name),
                   HOTSPOTS_PU_PATH=str(args.data_dir / Path(cell["hotspot_path"]).name))
        cmd = ["/opt/spark/bin/spark-submit", "--master", "spark://spark-master:7077",
               "--deploy-mode", "client", "--executor-memory", "1500m",
               "/opt/spark/scripts/chapter_03/run_benchmark_nd3.py", "--mode", "full",
               "--include-auto-broadcast", "--official-iterations", str(args.blocks),
               "--order-seed", str(manifest["seed"] + index)]
        started = time.time()
        with (cell_dir / "runner.log").open("w") as log:
            exit_code = subprocess.run(cmd, env=env, stdout=log, stderr=subprocess.STDOUT).returncode
        status = {"cell_id": cell["cell_id"], "split": cell["split"],
                  "dimension_parquet_bytes": cell["dimension_parquet_bytes"],
                  "exit_code": exit_code, "elapsed_seconds": time.time() - started}
        statuses.append(status)
        (args.output / "grid_status.json").write_text(json.dumps(statuses, indent=2))
        print(json.dumps(status), flush=True)
    if any(row["exit_code"] for row in statuses):
        raise SystemExit("Grid has failed cells; inspect per-cell logs before decision analysis")


if __name__ == "__main__":
    main()
