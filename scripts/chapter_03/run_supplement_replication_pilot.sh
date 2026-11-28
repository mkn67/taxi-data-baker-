#!/usr/bin/env bash
set -euo pipefail
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$PROJECT_ROOT"
INPUT_DIR="$PROJECT_ROOT/data/benchmarks/phase_c/replication_pilot_20261128"
[[ -f "$INPUT_DIR/manifest.json" ]] || { printf 'Missing dimension manifest\n' >&2; exit 1; }
RUN_DIR="$(mktemp -d "$INPUT_DIR/run_$(date -u +%Y%m%dT%H%M%SZ)_XXXXXX")"
printf 'Pilot artifacts: %s\n' "$RUN_DIR"
podman exec --env BENCHMARKS_DIR="$RUN_DIR/runtime" --env SPARK_DRIVER_HOST=spark-driver --env SPARK_EXPECTED_VERSION=3.5.9 \
  spark-driver bash -lc "cd '$PROJECT_ROOT' && /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.driver.host=spark-driver --conf spark.driver.bindAddress=0.0.0.0 scripts/chapter_03/run_supplement_replication_pilot.py --manifest '$INPUT_DIR/manifest.json' --output '$RUN_DIR/results' --seed 20261128"
printf 'Pilot execution complete: %s\n' "$RUN_DIR/results"
