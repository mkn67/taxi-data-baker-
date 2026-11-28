#!/usr/bin/env bash
set -euo pipefail
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$PROJECT_ROOT"
INPUT="$PROJECT_ROOT/data/benchmarks/phase_c/replication_pilot_20261128/manifest.json"
[[ -f "$INPUT" ]] || { printf 'Missing input manifest: %s\n' "$INPUT" >&2; exit 1; }
curl -fsS --max-time 5 http://127.0.0.1:8080/json/ | python3 -c 'import json,sys; x=json.load(sys.stdin); assert x.get("aliveworkers",0)>=2, "Need two live workers"; assert not x.get("activeapps"), "Spark cluster is busy"; print("Spark preflight: two live workers, no active applications")'
BASE="$PROJECT_ROOT/data/benchmarks/phase_c/same_plan_noise_pilot"
mkdir -p "$BASE"
RUN_DIR="$(mktemp -d "$BASE/diagnostic8_$(date -u +%Y%m%dT%H%M%SZ)_XXXXXX")"
printf 'Diagnostic artifacts: %s\n' "$RUN_DIR"
git rev-parse HEAD > "$RUN_DIR/git_revision.txt"
git status --short > "$RUN_DIR/git_status.txt"
podman exec --env BENCHMARKS_DIR="$RUN_DIR/runtime" --env SPARK_DRIVER_HOST=spark-driver --env SPARK_EXPECTED_VERSION=3.5.9 \
  spark-driver bash -lc "cd '$PROJECT_ROOT' && /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.driver.host=spark-driver --conf spark.driver.bindAddress=0.0.0.0 scripts/chapter_03/run_same_plan_noise_pilot.py --manifest '$INPUT' --output '$RUN_DIR/results' --blocks 8"
printf 'Diagnostic execution complete: %s\n' "$RUN_DIR/results"
