#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$PROJECT_ROOT"

PILOT_ID="${1:-supplement_pilot_20261125_OQ8HCM}"
if [[ ! "$PILOT_ID" =~ ^supplement_pilot_20261125_[[:alnum:]]+$ ]]; then
  printf 'Invalid pilot ID: %s\n' "$PILOT_ID" >&2
  exit 2
fi
PILOT_PATH="$PROJECT_ROOT/data/benchmarks/phase_c/$PILOT_ID"
[[ -f "$PILOT_PATH/fact_strong_10m.parquet" && -f "$PILOT_PATH/hotspots_strong_10m.parquet" ]] || {
  printf 'Pilot inputs missing in %s\n' "$PILOT_PATH" >&2
  exit 1
}

RUN_ID="phase_c_supplement_confirmation_$(date -u +%Y%m%dT%H%M%SZ)"
RUN_DIR="$PILOT_PATH/results/$RUN_ID"
WORKLOADS="$PILOT_PATH/confirmation_workloads.json"
python3 - "$PROJECT_ROOT" "$PILOT_PATH" "$WORKLOADS" <<'PY'
import json
import sys
from pathlib import Path

root, pilot, out = map(Path, sys.argv[1:])
v3 = root / "data/benchmarks/phase_c/synthetic_v3"
manifest = {
    "protocol_id": "phase_c_supplement_ablation_v1",
    "workloads": [
        {
            "workload_id": "coalescing_only_strong_large_v3",
            "purpose": "AQE coalescing-only candidate; same strong skew class and 5M-row dimension as the split workload, 2M fact rows.",
            "fact_path": str(v3 / "fact_strong.parquet"),
            "dimension_path": str(v3 / "dimension_large.parquet"),
            "hotspot_path": str(v3 / "hotspots_strong.parquet"),
            "prior_plan_evidence": "data/benchmarks/phase_c/synthetic_grid_v3_20261123/strong_large/; five AQE final plans audited as coalescing without skew split"
        },
        {
            "workload_id": "skew_split_strong_10m_pilot",
            "purpose": "10M Zipf(2.0) pilot workload; final plan from the completed pilot confirmed skew=true and AQEShuffleRead skewed.",
            "fact_path": str(pilot / "fact_strong_10m.parquet"),
            "dimension_path": str(v3 / "dimension_large.parquet"),
            "hotspot_path": str(pilot / "hotspots_strong_10m.parquet"),
            "prior_plan_evidence": str(pilot / "results/nd3_plans/pilot_standalone/nd3_Spark_AQE_Skew_Join_iter1_20261125T141241Z.txt")
        }
    ]
}
Path(out).write_text(json.dumps(manifest, indent=2))
print(f"Workload manifest: {out}")
PY

printf 'Starting 96 measured actions (2 workloads x 6 arms x 8 blocks). Output: %s\n' "$RUN_DIR"
podman exec \
  --env BENCHMARKS_DIR="$RUN_DIR" \
  --env SPARK_MASTER_URL="spark://spark-master:7077" \
  --env SPARK_DRIVER_HOST="spark-driver" \
  --env SPARK_UI_URL="http://spark-driver:4040" \
  --env SPARK_EXPECTED_VERSION="3.5.9" \
  spark-driver \
  bash -lc "cd '$PROJECT_ROOT' && /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.driver.host=spark-driver --conf spark.driver.bindAddress=0.0.0.0 scripts/chapter_03/run_phase_c_supplement_ablation.py --workloads '$WORKLOADS' --output-dir '$RUN_DIR' --blocks 8 --partitions 200 --skew-factor 2 --skew-threshold-mb 16 --order-seed 20261126"

printf 'Confirmation completed. Artifacts: %s\n' "$RUN_DIR"
