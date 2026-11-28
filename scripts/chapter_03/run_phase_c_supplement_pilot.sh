#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$PROJECT_ROOT"

if (( $# > 1 )); then
  printf 'Usage: bash %s [existing-pilot-id]\n' "$0" >&2
  exit 2
fi

GENERATE_INPUT=true
if (( $# == 1 )); then
  PILOT_ID="$1"
  if [[ ! "$PILOT_ID" =~ ^supplement_pilot_20261125_[[:alnum:]]+$ ]]; then
    printf 'Invalid pilot ID: %s\n' "$PILOT_ID" >&2
    exit 2
  fi
  PILOT_DIR="data/benchmarks/phase_c/$PILOT_ID"
  if [[ ! -f "$PILOT_DIR/fact_strong_10m.parquet" || ! -f "$PILOT_DIR/hotspots_strong_10m.parquet" || ! -f "$PILOT_DIR/pilot_manifest.json" ]]; then
    printf 'Pilot input files are incomplete in %s\n' "$PILOT_DIR" >&2
    exit 1
  fi
  GENERATE_INPUT=false
else
  PILOT_DIR="$(mktemp -d data/benchmarks/phase_c/supplement_pilot_20261125_XXXXXX)"
  PILOT_ID="$(basename "$PILOT_DIR")"
fi

PILOT_PATH="$PROJECT_ROOT/$PILOT_DIR"
FACT_PATH="$PILOT_PATH/fact_strong_10m.parquet"
HOTSPOT_PATH="$PILOT_PATH/hotspots_strong_10m.parquet"
DIMENSION_PATH="$PROJECT_ROOT/data/benchmarks/phase_c/synthetic_v3/dimension_large.parquet"

if [[ "$GENERATE_INPUT" == true ]]; then
  "$PROJECT_ROOT/.venv/bin/python" - "$PILOT_PATH" "$DIMENSION_PATH" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.chapter_03.generate_phase_c_synthetic import fingerprint, write_fact

pilot_path = Path(sys.argv[1])
dimension_path = Path(sys.argv[2])
fact_path = pilot_path / "fact_strong_10m.parquet"
hotspot_path = pilot_path / "hotspots_strong_10m.parquet"
seed = 20261125
row_count = 10_000_000
zipf_exponent = 2.0

fact_info = write_fact(fact_path, row_count, zipf_exponent, seed)
pq.write_table(
    pa.table({"PULocationID": np.asarray(fact_info["hotspot_ids"], dtype=np.int32)}),
    hotspot_path,
)
manifest = {
    "pilot_id": pilot_path.name,
    "created_at_utc": datetime.now(timezone.utc).isoformat(),
    "fact_rows": row_count,
    "zipf_exponent": zipf_exponent,
    "seed": seed,
    "hotspot_ids": fact_info["hotspot_ids"],
    "p95_threshold": fact_info["p95_threshold"],
    "hotspot_trip_share": fact_info["hotspot_trip_share"],
    "fact_path": str(fact_path),
    "fact_sha256": fingerprint(fact_path),
    "hotspot_path": str(hotspot_path),
    "dimension_path": str(dimension_path),
    "dimension_sha256": fingerprint(dimension_path),
    "aqe_skew_factor": 2,
    "aqe_skew_threshold_mb": 16,
    "shuffle_partitions": 200,
    "measured_blocks": 1,
    "purpose": "Pilot to inspect whether the AQE final plan activates skew splitting",
}
(pilot_path / "pilot_manifest.json").write_text(json.dumps(manifest, indent=2))
print(json.dumps(manifest, indent=2))
PY
else
  printf 'Reusing pilot input %s\n' "$PILOT_PATH"
fi

podman exec \
  --env SILVER_PATH="$FACT_PATH" \
  --env OD_MATRIX_PATH="$DIMENSION_PATH" \
  --env HOTSPOTS_PU_PATH="$HOTSPOT_PATH" \
  --env BENCHMARKS_DIR="$PILOT_PATH/results" \
  --env SPARK_MASTER_URL="spark://spark-master:7077" \
  --env SPARK_DRIVER_HOST="spark-driver" \
  --env SPARK_UI_URL="http://spark-driver:4040" \
  --env SPARK_EXPECTED_VERSION="3.5.9" \
  spark-driver \
  bash -lc "cd '$PROJECT_ROOT' && /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.driver.host=spark-driver --conf spark.driver.bindAddress=0.0.0.0 scripts/chapter_03/run_benchmark_nd3.py --mode pilot --master spark://spark-master:7077 --partitions 200 --aqe-skew-threshold-mb 16 --aqe-skew-factor 2 --pilot-iterations 1"

printf 'Pilot output directory: %s\n' "$PILOT_PATH"
