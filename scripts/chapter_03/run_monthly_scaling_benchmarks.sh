#!/usr/bin/env bash
set -euo pipefail

# Run full ND2/ND3 Standalone benchmarks for each independent month (02–05).
# January official runs are already stored in the standard benchmark directory.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LOG_DIR="$ROOT_DIR/data/benchmarks/scaling"
mkdir -p "$LOG_DIR"

for month in 02 03 04 05; do
  benchmark_dir="/opt/spark/data/benchmarks/scaling/2026_${month}"
  silver="/opt/spark/data/silver/fhvhv_2026_${month}_clean.parquet"
  hotspots="/opt/spark/data/silver/hotspots_p95_pu_2026_${month}"
  od_matrix="/opt/spark/data/silver/od_matrix_2026_${month}.parquet"

  echo "=== ND2 month 2026-${month} ==="
  docker compose exec -T \
    -e SILVER_PATH="$silver" \
    -e HOTSPOTS_PU_PATH="$hotspots" \
    -e BENCHMARKS_DIR="$benchmark_dir" \
    spark-driver /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      /opt/spark/scripts/chapter_03/run_benchmark_nd2.py --mode full \
      > "$LOG_DIR/nd2_2026_${month}.log" 2>&1

  echo "=== ND3 month 2026-${month} ==="
  docker compose exec -T \
    -e SILVER_PATH="$silver" \
    -e HOTSPOTS_PU_PATH="$hotspots" \
    -e OD_MATRIX_PATH="$od_matrix" \
    -e BENCHMARKS_DIR="$benchmark_dir" \
    spark-driver /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      /opt/spark/scripts/chapter_03/run_benchmark_nd3.py --mode full \
      > "$LOG_DIR/nd3_2026_${month}.log" 2>&1
done

echo "All monthly full benchmark runs completed. See $LOG_DIR for logs and tier artifacts."
