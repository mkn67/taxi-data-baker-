#!/usr/bin/env bash
# Stop only host fallback Spark processes started by start_spark_host_fallback.sh.
set -euo pipefail
RUNTIME_DIR="${SPARK_HOST_RUNTIME_DIR:-/tmp/taxi-spark-host}"
PID_DIR="$RUNTIME_DIR/pids"

for name in worker-2 worker-1 master; do
  pidfile="$PID_DIR/$name.pid"
  [[ -f "$pidfile" ]] || continue
  pid="$(cat "$pidfile")"
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid"
    echo "Stopped $name (pid $pid)"
  fi
  rm -f "$pidfile"
done
