#!/usr/bin/env bash
# Start a two-worker Spark Standalone fallback from the project's PySpark venv.
# This is a host process setup, not a Docker/Podman Compose replacement.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SPARK_APP_HOME="${SPARK_HOME:-$PROJECT_ROOT/.venv/lib/python3.12/site-packages/pyspark}"
JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17}"
SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"
SPARK_LOCAL_HOSTNAME="${SPARK_LOCAL_HOSTNAME:-localhost}"
RUNTIME_DIR="${SPARK_HOST_RUNTIME_DIR:-/tmp/taxi-spark-host}"
PID_DIR="$RUNTIME_DIR/pids"
LOG_DIR="$RUNTIME_DIR/logs"
MASTER_URL="spark://$SPARK_LOCAL_IP:7077"

if [[ ! -x "$JAVA_HOME/bin/java" ]]; then
  echo "JDK not found at $JAVA_HOME. Set JAVA_HOME to a Java 17 installation." >&2
  exit 1
fi
if [[ ! -x "$SPARK_APP_HOME/bin/spark-class" ]]; then
  echo "PySpark 3.5.9 distribution not found at $SPARK_APP_HOME." >&2
  exit 1
fi
if [[ ! -x "$PROJECT_ROOT/.venv/bin/python" ]]; then
  echo "Project .venv is missing: $PROJECT_ROOT/.venv/bin/python" >&2
  exit 1
fi

mkdir -p "$PID_DIR" "$LOG_DIR" "$RUNTIME_DIR/worker-1" "$RUNTIME_DIR/worker-2"
export JAVA_HOME SPARK_HOME="$SPARK_APP_HOME" SPARK_LOCAL_IP SPARK_LOCAL_HOSTNAME

alive_workers() {
  curl -fsS "http://$SPARK_LOCAL_IP:8080/json/" 2>/dev/null \
    | "$PROJECT_ROOT/.venv/bin/python" -c 'import json,sys; print(json.load(sys.stdin).get("aliveworkers",0))' 2>/dev/null \
    || echo 0
}

if [[ "$(alive_workers)" -ge 2 ]]; then
  echo "Spark Standalone is already ready at $MASTER_URL with two active workers."
  echo "Master UI: http://$SPARK_LOCAL_IP:8080"
  exit 0
fi

for port in 7077 7078 7079 8080 8081 8082; do
  if "$PROJECT_ROOT/.venv/bin/python" - "$SPARK_LOCAL_IP" "$port" <<'PY'
import socket,sys
s=socket.socket()
try:
    s.bind((sys.argv[1],int(sys.argv[2])))
except OSError:
    raise SystemExit(1)
finally:
    s.close()
PY
  then :; else
    echo "Port $port is occupied but the expected two-worker cluster is not ready. Stop the conflicting service before retrying." >&2
    exit 1
  fi
done

start_service() {
  local name="$1"; shift
  nohup env JAVA_HOME="$JAVA_HOME" SPARK_HOME="$SPARK_APP_HOME" \
    SPARK_LOCAL_IP="$SPARK_LOCAL_IP" SPARK_LOCAL_HOSTNAME="$SPARK_LOCAL_HOSTNAME" \
    SPARK_WORKER_DIR="$RUNTIME_DIR/$name" \
    "$SPARK_APP_HOME/bin/spark-class" "$@" \
    >"$LOG_DIR/$name.log" 2>&1 </dev/null &
  echo "$!" > "$PID_DIR/$name.pid"
}

start_service master org.apache.spark.deploy.master.Master \
  --host "$SPARK_LOCAL_IP" --port 7077 --webui-port 8080

for _ in $(seq 1 30); do
  if curl -fsS "http://$SPARK_LOCAL_IP:8080/json/" >/dev/null 2>&1; then break; fi
  sleep 1
done
if ! curl -fsS "http://$SPARK_LOCAL_IP:8080/json/" >/dev/null 2>&1; then
  echo "Spark Master did not become ready. See $LOG_DIR/master.log" >&2
  exit 1
fi

start_service worker-1 org.apache.spark.deploy.worker.Worker \
  --host "$SPARK_LOCAL_IP" --port 7078 --webui-port 8081 --cores 2 --memory 2048m "$MASTER_URL"
start_service worker-2 org.apache.spark.deploy.worker.Worker \
  --host "$SPARK_LOCAL_IP" --port 7079 --webui-port 8082 --cores 2 --memory 2048m "$MASTER_URL"

for _ in $(seq 1 45); do
  if [[ "$(alive_workers)" -ge 2 ]]; then
    echo "Spark 3.5.9 Standalone ready: $MASTER_URL"
    echo "Two workers are active (2 cores / 2 GiB declared per worker; no container cgroup limits)."
    echo "Master UI: http://$SPARK_LOCAL_IP:8080"
    echo "Logs: $LOG_DIR"
    exit 0
  fi
  sleep 1
done

echo "Spark Master is up, but two workers did not register. Check logs in $LOG_DIR" >&2
exit 1
