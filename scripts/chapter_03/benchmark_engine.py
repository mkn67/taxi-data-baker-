"""Benchmark Engine for Apache Spark Skew Optimization Research (NYC HVFHV 2026).

Covers:
- Environment auto-detection (Java 17, PySpark Python mismatch, local vs standalone paths)
- Dual container path support (/opt/spark and /opt/bitnami/spark)
- Absolute Event Log directory resolution
- Flexible Parquet / CSV / Directory path resolution for DuckDB integration
- Reproducible stored/on-the-fly P95 hotspot loading (no hard-coded fallback)
- Spark REST API Task Metrics, TSR (Task Skew Ratio), and PSR (Partition Skew Ratio) collection
- Standalone Spark/version/worker-executor verification for official runs
- Physical execution plan extraction
- Full materialization guarantees against Catalyst Optimizer column pruning
- Filtering out invalid/failed runs from summary aggregations
- Ground truth validation with DuckDB
- Benchmark orchestration with warm-up and randomized/alternated iterations
"""

import json
import hashlib
import math
import os
import random
import shutil
import statistics
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Callable, Dict, List, Optional, Tuple

import duckdb
import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# ==============================================================================
# 1. ENVIRONMENT CONFIGURATION & PATH RESOLUTION
# ==============================================================================

CONTAINER_SPARK_PATHS = ["/opt/spark", "/opt/bitnami/spark"]


def configure_environment():
    """Ensure JVM and Python worker environments match and avoid JDK 25 incompatibilities."""
    if "JAVA_HOME" not in os.environ or "java-25" in os.environ.get("JAVA_HOME", ""):
        for candidate in ["/usr/lib/jvm/java-17", "/usr/lib/jvm/java-17-openjdk", "/usr/lib/jvm/jre-17"]:
            if os.path.isdir(candidate):
                os.environ["JAVA_HOME"] = candidate
                break

    # Prevent Python version mismatch between driver and worker
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


configure_environment()


def is_in_container() -> bool:
    """Check if execution is happening inside a Spark container environment."""
    return os.path.exists("/.dockerenv") or os.path.exists("/run/.containerenv") or (
        os.path.exists("/opt/spark/work-dir") and not os.path.exists("/home/kedora")
    )


def resolve_paths() -> Dict[str, str]:
    """Resolve data and project paths dynamically as absolute paths relative to project root."""
    in_container = is_in_container()

    # Prefer repository root relative to this file if data exists
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if os.path.isdir(os.path.join(repo_root, "data")):
        base_dir = repo_root
    elif in_container and os.path.isdir("/opt/spark/data"):
        base_dir = "/opt/spark"
    elif in_container and os.path.isdir("/opt/bitnami/spark/data"):
        base_dir = "/opt/bitnami/spark"
    else:
        base_dir = repo_root

    base_dir = os.path.abspath(base_dir)
    base_data = os.path.join(base_dir, "data")
    silver_path = os.environ.get("SILVER_PATH", os.path.join(base_data, "silver/fhvhv_2026_01_clean.parquet"))
    od_matrix_path = os.environ.get("OD_MATRIX_PATH", os.path.join(base_data, "silver/od_matrix_2026_01.parquet"))
    hotspots_pu_path = os.environ.get("HOTSPOTS_PU_PATH", os.path.join(base_data, "silver/hotspots_p95_pu_2026_01"))
    hotspots_do_path = os.path.join(base_data, "silver/hotspots_p95_do_2026_01")
    zone_lookup_path = os.path.join(base_data, "raw/taxi+_zone_lookup.csv")
    benchmarks_dir = os.environ.get("BENCHMARKS_DIR", os.path.join(base_data, "benchmarks"))
    plans_dir = os.path.join(benchmarks_dir, "nd3_plans")
    nd2_plans_dir = os.path.join(benchmarks_dir, "nd2_plans")
    # Compose mounts the shared host directory at this path in every Spark
    # container. Keep the host-side path for local functional runs.
    events_dir = (
        "/opt/spark/work-dir/logs/spark-events"
        if in_container and os.path.isdir("/opt/spark/work-dir")
        else os.path.join(base_dir, "logs/spark-events")
    )

    os.makedirs(benchmarks_dir, exist_ok=True)
    os.makedirs(plans_dir, exist_ok=True)
    os.makedirs(nd2_plans_dir, exist_ok=True)
    os.makedirs(events_dir, exist_ok=True)

    return {
        "in_container": in_container,
        "base_dir": base_dir,
        "base_data": base_data,
        "silver_path": silver_path,
        "od_matrix_path": od_matrix_path,
        "hotspots_pu_path": hotspots_pu_path,
        "hotspots_do_path": hotspots_do_path,
        "zone_lookup_path": zone_lookup_path,
        "benchmarks_dir": benchmarks_dir,
        "plans_dir": plans_dir,
        "nd2_plans_dir": nd2_plans_dir,
        "events_dir": events_dir,
    }


def _to_duckdb_parquet_glob(path_str: str) -> str:
    """Convert a file or directory path into a valid DuckDB read_parquet glob pattern."""
    if "*" in path_str:
        return path_str
    if os.path.isdir(path_str):
        return os.path.join(path_str, "*.parquet")
    if os.path.isfile(path_str):
        return path_str
    if os.path.exists(path_str + ".parquet"):
        return path_str + ".parquet"
    return os.path.join(path_str, "*.parquet")


def dataset_manifest_fingerprint(path: str) -> str:
    """Hash a Parquet file manifest (relative names, sizes, mtimes), not full file contents."""
    files = [path] if os.path.isfile(path) else [
        os.path.join(root, name)
        for root, _, names in os.walk(path)
        for name in names if name.endswith(".parquet")
    ]
    if not files or any(not os.path.isfile(file_path) for file_path in files):
        raise FileNotFoundError(f"No Parquet input files found at {path}")
    entries = []
    for file_path in sorted(files):
        stat = os.stat(file_path)
        entries.append((os.path.relpath(file_path, os.path.dirname(os.path.dirname(path))), stat.st_size, stat.st_mtime_ns))
    encoded = json.dumps(entries, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def is_spark_cluster_available() -> bool:
    """Check if Spark Standalone Master is online and reachable with alive workers."""
    endpoints = [
        "http://spark-master:8080/json/" if is_in_container() else "http://127.0.0.1:8080/json/",
    ]
    for ep in endpoints:
        try:
            with urllib.request.urlopen(ep, timeout=1.5) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                if data.get("aliveworkers", 0) > 0:
                    return True
        except Exception:
            continue
    return False


def get_spark_master_url() -> str:
    """Determine Master URL:
    - SPARK_MASTER_URL env var if set
    - If Standalone cluster is alive:
        - inside container: spark://spark-master:7077
        - outside container (host): spark://127.0.0.1:7077
    - Fallback: local[4]
    """
    if "SPARK_MASTER_URL" in os.environ:
        return os.environ["SPARK_MASTER_URL"]
    
    in_cont = is_in_container()
    if is_spark_cluster_available():
        if in_cont:
            return "spark://spark-master:7077"
        else:
            return "spark://127.0.0.1:7077"

    if in_cont:
        return "spark://spark-master:7077"
    return "local[4]"


def create_benchmark_spark_session(
    app_name: str,
    master_url: Optional[str] = None,
    shuffle_partitions: int = 200,
    enable_aqe: bool = False,
    extra_configs: Optional[Dict[str, str]] = None,
    require_standalone: bool = False,
) -> SparkSession:
    """Create a standardized SparkSession with absolute Event Log paths."""
    configure_environment()
    paths = resolve_paths()
    events_dir = paths["events_dir"]
    master = master_url or get_spark_master_url()

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true" if enable_aqe else "false")
        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", "4040")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", f"file://{events_dir}")
    )

    if master.startswith("spark://"):
        if not is_in_container():
            # Configure host driver networking so Podman container workers can communicate back
            builder = (
                builder
                .config("spark.driver.host", os.environ.get("SPARK_DRIVER_HOST", "host.containers.internal"))
                .config("spark.driver.bindAddress", "0.0.0.0")
                .config("spark.driver.port", "7076")
                .config("spark.blockManager.port", "7075")
                .config("spark.pyspark.python", "/usr/bin/python3")
                .config("spark.pyspark.driver.python", sys.executable)
            )
        else:
            if os.environ.get("SPARK_DRIVER_HOST"):
                builder = builder.config("spark.driver.host", os.environ["SPARK_DRIVER_HOST"])
                builder = builder.config("spark.driver.bindAddress", "0.0.0.0")
            builder = (
                builder
                .config("spark.pyspark.python", "/usr/bin/python3")
                .config("spark.pyspark.driver.python", sys.executable)
            )

    if master.startswith("local"):
        builder = builder.config("spark.driver.memory", "3g")

    if extra_configs:
        for k, v in extra_configs.items():
            builder = builder.config(k, v)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f">>> SparkSession Initialized | App: {app_name} | Master: {spark.sparkContext.master}")
    print(f">>> Event Log Directory: file://{events_dir}")
    if require_standalone:
        verify_spark_master_connection(spark)
        expected_version = os.environ.get("SPARK_EXPECTED_VERSION", "3.5.9")
        if spark.version != expected_version:
            raise RuntimeError(
                f"Standalone benchmark expects Spark {expected_version} from docker-compose.yml, "
                f"but the driver reports Spark {spark.version}. Use a matching PySpark/Spark distribution."
            )
        # Standalone may not allocate executors until the first job is submitted.
        # Run a tiny unmeasured probe before asking the REST API for worker evidence.
        probe_result = spark.sparkContext.parallelize(range(4), 4).map(lambda value: value + 1).sum()
        if probe_result != 10:
            raise RuntimeError(f"Standalone executor probe returned an unexpected result: {probe_result}")
        evidence = verify_distributed_executors(spark, require_two_workers=True)
        print(f">>> Active worker executor evidence: {evidence}")
    return spark


def verify_spark_master_connection(spark: SparkSession, expected_master_prefix: str = "spark://"):
    """Verify that SparkSession is connected to the intended Standalone Cluster Master."""
    actual_master = spark.sparkContext.master
    print(f">>> Actual Spark Master in use: {actual_master}")
    if expected_master_prefix and not actual_master.startswith(expected_master_prefix):
        raise RuntimeError(
            f"Standalone benchmark required, but Spark connected to '{actual_master}'. "
            "Use the Compose Spark driver and the configured Standalone Master URL."
        )


def verify_distributed_executors(spark: SparkSession, require_two_workers: bool = True) -> Dict[str, Any]:
    """Check live Spark executor evidence through the driver's Spark REST API."""
    # A host-side driver may advertise a container-only hostname. Allow the
    # operator to point the REST collector at the host-published Spark UI.
    ui_url = (os.environ.get("SPARK_UI_URL") or spark.sparkContext.uiWebUrl or "").rstrip("/")
    app_id = spark.sparkContext.applicationId
    if not ui_url:
        raise RuntimeError("Spark UI URL is unavailable; cannot verify executor distribution.")
    try:
        with urllib.request.urlopen(
            f"{ui_url}/api/v1/applications/{app_id}/executors", timeout=10
        ) as response:
            executors = json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        raise RuntimeError(f"Could not verify executors through Spark REST API: {exc}") from exc

    active = [e for e in executors if e.get("isActive", True)]
    workers = [e for e in active if str(e.get("id")) != "driver"]
    hosts = sorted({str(e.get("hostPort", "")).rsplit(":", 1)[0] for e in workers})
    hosts = [host for host in hosts if host]
    evidence = {
        "executor_count_active": len(workers),
        "executor_hosts": hosts,
        "executor_ids": [str(e.get("id")) for e in workers],
    }
    if require_two_workers and (len(workers) < 2 or len(hosts) < 2):
        raise RuntimeError(
            "Standalone cluster was requested, but Spark REST reports fewer than two active "
            f"worker hosts: {evidence}. Ensure both Compose workers are registered and cores are allocated."
        )
    return evidence


def write_csv_preserving_history(frame: pd.DataFrame, path: str) -> str:
    """Atomically write a CSV, retaining any prior canonical output as a timestamped backup."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.exists(path):
        stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        backup = f"{path}.bak_{stamp}"
        suffix = 1
        while os.path.exists(backup):
            backup = f"{path}.bak_{stamp}_{suffix}"
            suffix += 1
        shutil.copy2(path, backup)
    tmp_path = f"{path}.tmp"
    frame.to_csv(tmp_path, index=False)
    os.replace(tmp_path, path)
    return path


def materialize_dataframe(df: DataFrame) -> int:
    """Force complete evaluation of all output columns to prevent Catalyst Optimizer column pruning."""
    rows = df.collect()
    return len(rows)


# ==============================================================================
# 2. METRICS COLLECTION VIA SPARK REST API (TSR & PSR)
# ==============================================================================

class SparkMetricsCollector:
    """Collect Task/Stage metrics, TSR (Task Skew Ratio), and PSR (Partition Skew Ratio) via Spark REST API.

    Target Stage Selection Strategy:
    - Isolates the stage with maximum Shuffle Activity (shuffleReadBytes + shuffleWriteBytes).

    Metrics Definitions:
    1. TSR (Task Skew Ratio - Execution Time Skew):
       TSR = max(task_execution_time) / median(task_execution_time)
       Measures runtime imbalance across workers.

    2. PSR (Partition Skew Ratio - Record Distribution Skew):
       PSR = max(task_records_processed) / median(task_records_processed)
       Measures record volume imbalance across partitions.
    """

    def __init__(self, spark: SparkSession, ui_url: Optional[str] = None):
        self.spark = spark
        self.sc = spark.sparkContext
        self.app_id = self.sc.applicationId
        self.ui_url = (ui_url or self.sc.uiWebUrl or "http://127.0.0.1:4040").rstrip("/")
        self._start_time = 0.0

    def start_run(self, job_group_id: str, description: str):
        self.sc.setJobGroup(job_group_id, description, interruptOnCancel=True)
        self._start_time = time.perf_counter()

    def end_run(self, job_group_id: str) -> Dict[str, Any]:
        wall_clock_duration = time.perf_counter() - self._start_time

        record = self._empty_metric_payload("NO_DATA")
        record["job_group"] = job_group_id
        record["wall_clock_duration_s"] = round(wall_clock_duration, 4)

        try:
            last_payload: Optional[Dict[str, Any]] = None
            for attempt in range(4):
                payload = self._collect_metrics(str(job_group_id))
                status = payload.get("collector_status")
                last_payload = payload
                if status in ("OK", "NO_COMPLETED_STAGES"):
                    record.update(payload)
                    return record
                if status == "JOB_GROUP_NOT_FOUND" and attempt < 3:
                    time.sleep(0.5)
                    continue
                break
            if last_payload:
                record.update(last_payload)
        except Exception as e:
            record["collector_status"] = f"EXCEPTION: {str(e)}"

        return record

    def _collect_metrics(self, job_group_id: str) -> Dict[str, Any]:
        jobs = self._http_get_json(f"{self.ui_url}/api/v1/applications/{self.app_id}/jobs")
        if jobs is None:
            return {"collector_status": "ERR_FETCH_JOBS"}

        target_stage_ids: set = set()
        num_jobs = 0
        for job in jobs:
            if job.get("jobGroup") == job_group_id:
                target_stage_ids.update(job.get("stageIds", []))
                num_jobs += 1

        if not target_stage_ids:
            return {"collector_status": "JOB_GROUP_NOT_FOUND"}

        stages = self._http_get_json(f"{self.ui_url}/api/v1/applications/{self.app_id}/stages")
        if stages is None:
            return {"collector_status": "ERR_FETCH_STAGES"}

        latest_attempts: Dict[int, Dict[str, Any]] = {}
        for s in stages:
            sid = s.get("stageId")
            if sid in target_stage_ids and s.get("status") == "COMPLETE":
                att = s.get("attemptId", 0)
                if sid not in latest_attempts or att > latest_attempts[sid].get("attemptId", 0):
                    latest_attempts[sid] = s

        target_stages = list(latest_attempts.values())
        if not target_stages:
            return {"collector_status": "NO_COMPLETED_STAGES"}

        def stage_total(metric: str) -> Optional[int]:
            values = [s.get(metric) for s in target_stages]
            return sum(values) if values and all(value is not None for value in values) else None

        total_disk = stage_total("diskBytesSpilled")
        total_mem = stage_total("memoryBytesSpilled")
        total_shuffle_read = stage_total("shuffleReadBytes")
        total_shuffle_wr = stage_total("shuffleWriteBytes")
        total_sh_read_rec = stage_total("shuffleReadRecords")
        total_sh_wr_rec = stage_total("shuffleWriteRecords")

        target_stage = max(
            target_stages,
            key=lambda x: (
                x.get("shuffleReadBytes", 0) + x.get("shuffleWriteBytes", 0),
                x.get("executorRunTime", 0),
            ),
        )
        target_sid = target_stage.get("stageId")
        target_att = target_stage.get("attemptId", 0)

        durations, records_processed, record_metric = self._fetch_stage_task_metrics(target_sid, target_att)

        max_dur = median_dur = total_dur = None
        tsr: Optional[float] = None
        if durations:
            total_dur = sum(durations)
            max_dur = max(durations)
            median_dur = statistics.median(durations)
            if median_dur > 0:
                tsr = round(max_dur / median_dur, 2)

        max_rec = median_rec = None
        psr: Optional[float] = None
        if records_processed:
            max_rec = max(records_processed)
            median_rec = statistics.median(records_processed)
            if median_rec > 0:
                psr = round(max_rec / median_rec, 2)

        stages_detail = [
            {
                "stage_id": s.get("stageId"),
                "attempt_id": s.get("attemptId", 0),
                "name": s.get("name", ""),
                "num_tasks": s.get("numTasks"),
                "shuffle_read_bytes": s.get("shuffleReadBytes"),
                "shuffle_write_bytes": s.get("shuffleWriteBytes"),
                "disk_bytes_spilled": s.get("diskBytesSpilled"),
                "memory_bytes_spilled": s.get("memoryBytesSpilled"),
                "is_target": s.get("stageId") == target_sid,
            }
            for s in target_stages
        ]

        return {
            "collector_status": "OK",
            "num_jobs": num_jobs,
            "num_stages": len(target_stages),
            "target_stage_id": target_sid,
            "target_stage_attempt": target_att,
            "total_tasks": len(durations) if durations else None,
            "tsr_scope": "successful tasks in selected completed stage (max shuffle bytes; tie by executor runtime)",
            "tsr_duration_metric": "executorRunTime milliseconds, falling back to Spark task duration when absent",
            "psr_scope": "successful tasks in selected completed stage; reported only when one common record metric exists for every successful task",
            "psr_record_metric": record_metric,
            "total_task_duration_ms": total_dur,
            "max_task_duration_ms": max_dur,
            "median_task_duration_ms": round(median_dur, 1) if median_dur is not None else None,
            "task_skew_ratio_max": tsr,
            "partition_skew_ratio_max": psr,
            "max_task_records_read": max_rec,
            "median_task_records_read": round(median_rec, 1) if median_rec is not None else None,
            "shuffle_read_bytes": total_shuffle_read,
            "shuffle_write_bytes": total_shuffle_wr,
            "shuffle_read_records": total_sh_read_rec,
            "shuffle_write_records": total_sh_wr_rec,
            "memory_bytes_spilled": total_mem,
            "disk_bytes_spilled": total_disk,
            "stages_detail": stages_detail,
        }

    def _fetch_stage_task_metrics(self, stage_id: int, attempt_id: int) -> Tuple[List[int], List[int], Optional[str]]:
        url = f"{self.ui_url}/api/v1/applications/{self.app_id}/stages/{stage_id}/{attempt_id}"
        data = self._http_get_json(url)
        if not data:
            return [], [], None

        tasks_raw = data.get("tasks", [])
        tasks_list = tasks_raw.values() if isinstance(tasks_raw, dict) else tasks_raw

        durations: List[int] = []
        records_by_task: List[Dict[str, Optional[int]]] = []
        for t in tasks_list:
            if t.get("status") != "SUCCESS":
                continue
            tm = t.get("taskMetrics", {}) or {}
            dur = tm.get("executorRunTime", t.get("duration", 0)) or 0

            sh_read = tm.get("shuffleReadMetrics", {}) or {}
            sh_write = tm.get("shuffleWriteMetrics", {}) or {}
            in_metrics = tm.get("inputMetrics", {}) or {}
            out_metrics = tm.get("outputMetrics", {}) or {}

            durations.append(int(dur))
            records_by_task.append({
                "shuffle_read_records": sh_read.get("recordsRead"),
                "shuffle_write_records": sh_write.get("recordsWritten"),
                "input_records": in_metrics.get("recordsRead"),
                "output_records": out_metrics.get("recordsWritten"),
            })

        for metric in ("shuffle_read_records", "shuffle_write_records", "input_records", "output_records"):
            values = [task[metric] for task in records_by_task]
            if values and all(value is not None for value in values):
                return durations, [int(value) for value in values], metric
        return durations, [], None

    def _http_get_json(self, url: str) -> Optional[Any]:
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception:
            return None

    def _empty_metric_payload(self, status_reason: str) -> Dict[str, Any]:
        return {
            "collector_status": status_reason,
            "num_jobs": None,
            "num_stages": None,
            "target_stage_id": None,
            "target_stage_attempt": None,
            "total_tasks": None,
            "tsr_scope": None,
            "tsr_duration_metric": None,
            "psr_scope": None,
            "psr_record_metric": None,
            "total_task_duration_ms": None,
            "max_task_duration_ms": None,
            "median_task_duration_ms": None,
            "task_skew_ratio_max": None,
            "partition_skew_ratio_max": None,
            "max_task_records_read": None,
            "median_task_records_read": None,
            "shuffle_read_bytes": None,
            "shuffle_write_bytes": None,
            "shuffle_read_records": None,
            "shuffle_write_records": None,
            "memory_bytes_spilled": None,
            "disk_bytes_spilled": None,
            "stages_detail": [],
        }

# ==============================================================================
# 3. NỘI DUNG 2: AGGREGATION SKEW PIPELINE
# ==============================================================================

def load_hotspot_pu_ids(hotspots_pu_path: str, silver_path: Optional[str] = None) -> List[int]:
    """Read verified P95 pickup hotspot zone IDs safely from Parquet/CSV directory or file."""
    con = duckdb.connect()
    try:
        # 1. Direct directory/file existence check
        if os.path.isdir(hotspots_pu_path):
            glob_p = os.path.join(hotspots_pu_path, "*.parquet")
            files = [f for f in os.listdir(hotspots_pu_path) if f.endswith(".parquet")]
            if files:
                df_hp = con.query(f"SELECT DISTINCT PULocationID FROM read_parquet('{glob_p}')").df()
                return sorted(df_hp["PULocationID"].dropna().astype(int).tolist())

        if os.path.isfile(hotspots_pu_path):
            if hotspots_pu_path.endswith(".csv"):
                df_hp = con.query(f"SELECT DISTINCT PULocationID FROM read_csv_auto('{hotspots_pu_path}')").df()
            else:
                df_hp = con.query(f"SELECT DISTINCT PULocationID FROM read_parquet('{hotspots_pu_path}')").df()
            return sorted(df_hp["PULocationID"].dropna().astype(int).tolist())

        # 2. Check alternative extension suffixes
        for alt_path in [hotspots_pu_path + ".csv", hotspots_pu_path + ".parquet"]:
            if os.path.exists(alt_path):
                if alt_path.endswith(".csv"):
                    df_hp = con.query(f"SELECT DISTINCT PULocationID FROM read_csv_auto('{alt_path}')").df()
                else:
                    df_hp = con.query(f"SELECT DISTINCT PULocationID FROM read_parquet('{alt_path}')").df()
                return sorted(df_hp["PULocationID"].dropna().astype(int).tolist())

        # 3. Fallback: On-the-fly P95 calculation if silver_path provided
        if silver_path:
            s_glob = _to_duckdb_parquet_glob(silver_path)
            print("[INFO] hotspots_pu_path không tồn tại. Đang tự động tính P95 Hotspots từ dữ liệu Silver...")
            df_p95 = con.query(f"""
                WITH counts AS (
                    SELECT PULocationID, COUNT(*) as cnt
                    FROM read_parquet('{s_glob}')
                    WHERE PULocationID IS NOT NULL
                    GROUP BY PULocationID
                ),
                p95_val AS (
                    SELECT QUANTILE_DISC(cnt, 0.95) as threshold FROM counts
                )
                SELECT PULocationID FROM counts, p95_val WHERE cnt >= threshold
            """).df()
            return sorted(df_p95["PULocationID"].dropna().astype(int).tolist())

        raise FileNotFoundError(
            f"Hotspot artifact not found at {hotspots_pu_path}; provide the recorded P95 artifact "
            "or the Silver dataset so the implemented P95 set can be reproduced."
        )
    except Exception as e:
        if isinstance(e, FileNotFoundError):
            raise
        raise RuntimeError(f"Could not load/reproduce P95 hotspot artifact: {e}") from e
    finally:
        con.close()


def aggregate_baseline(df: DataFrame) -> DataFrame:
    """Baseline Direct GroupBy on PULocationID."""
    return df.groupBy("PULocationID").agg(
        F.count("*").alias("trip_count"),
        (F.sum("base_passenger_fare") / F.count("base_passenger_fare")).alias("avg_fare"),
        F.sum("trip_miles").alias("total_miles"),
    )


def aggregate_uniform_salting(df: DataFrame, num_salts: int = 8) -> DataFrame:
    """Uniform Salting: All keys partitioned into S buckets with Two-Stage Aggregation."""
    df_salted = df.withColumn(
        "salt",
        F.pmod(
            F.abs(F.hash(F.col("pickup_datetime"), F.col("dropoff_datetime"), F.col("trip_miles"))),
            F.lit(num_salts),
        ),
    )
    stage1 = df_salted.groupBy("PULocationID", "salt").agg(
        F.count("*").alias("partial_count"),
        F.sum("base_passenger_fare").alias("partial_fare_sum"),
        F.count("base_passenger_fare").alias("partial_fare_count"),
        F.sum("trip_miles").alias("partial_miles_sum"),
    )
    stage2 = stage1.groupBy("PULocationID").agg(
        F.sum("partial_count").alias("trip_count"),
        (F.sum("partial_fare_sum") / F.sum("partial_fare_count")).alias("avg_fare"),
        F.sum("partial_miles_sum").alias("total_miles"),
    )
    return stage2


def aggregate_selective_salting(df: DataFrame, hotspot_ids: List[int], num_salts: int = 8) -> DataFrame:
    """Selective Salting: Only P95 hotspots split into S buckets, others retain salt=0."""
    df_salted = df.withColumn(
        "salt",
        F.when(
            F.col("PULocationID").isin(hotspot_ids),
            F.pmod(
                F.abs(F.hash(F.col("pickup_datetime"), F.col("dropoff_datetime"), F.col("trip_miles"))),
                F.lit(num_salts),
            ),
        ).otherwise(F.lit(0)),
    )
    stage1 = df_salted.groupBy("PULocationID", "salt").agg(
        F.count("*").alias("partial_count"),
        F.sum("base_passenger_fare").alias("partial_fare_sum"),
        F.count("base_passenger_fare").alias("partial_fare_count"),
        F.sum("trip_miles").alias("partial_miles_sum"),
    )
    stage2 = stage1.groupBy("PULocationID").agg(
        F.sum("partial_count").alias("trip_count"),
        (F.sum("partial_fare_sum") / F.sum("partial_fare_count")).alias("avg_fare"),
        F.sum("partial_miles_sum").alias("total_miles"),
    )
    return stage2


def verify_aggregation_with_duckdb(
    spark_df: DataFrame, silver_path: str, fare_tol: float = 1e-4, miles_tol: float = 1e-2
) -> Tuple[bool, Dict[str, Any], pd.DataFrame]:
    """Independent verification against DuckDB ground truth."""
    silver_glob = _to_duckdb_parquet_glob(silver_path)
    duck_sql = f"""
    SELECT 
        PULocationID,
        COUNT(*) AS trip_count_duck,
        AVG(base_passenger_fare) AS avg_fare_duck,
        SUM(trip_miles) AS total_miles_duck
    FROM read_parquet('{silver_glob}')
    GROUP BY PULocationID
    ORDER BY PULocationID;
    """
    df_duck = duckdb.query(duck_sql).df()

    df_spark = spark_df.select("PULocationID", "trip_count", "avg_fare", "total_miles").toPandas()
    df_spark["PULocationID"] = df_spark["PULocationID"].astype(int)
    df_spark = df_spark.sort_values("PULocationID").reset_index(drop=True)

    merged = pd.merge(df_spark, df_duck, on="PULocationID", how="outer")

    merged["count_diff"] = merged["trip_count"] - merged["trip_count_duck"]
    merged["fare_diff"] = (merged["avg_fare"] - merged["avg_fare_duck"]).abs()
    merged["miles_diff"] = (merged["total_miles"] - merged["total_miles_duck"]).abs()

    count_mismatch = (merged["count_diff"] != 0).sum()
    fare_mismatch = (merged["fare_diff"] > fare_tol).sum()
    miles_mismatch = (merged["miles_diff"] > miles_tol).sum()

    passed = (count_mismatch == 0) and (fare_mismatch == 0) and (miles_mismatch == 0) and (len(merged) == len(df_duck))

    summary = {
        "status": "PASS" if passed else "FAIL",
        "total_zones": len(merged),
        "count_mismatches": int(count_mismatch),
        "fare_mismatches": int(fare_mismatch),
        "miles_mismatches": int(miles_mismatch),
        "max_fare_diff": float(merged["fare_diff"].max()),
        "max_miles_diff": float(merged["miles_diff"].max()),
    }

    return passed, summary, merged


# ==============================================================================
# 4. NỘI DUNG 3: JOIN SKEW PIPELINE (OD MATRIX JOIN)
# ==============================================================================

def join_baseline_smj(spark: SparkSession, df_trips: DataFrame, df_od: DataFrame) -> DataFrame:
    """Baseline Sort-Merge Join on OD composite key (AQE off, Broadcast disabled)."""
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    return df_trips.hint("merge").join(
        df_od.hint("merge"),
        ["PULocationID", "DOLocationID"],
        "inner",
    )


def join_aqe_skew(
    spark: SparkSession,
    df_trips: DataFrame,
    df_od: DataFrame,
    skew_factor: int = 2,
    skew_threshold_mb: int = 16,
) -> DataFrame:
    """Spark AQE Adaptive Skew Join on OD composite key (AQE on, Broadcast disabled)."""
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", str(skew_factor))
    spark.conf.set(
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes",
        str(skew_threshold_mb * 1024 * 1024),
    )
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    return df_trips.hint("merge").join(
        df_od.hint("merge"),
        ["PULocationID", "DOLocationID"],
        "inner",
    )


def join_salting(
    spark: SparkSession,
    df_trips: DataFrame,
    df_od: DataFrame,
    hotspot_ids: List[int],
    num_salts: int = 8,
) -> DataFrame:
    """Salting Join: Replicate hotspot OD rows in OD matrix and salt matching trips (AQE off)."""
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    # Big table (trips): salt trips originating from hotspot pickup zones
    df_trips_salted = df_trips.withColumn(
        "salt",
        F.when(
            F.col("PULocationID").isin(hotspot_ids),
            F.pmod(
                F.abs(F.hash(F.col("pickup_datetime"), F.col("dropoff_datetime"), F.col("trip_miles"))),
                F.lit(num_salts),
            ),
        ).otherwise(F.lit(0)),
    )

    # OD Matrix table: replicate rows where PULocationID is a hotspot across S buckets
    salt_array = F.when(
        F.col("PULocationID").isin(hotspot_ids),
        F.array([F.lit(i) for i in range(num_salts)]),
    ).otherwise(F.array([F.lit(0)]))

    df_od_salted = df_od.withColumn("salt", F.explode(salt_array))

    # Join on composite key (PULocationID, DOLocationID, salt)
    df_joined = df_trips_salted.hint("merge").join(
        df_od_salted.hint("merge"),
        ["PULocationID", "DOLocationID", "salt"],
        "inner",
    ).drop("salt")

    return df_joined


def verify_join_with_duckdb(
    spark_joined_df: DataFrame,
    silver_path: str,
    od_path: str,
    fare_tol: float = 0.05,
    miles_tol: float = 0.05,
) -> Tuple[bool, Dict[str, Any], pd.DataFrame]:
    """Independent verification of OD Join output against DuckDB ground truth grouped by Borough pair."""
    silver_glob = _to_duckdb_parquet_glob(silver_path)
    od_glob = _to_duckdb_parquet_glob(od_path)
    duck_sql = f"""
    SELECT 
        m.PU_Borough,
        m.DO_Borough,
        COUNT(*) AS trip_count_duck,
        ROUND(SUM(t.trip_miles), 2) AS total_miles_duck,
        ROUND(SUM(t.base_passenger_fare), 2) AS total_fare_duck,
        COUNT(DISTINCT t.PULocationID) AS distinct_pu_duck
    FROM read_parquet('{silver_glob}') t
    JOIN read_parquet('{od_glob}') m
      ON t.PULocationID = m.PULocationID AND t.DOLocationID = m.DOLocationID
    GROUP BY m.PU_Borough, m.DO_Borough
    ORDER BY m.PU_Borough, m.DO_Borough;
    """
    df_duck = duckdb.query(duck_sql).df()

    df_spark_agg = (
        spark_joined_df.groupBy("PU_Borough", "DO_Borough")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.sum("trip_miles"), 2).alias("total_miles"),
            F.round(F.sum("base_passenger_fare"), 2).alias("total_fare"),
            F.countDistinct("PULocationID").alias("distinct_pu"),
        )
        .toPandas()
        .sort_values(["PU_Borough", "DO_Borough"])
        .reset_index(drop=True)
    )

    merged = pd.merge(df_spark_agg, df_duck, on=["PU_Borough", "DO_Borough"], how="outer")
    merged["count_diff"] = merged["trip_count"] - merged["trip_count_duck"]
    merged["fare_diff"] = (merged["total_fare"] - merged["total_fare_duck"]).abs()
    merged["miles_diff"] = (merged["total_miles"] - merged["total_miles_duck"]).abs()
    merged["distinct_pu_diff"] = merged["distinct_pu"] - merged["distinct_pu_duck"]

    count_mismatch = (merged["count_diff"] != 0).sum()
    fare_mismatch = (merged["fare_diff"] > fare_tol).sum()
    miles_mismatch = (merged["miles_diff"] > miles_tol).sum()
    distinct_pu_mismatch = (merged["distinct_pu_diff"] != 0).sum()

    passed = (count_mismatch == 0) and (fare_mismatch == 0) and (miles_mismatch == 0) and (distinct_pu_mismatch == 0) and (len(merged) == len(df_duck))

    summary = {
        "status": "PASS" if passed else "FAIL",
        "total_borough_pairs": len(merged),
        "total_trips_spark": int(merged["trip_count"].sum()),
        "total_trips_duck": int(merged["trip_count_duck"].sum()),
        "count_mismatches": int(count_mismatch),
        "fare_mismatches": int(fare_mismatch),
        "miles_mismatches": int(miles_mismatch),
        "distinct_pu_mismatches": int(distinct_pu_mismatch),
        "max_fare_diff": float(merged["fare_diff"].max()),
        "max_miles_diff": float(merged["miles_diff"].max()),
    }

    return passed, summary, merged

# ==============================================================================
# 5. APPLICATION-LEVEL VALIDATION
# ==============================================================================

def validate_results_against_baseline(
    df_baseline_res: DataFrame,
    df_method_res: DataFrame,
    group_cols: List[str],
    agg_cols,
    rtol: float = 1e-5,
) -> bool:
    """Compare method results against Baseline across row count, group keys, and aggregate column values."""
    if isinstance(agg_cols, str):
        agg_cols = [agg_cols]

    count_base = materialize_dataframe(df_baseline_res)
    count_method = materialize_dataframe(df_method_res)
    if count_base != count_method:
        raise ValueError(
            f"Row count mismatch! Baseline: {count_base}, Method: {count_method}"
        )

    pdf_base   = df_baseline_res.orderBy(*group_cols).toPandas()
    pdf_method = df_method_res.orderBy(*group_cols).toPandas()

    keys_base   = pdf_base[group_cols].to_numpy()
    keys_method = pdf_method[group_cols].to_numpy()
    if not np.array_equal(keys_base, keys_method):
        raise ValueError("Group keys mismatch between Baseline and Method (sau orderBy).")

    val_base   = pdf_base[agg_cols].to_numpy(dtype=float)
    val_method = pdf_method[agg_cols].to_numpy(dtype=float)

    if not np.allclose(val_base, val_method, rtol=rtol, equal_nan=True):
        abs_diff = np.abs(val_base - val_method)
        flat_idx = int(np.nanargmax(abs_diff))
        if abs_diff.ndim > 1:
            row_idx, col_idx = np.unravel_index(flat_idx, abs_diff.shape)
            worst = f"{agg_cols[col_idx]} @ row {row_idx}"
        else:
            worst = f"{agg_cols[0]} @ row {flat_idx}"
        max_diff = float(abs_diff.flat[flat_idx])
        raise ValueError(
            f"Aggregate mismatch! Max |diff| = {max_diff:.6g} at {worst}"
        )

    print(
        f">>> [VALIDATION PASSED] Khớp Baseline (rtol={rtol}, "
        f"agg_cols={agg_cols}, rows={count_base})."
    )
    return True

# ==============================================================================
# 6. BENCHMARK ORCHESTRATION — SUMMARY & CSV EXPORT
# ==============================================================================

def generate_benchmark_summary(
    df_runs_raw: pd.DataFrame,
    baseline_method_name: str,
    dataset_fingerprint: str,
    out_dir: str,
    tier_label: str,
    nb_prefix: str = "nd2",
) -> pd.DataFrame:
    """Summarize benchmark results, filtering out invalid or failed runs before calculating metrics."""
    required = {
        "method", "run_type", "wall_clock_duration_s",
        "task_skew_ratio_max", "disk_bytes_spilled",
        "memory_bytes_spilled", "shuffle_write_bytes",
    }
    missing = required - set(df_runs_raw.columns)
    if missing:
        raise ValueError(f"df_runs_raw thiếu cột: {sorted(missing)}")

    # Strict filtering: keeps ONLY successful MEASURED runs
    valid_mask = (df_runs_raw["run_type"] == "MEASURED")
    if "collector_status" in df_runs_raw.columns:
        valid_mask &= (df_runs_raw["collector_status"] == "OK")
    if "validation_status" in df_runs_raw.columns:
        valid_mask &= (df_runs_raw["validation_status"] == "PASS")

    df_valid = df_runs_raw[valid_mask].copy()
    num_dropped = len(df_runs_raw[df_runs_raw["run_type"] == "MEASURED"]) - len(df_valid)
    if num_dropped > 0:
        print(f"[CẢNH BÁO] Đã loại bỏ {num_dropped} lượt chạy MEASURED do bị LỖI/FAIL validation!")

    if df_valid.empty:
        raise RuntimeError("Không có lượt MEASURED nào hợp lệ (tất cả bị FAIL/ERROR hoặc chỉ có WARMUP).")

    base_runs = (
        df_valid.loc[df_valid["method"] == baseline_method_name, "wall_clock_duration_s"]
        .dropna()
    )
    if base_runs.empty:
        raise RuntimeError(f"Không có lượt đo hợp lệ cho Baseline ({baseline_method_name}).")
    base_median_s = float(base_runs.median())

    summary_rows = []
    for method, group in df_valid.groupby("method", sort=False):
        durations = group["wall_clock_duration_s"].dropna()
        if durations.empty:
            continue

        tsr_series = group["task_skew_ratio_max"].dropna()
        tsr_median = round(float(tsr_series.median()), 2) if not tsr_series.empty else np.nan

        psr_series = group.get("partition_skew_ratio_max", pd.Series(dtype=float)).dropna()
        psr_median = round(float(psr_series.median()), 2) if not psr_series.empty else np.nan

        summary_rows.append({
            "method": method,
            "result_tier": tier_label,
            "dataset_fingerprint": dataset_fingerprint,
            "measured_runs": len(durations),
            "e2e_duration_median_s": round(float(durations.median()), 3),
            "e2e_duration_mean_s":   round(float(durations.mean()), 3),
            "e2e_duration_min_s":    round(float(durations.min()), 3),
            "e2e_duration_max_s":    round(float(durations.max()), 3),
            "speedup_vs_baseline":   round(base_median_s / float(durations.median()), 2),
            "tsr_target_stage_median": tsr_median,
            "psr_target_stage_median": psr_median,
            "disk_spill_median_mb":   round(float(group["disk_bytes_spilled"].median())  / (1024 * 1024), 2),
            "memory_spill_median_mb": round(float(group["memory_bytes_spilled"].median()) / (1024 * 1024), 2),
            "shuffle_write_median_mb": round(float(group["shuffle_write_bytes"].median()) / (1024 * 1024), 2),
        })

    df_summary = pd.DataFrame(summary_rows)

    df_summary["_is_base"] = (df_summary["method"] == baseline_method_name).astype(int)
    df_summary = (
        df_summary
        .sort_values(["_is_base", "e2e_duration_median_s"], ascending=[False, True])
        .drop(columns="_is_base")
        .reset_index(drop=True)
    )

    os.makedirs(out_dir, exist_ok=True)
    run_id = "run_" + time.strftime("%Y%m%d_%H%M%S")

    raw_path           = os.path.join(out_dir, f"benchmark_{nb_prefix}_runs_raw_{tier_label}_{run_id}.csv")
    summary_path       = os.path.join(out_dir, f"benchmark_{nb_prefix}_summary_{tier_label}_{run_id}.csv")
    latest_summary_path = os.path.join(out_dir, f"benchmark_{nb_prefix}_summary_{tier_label}_latest.csv")

    write_csv_preserving_history(df_runs_raw, raw_path)
    write_csv_preserving_history(df_summary, summary_path)
    write_csv_preserving_history(df_summary, latest_summary_path)

    print(f">>> ĐÃ XUẤT KẾT QUẢ BENCHMARK ({run_id}):")
    print(f"    + Raw File     : {raw_path}")
    print(f"    + Summary File : {summary_path}")
    print(f"    + Latest Link  : {latest_summary_path}\n")

    return df_summary

# ==============================================================================
# 7. BENCHMARK ORCHESTRATION — WARM-UP + RANDOMIZED MEASURED RUNS
# ==============================================================================

def run_benchmark_matrix(
    spark: SparkSession,
    methods: Dict[str, Callable[[DataFrame], DataFrame]],
    df_input: DataFrame,
    df_baseline_result: DataFrame,
    group_cols: List[str],
    agg_cols,
    runs_per_method: int = 5,
    warmup_runs: int = 1,
    out_dir: str = ".",
    dataset_fingerprint: str = "fhvhv_2026_01",
    tier_label: str = "FULL",
    nb_prefix: str = "nd2",
    baseline_method_name: Optional[str] = None,
) -> pd.DataFrame:
    """Orchestrate benchmark matrix: warm-up, randomized measured iterations, and full materialization validation."""
    verify_spark_master_connection(spark)
    collector = SparkMetricsCollector(spark)
    df_runs: List[Dict[str, Any]] = []

    df_input = df_input.cache()
    df_baseline_result = df_baseline_result.cache()
    materialize_dataframe(df_input)
    materialize_dataframe(df_baseline_result)

    baseline_name = baseline_method_name or next(iter(methods))

    # 1) Warm-up
    for w in range(warmup_runs):
        for method in methods:
            gid = f"warmup-{method}-{w}"
            collector.start_run(gid, f"warmup {method} #{w}")
            
            df_warmup = methods[method](df_input)
            materialize_dataframe(df_warmup)
            
            rec = collector.end_run(gid)
            rec["method"]   = method
            rec["run_type"] = "WARMUP"
            df_runs.append(rec)

    # 2) Measured runs — randomized schedule
    schedule = [m for m in methods for _ in range(runs_per_method)]
    random.shuffle(schedule)

    for i, method in enumerate(schedule):
        gid = f"run-{method}-{i}"
        collector.start_run(gid, f"{method} run #{i}")

        df_result = methods[method](df_input)
        materialize_dataframe(df_result)

        rec = collector.end_run(gid)

        try:
            validate_results_against_baseline(
                df_baseline_result, df_result,
                group_cols=group_cols, agg_cols=agg_cols,
            )
            rec["validation_status"] = "PASS"
        except Exception as e:
            rec["validation_status"] = f"FAIL: {str(e)}"

        rec["method"]   = method
        rec["run_type"] = "MEASURED"
        df_runs.append(rec)

    df_runs_raw = pd.DataFrame(df_runs)

    # 3) Summary + CSV Export
    return generate_benchmark_summary(
        df_runs_raw=df_runs_raw,
        baseline_method_name=baseline_name,
        dataset_fingerprint=dataset_fingerprint,
        out_dir=out_dir,
        tier_label=tier_label,
        nb_prefix=nb_prefix,
    )
