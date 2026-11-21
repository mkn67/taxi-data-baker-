"""Airflow orchestration for the March 2026 ND2/ND3 benchmark (Nội dung 4).

Each runner performs one unmeasured warm-up, then three measured iterations,
collects Spark REST/Event Log telemetry, writes run-level provenance, and runs
its DuckDB correctness audit. The tasks are deliberately sequential so they do
not compete for the same Spark workers.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

ROOT = "/opt/airflow"
DATA = "/opt/spark/data"
BENCH = (
    f"{DATA}/benchmarks/nd4_airflow/2026_03/"
    "{{ dag_run.run_id | replace(':', '_') | replace('+', '_') | replace('/', '_') }}"
)
COMMON_ENV = {
    "SILVER_PATH": f"{DATA}/silver/fhvhv_2026_03_clean.parquet",
    "HOTSPOTS_PU_PATH": f"{DATA}/silver/hotspots_p95_pu_2026_03",
    "OD_MATRIX_PATH": f"{DATA}/silver/od_matrix_2026_03.parquet",
    "BENCHMARKS_DIR": BENCH,
    "SPARK_MASTER_URL": "spark://spark-master:7077",
    "SPARK_DRIVER_HOST": "airflow-standalone",
    "SPARK_UI_URL": "http://airflow-standalone:4040",
    "SPARK_EXPECTED_VERSION": "3.5.9",
    "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-amd64",
}

with DAG(
    dag_id="chapter4_nd4_march_2026_benchmark",
    description="Run March 2026 ND2 and ND3 in sequence with provenance and DuckDB validation",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "research", "retries": 0},
    tags=["chapter4", "benchmark", "march-2026"],
) as dag:
    nd2 = BashOperator(
        task_id="nd2_aggregation_full",
        bash_command=(
            f"mkdir -p '{BENCH}' && exec python "
            f"{ROOT}/scripts/chapter_03/run_benchmark_nd2.py "
            "--mode full --master spark://spark-master:7077 --partitions 200"
        ),
        env=COMMON_ENV,
        append_env=True,
        execution_timeout=timedelta(hours=3),
    )
    nd3 = BashOperator(
        task_id="nd3_join_full",
        bash_command=(
            f"mkdir -p '{BENCH}' && exec python "
            f"{ROOT}/scripts/chapter_03/run_benchmark_nd3.py "
            "--mode full --master spark://spark-master:7077 --partitions 200"
        ),
        env=COMMON_ENV,
        append_env=True,
        execution_timeout=timedelta(hours=3),
    )
    package = BashOperator(
        task_id="validate_and_package_results",
        bash_command=(
            f"exec python {ROOT}/scripts/chapter_04/build_nd4_march_artifacts.py "
            f"--airflow-run-id '{{{{ dag_run.run_id }}}}' --run-dir '{BENCH}'"
        ),
        env=COMMON_ENV,
        append_env=True,
        execution_timeout=timedelta(minutes=30),
    )
    nd2 >> nd3 >> package
