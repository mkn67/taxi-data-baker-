#!/usr/bin/env python3
"""Validate and package the completed March 2026 Airflow benchmark evidence."""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.ticker import FuncFormatter
import shutil

ROOT = Path(__file__).resolve().parents[2]
ASSETS = ROOT / "docs/assets/chapter4"
RUN_DIR = ROOT / "data/benchmarks/nd4_airflow/2026_03"
METHODS = {
    "ND2": ["Baseline_GroupBy", "Selective_Salting_S8", "Uniform_Salting_S8"],
    "ND3": ["Baseline_SMJ", "Spark_AQE_Skew_Join", "Salting_Join_S8"],
}
LABELS = {
    "Baseline_GroupBy": "Baseline groupBy",
    "Selective_Salting_S8": "Selective S=8",
    "Uniform_Salting_S8": "Uniform S=8",
    "Baseline_SMJ": "Baseline SMJ",
    "Spark_AQE_Skew_Join": "AQE Skew Join",
    "Salting_Join_S8": "Salting Join S=8",
}
COLORS = {
    "Baseline_GroupBy": "#4e79a7",
    "Selective_Salting_S8": "#f28e2b",
    "Uniform_Salting_S8": "#9c89b8",
    "Baseline_SMJ": "#4e79a7",
    "Spark_AQE_Skew_Join": "#59a14f",
    "Salting_Join_S8": "#e15759",
}
def format_vietnamese_number(value: float, decimals: int = 2) -> str:
    """Format numbers with dot thousands separators and comma decimals."""
    return f"{value:,.{decimals}f}".replace(",", "§").replace(".", ",").replace("§", ".")


METRIC_COLUMNS = [
    "wall_clock_duration_s", "task_skew_ratio_max", "partition_skew_ratio_max",
    "shuffle_read_bytes", "shuffle_write_bytes", "memory_bytes_spilled", "disk_bytes_spilled",
]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def validate_run(df: pd.DataFrame, workload: str) -> None:
    expected = set(METHODS[workload])
    required = {
        "method", "iteration", "collector_status", "validation_status", "result_tier",
        "benchmark_mode", "spark_version", "application_id", "worker_evidence",
        "run_timestamp_utc", "trip_dataset_manifest_sha256", "dataset_manifest_sha256",
    }
    if workload == "ND2":
        required.remove("trip_dataset_manifest_sha256")
    else:
        required.remove("dataset_manifest_sha256")
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{workload} fields missing: {sorted(missing)}")
    if len(df) != 9 or set(df.method) != expected:
        raise ValueError(f"{workload}: expected 9 rows across the three methods")
    if not df.collector_status.eq("OK").all() or not df.validation_status.eq("PASS").all():
        raise ValueError(f"{workload}: collector or correctness audit did not pass for every run")
    if set(df.result_tier) != {"official_standalone"} or set(df.benchmark_mode) != {"full"}:
        raise ValueError(f"{workload}: results are not full Standalone tier")
    if set(df.spark_version.astype(str)) != {"3.5.9"}:
        raise ValueError(f"{workload}: Spark version is not consistently 3.5.9")
    if df.application_id.nunique() != 1:
        raise ValueError(f"{workload}: measurements span more than one Spark application")
    for method in expected:
        group = df[df.method.eq(method)]
        if len(group) != 3 or set(group.iteration.astype(int)) != {1, 2, 3}:
            raise ValueError(f"{workload}/{method}: expected iterations 1, 2, and 3")
        for raw in group.worker_evidence:
            evidence = json.loads(raw)
            if evidence.get("executor_count_active") != 2 or len(evidence.get("executor_hosts", [])) != 2:
                raise ValueError(f"{workload}/{method}: two-worker evidence is missing")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--airflow-run-id", required=True)
    parser.add_argument("--run-dir", type=Path, default=RUN_DIR)
    parser.add_argument("--publish-figure", action="store_true",
                        help="Copy this run's figure into the chapter draft asset path")
    parser.add_argument("--run-date", default=datetime.now(timezone.utc).date().isoformat())
    parser.add_argument("--nd3-no-warmup", action="store_true",
                        help="Document legacy ND3 runs whose warm-up built a plan but ran no Spark action")
    args = parser.parse_args()
    run_dir = args.run_dir.resolve()
    run_dir.mkdir(parents=True, exist_ok=True)

    source_files = {
        "ND2": run_dir / "benchmark_nd2_aggregation_runs_official_standalone.csv",
        "ND3": run_dir / "benchmark_nd3_join_runs_official_standalone.csv",
    }
    frames = {key: pd.read_csv(path) for key, path in source_files.items()}
    for key, frame in frames.items():
        validate_run(frame, key)
    input_manifests = {
        "ND2": set(frames["ND2"]["dataset_manifest_sha256"].dropna().astype(str)),
        "ND3": set(frames["ND3"]["trip_dataset_manifest_sha256"].dropna().astype(str)),
    }
    if len(input_manifests["ND2"]) != 1 or input_manifests["ND2"] != input_manifests["ND3"]:
        raise ValueError(f"ND2 and ND3 must use the same single Silver manifest: {input_manifests}")

    summary_rows = []
    app_ids, manifests, workers = {}, {}, {}
    for workload, frame in frames.items():
        app_ids[workload] = frame.application_id.iloc[0]
        manifest_col = "dataset_manifest_sha256" if workload == "ND2" else "trip_dataset_manifest_sha256"
        manifests[workload] = sorted(frame[manifest_col].dropna().astype(str).unique().tolist())
        workers[workload] = sorted({
            host for raw in frame.worker_evidence for host in json.loads(raw).get("executor_hosts", [])
        })
        for method in METHODS[workload]:
            group = frame[frame.method.eq(method)]
            row = {"workload": workload, "method": method, "valid_iterations": len(group)}
            for metric in METRIC_COLUMNS:
                values = group[metric]
                row[f"{metric}_median"] = values.median()
                row[f"{metric}_min"] = values.min()
                row[f"{metric}_max"] = values.max()
            row["application_id"] = app_ids[workload]
            row["dataset_manifest_sha256"] = manifests[workload][0]
            row["correctness"] = "PASS"
            row["collector_status"] = "OK"
            summary_rows.append(row)

    summary_path = run_dir / "nd4_march_2026_summary.csv"
    summary = pd.DataFrame(summary_rows)
    summary.to_csv(summary_path, index=False)

    figure_path = run_dir / "nd4_march_2026_wall_time.png"
    fig, axes = plt.subplots(1, 2, figsize=(12, 5.2))
    for ax, workload in zip(axes, ("ND2", "ND3")):
        part = summary[summary.workload.eq(workload)].set_index("method").loc[METHODS[workload]]
        med = part["wall_clock_duration_s_median"].to_numpy(float)
        low = med - part["wall_clock_duration_s_min"].to_numpy(float)
        high = part["wall_clock_duration_s_max"].to_numpy(float) - med
        ax.bar(range(len(part)), med, color=[COLORS[x] for x in part.index],
               yerr=np.vstack([low, high]), capsize=4, edgecolor="#333333", linewidth=.5)
        ax.set_xticks(range(len(part)), [LABELS[x] for x in part.index], rotation=18, ha="right")
        ax.set_ylabel("Thời gian thực thi (giây)")
        ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: format_vietnamese_number(x)))
        ax.set_title(workload)
        ax.grid(axis="y", alpha=.25)
        ax.set_axisbelow(True)
        for i, value in enumerate(med):
            ax.text(i, value, format_vietnamese_number(value), ha="center", va="bottom", fontsize=8)
    fig.suptitle("Kết quả ND2 và ND3 trên dữ liệu tháng 03/2026 — trung vị và khoảng nhỏ nhất–lớn nhất của 3 lượt đo")
    warmup_label = ("ND2 có warm-up; ND3 chưa có action warm-up; Spark Standalone 3.5.9."
                    if args.nd3_no_warmup else
                    "ND2 và ND3 có action warm-up; Spark Standalone 3.5.9.")
    fig.text(.5, .01, warmup_label,
             ha="center", fontsize=8)
    fig.tight_layout(rect=[0, .04, 1, .93])
    fig.savefig(figure_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    if args.publish_figure:
        ASSETS.mkdir(parents=True, exist_ok=True)
        shutil.copy2(figure_path, ASSETS / "figure_4_4_nd4_airflow.png")

    run_manifest = {
        "dag_id": "chapter4_nd4_march_2026_benchmark",
        "dag_run_id": args.airflow_run_id,
        "run_date": args.run_date,
        "benchmark_input_month": "2026-03",
        "result_tier": "official_standalone",
        "benchmark_mode": "full",
        "warmups_per_method": {"ND2": 1, "ND3": 0 if args.nd3_no_warmup else 1},
        "measured_iterations_per_method": 3,
        "spark_version": "3.5.9",
        "spark_master": "spark://spark-master:7077",
        "shuffle_partitions": 200,
        "worker_hosts": workers,
        "application_ids": app_ids,
        "dataset_manifest_sha256": manifests,
        "collector_status": "OK for all 18 run records",
        "duckdb_validation": "PASS for all 18 run records",
        "artifacts": {
            path.name: {"bytes": path.stat().st_size, "sha256": sha256(path)}
            for path in [*source_files.values(), summary_path, figure_path]
        },
    }
    manifest_path = run_dir / "nd4_march_2026_airflow_provenance.json"
    manifest_path.write_text(json.dumps(run_manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Validated 18 records: 9 ND2 + 9 ND3; collector OK; DuckDB PASS.")
    print(f"Summary: {summary_path}")
    print(f"Figure: {figure_path}")
    print(f"Airflow provenance: {manifest_path}")


if __name__ == "__main__":
    main()
