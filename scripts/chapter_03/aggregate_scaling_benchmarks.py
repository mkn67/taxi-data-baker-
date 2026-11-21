#!/usr/bin/env python3
"""Summarize official monthly ND2/ND3 runs directly from run-level CSVs.

Each month is an independent Silver input. The figure is descriptive and uses
median with min-max range for the three measured runs of each method.
"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
BENCH = ROOT / "data/benchmarks"
OUT_CSV = BENCH / "scaling/monthly_benchmark_summary.csv"
OUT_PNG = BENCH / "figure_3_5_benchmark_scaling.png"
MIB = 1024 * 1024
METHODS = {
    "ND2": ["Baseline_GroupBy", "Selective_Salting_S8", "Uniform_Salting_S8"],
    "ND3": ["Baseline_SMJ", "Spark_AQE_Skew_Join", "Salting_Join_S8"],
}


def run_file(month: int, workload: str) -> Path:
    stem = "benchmark_nd2_aggregation" if workload == "ND2" else "benchmark_nd3_join"
    folder = BENCH if month == 1 else BENCH / "scaling" / f"2026_{month:02d}"
    return folder / f"{stem}_runs_official_standalone.csv"


def validate(df: pd.DataFrame, month: int, workload: str) -> None:
    required = {
        "method", "iteration", "collector_status", "validation_status",
        "result_tier", "benchmark_mode", "spark_version", "worker_evidence",
        "application_id", "run_timestamp_utc", "wall_clock_duration_s",
        "task_skew_ratio_max", "partition_skew_ratio_max",
        "shuffle_read_bytes", "shuffle_write_bytes", "memory_bytes_spilled",
        "disk_bytes_spilled",
    }
    if required - set(df.columns):
        raise ValueError(f"{workload} 2026-{month:02}: missing fields {sorted(required - set(df.columns))}")
    if len(df) != 9 or set(df.method) != set(METHODS[workload]):
        raise ValueError(f"{workload} 2026-{month:02}: expected 9 rows across three methods")
    if not df.collector_status.eq("OK").all() or not df.validation_status.eq("PASS").all():
        raise ValueError(f"{workload} 2026-{month:02}: collector or correctness validation failed")
    if set(df.result_tier.dropna()) != {"official_standalone"} or set(df.benchmark_mode.dropna()) != {"full"}:
        raise ValueError(f"{workload} 2026-{month:02}: non-official/non-full row found")
    if set(df.spark_version.dropna().astype(str)) != {"3.5.9"}:
        raise ValueError(f"{workload} 2026-{month:02}: Spark version is not consistently 3.5.9")
    if df.application_id.nunique() != 1:
        raise ValueError(f"{workload} 2026-{month:02}: rows span multiple applications")
    for method in METHODS[workload]:
        group = df[df.method.eq(method)]
        if len(group) != 3 or set(group.iteration.astype(int)) != {1, 2, 3}:
            raise ValueError(f"{workload} 2026-{month:02} {method}: expected three measured iterations")
        for raw in group.worker_evidence:
            evidence = json.loads(raw) if isinstance(raw, str) else raw
            if evidence.get("executor_count_active") != 2:
                raise ValueError(f"{workload} 2026-{month:02}: missing two-worker evidence")


def main() -> None:
    raw = pd.read_csv(BENCH / "figure_3_4_data_scale.csv", dtype={"month": str}).set_index("month")
    rows: list[dict] = []
    for month in range(1, 6):
        mm = f"{month:02d}"
        silver_path = ROOT / "data/silver" / f"fhvhv_2026_{mm}_clean.parquet"
        silver_rows = int(duckdb.execute(
            "SELECT COUNT(*) FROM read_parquet(?)", [str(silver_path / "*.parquet")]
        ).fetchone()[0])
        silver_bytes = sum(f.stat().st_size for f in silver_path.glob("*.parquet"))
        for workload in ("ND2", "ND3"):
            path = run_file(month, workload)
            runs = pd.read_csv(path)
            validate(runs, month, workload)
            manifest_col = "dataset_manifest_sha256" if "dataset_manifest_sha256" in runs else "trip_dataset_manifest_sha256"
            if manifest_col not in runs:
                raise ValueError(f"{workload} 2026-{month:02}: manifest field missing")
            for method in METHODS[workload]:
                group = runs[runs.method.eq(method)]
                worker_hosts = sorted({
                    host for item in group.worker_evidence
                    for host in json.loads(item).get("executor_hosts", [])
                })
                rows.append({
                    "month": f"2026-{mm}",
                    "benchmark": workload,
                    "method": method,
                    "raw_trip_rows": int(raw.loc[mm, "rows"]),
                    "raw_parquet_mb": float(raw.loc[mm, "size_mb"]),
                    "silver_trip_rows": silver_rows,
                    "silver_parquet_mb": silver_bytes / 1_000_000,
                    "valid_iterations": len(group),
                    "median_wall_time_s": group.wall_clock_duration_s.median(),
                    "mean_wall_time_s": group.wall_clock_duration_s.mean(),
                    "std_wall_time_s": group.wall_clock_duration_s.std(ddof=1),
                    "min_wall_time_s": group.wall_clock_duration_s.min(),
                    "max_wall_time_s": group.wall_clock_duration_s.max(),
                    "median_tsr": group.task_skew_ratio_max.median(),
                    "median_psr": group.partition_skew_ratio_max.median(),
                    "median_shuffle_read_mb": group.shuffle_read_bytes.median() / 1_000_000,
                    "median_shuffle_write_mb": group.shuffle_write_bytes.median() / 1_000_000,
                    "median_memory_spill_mb": group.memory_bytes_spilled.median() / 1_000_000,
                    "median_disk_spill_mb": group.disk_bytes_spilled.median() / 1_000_000,
                    "correctness": "PASS",
                    "collector_status": "OK",
                    "worker_hosts": ";".join(worker_hosts),
                    "application_id": group.application_id.iloc[0],
                    "run_date_utc": str(group.run_timestamp_utc.min())[:10],
                    "silver_manifest_sha256": group[manifest_col].iloc[0],
                    "source_run_csv": str(path.relative_to(ROOT)),
                })

    result = pd.DataFrame(rows).sort_values(["benchmark", "month", "method"])
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(OUT_CSV, index=False)

    fig, axes = plt.subplots(1, 2, figsize=(14, 6.5), sharex=True)
    months = [f"2026-{m:02d}" for m in range(1, 6)]
    for ax, workload in zip(axes, ("ND2", "ND3")):
        subset = result[result.benchmark.eq(workload)]
        for method in METHODS[workload]:
            group = subset[subset.method.eq(method)].set_index("month").reindex(months)
            median = group.median_wall_time_s.to_numpy(float)
            low = median - group.min_wall_time_s.to_numpy(float)
            high = group.max_wall_time_s.to_numpy(float) - median
            ax.errorbar(months, median, yerr=np.vstack([low, high]), marker="o",
                        capsize=3, linewidth=1.8, label=method)
        ax.set_title(f"{workload}: median và min–max (3 lượt)")
        ax.set_ylabel("Thời gian (giây)")
        ax.set_xlabel("Tháng Silver độc lập")
        ax.grid(axis="y", alpha=0.25)
        ax.tick_params(axis="x", rotation=30)
        ax.legend(fontsize=8)
    fig.suptitle("Hình 3.5 — Benchmark ND2/ND3 trên các tập tháng độc lập (01–05/2026)", y=1.02)
    fig.text(.5, .005,
             "Tháng 01 chạy 03/11/2026; tháng 02–05 chạy 30/10/2026. Hai Worker, Spark 3.5.9; không phải dữ liệu tích lũy.",
             ha="center", fontsize=9)
    fig.tight_layout(rect=(0, 0.04, 1, 0.98))
    fig.savefig(OUT_PNG, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {OUT_CSV} ({len(result)} rows from run-level CSVs)")
    print(f"Saved {OUT_PNG}")


if __name__ == "__main__":
    main()
