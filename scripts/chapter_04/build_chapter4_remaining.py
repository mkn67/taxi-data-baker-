#!/usr/bin/env python3
"""Build Chapter 4 monthly stability and spatial result artifacts from saved evidence.

This script does not launch Spark. It validates existing official run CSVs and
reads the saved spatial Gold CSV, then writes derived tables and figures.
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
from matplotlib.ticker import FuncFormatter

ROOT = Path(__file__).resolve().parents[2]
BENCH = ROOT / "data/benchmarks"
EVIDENCE = BENCH / "chapter4_evidence"
ASSETS = ROOT / "docs/assets/chapter4"
MIB = 1024 * 1024
METHODS = {
    "ND2": ["Baseline_GroupBy", "Selective_Salting_S8", "Uniform_Salting_S8"],
    "ND3": ["Baseline_SMJ", "Spark_AQE_Skew_Join", "Salting_Join_S8"],
}
LABELS = {
    "Baseline_GroupBy": "Baseline groupBy",
    "Selective_Salting_S8": "Selective S=8",
    "Uniform_Salting_S8": "Uniform S=8",
    "Baseline_SMJ": "Baseline SMJ",
    "Spark_AQE_Skew_Join": "AQE",
    "Salting_Join_S8": "Salting Join S=8",
}
def format_vietnamese_number(value: float, decimals: int = 2) -> str:
    """Format numbers with dot thousands separators and comma decimals."""
    return f"{value:,.{decimals}f}".replace(",", "§").replace(".", ",").replace("§", ".")


COLORS = {
    "Baseline_GroupBy": "#4e79a7",
    "Selective_Salting_S8": "#f28e2b",
    "Uniform_Salting_S8": "#9c89b8",
    "Baseline_SMJ": "#4e79a7",
    "Spark_AQE_Skew_Join": "#59a14f",
    "Salting_Join_S8": "#e15759",
}


def _run_file(month: int, workload: str) -> Path:
    stem = "benchmark_nd2_aggregation" if workload == "ND2" else "benchmark_nd3_join"
    folder = BENCH if month == 1 else BENCH / "scaling" / f"2026_{month:02d}"
    return folder / f"{stem}_runs_official_standalone.csv"


def _validate(df: pd.DataFrame, month: int, workload: str) -> None:
    methods = METHODS[workload]
    required = {
        "method", "iteration", "collector_status", "validation_status",
        "result_tier", "benchmark_mode", "spark_version", "worker_evidence",
        "application_id", "run_timestamp_utc", "wall_clock_duration_s",
        "task_skew_ratio_max", "partition_skew_ratio_max",
        "shuffle_read_bytes", "shuffle_write_bytes", "memory_bytes_spilled",
        "disk_bytes_spilled",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{workload} {month:02}: missing fields: {sorted(missing)}")
    if len(df) != 9 or set(df.method) != set(methods):
        raise ValueError(f"{workload} {month:02}: expected 9 rows across {methods}")
    if not df.collector_status.eq("OK").all() or not df.validation_status.eq("PASS").all():
        raise ValueError(f"{workload} {month:02}: collector or correctness validation failed")
    if set(df.result_tier.dropna()) != {"official_standalone"} or set(df.benchmark_mode.dropna()) != {"full"}:
        raise ValueError(f"{workload} {month:02}: non-official or non-full run found")
    if set(df.spark_version.dropna().astype(str)) != {"3.5.9"}:
        raise ValueError(f"{workload} {month:02}: Spark version mismatch")
    if df.application_id.nunique() != 1:
        raise ValueError(f"{workload} {month:02}: rows span multiple applications")
    for method in methods:
        g = df[df.method.eq(method)]
        if len(g) != 3 or set(g.iteration.astype(int)) != {1, 2, 3}:
            raise ValueError(f"{workload} {month:02} {method}: expected 3 measured iterations")
        for raw in g.worker_evidence:
            evidence = json.loads(raw) if isinstance(raw, str) else raw
            if evidence.get("executor_count_active") != 2:
                raise ValueError(f"{workload} {month:02} {method}: two-worker evidence missing")


def load_monthly() -> tuple[pd.DataFrame, pd.DataFrame]:
    raw_stats = pd.read_csv(BENCH / "figure_3_4_data_scale.csv", dtype={"month": str}).set_index("month")
    rows: list[dict] = []
    for month in range(1, 6):
        mm = f"{month:02d}"
        silver = ROOT / "data/silver" / f"fhvhv_2026_{mm}_clean.parquet"
        if not silver.is_dir():
            raise FileNotFoundError(silver)
        silver_rows = int(duckdb.execute(
            "SELECT COUNT(*) FROM read_parquet(?)", [str(silver / "*.parquet")]
        ).fetchone()[0])
        silver_bytes = sum(f.stat().st_size for f in silver.glob("*.parquet"))
        for workload in ("ND2", "ND3"):
            path = _run_file(month, workload)
            df = pd.read_csv(path)
            _validate(df, month, workload)
            manifest_column = (
                "dataset_manifest_sha256" if "dataset_manifest_sha256" in df.columns
                else "trip_dataset_manifest_sha256"
            )
            if manifest_column not in df.columns:
                raise ValueError(f"{workload} {month:02}: dataset manifest field missing")
            for method in METHODS[workload]:
                g = df[df.method.eq(method)].copy()
                rows.append({
                    "month": f"2026-{mm}",
                    "workload": workload,
                    "method": method,
                    "raw_trip_rows": int(raw_stats.loc[mm, "rows"]),
                    "raw_parquet_mib": float(raw_stats.loc[mm, "size_mb"]) * 1000 / 1024,
                    "silver_trip_rows": silver_rows,
                    "silver_parquet_mib": silver_bytes / MIB,
                    "valid_iterations": len(g),
                    "wall_median_s": g.wall_clock_duration_s.median(),
                    "wall_min_s": g.wall_clock_duration_s.min(),
                    "wall_max_s": g.wall_clock_duration_s.max(),
                    "tsr_median": g.task_skew_ratio_max.median(),
                    "psr_median": g.partition_skew_ratio_max.median(),
                    "shuffle_read_mib_median": g.shuffle_read_bytes.median() / MIB,
                    "shuffle_write_mib_median": g.shuffle_write_bytes.median() / MIB,
                    "memory_spill_mib_median": g.memory_bytes_spilled.median() / MIB,
                    "disk_spill_mib_median": g.disk_bytes_spilled.median() / MIB,
                    "spark_version": g.spark_version.iloc[0],
                    "application_id": g.application_id.iloc[0],
                    "run_date_utc": str(g.run_timestamp_utc.min())[:10],
                    "silver_manifest_sha256": g[manifest_column].iloc[0],
                    "worker_hosts": ";".join(sorted({
                        host for raw in g.worker_evidence
                        for host in json.loads(raw).get("executor_hosts", [])
                    })),
                    "correctness": "PASS",
                    "collector_status": "OK",
                })
    result = pd.DataFrame(rows).sort_values(["workload", "month", "method"])
    return result, raw_stats


def plot_monthly(df: pd.DataFrame, workload: str, filename: str) -> Path:
    fig, ax = plt.subplots(figsize=(9.5, 5.5))
    months = [f"2026-{m:02d}" for m in range(1, 6)]
    sub = df[df.workload.eq(workload)]
    for method in METHODS[workload]:
        g = sub[sub.method.eq(method)].set_index("month").reindex(months)
        median = g.wall_median_s.to_numpy(float)
        low = median - g.wall_min_s.to_numpy(float)
        high = g.wall_max_s.to_numpy(float) - median
        ax.errorbar(months, median, yerr=np.vstack([low, high]), marker="o",
                    capsize=4, linewidth=2, color=COLORS[method], label=LABELS[method])
    ax.set_title(f"{workload}: thời gian giữa các tháng độc lập")
    ax.set_ylabel("Thời gian toàn phần (giây)")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: format_vietnamese_number(x)))
    ax.set_xlabel("Tháng dữ liệu Silver")
    ax.grid(axis="y", alpha=.25)
    ax.set_axisbelow(True)
    ax.legend(frameon=False)
    fig.text(.5, .015,
             "Trung vị và khoảng nhỏ nhất–lớn nhất của 3 lượt/phương án. Tháng 01 chạy 03/11/2026; tháng 02–05 chạy 30/10/2026.",
             ha="center", fontsize=8)
    fig.tight_layout(rect=[0, .05, 1, 1])
    out = ASSETS / filename
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return out


def spatial_summary() -> pd.DataFrame:
    gold_path = ROOT / "data/gold/dropoff_density_by_zone_period.csv"
    gold = pd.read_csv(gold_path)
    groups = ["SOUTH_OF_APPROX_60TH", "NORTH_OF_APPROX_60TH", "BOUNDARY_AMBIGUOUS"]
    gold = gold[gold.analytical_group.isin(groups)].copy()
    rows = []
    for (group, period), part in gold.groupby(["analytical_group", "time_period"], sort=True):
        valid_density = part.dropoff_density_trips_per_km2.dropna()
        hours = 16 if period == "Daytime" else 8
        area = part.polygon_area_km2.sum()
        trips = int(part.trip_count.sum())
        rows.append({
            "analytical_group": group,
            "time_period": period,
            "zone_count": int(part.LocationID.nunique()),
            "trip_count": trips,
            "area_km2": area,
            "pooled_density_trips_per_km2": trips / area if area else float("nan"),
            "median_zone_density_trips_per_km2": valid_density.median(),
            "analysis_days": 31,
            "median_zone_density_per_period_hour": valid_density.median() / (31 * hours),
        })
    return pd.DataFrame(rows)


def plot_spatial(summary: pd.DataFrame) -> Path:
    labels = {
        "SOUTH_OF_APPROX_60TH": "Phía Nam",
        "NORTH_OF_APPROX_60TH": "Phía Bắc",
    }
    periods = [("Daytime", 16), ("Nighttime", 8)]
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.8), sharey=True)
    colors = {"SOUTH_OF_APPROX_60TH": "#e15759", "NORTH_OF_APPROX_60TH": "#4e79a7"}
    for ax, (period, hours) in zip(axes, periods):
        g = summary[(summary.time_period.eq(period)) & summary.analytical_group.isin(labels)]
        groups = ["SOUTH_OF_APPROX_60TH", "NORTH_OF_APPROX_60TH"]
        values = [float(g.loc[g.analytical_group.eq(group), "median_zone_density_trips_per_km2"].iloc[0])
                  for group in groups]
        bars = ax.bar([labels[x] for x in groups], values,
                      color=[colors[x] for x in groups], edgecolor="#333333", linewidth=.6)
        ax.set_title(f"{period} ({hours} giờ/ngày)")
        ax.set_ylabel("Trung vị mật độ zone (chuyến/km²)")
        ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: format_vietnamese_number(x, 0)))
        ax.grid(axis="y", alpha=.25)
        ax.set_axisbelow(True)
        for bar, value in zip(bars, values):
            ax.text(bar.get_x() + bar.get_width()/2, value, format_vietnamese_number(value, 0),
                    ha="center", va="bottom", fontsize=8)
    fig.suptitle("Mật độ trả khách theo nhóm Taxi Zone — tháng 01/2026\nMô tả theo trung vị từng zone; không phải ước lượng tác động chính sách")
    fig.tight_layout(rect=[0, 0, 1, .88])
    out = ASSETS / "figure_4_7_manhattan_dropoff_density.png"
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return out


def main() -> None:
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 9})
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    ASSETS.mkdir(parents=True, exist_ok=True)
    monthly, _ = load_monthly()
    monthly.to_csv(EVIDENCE / "monthly_stability_summary.csv", index=False)
    spatial = spatial_summary()
    spatial.to_csv(EVIDENCE / "spatial_group_density_summary.csv", index=False)
    for workload, filename in [("ND2", "figure_4_5_nd2_monthly_stability.png"),
                               ("ND3", "figure_4_6_nd3_monthly_stability.png")]:
        print(plot_monthly(monthly, workload, filename))
    print(plot_spatial(spatial))
    print(EVIDENCE / "monthly_stability_summary.csv")
    print(EVIDENCE / "spatial_group_density_summary.csv")
    print(f"Validated official records: {len(monthly)}; input months: {monthly.month.nunique()}")


if __name__ == "__main__":
    main()
