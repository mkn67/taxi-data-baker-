#!/usr/bin/env python3
"""Combine official monthly ND2/ND3 runs and plot Chapter 3 scaling results."""
from __future__ import annotations

import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import duckdb

ROOT = Path(__file__).resolve().parents[2]
BENCH = ROOT / "data/benchmarks"
OUT_CSV = BENCH / "scaling/monthly_benchmark_summary.csv"
OUT_PNG = BENCH / "figure_3_5_benchmark_scaling.png"


def run_files(month: int, benchmark: str) -> tuple[Path, Path]:
    if month == 1:
        prefix = "benchmark_nd2_aggregation" if benchmark == "ND2" else "benchmark_nd3_join"
        runs = BENCH / f"{prefix}_runs_official_standalone.csv"
        summary = BENCH / f"{prefix}_summary_official_standalone.csv"
    else:
        base = BENCH / "scaling" / f"2026_{month:02d}"
        prefix = "benchmark_nd2_aggregation" if benchmark == "ND2" else "benchmark_nd3_join"
        runs = base / f"{prefix}_runs_official_standalone.csv"
        summary = base / f"{prefix}_summary_official_standalone.csv"
    return runs, summary


def main() -> None:
    raw = pd.read_csv(BENCH / "figure_3_4_data_scale.csv", dtype={"month": str}).set_index("month")
    rows: list[dict] = []
    for month in range(1, 6):
        silver_path = ROOT / f"data/silver/fhvhv_2026_{month:02d}_clean.parquet"
        silver_rows = int(duckdb.execute(
            "SELECT COUNT(*) FROM read_parquet(?)", [str(silver_path / "*.parquet")]
        ).fetchone()[0])
        silver_bytes = sum(f.stat().st_size for f in silver_path.glob("*.parquet"))
        for benchmark in ("ND2", "ND3"):
            runs_path, summary_path = run_files(month, benchmark)
            runs = pd.read_csv(runs_path)
            summary = pd.read_csv(summary_path)
            if len(runs) != 9 or not runs["collector_status"].eq("OK").all():
                raise ValueError(f"{benchmark} 2026-{month:02d} must have 9 collector-OK runs: {runs_path}")
            if not runs["validation_status"].eq("PASS").all():
                raise ValueError(f"{benchmark} 2026-{month:02d} has failed correctness validation: {runs_path}")
            if any(len(json.loads(value).get("executor_hosts", [])) < 2 for value in runs["worker_evidence"]):
                raise ValueError(f"{benchmark} 2026-{month:02d} is missing two-worker evidence: {runs_path}")
            if runs["application_id"].nunique() != 1:
                raise ValueError(f"{benchmark} 2026-{month:02d} contains multiple application IDs")
            for item in summary.to_dict("records"):
                method = item.get("Phương án")
                method_runs = runs[runs["method"] == method]
                if len(method_runs) != 3:
                    raise ValueError(f"Expected three valid runs: {benchmark}/{month}/{method}")
                rows.append({
                    "month": f"2026-{month:02d}",
                    "benchmark": benchmark,
                    "method": method,
                    "raw_trip_rows": int(raw.loc[f"{month:02d}", "rows"]),
                    "raw_parquet_mb": float(raw.loc[f"{month:02d}", "size_mb"]),
                    "silver_trip_rows": silver_rows,
                    "silver_parquet_mb": round(silver_bytes / (1024 * 1024), 2),
                    "valid_iterations": len(method_runs),
                    "median_wall_time_s": float(item["Thời gian Median (s)"]),
                    "mean_wall_time_s": float(item["Thời gian Mean (s)"]),
                    "std_wall_time_s": float(item["Thời gian Std (s)"]),
                    "median_tsr": pd.to_numeric(item.get("TSR Max Median"), errors="coerce"),
                    "median_psr": pd.to_numeric(item.get("PSR Median"), errors="coerce"),
                    "median_shuffle_read_mb": item.get("Shuffle Read Median (MB)"),
                    "median_shuffle_write_mb": item.get("Shuffle Write Median (MB)"),
                    "median_memory_spill_mb": item.get("Memory Spill Median (MB)"),
                    "median_disk_spill_mb": item.get("Disk Spill Median (MB)"),
                    "correctness": "PASS",
                    "collector_status": "OK",
                    "worker_hosts": ";".join(sorted(set(h for value in method_runs["worker_evidence"]
                                              for h in json.loads(value).get("executor_hosts", [])))),
                    "application_id": method_runs["application_id"].iloc[0],
                    "silver_manifest_sha256": method_runs.get("dataset_manifest_sha256",
                                                    method_runs.get("trip_dataset_manifest_sha256")).iloc[0],
                })

    result = pd.DataFrame(rows).sort_values(["benchmark", "month", "method"])
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(OUT_CSV, index=False)

    fig, axes = plt.subplots(1, 2, figsize=(14, 6.5), sharex=True)
    method_order = {
        "ND2": ["Baseline_GroupBy", "Selective_Salting_S8", "Uniform_Salting_S8"],
        "ND3": ["Baseline_SMJ", "Spark_AQE_Skew_Join", "Salting_Join_S8"],
    }
    months = [f"2026-{m:02d}" for m in range(1, 6)]
    for ax, benchmark in zip(axes, ("ND2", "ND3")):
        subset = result[result["benchmark"] == benchmark]
        for method in method_order[benchmark]:
            values = subset[subset["method"] == method].set_index("month").reindex(months)
            ax.errorbar(months, values["median_wall_time_s"], yerr=values["std_wall_time_s"],
                        marker="o", capsize=3, linewidth=1.8, label=method)
        ax.set_title(f"{benchmark}: median wall time ± SD (3 lượt)")
        ax.set_ylabel("Thời gian (giây)")
        ax.set_xlabel("Tệp Silver của tháng độc lập")
        ax.grid(axis="y", alpha=0.25)
        ax.tick_params(axis="x", rotation=30)
        ax.legend(fontsize=8)
    fig.suptitle("Hình 3.5 — Benchmark ND2/ND3 trên các tập tháng độc lập (01–05/2026)", y=1.02)
    fig.text(0.5, 0.005,
             "Mỗi tháng là một tập độc lập; Spark 3.5.9 Standalone, hai Worker, 200 shuffle partitions; ba lượt/phương án.",
             ha="center", fontsize=9)
    fig.tight_layout(rect=(0, 0.04, 1, 0.98))
    fig.savefig(OUT_PNG, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {OUT_CSV} ({len(result)} rows)")
    print(f"Saved {OUT_PNG}")


if __name__ == "__main__":
    main()
