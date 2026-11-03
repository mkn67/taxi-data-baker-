#!/usr/bin/env python3
"""Summarize raw monthly Parquet scale and create Chapter 3 Figure 3.4."""
from __future__ import annotations

import glob
import os
from pathlib import Path

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
RAW_PATTERN = os.environ.get(
    "SCALING_RAW_PATTERN", str(ROOT / "data/raw/fhvhv_tripdata_2026-0*.parquet")
)
FIGURE_PATH = Path(os.environ.get(
    "SCALING_FIGURE_PATH", ROOT / "data/benchmarks/figure_3_4_data_scale.png"
))
TABLE_PATH = Path(os.environ.get(
    "SCALING_TABLE_PATH", ROOT / "data/benchmarks/figure_3_4_data_scale.csv"
))


def collect_monthly_stats(pattern: str) -> pd.DataFrame:
    files = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No raw Parquet files match: {pattern}")

    rows = []
    for file_path in files:
        path = Path(file_path)
        month = path.stem.rsplit("-", 1)[-1]
        row_count = duckdb.execute(
            "SELECT COUNT(*) FROM read_parquet(?)", [str(path)]
        ).fetchone()[0]
        size_bytes = path.stat().st_size
        rows.append({
            "month": month,
            "file": path.name,
            "rows": int(row_count),
            "size_bytes": int(size_bytes),
            "size_mb": size_bytes / (1024 * 1024),
        })
    return pd.DataFrame(rows).sort_values("month").reset_index(drop=True)


def plot_figure(stats: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig, ax_rows = plt.subplots(figsize=(10, 5.8))
    ax_size = ax_rows.twinx()
    x = range(len(stats))

    bars = ax_rows.bar(x, stats["rows"] / 1_000_000, width=0.58,
                       color="#3478a8", alpha=0.88, label="Số bản ghi raw")
    line, = ax_size.plot(x, stats["size_mb"], color="#e07a28", marker="o",
                         linewidth=2.2, label="Dung lượng Parquet")

    ax_rows.set_xticks(list(x), [f"Tháng {month}" for month in stats["month"]])
    ax_rows.set_ylabel("Số bản ghi (triệu)")
    ax_size.set_ylabel("Dung lượng tệp Parquet (MiB)")
    ax_rows.set_title("Hình 3.4 — Quy mô dữ liệu HVFHV raw theo tháng")
    ax_rows.grid(axis="y", alpha=0.25)
    ax_rows.bar_label(bars, labels=[f"{v:.2f}" for v in stats["rows"] / 1_000_000],
                      padding=3, fontsize=8)
    ax_rows.legend([bars, line], ["Số bản ghi raw (triệu)", "Dung lượng Parquet (MiB)"],
                   loc="upper left", frameon=True)
    fig.text(0.5, 0.01,
             "Mỗi điểm là một tệp tháng riêng; đây không phải benchmark ND2/ND3 trên dữ liệu tích lũy.",
             ha="center", fontsize=9)
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    stats = collect_monthly_stats(RAW_PATTERN)
    TABLE_PATH.parent.mkdir(parents=True, exist_ok=True)
    stats.to_csv(TABLE_PATH, index=False, encoding="utf-8")
    plot_figure(stats, FIGURE_PATH)

    total_rows = int(stats["rows"].sum())
    total_mb = float(stats["size_mb"].sum())
    print("| Tháng | Tệp Parquet | Dung lượng | Số bản ghi thô |")
    print("| :---: | :--- | :---: | :---: |")
    for row in stats.itertuples(index=False):
        print(f"| {row.month} | {row.file} | {row.size_mb:.1f} MiB | {row.rows:,} |")
    print(f"| **Tổng** | **{len(stats)} tháng** | **{total_mb:.1f} MiB** | **{total_rows:,}** |")
    print(f"Đã lưu hình: {FIGURE_PATH}")
    print(f"Đã lưu bảng dữ liệu: {TABLE_PATH}")


if __name__ == "__main__":
    main()
