#!/usr/bin/env python3
"""Create Chapter 4 January benchmark figures from official run CSVs."""
from __future__ import annotations

from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import shutil

ROOT = Path(__file__).resolve().parents[2]
BENCH = ROOT / "data/benchmarks"
ASSETS = ROOT / "docs/assets/chapter4"
MIB = 1024 * 1024


def load_summary(path: Path, methods: list[str], shuffle: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    expected = df.loc[df.method.isin(methods)].copy()
    if set(expected.method) != set(methods) or not expected.collector_status.eq("OK").all() or not expected.validation_status.eq("PASS").all():
        raise ValueError(f"Run file is incomplete or failed validation: {path}")
    if not expected.result_tier.eq("official_standalone").all():
        raise ValueError(f"Non-official run row found: {path}")
    records = []
    for method in methods:
        g = expected.loc[expected.method.eq(method)]
        if len(g) != 3:
            raise ValueError(f"Expected three measured runs for {method}; got {len(g)}")
        records.append({
            "method": method,
            "median": g.wall_clock_duration_s.median(),
            "minimum": g.wall_clock_duration_s.min(),
            "maximum": g.wall_clock_duration_s.max(),
            "tsr": g.task_skew_ratio_max.median(),
            "psr": g.partition_skew_ratio_max.median(),
            "shuffle_mib": g[shuffle].median() / MIB,
            "memory_spill_mib": g.memory_bytes_spilled.median() / MIB,
            "disk_spill_mib": g.disk_bytes_spilled.median() / MIB,
        })
    return pd.DataFrame(records).set_index("method").loc[methods].reset_index()


def bar(ax, labels: list[str], values: np.ndarray, title: str, ylabel: str, colors: list[str]) -> None:
    x = np.arange(len(values))
    bars = ax.bar(x, values, color=colors, edgecolor="#333333", linewidth=.6)
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    ax.set_xticks(x, labels)
    ax.grid(axis="y", alpha=.25)
    ax.set_axisbelow(True)
    top = max(float(np.nanmax(values)) if len(values) else 0, .01)
    ax.set_ylim(0, top * 1.27)
    for rect, value in zip(bars, values):
        ax.text(rect.get_x() + rect.get_width()/2, rect.get_height() + top*.025,
                f"{value:.2f}", ha="center", va="bottom", fontsize=8, fontweight="bold")


def plot_nd2() -> Path:
    methods = ["Baseline_GroupBy", "Selective_Salting_S8", "Uniform_Salting_S8"]
    labels = ["Baseline\ngroupBy", "Selective\nSalting S=8", "Uniform\nSalting S=8"]
    colors = ["#4e79a7", "#f28e2b", "#9c89b8"]
    df = load_summary(BENCH / "benchmark_nd2_aggregation_runs_official_standalone.csv", methods, "shuffle_write_bytes")
    fig, axes = plt.subplots(2, 2, figsize=(11, 7.2))
    x = np.arange(3)
    med = df["median"].to_numpy(float)
    errors = np.vstack([med - df.minimum.to_numpy(float), df.maximum.to_numpy(float) - med])
    axes[0, 0].bar(x, med, color=colors, edgecolor="#333333", linewidth=.6)
    axes[0, 0].errorbar(x, med, yerr=errors, fmt="none", ecolor="#222222", capsize=4, linewidth=1)
    axes[0, 0].set_title("Thời gian toàn phần: trung vị và min–max")
    axes[0, 0].set_ylabel("Giây")
    axes[0, 0].set_xticks(x, labels)
    for ax, vals, title, ylabel in [
        (axes[0, 1], df.shuffle_mib.to_numpy(float), "Dung lượng ghi shuffle: trung vị", "MiB"),
        (axes[1, 0], df.tsr.to_numpy(float), "TSR: trung vị", "Tỷ số"),
        (axes[1, 1], df.psr.to_numpy(float), "PSR: trung vị", "Tỷ số"),
    ]:
        bar(ax, labels, vals, title, ylabel, colors)
    for ax in axes.flat:
        ax.grid(axis="y", alpha=.25)
        ax.set_axisbelow(True)
    fig.suptitle("Hình 4.2 — Kết quả ND2 trên cụm Spark Standalone, Silver tháng 01/2026\n3 lượt đo; thanh sai số biểu thị min–max", fontsize=12, fontweight="bold")
    fig.tight_layout(rect=[0, 0, 1, .92])
    path = BENCH / "figure_4_2_nd2_aggregation_official.png"
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    ASSETS.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, ASSETS / path.name)
    return path


def plot_nd3() -> Path:
    methods = ["Baseline_SMJ", "Spark_AQE_Skew_Join", "Salting_Join_S8"]
    labels = ["Baseline\nSort-Merge", "Spark\nAQE", "Salting Join\nS=8"]
    colors = ["#4e79a7", "#59a14f", "#e15759"]
    df = load_summary(BENCH / "benchmark_nd3_join_runs_official_standalone.csv", methods, "shuffle_read_bytes")
    fig, axes = plt.subplots(2, 2, figsize=(11, 7.2))
    x = np.arange(3)
    med = df["median"].to_numpy(float)
    errors = np.vstack([med - df.minimum.to_numpy(float), df.maximum.to_numpy(float) - med])
    axes[0, 0].bar(x, med, color=colors, edgecolor="#333333", linewidth=.6)
    axes[0, 0].errorbar(x, med, yerr=errors, fmt="none", ecolor="#222222", capsize=4, linewidth=1)
    axes[0, 0].set_title("Thời gian toàn phần: trung vị và min–max")
    axes[0, 0].set_ylabel("Giây")
    axes[0, 0].set_xticks(x, labels)
    for ax, vals, title, ylabel in [
        (axes[0, 1], df.tsr.to_numpy(float), "TSR: trung vị", "Tỷ số"),
        (axes[1, 0], df.shuffle_mib.to_numpy(float), "Dung lượng đọc shuffle: trung vị", "MiB"),
    ]:
        bar(ax, labels, vals, title, ylabel, colors)
    ax = axes[1, 1]
    w = .34
    b1 = ax.bar(x-w/2, df.memory_spill_mib, w, label="Spill trong bộ nhớ", color="#f28e2b", edgecolor="#333333", linewidth=.5)
    b2 = ax.bar(x+w/2, df.disk_spill_mib, w, label="Spill ra đĩa", color="#b07aa1", edgecolor="#333333", linewidth=.5)
    ax.set_title("Dung lượng spill: trung vị")
    ax.set_ylabel("MiB")
    ax.set_xticks(x, labels)
    ax.legend(frameon=False, fontsize=8)
    top = max(float(df[["memory_spill_mib", "disk_spill_mib"]].to_numpy().max()), .01)
    ax.set_ylim(0, top*1.28)
    for bars in (b1, b2):
        for rect in bars:
            ax.text(rect.get_x()+rect.get_width()/2, rect.get_height()+top*.02,
                    f"{rect.get_height():.1f}", ha="center", va="bottom", fontsize=8)
    for ax in axes.flat:
        ax.grid(axis="y", alpha=.25)
        ax.set_axisbelow(True)
    fig.suptitle("Hình 4.4 — Kết quả ND3 trên cụm Spark Standalone, Silver tháng 01/2026\n3 lượt đo; thanh sai số biểu thị min–max", fontsize=12, fontweight="bold")
    fig.tight_layout(rect=[0, 0, 1, .92])
    path = BENCH / "figure_4_4_nd3_join_official.png"
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    ASSETS.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, ASSETS / path.name)
    return path


def main() -> None:
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 9})
    print("Generated:")
    print(plot_nd2())
    print(plot_nd3())
    print(ASSETS)


if __name__ == "__main__":
    main()
