#!/usr/bin/env python3
"""Create Chapter 3 pipeline and Spark cluster architecture figures."""
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "data/benchmarks"


def box(ax, xy, wh, text, *, fc="#eef4f8", ec="#31566f", fontsize=10, lw=1.5):
    x, y = xy
    w, h = wh
    patch = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.025",
                           facecolor=fc, edgecolor=ec, linewidth=lw)
    ax.add_patch(patch)
    ax.text(x+w/2, y+h/2, text, ha="center", va="center", fontsize=fontsize, wrap=True)
    return (x, y, w, h)


def arrow(ax, start, end, *, color="#506575", style="-|>", lw=1.5, rad=0):
    ax.add_patch(FancyArrowPatch(start, end, arrowstyle=style, mutation_scale=13,
                                 linewidth=lw, color=color,
                                 connectionstyle=f"arc3,rad={rad}"))


def pipeline_figure():
    fig, ax = plt.subplots(figsize=(14, 8))
    ax.set_xlim(0, 1); ax.set_ylim(0, 1); ax.axis("off")
    box(ax, (0.035, .69), (.17, .16), "HVFHV raw\nParquet tháng 01/2026", fc="#f4f4f4")
    box(ax, (.285, .69), (.20, .16), "Tiền xử lý Spark\nLàm sạch → Silver\nHotspot P95 pickup", fc="#e7f1fb")
    arrow(ax, (.205, .77), (.285, .77))

    box(ax, (.58, .76), (.18, .14), "ND2 — Aggregation\nGroupBy / Uniform S=8 /\nSelective S=8", fc="#eaf4e5", ec="#4f7b45")
    box(ax, (.58, .51), (.18, .14), "ND3 — OD Join\nBaseline SMJ / AQE /\nSalting S=8", fc="#fff1db", ec="#9b6c27")
    arrow(ax, (.485, .79), (.58, .82), rad=-.05)
    arrow(ax, (.485, .74), (.58, .58), rad=.08)
    box(ax, (.82, .63), (.15, .18), "DuckDB\nCorrectness audit\nPer-zone / borough-pair", fc="#f4eafa", ec="#74518c")
    arrow(ax, (.76, .82), (.82, .75), rad=.05)
    arrow(ax, (.76, .58), (.82, .68), rad=-.05)
    box(ax, (.29, .22), (.19, .18), "Silver Parquet\nJanuary cleaned trips", fc="#e7f1fb")
    arrow(ax, (.385, .69), (.385, .40))
    box(ax, (.58, .18), (.18, .21), "Spatial density branch\nDuckDB aggregation +\nGeoPandas / Taxi Zones\nGold + map artifacts", fc="#e9f4f3", ec="#3b7772")
    arrow(ax, (.48, .30), (.58, .30))
    box(ax, (.035, .21), (.17, .18), "Taxi Zone\nshapefile", fc="#f4f4f4")
    arrow(ax, (.205, .30), (.29, .30))
    box(ax, (.82, .22), (.15, .18), "Audit CSVs, plans,\nbenchmark summaries,\nGold GeoParquet/CSV", fc="#f4eafa", ec="#74518c")
    arrow(ax, (.76, .29), (.82, .29))
    ax.text(.5, .96, "Hình 3.1 — Kiến trúc pipeline và luồng dữ liệu Chương 3",
            ha="center", va="center", fontsize=15, weight="bold")
    ax.text(.5, .08, "ND2/ND3 là các workload Spark; nhánh spatial dùng DuckDB/GeoPandas, không phải Spark.",
            ha="center", fontsize=10, color="#444")
    fig.savefig(OUT / "chapter3_pipeline_architecture.png", dpi=180, bbox_inches="tight")
    plt.close(fig)


def cluster_figure():
    fig, ax = plt.subplots(figsize=(11, 6.5))
    ax.set_xlim(0, 1); ax.set_ylim(0, 1); ax.axis("off")
    box(ax, (.34, .78), (.32, .13), "Spark Master\nQuản lý ứng dụng và cấp phát tài nguyên", fc="#e7f1fb")
    box(ax, (.07, .43), (.34, .22), "Spark Worker 1\nExecutor(s)\nTask partitions", fc="#eaf4e5", ec="#4f7b45")
    box(ax, (.59, .43), (.34, .22), "Spark Worker 2\nExecutor(s)\nTask partitions", fc="#eaf4e5", ec="#4f7b45")
    box(ax, (.34, .10), (.32, .17), "Driver\nND2 / ND3 benchmark runner\nSpark UI + event log", fc="#fff1db", ec="#9b6c27")
    arrow(ax, (.43, .78), (.32, .65))
    arrow(ax, (.57, .78), (.68, .65))
    arrow(ax, (.40, .27), (.28, .43), color="#9b6c27")
    arrow(ax, (.60, .27), (.72, .43), color="#9b6c27")
    ax.text(.5, .96, "Hình 3.2 — Topology Spark Standalone được khai báo trong Docker Compose",
            ha="center", va="center", fontsize=14, weight="bold")
    ax.text(.5, .025, "Sơ đồ mô tả cấu hình; cần executor/worker evidence của từng application để xác nhận lần chạy thực tế.",
            ha="center", fontsize=9, color="#444")
    fig.savefig(OUT / "chapter3_spark_topology.png", dpi=180, bbox_inches="tight")
    plt.close(fig)


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    pipeline_figure()
    cluster_figure()
    print(OUT / "chapter3_pipeline_architecture.png")
    print(OUT / "chapter3_spark_topology.png")

if __name__ == "__main__":
    main()
