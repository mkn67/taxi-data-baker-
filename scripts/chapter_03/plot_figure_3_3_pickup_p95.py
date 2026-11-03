#!/usr/bin/env python3
"""Create Figure 3.3: pickup-zone trip distribution and exact P95 hotspots.

The figure is built from January 2026 Silver Parquet. The per-zone counts and
nearest-rank P95 threshold are recomputed with DuckDB, then checked against the
stored pickup-hotspot Parquet artifact before the PNG is written.

Optional environment variables:
  SILVER_PATH       Silver Parquet file or directory
  HOTSPOT_PU_PATH   stored exact P95 pickup-hotspot Parquet directory/file
  FIGURE_3_3_PATH   output PNG path
"""
from __future__ import annotations

import math
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SILVER_DEFAULT = ROOT / "data/silver/fhvhv_2026_01_clean.parquet"
HOTSPOT_DEFAULT = ROOT / "data/silver/hotspots_p95_pu_2026_01"
FIGURE_DEFAULT = ROOT / "data/benchmarks/figure_3_3_pickup_p95_hotspots.png"


def _parquet_source(path: Path) -> str:
    """Build a safely quoted DuckDB read_parquet source for files/directories."""
    if path.is_file():
        files = [path]
    elif path.is_dir():
        files = sorted(path.rglob("*.parquet"))
    else:
        files = []
    if not files:
        raise FileNotFoundError(f"No Parquet files found at: {path}")
    quoted = ", ".join("'" + str(p.resolve()).replace("'", "''") + "'" for p in files)
    return f"read_parquet([{quoted}], union_by_name=true)"


def _backup_existing(path: Path) -> None:
    if not path.exists():
        return
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = path.with_name(f"{path.stem}.bak_{stamp}{path.suffix}")
    counter = 1
    while backup.exists():
        backup = path.with_name(f"{path.stem}.bak_{stamp}_{counter}{path.suffix}")
        counter += 1
    shutil.copy2(path, backup)


def load_and_validate_counts(silver_path: Path, hotspot_path: Path) -> tuple[pd.DataFrame, int, int]:
    silver = _parquet_source(silver_path)
    hotspots = _parquet_source(hotspot_path)
    con = duckdb.connect()
    try:
        silver_columns = {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM {silver}").fetchall()}
        hotspot_columns = {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM {hotspots}").fetchall()}
        if "PULocationID" not in silver_columns:
            raise ValueError("Silver input must contain PULocationID.")
        if not {"PULocationID", "trip_count"}.issubset(hotspot_columns):
            raise ValueError("Hotspot artifact must contain PULocationID and trip_count.")

        counts = con.execute(f"""
            SELECT PULocationID, COUNT(*)::BIGINT AS trip_count
            FROM {silver}
            WHERE PULocationID IS NOT NULL
            GROUP BY PULocationID
            ORDER BY trip_count ASC, PULocationID ASC
        """).df()
        stored = con.execute(f"""
            SELECT PULocationID, trip_count
            FROM {hotspots}
            ORDER BY PULocationID
        """).df()
    finally:
        con.close()

    if counts.empty:
        raise ValueError("No non-null pickup zones found in Silver input.")
    if counts["PULocationID"].duplicated().any() or stored["PULocationID"].duplicated().any():
        raise ValueError("Pickup IDs must be unique in grouped counts and hotspot artifact.")

    rank = math.ceil(0.95 * len(counts))  # one-based nearest rank
    threshold = int(counts.iloc[rank - 1]["trip_count"])
    counts["trip_count"] = counts["trip_count"].astype("int64")
    hot = counts[counts["trip_count"] >= threshold].copy()

    expected = {
        int(row.PULocationID): int(row.trip_count)
        for row in hot.itertuples(index=False)
    }
    observed = {
        int(row.PULocationID): int(row.trip_count)
        for row in stored.itertuples(index=False)
    }
    if expected != observed:
        raise ValueError(
            "Stored pickup P95 artifact does not match counts recomputed from Silver. "
            "Rebuild the hotspot artifact before plotting."
        )
    return counts, threshold, rank


def plot_figure(counts: pd.DataFrame, threshold: int, rank: int, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    ordered = counts.sort_values("trip_count", ascending=False).reset_index(drop=True)
    ordered["rank_desc"] = range(1, len(ordered) + 1)
    ordered["is_hot"] = ordered["trip_count"] >= threshold
    hot = ordered[ordered["is_hot"]].sort_values("trip_count", ascending=True)

    fig, (ax_all, ax_hot) = plt.subplots(
        2, 1, figsize=(13, 9), gridspec_kw={"height_ratios": [1.35, 1]}, constrained_layout=True
    )
    normal = ordered[~ordered["is_hot"]]
    ax_all.scatter(normal["rank_desc"], normal["trip_count"] / 1000,
                   s=18, color="#8da0cb", alpha=0.8, label="Các vùng còn lại")
    ax_all.scatter(ordered.loc[ordered["is_hot"], "rank_desc"],
                   ordered.loc[ordered["is_hot"], "trip_count"] / 1000,
                   s=32, color="#d73027", label=f"Khóa nóng H ({len(hot)} vùng)", zorder=3)
    ax_all.axhline(threshold / 1000, color="#d73027", linestyle="--", linewidth=1.4,
                   label=f"Ngưỡng P95 = {threshold:,} chuyến")
    ax_all.set_xlim(0, len(ordered) + 1)
    ax_all.set_ylabel("Số chuyến (nghìn)")
    ax_all.set_xlabel("Thứ hạng vùng pickup (giảm dần theo số chuyến)")
    ax_all.set_title("Phân bố số chuyến theo PULocationID")
    ax_all.grid(axis="y", alpha=0.25)
    ax_all.legend(loc="upper right", frameon=True)

    ax_hot.barh(hot["PULocationID"].astype(str), hot["trip_count"] / 1000,
                color="#d73027", alpha=0.88)
    ax_hot.axvline(threshold / 1000, color="#333333", linestyle="--", linewidth=1.2,
                   label=f"P95 = {threshold:,}")
    ax_hot.set_xlabel("Số chuyến (nghìn)")
    ax_hot.set_ylabel("PULocationID")
    ax_hot.set_title("Các khóa thuộc tập H")
    ax_hot.grid(axis="x", alpha=0.25)
    ax_hot.legend(loc="lower right")

    fig.suptitle(
        "Hình 3.3 — Phân bố chuyến theo vùng pickup và nhận diện khóa nóng P95\n"
        f"Silver tháng 01/2026 | n = {len(counts)} vùng | nearest-rank hạng {rank} | "
        f"T95 = {threshold:,} chuyến | |H| = {len(hot)}",
        fontsize=13,
    )
    fd, temp_name = tempfile.mkstemp(prefix=f".{output.stem}.", suffix=output.suffix, dir=output.parent)
    os.close(fd)
    temp_path = Path(temp_name)
    try:
        fig.savefig(temp_path, dpi=180, bbox_inches="tight")
        plt.close(fig)
        if not temp_path.is_file() or temp_path.stat().st_size == 0:
            raise RuntimeError("Figure rendering produced an empty file.")
        temp_path.chmod(0o644)
        _backup_existing(output)
        os.replace(temp_path, output)
    finally:
        temp_path.unlink(missing_ok=True)


def main() -> None:
    silver_path = Path(os.environ.get("SILVER_PATH", SILVER_DEFAULT))
    hotspot_path = Path(os.environ.get("HOTSPOT_PU_PATH", HOTSPOT_DEFAULT))
    output_path = Path(os.environ.get("FIGURE_3_3_PATH", FIGURE_DEFAULT))
    counts, threshold, rank = load_and_validate_counts(silver_path, hotspot_path)
    plot_figure(counts, threshold, rank, output_path)
    hot = counts[counts["trip_count"] >= threshold].sort_values("trip_count", ascending=False)
    print(f"Silver pickup zones: {len(counts)}")
    print(f"Nearest-rank P95: rank {rank}, threshold {threshold:,}")
    print(f"Hotspot zones: {len(hot)} ({', '.join(map(str, hot['PULocationID'].astype(int)))})")
    print(f"Saved figure: {output_path}")


if __name__ == "__main__":
    main()
