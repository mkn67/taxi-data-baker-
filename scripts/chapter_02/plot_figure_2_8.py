#!/usr/bin/env python3
"""Create Figure 2.8 from the selected Silver month, using the project P95 rule."""
from __future__ import annotations

import math
import os
from pathlib import Path

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SILVER = Path(os.environ.get("SILVER_PATH", ROOT / "data/silver/fhvhv_2026_01_clean.parquet"))
HOTSPOTS_PU = Path(os.environ.get("HOTSPOTS_PU_PATH", ROOT / "data/silver/hotspots_p95_pu_2026_01"))
HOTSPOTS_DO = Path(os.environ.get("HOTSPOTS_DO_PATH", ROOT / "data/silver/hotspots_p95_do_2026_01"))
LOOKUP = Path(os.environ.get("ZONE_LOOKUP_PATH", ROOT / "data/raw/taxi+_zone_lookup.csv"))
OUTPUT = Path(os.environ.get("FIGURE_2_8_PATH", ROOT / "data/zones/figure_2_8_hotspots_top10.png"))


def parquet_source(path: Path) -> str:
    if path.is_file():
        files = [path]
    elif path.is_dir():
        files = sorted(path.rglob("*.parquet"))
    else:
        files = []
    if not files:
        raise FileNotFoundError(f"No Parquet input found: {path}")
    literals = ", ".join("'" + str(item.resolve()).replace("'", "''") + "'" for item in files)
    return f"read_parquet([{literals}], union_by_name=true)"


def zone_counts(silver_source: str, column: str) -> pd.DataFrame:
    if column not in {"PULocationID", "DOLocationID"}:
        raise ValueError(f"Unsupported zone column: {column}")
    return duckdb.execute(f"""
        SELECT {column} AS LocationID, COUNT(*)::BIGINT AS trip_count
        FROM {silver_source}
        WHERE {column} IS NOT NULL
        GROUP BY {column}
        ORDER BY trip_count DESC, LocationID
    """).df()


def nearest_rank_p95(counts: pd.Series) -> int:
    ordered = sorted(int(value) for value in counts)
    if not ordered:
        raise ValueError("Cannot calculate P95 for an empty zone-count distribution.")
    return ordered[math.ceil(0.95 * len(ordered)) - 1]


def validate_stored_hotspots(
    counts: pd.DataFrame, threshold: int, hotspot_path: Path, column: str, label: str
) -> None:
    """Stop if the stored hotspot artifact does not correspond to this Silver distribution."""
    stored = duckdb.execute(
        f"SELECT {column} AS LocationID, trip_count FROM {parquet_source(hotspot_path)}"
    ).df()
    expected = counts.loc[counts["trip_count"] >= threshold, ["LocationID", "trip_count"]]
    expected = expected.sort_values("LocationID").reset_index(drop=True)
    stored = stored.sort_values("LocationID").reset_index(drop=True)
    if not expected.equals(stored):
        raise ValueError(
            f"Stored {label} hotspots do not match the P95 counts in {SILVER}. "
            "Rebuild the hotspot artifact from the selected Silver input before plotting."
        )


def main() -> None:
    if not SILVER.exists():
        raise FileNotFoundError(f"Silver input not found: {SILVER}")
    if not LOOKUP.is_file():
        raise FileNotFoundError(f"Taxi Zone lookup not found: {LOOKUP}")

    lookup = pd.read_csv(LOOKUP)
    required = {"LocationID", "Zone", "Borough"}
    if not required.issubset(lookup.columns):
        raise ValueError(f"Zone lookup lacks columns: {sorted(required - set(lookup.columns))}")
    zone_names = {
        int(row.LocationID): f"{row.Zone} ({int(row.LocationID)})"
        for row in lookup.itertuples(index=False)
        if pd.notna(row.LocationID) and pd.notna(row.Zone)
    }

    source = parquet_source(SILVER)
    distributions = []
    for column, artifact, label in (
        ("PULocationID", HOTSPOTS_PU, "PU"),
        ("DOLocationID", HOTSPOTS_DO, "DO"),
    ):
        counts = zone_counts(source, column)
        threshold = nearest_rank_p95(counts["trip_count"])
        validate_stored_hotspots(counts, threshold, artifact, column, label)
        top10 = counts.head(10).copy().sort_values("trip_count")
        top10["zone_label"] = top10["LocationID"].map(zone_names).fillna(
            top10["LocationID"].map(lambda value: f"Zone {int(value)}")
        )
        distributions.append((top10, threshold, label))

    fig, axes = plt.subplots(1, 2, figsize=(13, 6), dpi=180)
    colors = ("#2878a5", "#39875a")
    for ax, (top10, threshold, label), color in zip(axes, distributions, colors):
        values = top10["trip_count"] / 1000
        bars = ax.barh(top10["zone_label"], values, color=color, edgecolor="#263746", height=.68)
        threshold_k = threshold / 1000
        ax.axvline(threshold_k, color="#bb3e36", linestyle="--", linewidth=1.5,
                   label=f"Ngưỡng P95: {threshold:,}")
        ax.set_xlabel("Số chuyến (nghìn)")
        ax.set_title("Điểm đón khách (PULocationID)" if label == "PU" else "Điểm trả khách (DOLocationID)")
        ax.grid(axis="x", linestyle="--", alpha=.35)
        ax.set_axisbelow(True)
        for bar, count in zip(bars, top10["trip_count"]):
            ax.text(bar.get_width() + max(values) * .012, bar.get_y() + bar.get_height() / 2,
                    f"{int(count):,}", va="center", fontsize=8)
        ax.legend(frameon=False, loc="lower right", fontsize=8)

    fig.suptitle(f"Hình 2.8 — Các zone có tần suất chuyến cao và ngưỡng P95\nNguồn: Silver {SILVER.name}",
                 fontsize=12, fontweight="bold")
    fig.tight_layout(rect=(0, 0, 1, .91))
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUTPUT, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"Hình đã lưu: {OUTPUT}")


if __name__ == "__main__":
    main()
