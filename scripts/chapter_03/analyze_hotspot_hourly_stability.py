#!/usr/bin/env python3
"""Audit exact-P95 pickup hotspots by local Silver pickup hour and month.

Uses DuckDB over Silver Parquet and writes a compact CSV/JSON audit. This is an
offline diagnostic; it does not run Spark or change benchmark artifacts.
"""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import duckdb
import pandas as pd


def quote_path(path: Path) -> str:
    files = sorted(path.rglob("*.parquet")) if path.is_dir() else [path]
    if not files or any(not file.is_file() for file in files):
        raise FileNotFoundError(path)
    literals = ",".join("'" + str(file.resolve()).replace("'", "''") + "'" for file in files)
    return f"[{literals}]"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--silver-root", type=Path, default=Path("data/silver"))
    ap.add_argument("--output", type=Path, default=Path("data/benchmarks/phase_c/hotspot_stability_hourly"))
    ap.add_argument("--months", nargs="+", default=["2026_01", "2026_02", "2026_03", "2026_04", "2026_05"])
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    per_month: dict[str, dict[int, set[int]]] = {}
    rows: list[dict] = []
    with duckdb.connect() as con:
        con.execute("SET threads=2")
        con.execute("SET memory_limit='2GB'")
        for month in args.months:
            path = args.silver_root / f"fhvhv_{month}_clean.parquet"
            if not path.exists():
                raise FileNotFoundError(path)
            source = f"read_parquet({quote_path(path)}, union_by_name=true)"
            grouped = con.execute(f"""
                SELECT date_part('hour', pickup_datetime)::INTEGER AS pickup_hour,
                       PULocationID, COUNT(*)::BIGINT AS trip_count
                FROM {source}
                WHERE pickup_datetime IS NOT NULL AND PULocationID IS NOT NULL
                GROUP BY 1, 2
                ORDER BY 1, 2
            """).df()
            totals = con.execute(f"""
                SELECT COUNT(*)::BIGINT AS input_rows,
                       COUNT(*) FILTER (WHERE pickup_datetime IS NULL)::BIGINT AS null_pickup_time,
                       COUNT(*) FILTER (WHERE PULocationID IS NULL)::BIGINT AS null_pickup_zone,
                       COUNT(*) FILTER (WHERE pickup_datetime IS NOT NULL AND PULocationID IS NOT NULL)::BIGINT AS eligible_rows
                FROM {source}
            """).fetchone()
            per_month[month] = {}
            for hour, frame in grouped.groupby("pickup_hour", sort=True):
                frame = frame.sort_values(["trip_count", "PULocationID"]).reset_index(drop=True)
                n = len(frame)
                rank = math.ceil(0.95 * n)
                threshold = int(frame.iloc[rank - 1]["trip_count"])
                hot = frame.loc[frame.trip_count >= threshold]
                ids = set(hot.PULocationID.astype(int))
                per_month[month][int(hour)] = ids
                total = int(frame.trip_count.sum())
                rows.append({
                    "month": month,
                    "silver_path": str(path),
                    "pickup_hour": int(hour),
                    "zone_count_with_trips": n,
                    "p95_nearest_rank": rank,
                    "p95_threshold_trips_per_hour": threshold,
                    "hotspot_count": len(ids),
                    "hour_trip_count": total,
                    "hotspot_trip_count": int(hot.trip_count.sum()),
                    "hotspot_trip_share": float(hot.trip_count.sum() / total) if total else None,
                    "hotspot_ids": json.dumps(sorted(ids)),
                    "input_rows": int(totals[0]),
                    "null_pickup_time_rows": int(totals[1]),
                    "null_pickup_zone_rows": int(totals[2]),
                    "eligible_rows": int(totals[3]),
                })

    rows.sort(key=lambda r: (r["month"], r["pickup_hour"]))
    month_positions = {m: i for i, m in enumerate(args.months)}
    for row in rows:
        month, hour = row["month"], row["pickup_hour"]
        ids = per_month[month][hour]
        previous_hour = per_month[month].get((hour - 1) % 24)
        row["previous_hour_jaccard"] = (
            len(ids & previous_hour) / len(ids | previous_hour)
            if previous_hour is not None and (ids | previous_hour) else None
        )
        pos = month_positions[month]
        previous_month = args.months[pos - 1] if pos > 0 else None
        previous_month_set = per_month.get(previous_month, {}).get(hour) if previous_month else None
        row["previous_month"] = previous_month
        row["same_hour_previous_month_jaccard"] = (
            len(ids & previous_month_set) / len(ids | previous_month_set)
            if previous_month_set is not None and (ids | previous_month_set) else None
        )

    frame = pd.DataFrame(rows)
    frame.to_csv(args.output / "hourly_hotspot_stability.csv", index=False)
    summary = {
        "method": "Exact nearest-rank P95 over pickup-zone trip counts, separately per Silver pickup hour and month",
        "hour_basis": "hour component of timezone-naive Silver pickup_datetime; no timezone conversion",
        "jaccard_definitions": {
            "previous_hour": "within-month adjacent cyclic hours; hour 0 compared with hour 23",
            "same_hour_previous_month": "same pickup hour compared with immediately previous listed month",
        },
        "interpretation": "Each hourly P95 is a separate descriptive hotspot set; it is not the project ND3 label and is not used to select benchmark methods.",
        "months": args.months,
        "hour_rows": len(rows),
        "summary_by_month": frame.groupby("month").agg(
            hours=("pickup_hour", "nunique"),
            mean_hotspot_trip_share=("hotspot_trip_share", "mean"),
            min_hotspot_trip_share=("hotspot_trip_share", "min"),
            max_hotspot_trip_share=("hotspot_trip_share", "max"),
            mean_adjacent_hour_jaccard=("previous_hour_jaccard", "mean"),
            mean_same_hour_month_jaccard=("same_hour_previous_month_jaccard", "mean"),
        ).reset_index().to_dict("records"),
    }
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2))
    print(frame.groupby("month").agg(
        hours=("pickup_hour", "nunique"),
        hotspot_share_min=("hotspot_trip_share", "min"),
        hotspot_share_median=("hotspot_trip_share", "median"),
        hotspot_share_max=("hotspot_trip_share", "max"),
        prev_hour_jaccard_median=("previous_hour_jaccard", "median"),
        prev_month_jaccard_median=("same_hour_previous_month_jaccard", "median"),
    ).to_string())
    print(f"Wrote hourly stability evidence to {args.output}")


if __name__ == "__main__":
    main()
