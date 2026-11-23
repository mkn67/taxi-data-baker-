#!/usr/bin/env python3
"""Offline monthly exact-P95 pickup-hotspot stability audit.

Computes hotspot sets directly from monthly Silver Parquet with DuckDB and
reports both adjacent-month Jaccard and the share of trips covered. It does not
run Spark or alter stored benchmark artifacts.
"""
from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path

import duckdb
import pandas as pd


def sql_path(path: Path) -> str:
    files = sorted(path.rglob("*.parquet")) if path.is_dir() else [path]
    if not files:
        raise FileNotFoundError(path)
    literals = ",".join("'" + str(p.resolve()).replace("'", "''") + "'" for p in files)
    return f"read_parquet([{literals}], union_by_name=true)"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--silver-root", type=Path, default=Path("data/silver"))
    ap.add_argument("--output", type=Path, default=Path("data/benchmarks/phase_c/hotspot_stability"))
    ap.add_argument("--months", nargs="+", default=["2026_01", "2026_02", "2026_03", "2026_04", "2026_05"])
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    rows, sets = [], {}
    with duckdb.connect() as con:
        con.execute("SET threads=2")
        con.execute("SET memory_limit='2GB'")
        for month in args.months:
            path = args.silver_root / f"fhvhv_{month}_clean.parquet"
            source = sql_path(path)
            started = time.perf_counter()
            counts = con.execute(f"SELECT PULocationID, COUNT(*) AS trip_count FROM {source} "
                                 "WHERE PULocationID IS NOT NULL GROUP BY 1").df()
            elapsed = time.perf_counter() - started
            n = len(counts)
            rank = math.ceil(.95 * n)
            threshold = int(counts.sort_values(["trip_count", "PULocationID"]).iloc[rank - 1].trip_count)
            hot = counts.loc[counts.trip_count >= threshold]
            sets[month] = set(hot.PULocationID.astype(int))
            rows.append({"month": month, "silver_path": str(path), "zone_count": n,
                         "nearest_rank_p95_rank": rank, "p95_threshold_trips": threshold,
                         "hotspot_count": len(hot), "total_pickup_trips": int(counts.trip_count.sum()),
                         "hotspot_trip_count": int(hot.trip_count.sum()),
                         "hotspot_trip_share": float(hot.trip_count.sum() / counts.trip_count.sum()),
                         "detection_scan_seconds": elapsed,
                         "hotspot_ids": json.dumps(sorted(sets[month]))})
    for i in range(1, len(rows)):
        prev, cur = rows[i - 1], rows[i]
        a, b = sets[prev["month"]], sets[cur["month"]]
        union = a | b
        cur["compared_to_month"] = prev["month"]
        cur["adjacent_jaccard"] = len(a & b) / len(union) if union else None
    df = pd.DataFrame(rows)
    df.to_csv(args.output / "monthly_hotspot_stability.csv", index=False)
    (args.output / "summary.json").write_text(json.dumps({
        "method": "Exact nearest-rank P95 over per-month pickup-zone counts",
        "jaccard_definition": "|hot_t intersect hot_t-1| / |hot_t union hot_t-1|",
        "detection_runtime_note": "DuckDB scan+group-by time; offline diagnostic, not added to Spark join wall-time",
        "months": rows,
    }, indent=2))
    print(df[["month", "hotspot_count", "hotspot_trip_share", "detection_scan_seconds", "adjacent_jaccard"]].to_string(index=False))


if __name__ == "__main__":
    main()
