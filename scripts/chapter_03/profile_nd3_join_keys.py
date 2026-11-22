#!/usr/bin/env python3
"""Describe exact OD-key frequency/fanout; this is not Flow-Join++ prediction.

Run after the timed benchmark, so this offline audit cannot affect its runtime.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scripts.chapter_03.benchmark_engine import dataset_manifest_fingerprint


def parquet_source(path: Path) -> str:
    files = sorted(path.rglob("*.parquet")) if path.is_dir() else [path]
    if not files or any(not item.is_file() for item in files):
        raise FileNotFoundError(f"Parquet input missing: {path}")
    literals = ",".join("'" + str(item.resolve()).replace("'", "''") + "'" for item in files)
    return f"read_parquet([{literals}])"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver", type=Path, required=True)
    parser.add_argument("--od", type=Path, required=True)
    parser.add_argument("--hotspots", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    # A unique output folder preserves prior offline audits and run artifacts.
    args.output.mkdir(parents=True, exist_ok=False)
    with duckdb.connect() as con:
        con.execute("SET threads=2")
        con.execute("SET memory_limit='1GB'")
        con.execute(f"CREATE TEMP TABLE od_counts AS SELECT PULocationID, DOLocationID, "
                    f"COUNT(*) AS dimension_rows FROM {parquet_source(args.od)} GROUP BY 1,2")
        invalid = con.execute("SELECT COUNT(*) FROM od_counts WHERE PULocationID IS NULL "
                              "OR DOLocationID IS NULL OR dimension_rows <> 1").fetchone()[0]
        if invalid:
            raise ValueError(f"OD has {invalid} duplicate/null key groups")
        con.execute(f"CREATE TEMP TABLE key_counts AS SELECT PULocationID, DOLocationID, "
                    f"COUNT(*) AS trip_count FROM {parquet_source(args.silver)} GROUP BY 1,2")
        con.execute(f"CREATE TEMP TABLE hot AS SELECT DISTINCT PULocationID "
                    f"FROM {parquet_source(args.hotspots)}")
        con.execute("CREATE TEMP TABLE profile AS SELECT k.*, COALESCE(o.dimension_rows,0) "
                    "AS matched_dimension_rows, k.trip_count*COALESCE(o.dimension_rows,0) "
                    "AS exact_join_output_rows, h.PULocationID IS NOT NULL AS pickup_p95_selected "
                    "FROM key_counts k LEFT JOIN od_counts o USING(PULocationID,DOLocationID) "
                    "LEFT JOIN hot h USING(PULocationID)")
        frame = con.execute("SELECT * FROM profile ORDER BY trip_count DESC, "
                            "PULocationID, DOLocationID").df()
        pickups = con.execute("SELECT PULocationID, SUM(trip_count) AS trip_count "
                              "FROM key_counts GROUP BY 1 ORDER BY trip_count, PULocationID").df()
        stored_hot = set(con.execute("SELECT PULocationID FROM hot").fetchnumpy()["PULocationID"].tolist())
        pu_threshold = int(pickups.iloc[math.ceil(.95 * len(pickups)) - 1]["trip_count"])
        exact_pu_hot = set(pickups.loc[pickups.trip_count >= pu_threshold, "PULocationID"].tolist())
        if stored_hot != exact_pu_hot:
            raise ValueError("Stored pickup hotspots disagree with exact nearest-rank P95 of Silver")
        threshold = int(sorted(frame.trip_count)[math.ceil(.95 * len(frame)) - 1])
        frame["od_frequency_p95_diagnostic"] = frame.trip_count >= threshold
        frame.to_csv(args.output / "od_key_frequency_fanout.csv", index=False)
        total = int(frame.trip_count.sum())
        selected = frame.loc[frame.pickup_p95_selected]
        diagnostic = frame.loc[frame.od_frequency_p95_diagnostic]
        summary = {
            "scope": "Exact offline diagnostics on observed OD keys; not a predicted Flow-Join++ threshold/model",
            "trip_dataset_manifest_sha256": dataset_manifest_fingerprint(str(args.silver)),
            "od_dataset_manifest_sha256": dataset_manifest_fingerprint(str(args.od)),
            "total_trip_rows": total,
            "distinct_observed_od_keys": len(frame),
            "od_dimension_keys": int(con.execute("SELECT COUNT(*) FROM od_counts").fetchone()[0]),
            "duplicate_or_null_od_key_groups": int(invalid),
            "unmatched_trip_rows": int(frame.loc[frame.matched_dimension_rows == 0, "trip_count"].sum()),
            "exact_join_output_rows": int(frame.exact_join_output_rows.sum()),
            "matching_dimension_rows_per_input_trip_min": int(frame.matched_dimension_rows.min()),
            "matching_dimension_rows_per_input_trip_max": int(frame.matched_dimension_rows.max()),
            "max_od_key_trip_count": int(frame.trip_count.max()),
            "pickup_p95_threshold": pu_threshold,
            "pickup_p95_hotspot_count": len(stored_hot),
            "pickup_selected_od_keys": len(selected),
            "pickup_selected_trip_rows": int(selected.trip_count.sum()),
            "pickup_selected_trip_share": float(selected.trip_count.sum() / total),
            "od_frequency_p95_threshold_diagnostic": threshold,
            "od_frequency_p95_key_count_diagnostic": len(diagnostic),
            "od_frequency_p95_trip_share_diagnostic": float(diagnostic.trip_count.sum() / total),
            "od_p95_keys_covered_by_pickup_hotspots": int(diagnostic.pickup_p95_selected.sum()),
            "interpretation": "For unique OD dimension keys, each matched trip has one match; per-key join-output fanout equals that OD key's trip frequency. Pickup P95 and composite-key frequency select different units. OD P95 is descriptive only; the timed salting rule is unchanged.",
        }
    (args.output / "join_key_profile.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2))
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
