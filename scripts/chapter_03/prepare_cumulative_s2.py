#!/usr/bin/env python3
"""Prepare a consistent Jan–Mar Silver/OD/P95 input for the S2 benchmark.

Run with spark-submit on the Compose cluster. Outputs are new derived artifacts;
the three monthly Silver inputs are never overwritten.
"""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


MONTHS = ("01", "02", "03")


def parquet_manifest(path: Path) -> dict:
    files = sorted(path.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No Parquet files in {path}")
    return {
        "path": str(path),
        "files": [{"name": str(p.relative_to(path)), "bytes": p.stat().st_size,
                   "mtime_ns": p.stat().st_mtime_ns} for p in files],
        "total_bytes": sum(p.stat().st_size for p in files),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=Path("/opt/spark/data"))
    parser.add_argument("--output", type=Path, default=Path("/opt/spark/data/benchmarks/scaling/s2_jan_mar"))
    args = parser.parse_args()
    source_paths = [args.data_root / "silver" / f"fhvhv_2026_{m}_clean.parquet" for m in MONTHS]
    for path in source_paths:
        if not path.is_dir():
            parser.error(f"Missing monthly Silver input: {path}")
    output = args.output.resolve()
    silver_out = output / "silver"
    od_out = output / "od_matrix"
    hotspot_out = output / "hotspots_p95_pu"
    manifest_out = output / "s2_input_manifest.json"
    if any(p.exists() for p in (silver_out, od_out, hotspot_out, manifest_out)):
        parser.error(f"S2 output already exists at {output}; use a new output path to preserve provenance")
    output.mkdir(parents=True, exist_ok=True)

    spark = SparkSession.builder.appName("Prepare_Cumulative_S2_Jan_Mar").getOrCreate()
    try:
        source_counts = {m: spark.read.parquet(str(p)).count() for m, p in zip(MONTHS, source_paths)}
        trips = spark.read.option("mergeSchema", "true").parquet(*(str(p) for p in source_paths))
        trips.write.mode("errorifexists").parquet(str(silver_out))
        cumulative = spark.read.parquet(str(silver_out))
        total_rows = cumulative.count()
        if total_rows != sum(source_counts.values()):
            raise AssertionError(f"S2 row reconciliation failed: {source_counts} => {total_rows}")

        lookup = (spark.read.option("header", True).csv(str(args.data_root / "raw/taxi+_zone_lookup.csv"))
                  .select(F.col("LocationID").cast("int").alias("LocationID"),
                          F.col("Zone"), F.col("Borough")))
        pu = lookup.select(F.col("LocationID").alias("PULocationID"),
                           F.col("Zone").alias("PU_Zone"), F.col("Borough").alias("PU_Borough"))
        do = lookup.select(F.col("LocationID").alias("DOLocationID"),
                           F.col("Zone").alias("DO_Zone"), F.col("Borough").alias("DO_Borough"))
        od = (cumulative.groupBy("PULocationID", "DOLocationID")
              .agg(F.count(F.lit(1)).alias("od_trip_count"),
                   F.avg("base_passenger_fare").alias("od_avg_fare"),
                   F.avg("trip_miles").alias("od_avg_miles"),
                   F.avg("trip_time").alias("od_avg_time_sec"))
              .join(pu, "PULocationID", "left").join(do, "DOLocationID", "left"))
        od.write.mode("errorifexists").parquet(str(od_out))
        od_rows = spark.read.parquet(str(od_out)).count()
        od_key_errors = (spark.read.parquet(str(od_out))
                         .groupBy("PULocationID", "DOLocationID").count()
                         .filter(F.col("PULocationID").isNull() | F.col("DOLocationID").isNull() |
                                 (F.col("count") > 1)).count())
        if od_key_errors:
            raise AssertionError(f"S2 OD has {od_key_errors} null/duplicate key groups")

        zone_counts = (cumulative.filter(F.col("PULocationID").isNotNull())
                       .groupBy("PULocationID").count()
                       .withColumnRenamed("count", "trip_count"))
        counts = [r.trip_count for r in zone_counts.select("trip_count").collect()]
        if not counts:
            raise ValueError("No pickup zones in cumulative Silver")
        rank = math.ceil(0.95 * len(counts))
        threshold = sorted(counts)[rank - 1]
        hotspots = zone_counts.filter(F.col("trip_count") >= threshold)
        hotspot_ids = sorted(r.PULocationID for r in hotspots.select("PULocationID").collect())
        hotspots.write.mode("errorifexists").parquet(str(hotspot_out))

        metadata = {
            "tier": "derived_s2_input_not_benchmark",
            "months": [f"2026-{m}" for m in MONTHS],
            "source_rows": source_counts,
            "total_rows": total_rows,
            "source_manifests": [parquet_manifest(p) for p in source_paths],
            "silver_manifest": parquet_manifest(silver_out),
            "od_manifest": parquet_manifest(od_out),
            "hotspot_manifest": parquet_manifest(hotspot_out),
            "od_rows": od_rows,
            "od_null_or_duplicate_key_groups": od_key_errors,
            "p95_rule": "exact nearest-rank over non-null pickup zones with trips; ties included",
            "p95_rank": rank,
            "p95_threshold_trips": threshold,
            "hotspot_ids": hotspot_ids,
            "spark_version": spark.version,
            "application_id": spark.sparkContext.applicationId,
        }
        manifest_out.write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"S2_READY rows={total_rows} od_rows={od_rows} hotspots={len(hotspot_ids)} manifest={manifest_out}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
