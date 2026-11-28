#!/usr/bin/env python3
"""Measure exact nearest-rank P95 pickup detection on a frozen Silver input."""
from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--silver", required=True)
    ap.add_argument("--stored-hotspots", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--repetitions", type=int, default=3)
    args = ap.parse_args()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    spark = (SparkSession.builder.appName("PhaseC_P95_Discovery_Cost")
             .config("spark.sql.shuffle.partitions", "200").getOrCreate())
    try:
        source = args.silver if Path(args.silver).is_file() else args.silver + "/*.parquet"
        trips = spark.read.parquet(source)
        stored = {int(r[0]) for r in spark.read.parquet(args.stored_hotspots).select("PULocationID").collect()}
        observations = []
        for iteration in range(1, args.repetitions + 1):
            started = time.perf_counter()
            counts = trips.where(F.col("PULocationID").isNotNull()).groupBy("PULocationID").count().collect()
            elapsed = time.perf_counter() - started
            ordered = sorted((int(r[0]), int(r[1])) for r in counts)
            threshold = sorted(value for _, value in ordered)[math.ceil(.95 * len(ordered)) - 1]
            selected = sorted(zone for zone, value in ordered if value >= threshold)
            if set(selected) != stored:
                raise ValueError(f"Calculated P95 differs from stored artifact at iteration {iteration}")
            observations.append({"iteration": iteration, "seconds": elapsed, "zone_count": len(ordered),
                                 "threshold": threshold, "hotspot_ids": selected})
        output.write_text(json.dumps({"silver": args.silver, "stored_hotspots": args.stored_hotspots,
                                      "application_id": spark.sparkContext.applicationId,
                                      "spark_version": spark.version,
                                      "scope": "Spark groupBy count action; excludes input construction and writing artifact",
                                      "observations": observations}, indent=2))
        print(json.dumps(observations, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
