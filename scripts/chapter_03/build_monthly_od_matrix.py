#!/usr/bin/env python3
"""Build the monthly OD dimension used by ND3 scaling runs."""
from __future__ import annotations

import os

from pyspark.sql import SparkSession, functions as F


def main() -> None:
    silver = os.environ["SILVER_PATH"]
    output = os.environ["OD_MATRIX_PATH"]
    lookup_path = os.environ.get("ZONE_LOOKUP_PATH", "/opt/spark/data/raw/taxi+_zone_lookup.csv")

    spark = SparkSession.builder.appName("Build_Monthly_OD_Matrix").getOrCreate()
    try:
        trips = spark.read.parquet(silver)
        lookup = (
            spark.read.option("header", True).csv(lookup_path)
            .select(F.col("LocationID").cast("int").alias("LocationID"),
                    F.col("Zone").alias("Zone"), F.col("Borough").alias("Borough"))
        )
        od = trips.groupBy("PULocationID", "DOLocationID").agg(
            F.count(F.lit(1)).alias("od_trip_count"),
            F.avg("base_passenger_fare").alias("od_avg_fare"),
            F.avg("trip_miles").alias("od_avg_miles"),
            F.avg("trip_time").alias("od_avg_time_sec"),
        )
        pu = lookup.select(F.col("LocationID").alias("PULocationID"),
                           F.col("Zone").alias("PU_Zone"), F.col("Borough").alias("PU_Borough"))
        do = lookup.select(F.col("LocationID").alias("DOLocationID"),
                           F.col("Zone").alias("DO_Zone"), F.col("Borough").alias("DO_Borough"))
        od = od.join(pu, "PULocationID", "left").join(do, "DOLocationID", "left")
        od.write.mode("overwrite").parquet(output)
        print(f"OD_MATRIX_ROWS={od.count()} OUTPUT={output}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
