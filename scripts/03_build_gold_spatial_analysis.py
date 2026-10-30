#!/usr/bin/env python3
"""Build reproducible taxi-zone drop-off density data for the Chapter 3 analysis.

Uses Spark for trip-level aggregation and GeoPandas only for the 263-zone
geometry table. Set SPARK_MASTER_URL when the data are mounted in a cluster.
"""
from __future__ import annotations

import csv
import os
import shutil
import tempfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, Point, Polygon
from shapely.validation import make_valid

ROOT = Path(__file__).resolve().parents[1]
SILVER = Path(os.environ.get("SILVER_PATH", ROOT / "data/silver/fhvhv_2026_01_clean.parquet"))
SHAPEFILE = Path(os.environ.get("TAXI_ZONES_SHP", ROOT / "data/zones/shp/taxi_zones.shp"))
GOLD = Path(os.environ.get("SPATIAL_GOLD_PATH", ROOT / "data/gold/dropoff_density_2026_01.geoparquet"))
AUDIT = Path(os.environ.get("SPATIAL_AUDIT_PATH", ROOT / "data/benchmarks/spatial_analysis_audit.csv"))
PROJECTED_CRS = "EPSG:32618"  # UTM zone 18N; coordinates and polygon areas are metres.

# Same approximate E/W 60th Street reference points and area tolerance as 02b.
EAST = (40.7590, -73.9585)
WEST = (40.7738, -73.9918)
AREA_SIDE_TOLERANCE_PCT = 5.0
PERIODS = ("Daytime", "Nighttime")


def _atomic_csv(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    os.close(fd)
    try:
        pd.DataFrame(rows).to_csv(tmp, index=False)
        _backup_existing(path)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def _backup_existing(path: Path) -> None:
    if path.exists():
        stamp = pd.Timestamp.utcnow().strftime("%Y%m%dT%H%M%SZ")
        backup = path.with_name(f"{path.name}.bak_{stamp}")
        suffix = 1
        while backup.exists():
            backup = path.with_name(f"{path.name}.bak_{stamp}_{suffix}")
            suffix += 1
        shutil.copy2(path, backup)


def _classification(zones: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if zones.crs is None:
        raise ValueError("Taxi Zone shapefile has no declared CRS; cannot calculate area/density.")
    if "LocationID" not in zones or "borough" not in {c.lower() for c in zones.columns}:
        raise ValueError("Shapefile must include LocationID and Borough attributes.")
    borough_col = next(c for c in zones.columns if c.lower() == "borough")
    zones = zones[["LocationID", borough_col, "geometry"]].copy()
    zones = zones.to_crs(PROJECTED_CRS)

    # Dissolve any multipart fragments for the same Taxi Zone before area work.
    zones = zones.dissolve(by="LocationID", as_index=False, aggfunc={borough_col: "first"})
    zones["geometry_invalid"] = ~zones.geometry.is_valid
    invalid_count = int(zones["geometry_invalid"].sum())
    if invalid_count:
        zones["geometry"] = zones.geometry.map(make_valid)
    if not zones.geometry.is_valid.all():
        raise ValueError("At least one Taxi Zone remains invalid after make_valid repair.")
    zones["polygon_area_km2"] = zones.geometry.area / 1_000_000.0
    if (zones["polygon_area_km2"] <= 0).any() or zones.geometry.is_empty.any():
        raise ValueError("At least one Taxi Zone polygon has empty or non-positive projected area.")

    manhattan = zones[borough_col].astype(str).str.strip().str.casefold().eq("manhattan")
    pt_series = gpd.GeoSeries(
        [Point(EAST[1], EAST[0]), Point(WEST[1], WEST[0])], crs="EPSG:4326"
    ).to_crs(PROJECTED_CRS)
    (x1, y1), (x2, y2) = list(LineString(pt_series.tolist()).coords)
    dx, dy = x2 - x1, y2 - y1

    def side(point: Point) -> float:
        return (x2 - x1) * (point.y - y1) - (y2 - y1) * (point.x - x1)

    south_ref = zones.loc[zones["LocationID"].astype(int).eq(161) & manhattan]
    if south_ref.empty:
        raise ValueError("Zone 161 is required to orient the existing south-of-60th rule.")
    south_sign = 1 if side(south_ref.geometry.iloc[0].centroid) > 0 else -1
    nx, ny = y2 - y1, -(x2 - x1)
    norm = (nx * nx + ny * ny) ** 0.5
    nx, ny = nx / norm, ny / norm
    if side(Point(x1 + nx, y1 + ny)) * south_sign > 0:
        nx, ny = -nx, -ny
    big = 100_000.0
    north_half = Polygon([
        (x1 - 5 * dx, y1 - 5 * dy), (x2 + 5 * dx, y2 + 5 * dy),
        (x2 + 5 * dx + nx * big, y2 + 5 * dy + ny * big),
        (x1 - 5 * dx + nx * big, y1 - 5 * dy + ny * big),
    ])

    groups, methods = [], []
    for is_manhattan, geom in zip(manhattan, zones.geometry):
        if not is_manhattan:
            groups.append("OUTSIDE_MANHATTAN")
            methods.append("borough attribute")
            continue
        north_pct = 100.0 * geom.intersection(north_half).area / geom.area
        if north_pct <= AREA_SIDE_TOLERANCE_PCT:
            groups.append("SOUTH_OF_APPROX_60TH")
        elif north_pct >= 100.0 - AREA_SIDE_TOLERANCE_PCT:
            groups.append("NORTH_OF_APPROX_60TH")
        else:
            groups.append("BOUNDARY_AMBIGUOUS")
        methods.append("Manhattan polygon area split at approximate 60th Street; <=5% cross-side area")
    zones["analytical_group"] = groups
    zones["classification_method"] = methods
    zones.attrs["invalid_geometry_count"] = invalid_count
    return zones


def main() -> None:
    from pyspark.sql import SparkSession, functions as F

    if not SILVER.exists() or not SHAPEFILE.exists():
        raise FileNotFoundError(f"Required input missing: Silver={SILVER.exists()}, shapefile={SHAPEFILE.exists()}")
    zones = _classification(gpd.read_file(SHAPEFILE))
    invalid_geometry_count = int(zones.attrs.get("invalid_geometry_count", 0))
    zone_ids = [int(v) for v in zones.LocationID]

    master = os.environ.get("SPARK_MASTER_URL", "local[*]")
    builder = SparkSession.builder.appName("Chapter3_Manhattan_Dropoff_Density").master(master)
    if master.startswith("spark://") and os.environ.get("SPARK_DRIVER_HOST"):
        builder = (builder.config("spark.driver.host", os.environ["SPARK_DRIVER_HOST"])
                   .config("spark.driver.bindAddress", "0.0.0.0"))
    spark = builder.getOrCreate()
    try:
        trips = spark.read.parquet(str(SILVER))
        required = {"DOLocationID", "dropoff_datetime"}
        missing = required.difference(trips.columns)
        if missing:
            raise ValueError(f"Silver data lacks required drop-off fields: {sorted(missing)}")
        row_audit = trips.agg(
            F.count(F.lit(1)).alias("total_rows"),
            F.sum(F.when(F.col("dropoff_datetime").isNull(), 1).otherwise(0)).alias("null_time_rows"),
            F.sum(F.when(F.col("DOLocationID").isNull() | F.col("dropoff_datetime").isNull(), 1).otherwise(0)).alias("null_required_rows"),
        ).first()
        total_rows = int(row_audit["total_rows"])
        null_time_rows = int(row_audit["null_time_rows"] or 0)
        null_required_rows = int(row_audit["null_required_rows"] or 0)
        valid = trips.filter(F.col("DOLocationID").isNotNull() & F.col("dropoff_datetime").isNotNull())
        unknown = valid.join(spark.createDataFrame([(z,) for z in zone_ids], ["DOLocationID"]),
                             "DOLocationID", "left_anti")
        unmatched_zone_rows = unknown.count()

        # Explicit local-time bins: day 05:00 through 20:59; night 21:00 through 04:59.
        counts = (valid.withColumn("time_period",
                    F.when(F.hour("dropoff_datetime").between(5, 20), "Daytime").otherwise("Nighttime"))
                 .groupBy("DOLocationID", "time_period").agg(F.count(F.lit(1)).alias("trip_count"))
                 .toPandas())
    finally:
        spark.stop()

    # Small dimension/aggregate tables only (at most 263 x 2 records).
    counts = counts.rename(columns={"DOLocationID": "LocationID"})
    zone_attrs = zones.drop(columns="geometry").copy()
    out = zones.merge(
        pd.MultiIndex.from_product([zone_attrs.LocationID.tolist(), PERIODS],
                                   names=["LocationID", "time_period"]).to_frame(index=False),
        on="LocationID", how="right",
    )
    out = out.merge(counts, on=["LocationID", "time_period"], how="left")
    out["trip_count"] = out["trip_count"].fillna(0).astype("int64")
    out["dropoff_density_trips_per_km2"] = out["trip_count"] / out["polygon_area_km2"]
    out["analysis_month"] = "2026-01"
    out["time_definition"] = "Stored dropoff_datetime hour: Daytime 05:00-20:59; Nighttime 21:00-04:59"
    out["boundary_definition"] = "Approximate 60th Street line; Manhattan polygons with <=5% area across line assigned by dominant side"
    out = gpd.GeoDataFrame(out, geometry="geometry", crs=PROJECTED_CRS)

    included = out[~out.analytical_group.isin(["OUTSIDE_MANHATTAN", "BOUNDARY_AMBIGUOUS"])]
    included_rows = int(included.trip_count.sum())
    classified_rows = int(out[~out.analytical_group.eq("OUTSIDE_MANHATTAN")].trip_count.sum())
    audit_rows = [
        {"check": "spark_master", "value": master, "status": "recorded"},
        {"check": "input_trip_rows", "value": total_rows, "status": "measured"},
        {"check": "null_dropoff_timestamp_rows", "value": null_time_rows, "status": "measured"},
        {"check": "null_dropoff_zone_or_timestamp_rows", "value": null_required_rows, "status": "measured"},
        {"check": "valid_trip_rows_with_unmatched_zone_id", "value": unmatched_zone_rows, "status": "measured"},
        {"check": "taxi_zone_polygon_count", "value": len(zones), "status": "measured"},
        {"check": "invalid_geometries_repaired_with_make_valid", "value": invalid_geometry_count, "status": "measured"},
        {"check": "source_crs", "value": str(gpd.read_file(SHAPEFILE).crs), "status": "validated and projected to EPSG:32618"},
        {"check": "density_area_units", "value": "polygon area in km2 derived from EPSG:32618 metres", "status": "validated"},
        {"check": "trips_assigned_to_included_manhattan_groups", "value": included_rows, "status": "measured; excludes outside boroughs and ambiguous boundary zones"},
        {"check": "trips_in_any_manhattan_group_including_ambiguous", "value": classified_rows, "status": "measured"},
    ]

    GOLD.parent.mkdir(parents=True, exist_ok=True)
    tmp_gold = GOLD.with_name(GOLD.stem + ".tmp.geoparquet")
    out.to_parquet(tmp_gold, index=False)
    _backup_existing(GOLD)
    os.replace(tmp_gold, GOLD)
    _atomic_csv(audit_rows, AUDIT)
    print(f"Gold GeoParquet: {GOLD} ({len(out)} zone-period records)")
    print(f"Spatial audit: {AUDIT}")


if __name__ == "__main__":
    main()
