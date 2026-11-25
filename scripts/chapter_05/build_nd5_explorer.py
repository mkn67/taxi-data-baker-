#!/usr/bin/env python3
"""Build the ND5 Kepler.gl map, Manhattan rankings and Gold reconciliation."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import tempfile
from collections import Counter, defaultdict
from pathlib import Path

from shapely import from_wkt
from shapely.geometry import mapping

ROOT = Path(__file__).resolve().parents[2]
PERIODS = ("Daytime", "Nighttime")
GROUPS = {
    "SOUTH_OF_APPROX_60TH": "Phía Nam",
    "NORTH_OF_APPROX_60TH": "Phía Bắc",
    "BOUNDARY_AMBIGUOUS": "Vùng biên",
    "OUTSIDE_MANHATTAN": "Ngoài Manhattan",
}
FIELDS = [
    ("LocationID", "integer"), ("Taxi Zone", "string"), ("Quận", "string"),
    ("Nhóm vùng", "string"), ("Khung giờ", "string"), ("Lượt trả khách", "integer"),
    ("Diện tích (km²)", "real"), ("Mật độ (chuyến/km²)", "real"), ("geometry", "geojson"),
]
RANK_COLUMNS = ["rank", "analysis_month", "LocationID", "zone", "borough",
                "analytical_group", "time_period", "trip_count", "polygon_area_km2",
                "dropoff_density_trips_per_km2"]
DATA_ID = "nd5_dropoffs"


def read_gold(path: Path) -> tuple[str, list[dict]]:
    csv.field_size_limit(16 * 1024 * 1024)
    with path.open(encoding="utf-8-sig", newline="") as source:
        rows = list(csv.DictReader(source))
    months = {row["analysis_month"] for row in rows}
    if len(months) != 1 or not next(iter(months)):
        raise ValueError("Expected exactly one Gold analysis month.")
    by_zone: dict[int, dict] = defaultdict(dict)
    for row in rows:
        zone, period = int(row["LocationID"]), row["time_period"]
        if period not in PERIODS or period in by_zone[zone]:
            raise ValueError(f"Unexpected or duplicate Gold zone-period: {(zone, period)}")
        if row["analytical_group"] not in GROUPS:
            raise ValueError(f"Unknown analysis group for zone {zone}.")
        if row["geometry_crs"] != "EPSG:4326":
            raise ValueError("Map input geometry must use EPSG:4326.")
        trips = int(row["trip_count"])
        area = float(row["polygon_area_km2"])
        density = float(row["dropoff_density_trips_per_km2"]) if row["dropoff_density_trips_per_km2"] else None
        if trips < 0 or not math.isfinite(area) or area < 0:
            raise ValueError(f"Invalid Gold count or area for {(zone, period)}.")
        if area > 0:
            if density is None or not math.isclose(density, trips / area, rel_tol=1e-9, abs_tol=1e-9):
                raise ValueError(f"Density does not match trips / area for {(zone, period)}.")
        elif density is not None:
            raise ValueError(f"Zero-area zone must have missing density: {(zone, period)}.")
        row.update(LocationID=zone, trip_count=trips, polygon_area_km2=area,
                   dropoff_density_trips_per_km2=density)
        by_zone[zone][period] = row

    for zone, periods in by_zone.items():
        if set(periods) != set(PERIODS):
            raise ValueError(f"Missing period for zone {zone}.")
        day, night = (periods[period] for period in PERIODS)
        if any(day[field] != night[field] for field in
               ("zone", "borough", "analytical_group", "geometry_wkt", "polygon_area_km2")):
            raise ValueError(f"Inconsistent zone attributes across periods for {zone}.")
        geom = from_wkt(day["geometry_wkt"])
        if geom.geom_type not in {"Polygon", "MultiPolygon"} or geom.is_empty:
            raise ValueError(f"Expected a nonempty polygon for zone {zone}.")
        x1, y1, x2, y2 = geom.bounds
        if not (-180 <= x1 <= x2 <= 180 and -90 <= y1 <= y2 <= 90):
            raise ValueError(f"Invalid WGS84 bounds for zone {zone}.")
        geometry = json.dumps(mapping(geom), separators=(",", ":"), allow_nan=False)
        for row in periods.values():
            row["geometry"] = geometry
    return months.pop(), sorted(rows, key=lambda row: (row["LocationID"], row["time_period"]))


def build_map(month: str, rows: list[dict]) -> dict:
    # One dataset lets the three filters apply to the same layer.
    values = [[r["LocationID"], f'{r["zone"]} · {r["LocationID"]}', r["borough"],
               GROUPS[r["analytical_group"]], r["time_period"], r["trip_count"],
               r["polygon_area_km2"], r["dropoff_density_trips_per_km2"], r["geometry"]] for r in rows]
    fields = [{"name": name, "type": kind, "format": ""} for name, kind in FIELDS]
    filters = [
        {"id": f"nd5_{suffix}", "dataId": [DATA_ID], "name": [field], "type": "multiSelect",
         "value": selected, "enabled": True, "view": "side"}
        for suffix, field, selected in (
            ("period", "Khung giờ", ["Daytime"]),
            ("group", "Nhóm vùng", [v for k, v in GROUPS.items() if k != "OUTSIDE_MANHATTAN"]),
            ("zone", "Taxi Zone", []),
        )
    ]
    config = {
        "visState": {
            "filters": filters,
            "layers": [{
                "id": "nd5_density", "type": "geojson",
                "config": {"dataId": DATA_ID, "label": "Mật độ trả khách 3D · chuyến/km²",
                           "color": [14, 116, 144], "columns": {"geojson": "geometry"},
                           "isVisible": True, "visConfig": {
                               "filled": True, "stroked": True, "strokeColor": [75, 95, 110],
                               "opacity": 0.88, "thickness": 0.65, "enable3d": True,
                               "heightRange": [0, 450], "elevationScale": 1,
                               "fixedHeight": False, "wireframe": False,
                               "colorRange": {"name": "ND5 Teal", "type": "sequential",
                                              "category": "Custom",
                                              "colors": ["#ecf7f6", "#b8e4dc", "#6cc4b7", "#258f86", "#065b59"]},
                           }},
                "visualChannels": {"colorField": {"name": "Mật độ (chuyến/km²)", "type": "real"},
                                   "colorScale": "quantile",
                                   "heightField": {"name": "Mật độ (chuyến/km²)", "type": "real"},
                                   "heightScale": "linear"},
            }],
            "interactionConfig": {"tooltip": {"enabled": True, "fieldsToShow": {
                DATA_ID: [{"name": name, "format": None} for name, _ in FIELDS if name != "geometry"],
            }}},
            "layerBlending": "normal", "splitMaps": [],
        },
        "mapState": {"latitude": 40.777, "longitude": -73.968, "zoom": 11.1,
                     "bearing": -25, "pitch": 52, "dragRotate": True, "isSplit": False},
        "mapStyle": {"styleType": "light"},
    }
    return {
        "datasets": [{"version": "v1", "data": {"id": DATA_ID, "label": f"Taxi Zone · {month}",
                                               "allData": values, "fields": fields}}],
        "config": {"version": "v1", "config": config},
        "info": {"app": "kepler.gl", "title": f"ND5 · Mật độ trả khách NYC 3D · {month}",
                 "description": "Nhóm Bắc/Nam theo đường tham chiếu xấp xỉ phố 60th. Phân tích mô tả."},
    }


def ranks(rows: list[dict]) -> list[dict]:
    result = []
    for period in PERIODS:
        candidates = [r for r in rows if r["time_period"] == period
                      and r["analytical_group"] != "OUTSIDE_MANHATTAN"
                      and r["dropoff_density_trips_per_km2"] is not None]
        candidates.sort(key=lambda r: (-r["dropoff_density_trips_per_km2"], r["LocationID"]))
        for rank, row in enumerate(candidates[:10], start=1):
            result.append({k: rank if k == "rank" else row[k] for k in RANK_COLUMNS})
    return result


def write_bundle(output: Path, artifact: dict, ranking: list[dict], report: dict) -> None:
    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output) as directory:
        stage = Path(directory)
        with (stage / "nd5_kepler_map.json").open("w", encoding="utf-8") as stream:
            json.dump(artifact, stream, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
        with (stage / "nd5_manhattan_top10.csv").open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=RANK_COLUMNS)
            writer.writeheader()
            writer.writerows(ranking)
        report["output_sha256"] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in stage.iterdir()}
        (stage / "nd5_data_audit.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n",
                                                  encoding="utf-8")
        for name in ("nd5_kepler_map.json", "nd5_manhattan_top10.csv", "nd5_data_audit.json"):
            (stage / name).replace(output / name)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold", type=Path, default=ROOT / "data/gold/dropoff_density_by_zone_period.csv")
    parser.add_argument("--audit", type=Path, default=ROOT / "data/benchmarks/spatial_analysis_audit.csv")
    parser.add_argument("--output", type=Path, default=ROOT / "data/gold/nd5")
    args = parser.parse_args()
    month, rows = read_gold(args.gold)
    with args.audit.open(encoding="utf-8-sig", newline="") as source:
        audit = {r["check"]: r["value"] for r in csv.DictReader(source)}
    totals = {
        "expected_gold_zone_period_rows": len(rows),
        "taxi_zone_polygon_count": len({r["LocationID"] for r in rows}),
        "gold_total_trip_count": sum(r["trip_count"] for r in rows),
    }
    for key, actual in totals.items():
        if actual != int(audit[key]):
            raise ValueError(f"Mismatch against spatial audit for {key}: {actual} vs {audit[key]}.")
    report = {
        "analysis_month": month,
        "source_sha256": {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in (args.gold, args.audit)},
        "reconciliation": totals, "rows_by_period": dict(Counter(r["time_period"] for r in rows)),
        "zone_counts_by_group": dict(Counter(r["analytical_group"] for r in rows if r["time_period"] == "Daytime")),
        "ranking_scope": "Manhattan, including ambiguous boundary zones; rank per period by unrounded density",
        "validation_scope": "Gold keys, density formula, geometry bounds and spatial audit totals; browser QA reported separately",
    }
    ranking = ranks(rows)
    write_bundle(args.output, build_map(month, rows), ranking, report)
    print(f"ND5: {len(rows)} map rows, {len(ranking)} ranking rows. Output: {args.output}")


if __name__ == "__main__":
    main()
