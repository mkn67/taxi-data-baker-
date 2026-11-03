#!/usr/bin/env python3
"""Export the spatial Gold layer as a Kepler.gl saved-map JSON (.gl)."""
from __future__ import annotations

import json
import math
import os
from pathlib import Path
from uuid import uuid4

import geopandas as gpd

ROOT = Path(__file__).resolve().parents[2]
GOLD = Path(os.environ.get("SPATIAL_GOLD_PATH", ROOT / "data/gold/dropoff_density_by_zone_period.parquet"))
OUTPUT = Path(os.environ.get("KEPLER_GL_PATH", ROOT / "data/gold/dropoff_density_by_zone_period.gl"))
DATA_ID = "nyc_dropoff_density_2026_01"


def _json_value(value):
    if value is None:
        return None
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def main() -> None:
    if not GOLD.is_file():
        raise FileNotFoundError(f"Gold GeoParquet not found: {GOLD}")
    gold = gpd.read_parquet(GOLD).to_crs("EPSG:4326")
    fields = [
        {"name": "geometry", "format": "", "type": "geojson"},
        {"name": "LocationID", "format": "", "type": "integer"},
        {"name": "borough", "format": "", "type": "string"},
        {"name": "zone", "format": "", "type": "string"},
        {"name": "analytical_group", "format": "", "type": "string"},
        {"name": "time_period", "format": "", "type": "string"},
        {"name": "trip_count", "format": "", "type": "integer"},
        {"name": "dropoff_density_trips_per_km2", "format": "", "type": "real"},
    ]
    datasets = []
    layers = []
    tooltip_fields = []
    colors = {"Daytime": [237, 125, 49, 255], "Nighttime": [52, 120, 168, 255]}
    for period in ("Daytime", "Nighttime"):
        subset = gold[gold["time_period"] == period]
        rows = []
        for row in subset.itertuples(index=False):
            record = row._asdict()
            geometry = record["geometry"]
            rows.append([
                json.dumps(geometry.__geo_interface__, separators=(",", ":")),
                _json_value(record["LocationID"]), _json_value(record["borough"]),
                _json_value(record["zone"]),
                _json_value(record["analytical_group"]), period,
                _json_value(record["trip_count"]),
                _json_value(record["dropoff_density_trips_per_km2"]),
            ])
        period_id = f"{DATA_ID}_{period.lower()}"
        datasets.append({
            "version": "v1",
            "data": {
                "id": period_id,
                "label": f"Drop-off density — {period} (Jan 2026)",
                "color": colors[period],
                "allData": rows,
                "fields": fields,
            },
        })
        layers.append({
            "id": uuid4().hex[:7], "type": "geojson",
            "config": {
                "dataId": period_id, "label": f"{period} · chuyến/km²",
                "color": colors[period], "columns": {"geojson": "geometry"},
                "isVisible": period == "Daytime",
                "visConfig": {
                    "opacity": 0.72, "thickness": 0.7, "filled": True,
                    "stroked": True, "enable3d": False, "wireframe": False,
                    "colorRange": {"name": "UberPool 6", "type": "sequential", "category": "Uber",
                                   "colors": ["#f7fbff", "#c6dbef", "#6baed6", "#2171b5", "#08306b"]},
                    "radius": 10, "sizeRange": [0, 10], "radiusRange": [0, 50],
                    "heightRange": [0, 500], "elevationScale": 5,
                },
            },
            "visualChannels": {
                "colorField": {"name": "dropoff_density_trips_per_km2", "type": "real"},
                "colorScale": "quantile", "sizeField": None, "sizeScale": "linear",
                "heightField": None, "heightScale": "linear", "radiusField": None,
                "radiusScale": "linear",
            },
        })
        tooltip_fields.append({period_id: [
            "LocationID", "zone", "borough", "analytical_group", "time_period",
            "trip_count", "dropoff_density_trips_per_km2",
        ]})

    config = {
        "visState": {
            "filters": [], "layers": layers,
            "interactionConfig": {
                "tooltip": {"fieldsToShow": {k: v for d in tooltip_fields for k, v in d.items()}, "enabled": True},
                "brush": {"size": 0.5, "enabled": False},
            },
            "layerBlending": "normal", "splitMaps": [],
            "animationConfig": {"currentTime": None, "speed": 1},
        },
        "mapState": {"bearing": 0, "dragRotate": False, "latitude": 40.75,
                     "longitude": -73.98, "pitch": 0, "zoom": 10.4, "isSplit": False},
        "mapStyle": {"styleType": "light", "topLayerGroups": {},
                     "visibleLayerGroups": {"label": True, "road": True, "border": True,
                                            "building": False, "water": True, "land": True}},
    }
    artifact = {
        "datasets": datasets,
        "config": {"version": "v1", "config": config},
        "info": {"app": "kepler.gl", "title": "NYC Taxi drop-off density by zone and period — Jan 2026"},
    }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(artifact, ensure_ascii=False, allow_nan=False), encoding="utf-8")
    print(f"Saved {OUTPUT} with {sum(len(d['data']['allData']) for d in datasets)} zone-period polygons")


if __name__ == "__main__":
    main()
