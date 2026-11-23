#!/usr/bin/env python3
"""Generate reproducible synthetic OD joins and a frozen cell split for WP5."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def fingerprint(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_fact(path: Path, rows: int, exponent: float, seed: int) -> dict:
    rng = np.random.default_rng(seed)
    ranks = np.arange(1, 257, dtype=np.float64)
    weights = ranks ** (-exponent)
    pickup = rng.choice(np.arange(1, 257, dtype=np.int32), rows, p=weights / weights.sum())
    # Nonconstant timestamps prevent the salting hash from collapsing every hot key.
    seconds = rng.integers(0, 86400, rows, dtype=np.int64)
    base = np.datetime64("2026-01-15T00:00:00", "s")
    table = pa.table({
        "PULocationID": pickup,
        "DOLocationID": np.ones(rows, dtype=np.int32),
        "pickup_datetime": base + seconds.astype("timedelta64[s]"),
        "dropoff_datetime": base + (seconds + 600).astype("timedelta64[s]"),
        # Both vary at row level and are consumed by the benchmark aggregate.
        # This prevents Spark from treating the fact side as a tiny projected
        # relation and broadcasting it when the dimension grows.
        "trip_miles": rng.uniform(0.5, 25.0, rows),
        "base_passenger_fare": rng.uniform(2.0, 90.0, rows),
    })
    pq.write_table(table, path, compression="snappy")
    counts = np.bincount(pickup, minlength=257)[1:]
    threshold = int(np.sort(counts)[math.ceil(.95 * len(counts)) - 1])
    hot_ids = np.flatnonzero(counts >= threshold) + 1
    return {"fact_rows": rows, "hotspot_ids": hot_ids.tolist(), "p95_threshold": threshold,
            "hotspot_trip_share": float(counts[hot_ids - 1].sum() / rows)}


def write_dimension(path: Path, filler_rows: int) -> dict:
    ids = np.arange(1, 257 + filler_rows, dtype=np.int32)
    # Distinct filler borough strings keep the scanned dimension bytes material.
    borough = [f"PU_{int(i):08d}" for i in ids]
    table = pa.table({"PULocationID": ids, "DOLocationID": np.ones(len(ids), dtype=np.int32),
                      "PU_Borough": borough, "DO_Borough": ["DO"] * len(ids)})
    pq.write_table(table, path, compression="snappy")
    return {"dimension_rows": len(ids), "matched_keys": 256, "filler_rows": filler_rows}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--fact-rows", type=int, default=2_000_000)
    parser.add_argument("--seed", type=int, default=20261123)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    skew_levels = [("uniform", 0.0), ("moderate", 1.0), ("strong", 2.0)]
    dim_levels = [("tiny", 0), ("small", 500_000), ("medium", 2_000_000), ("large", 5_000_000)]
    facts = {}
    for index, (label, exponent) in enumerate(skew_levels):
        path = args.output / f"fact_{label}.parquet"
        info = write_fact(path, args.fact_rows, exponent, args.seed + index)
        hotpath = args.output / f"hotspots_{label}.parquet"
        pq.write_table(pa.table({"PULocationID": np.array(info["hotspot_ids"], dtype=np.int32)}), hotpath)
        facts[label] = {**info, "zipf_exponent": exponent, "fact_path": str(path),
                        "fact_sha256": fingerprint(path), "hotspot_path": str(hotpath)}
    dims = {}
    for label, filler in dim_levels:
        path = args.output / f"dimension_{label}.parquet"
        info = write_dimension(path, filler)
        dims[label] = {**info, "dimension_path": str(path), "dimension_parquet_bytes": path.stat().st_size,
                       "dimension_sha256": fingerprint(path)}
    cells = []
    for i, (skew_label, _) in enumerate(skew_levels):
        for j, (dim_label, _) in enumerate(dim_levels):
            cells.append({"cell_id": f"{skew_label}_{dim_label}", "skew": skew_label,
                          "dimension_level": dim_label, "split": "held_out" if (i + j) % 4 == 0 else "development",
                          "fact_path": facts[skew_label]["fact_path"],
                          "hotspot_path": facts[skew_label]["hotspot_path"],
                          "dimension_path": dims[dim_label]["dimension_path"],
                          "dimension_parquet_bytes": dims[dim_label]["dimension_parquet_bytes"]})
    manifest = {"generator": "fixed-seed numpy Zipf pickup distribution; unique OD keys; filler dimension rows",
                "seed": args.seed, "fact_rows": args.fact_rows, "skew_levels": facts, "dimension_levels": dims,
                "split_rule": "held_out if (skew_index + dimension_index) mod 4 == 0; frozen before measurement",
                "cells": cells,
                "warning": "Parquet bytes are observed storage bytes, not broadcast memory bytes. Synthetic results are separate from NYC."}
    (args.output / "synthetic_manifest.json").write_text(json.dumps(manifest, indent=2))
    print(json.dumps({"cells": len(cells), "held_out": sum(c["split"] == "held_out" for c in cells),
                      "dimension_bytes": {k: v["dimension_parquet_bytes"] for k, v in dims.items()}}, indent=2))


if __name__ == "__main__":
    main()
