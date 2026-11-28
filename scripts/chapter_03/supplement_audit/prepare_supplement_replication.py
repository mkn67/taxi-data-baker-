#!/usr/bin/env python3
"""Prepare two dimension interventions without changing the matched join rows."""
import argparse
import hashlib
import json
from pathlib import Path

import duckdb
import pyarrow.parquet as pq


def sha256(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(8 * 1024 * 1024):
            h.update(chunk)
    return h.hexdigest()


def quote(path):
    return "'" + str(path).replace("'", "''") + "'"


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--fact", type=Path, required=True)
    p.add_argument("--dimension", type=Path, required=True)
    p.add_argument("--hotspots", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--high-filler-hot", type=int, default=100000)
    args = p.parse_args()
    if args.high_filler_hot <= 0:
        raise ValueError("high-filler-hot must be positive")
    args.output.mkdir(parents=True, exist_ok=False)
    con = duckdb.connect()
    con.execute("SET threads = 4")
    con.execute("SET memory_limit = '2GB'")
    con.execute(f"CREATE VIEW fact AS SELECT * FROM read_parquet({quote(args.fact.resolve())})")
    con.execute(f"CREATE VIEW dim AS SELECT * FROM read_parquet({quote(args.dimension.resolve())})")
    con.execute(f"CREATE VIEW hotspots AS SELECT DISTINCT PULocationID FROM read_parquet({quote(args.hotspots.resolve())})")
    fact_keys = con.execute("SELECT count(*), min(DOLocationID), max(DOLocationID) FROM (SELECT DISTINCT PULocationID,DOLocationID FROM fact)").fetchone()
    if fact_keys != (256, 1, 1):
        raise ValueError(f"Expected fact keys PU=1..256 with DO=1; observed {fact_keys}")
    invalid = con.execute("SELECT count(*) FROM dim WHERE PULocationID IS NULL OR DOLocationID IS NULL").fetchone()[0]
    if invalid:
        raise ValueError("Dimension has null keys")
    old_rows = con.execute("SELECT count(*) FROM dim").fetchone()[0]
    unique = con.execute("SELECT count(*) FROM (SELECT DISTINCT PULocationID,DOLocationID FROM dim)").fetchone()[0]
    if old_rows != unique:
        raise ValueError("Dimension keys are not unique")
    available = con.execute("SELECT count(*) FROM dim WHERE PULocationID > 256 AND PULocationID <= ? AND DOLocationID=1", [256 + args.high_filler_hot]).fetchone()[0]
    if available != args.high_filler_hot:
        raise ValueError("Filler domain not as expected")
    hot_key = con.execute("SELECT min(PULocationID) FROM hotspots").fetchone()[0]
    source_fingerprint = {"fact":sha256(args.fact), "dimension":sha256(args.dimension), "hotspots":sha256(args.hotspots)}
    paths = {}
    for label in ("low", "high"):
        output = (args.output / f"dimension_{label}.parquet").resolve()
        if label == "low":
            query = "SELECT * FROM dim"
        else:
            query = (f"SELECT CASE WHEN PULocationID > 256 AND PULocationID <= {256 + args.high_filler_hot} AND DOLocationID=1 THEN CAST({int(hot_key)} AS INTEGER) ELSE PULocationID END AS PULocationID, "
                     f"CASE WHEN PULocationID > 256 AND PULocationID <= {256 + args.high_filler_hot} AND DOLocationID=1 THEN CAST(PULocationID AS INTEGER) ELSE DOLocationID END AS DOLocationID, PU_Borough, DO_Borough FROM dim")
        con.execute(f"COPY ({query}) TO {quote(output)} (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 122880)")
        paths[label] = output
        con.execute(f"CREATE VIEW {label} AS SELECT * FROM read_parquet({quote(output)})")
    for label, path in paths.items():
        count, unique_count, hot_count = con.execute(f"SELECT count(*), count(DISTINCT (PULocationID,DOLocationID)), count(*) FILTER (WHERE PULocationID IN (SELECT PULocationID FROM hotspots)) FROM {label}").fetchone()
        unmatched = con.execute(f"SELECT count(*) FROM (SELECT PULocationID,DOLocationID,PU_Borough,DO_Borough FROM {label} WHERE (PULocationID,DOLocationID) IN (SELECT DISTINCT PULocationID,DOLocationID FROM fact) EXCEPT SELECT PULocationID,DOLocationID,PU_Borough,DO_Borough FROM dim WHERE (PULocationID,DOLocationID) IN (SELECT DISTINCT PULocationID,DOLocationID FROM fact))").fetchone()[0]
        if count != old_rows or unique_count != count or unmatched:
            raise ValueError(f"Invariant failure for {label}: {count=} {unique_count=} {unmatched=}")
        meta = pq.ParquetFile(path).metadata
        paths[label] = {"path":str(path),"sha256":sha256(path),"bytes":path.stat().st_size,"rows":count,
                        "hot_dimension_rows":hot_count,"salt_extra_rows_expected":hot_count*7,"row_groups":meta.num_row_groups}
    if paths["high"]["hot_dimension_rows"] - paths["low"]["hot_dimension_rows"] != args.high_filler_hot:
        raise ValueError("Unexpected change in hot rows")
    manifest = {"purpose":"pilot intervention; changes unmatched dimension keys and may change physical encoding/hash distribution", "fact_path":str(args.fact.resolve()),"hotspots_path":str(args.hotspots.resolve()),"original_dimension_path":str(args.dimension.resolve()),"source_sha256":source_fingerprint,"high_filler_hot":args.high_filler_hot,"fact_key_count":fact_keys[0],"dimensions":paths,"s":8}
    (args.output/"manifest.json").write_text(json.dumps(manifest,indent=2))
    print(json.dumps(manifest,indent=2))


if __name__ == "__main__":
    main()
