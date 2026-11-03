#!/usr/bin/env python3
"""Audit OD dimension composite-key uniqueness for the ND3 Salting Join."""
from pathlib import Path
import os
import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OD_PATH = Path(os.environ.get("OD_MATRIX_PATH", ROOT / "data/silver/od_matrix_2026_01.parquet"))
OUT_PATH = Path(os.environ.get("OD_KEY_AUDIT_PATH", ROOT / "data/benchmarks/od_matrix_key_audit.csv"))


def main():
    if not OD_PATH.is_file():
        raise FileNotFoundError(OD_PATH)
    con = duckdb.connect()
    try:
        path_sql = "'" + str(OD_PATH.resolve()).replace("'", "''") + "'"
        result = con.execute(f"""
            WITH keys AS (
                SELECT PULocationID, DOLocationID, COUNT(*) AS copies
                FROM read_parquet({path_sql})
                GROUP BY PULocationID, DOLocationID
            )
            SELECT
                (SELECT COUNT(*) FROM read_parquet({path_sql})) AS total_rows,
                COUNT(*) AS distinct_composite_keys,
                COUNT(*) FILTER (WHERE PULocationID IS NULL OR DOLocationID IS NULL) AS null_key_groups,
                COUNT(*) FILTER (WHERE copies > 1) AS duplicate_composite_keys,
                COALESCE(SUM(CASE WHEN copies > 1 THEN copies - 1 ELSE 0 END), 0) AS excess_duplicate_rows
            FROM keys
        """).fetchone()
    finally:
        con.close()
    labels = ["od_total_rows", "distinct_composite_keys", "null_composite_key_groups",
              "duplicate_composite_keys", "excess_duplicate_rows"]
    rows = [{"check": label, "value": int(value)} for label, value in zip(labels, result)]
    rows += [
        {"check": "input_od_path", "value": str(OD_PATH)},
        {"check": "salting_join_dimension_unique", "value": int(result[3] == 0 and result[2] == 0)},
    ]
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(OUT_PATH, index=False)
    print(pd.DataFrame(rows).to_string(index=False))
    print(f"Saved: {OUT_PATH}")

if __name__ == "__main__":
    main()
