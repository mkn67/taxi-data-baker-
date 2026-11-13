#!/usr/bin/env python3
"""Audit OD dimension composite-key uniqueness for the ND3 Salting Join."""
from pathlib import Path
import os
import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OD_PATH = Path(os.environ.get("OD_MATRIX_PATH", ROOT / "data/silver/od_matrix_2026_01.parquet"))
month_token = next((token.replace("-", "_") for token in ("2026_01", "2026_02", "2026_03", "2026_04", "2026_05", "2026-01", "2026-02", "2026-03", "2026-04", "2026-05") if token in str(OD_PATH)), None)
default_audit_name = "od_matrix_key_audit.csv" if month_token in (None, "2026_01") else f"od_matrix_key_audit_{month_token}.csv"
OUT_PATH = Path(os.environ.get("OD_KEY_AUDIT_PATH", ROOT / "data/benchmarks" / default_audit_name))


def main():
    if not OD_PATH.exists():
        raise FileNotFoundError(OD_PATH)
    if OD_PATH.is_dir():
        parquet_files = sorted(OD_PATH.rglob("*.parquet"))
        if not parquet_files:
            raise FileNotFoundError(f"No parquet files inside OD matrix directory: {OD_PATH}")
        paths_sql = ", ".join("'" + str(path.resolve()).replace("'", "''") + "'" for path in parquet_files)
        source = f"read_parquet([{paths_sql}], union_by_name=true)"
    else:
        path_sql = "'" + str(OD_PATH.resolve()).replace("'", "''") + "'"
        source = f"read_parquet({path_sql})"
    con = duckdb.connect()
    try:
        result = con.execute(f"""
            WITH keys AS (
                SELECT PULocationID, DOLocationID, COUNT(*) AS copies
                FROM {source}
                GROUP BY PULocationID, DOLocationID
            )
            SELECT
                (SELECT COUNT(*) FROM {source}) AS total_rows,
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
