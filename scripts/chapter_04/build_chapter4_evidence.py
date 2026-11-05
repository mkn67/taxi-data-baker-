#!/usr/bin/env python3
"""Rebuild Ch. 4 summary/audit tables from saved official run artifacts.

Run from the repository root with:
    python scripts/chapter_04/build_chapter4_evidence.py

This reads saved CSV evidence; it does not launch Spark or modify run files.
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
BENCH = ROOT / "data/benchmarks"
OUT = BENCH / "chapter4_evidence"
MONTHS = range(1, 6)
MIB = 1024 * 1024
ND2 = ["Baseline_GroupBy", "Selective_Salting_S8", "Uniform_Salting_S8"]
ND3 = ["Baseline_SMJ", "Spark_AQE_Skew_Join", "Salting_Join_S8"]


def read(path: Path) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(path)
    return pd.read_csv(path)


def validate_run_file(df: pd.DataFrame, methods: list[str], name: str) -> None:
    required = {"method", "iteration", "collector_status", "validation_status", "result_tier", "benchmark_mode", "spark_version", "worker_evidence"}
    if required - set(df.columns):
        raise ValueError(f"{name} missing columns: {sorted(required - set(df.columns))}")
    if set(df.result_tier.dropna()) != {"official_standalone"} or set(df.benchmark_mode.dropna()) != {"full"}:
        raise ValueError(f"{name} contains a non-official/non-full row")
    if set(df.spark_version.dropna().astype(str)) != {"3.5.9"}:
        raise ValueError(f"{name} does not consistently record Spark 3.5.9")
    if not df.collector_status.eq("OK").all() or not df.validation_status.eq("PASS").all():
        raise ValueError(f"{name} has collector or validation failures")
    counts = df.groupby("method").size().to_dict()
    if set(counts) != set(methods) or any(counts.get(m) != 3 for m in methods):
        raise ValueError(f"{name}: expected 3 iterations/method; found {counts}")
    for item in df.worker_evidence.dropna():
        info = json.loads(item) if isinstance(item, str) else item
        if info.get("executor_count_active") != 2:
            raise ValueError(f"{name} lacks evidence of two active workers")


def load_monthly() -> tuple[pd.DataFrame, pd.DataFrame]:
    parts2, parts3 = [], []
    for month in MONTHS:
        folder = BENCH if month == 1 else BENCH / "scaling" / f"2026_{month:02}"
        d2 = read(folder / "benchmark_nd2_aggregation_runs_official_standalone.csv")
        d3 = read(folder / "benchmark_nd3_join_runs_official_standalone.csv")
        validate_run_file(d2, ND2, f"ND2 2026-{month:02}")
        validate_run_file(d3, ND3, f"ND3 2026-{month:02}")
        d2["month"] = f"2026-{month:02}"
        d3["month"] = f"2026-{month:02}"
        parts2.append(d2)
        parts3.append(d3)
    return pd.concat(parts2, ignore_index=True), pd.concat(parts3, ignore_index=True)


def summarize(df: pd.DataFrame, methods: list[str], shuffle_column: str) -> pd.DataFrame:
    rows = []
    for method in methods:
        group = df.loc[df.method.eq(method)]
        rows.append({
            "method": method,
            "n": len(group),
            "wall_median_s": group.wall_clock_duration_s.median(),
            "wall_min_s": group.wall_clock_duration_s.min(),
            "wall_max_s": group.wall_clock_duration_s.max(),
            "selected_stage_tasks_median": group.total_tasks.median(),
            "tsr_median": group.task_skew_ratio_max.median(),
            "psr_median": group.partition_skew_ratio_max.median(),
            "shuffle_median_mib": group[shuffle_column].median() / MIB,
            "memory_spill_median_mib": group.memory_bytes_spilled.median() / MIB,
            "disk_spill_median_mib": group.disk_bytes_spilled.median() / MIB,
        })
    return pd.DataFrame(rows)


def audit_rows(df: pd.DataFrame, workload: str) -> list[dict[str, object]]:
    methods = ND2 if workload == "ND2" else ND3
    tolerances = {"count_diff": 0, "fare_diff": 1e-4, "miles_diff": 1e-2} if workload == "ND2" else {
        "count_diff": 0, "fare_diff": 0.05, "miles_diff": 0.05, "distinct_pu_diff": 0,
    }
    out = []
    for method in methods:
        group = df.loc[df.method.eq(method)]
        if group.empty or not all((group[col].abs() <= tol).all() for col, tol in tolerances.items()):
            raise ValueError(f"{workload} DuckDB audit fails for {method}")
        out.append({
            "workload": workload, "method": method, "audit_groups_january": len(group),
            "all_diffs_within_tolerance": True,
            "max_count_diff": group.count_diff.abs().max(),
            "max_fare_diff": group.fare_diff.abs().max(),
            "max_miles_diff": group.miles_diff.abs().max(),
            "max_distinct_pu_diff": group.distinct_pu_diff.abs().max() if "distinct_pu_diff" in group else None,
            "tolerances": json.dumps(tolerances),
        })
    return out


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    nd2, nd3 = load_monthly()
    nd2_jan, nd3_jan = nd2.loc[nd2.month.eq("2026-01")], nd3.loc[nd3.month.eq("2026-01")]
    summarize(nd2_jan, ND2, "shuffle_write_bytes").to_csv(OUT / "nd2_january_summary.csv", index=False)
    summarize(nd3_jan, ND3, "shuffle_read_bytes").to_csv(OUT / "nd3_january_summary.csv", index=False)

    correctness = audit_rows(read(BENCH / "benchmark_nd2_aggregation_audit_official_standalone.csv"), "ND2")
    correctness += audit_rows(read(BENCH / "benchmark_nd3_join_audit_official_standalone.csv"), "ND3")
    pd.DataFrame(correctness).to_csv(OUT / "duckdb_january_correctness.csv", index=False)

    validity = []
    for workload, df, methods in [("ND2", nd2, ND2), ("ND3", nd3, ND3)]:
        for method in methods:
            group = df.loc[df.method.eq(method)]
            validity.append({
                "workload": workload, "method": method, "total_runs_jan_may": len(group),
                "collector_ok": int(group.collector_status.eq("OK").sum()),
                "validation_pass": int(group.validation_status.eq("PASS").sum()),
                "excluded": int((~group.collector_status.eq("OK") | ~group.validation_status.eq("PASS")).sum()),
            })
    pd.DataFrame(validity).to_csv(OUT / "official_run_validity_jan_may.csv", index=False)

    od = read(BENCH / "od_matrix_key_audit.csv").set_index("check").value
    if int(od["od_total_rows"]) != int(od["distinct_composite_keys"]) or int(od["null_composite_key_groups"]) or int(od["duplicate_composite_keys"]):
        raise ValueError("OD composite-key preflight failed")
    pd.DataFrame([{"od_rows": int(od["od_total_rows"]), "distinct_keys": int(od["distinct_composite_keys"]),
                   "null_key_groups": int(od["null_composite_key_groups"]), "duplicate_key_groups": int(od["duplicate_composite_keys"])}]).to_csv(OUT / "od_key_preflight.csv", index=False)
    print(f"Generated Chapter 4 evidence tables: {OUT.relative_to(ROOT)}")
    print(f"Official validated run records across Jan–May: ND2={len(nd2)}, ND3={len(nd3)}, total={len(nd2) + len(nd3)}")


if __name__ == "__main__":
    main()
