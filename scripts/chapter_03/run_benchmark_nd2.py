#!/usr/bin/env python3
"""CLI Runner for Nội dung 2: Aggregation Skew & Salting Benchmark (NYC HVFHV 2026).

Usage:
  python3 scripts/chapter_03/run_benchmark_nd2.py [--mode {pilot,full}] [--master <url>] [--partitions <num>]
"""

import argparse
import json
import os
import sys
import time
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from scripts.chapter_03.benchmark_engine import (
    configure_environment,
    resolve_paths,
    dataset_manifest_fingerprint,
    get_spark_master_url,
    create_benchmark_spark_session,
    SparkMetricsCollector,
    load_hotspot_pu_ids,
    aggregate_baseline,
    aggregate_uniform_salting,
    aggregate_selective_salting,
    verify_aggregation_with_duckdb,
    verify_distributed_executors,
    write_csv_preserving_history,
)


def main():
    parser = argparse.ArgumentParser(description="ND2 Aggregation Skew Benchmark Runner")
    parser.add_argument("--mode", choices=["pilot", "full"], default="full", help="Benchmark execution mode")
    parser.add_argument("--master", default=None, help="Spark Master URL")
    parser.add_argument("--partitions", type=int, default=200, help="Shuffle partitions count")
    args = parser.parse_args()

    configure_environment()
    paths = resolve_paths()
    hotspot_pu_ids = load_hotspot_pu_ids(paths["hotspots_pu_path"])
    silver_manifest = dataset_manifest_fingerprint(paths["silver_path"])

    num_iterations = 1 if args.mode == "pilot" else 3
    master_url = args.master or get_spark_master_url()
    require_standalone = args.mode == "full"
    if require_standalone and not master_url.startswith("spark://"):
        parser.error("--mode full requires a Standalone master URL (spark://...). Use --mode pilot for local functional runs.")

    print(f"\n{'='*70}")
    print(f">>> STARTING NỘI DUNG 2 BENCHMARK: AGGREGATION SKEW")
    print(f"  - Mode: {args.mode.upper()} (Iterations: {num_iterations})")
    print(f"  - Master: {master_url}")
    print(f"  - Partitions: {args.partitions}")
    print(f"  - Hotspots: {len(hotspot_pu_ids)} zones")
    print(f"{'='*70}\n")

    spark = create_benchmark_spark_session(
        app_name="02_Benchmark_Aggregation_Salting_CLI",
        master_url=master_url,
        shuffle_partitions=args.partitions,
        enable_aqe=False,
        require_standalone=require_standalone,
    )
    tier_suffix = "official_standalone" if require_standalone else (
        "local_functional" if spark.sparkContext.master.startswith("local") else "pilot_standalone"
    )
    plans_dir = os.path.join(paths["nd2_plans_dir"], tier_suffix)
    os.makedirs(plans_dir, exist_ok=True)
    collector = SparkMetricsCollector(spark)

    df_silver = spark.read.parquet(paths["silver_path"] + "/*.parquet")

    # 1. Warm-up
    print(">>> Giai đoạn Warm-up...")
    collector.start_run("warmup_base", "Warmup Baseline")
    aggregate_baseline(df_silver).collect()
    collector.end_run("warmup_base")

    collector.start_run("warmup_uni", "Warmup Uniform")
    aggregate_uniform_salting(df_silver, num_salts=8).collect()
    collector.end_run("warmup_uni")

    collector.start_run("warmup_sel", "Warmup Selective")
    aggregate_selective_salting(df_silver, hotspot_pu_ids, num_salts=8).collect()
    collector.end_run("warmup_sel")
    print(">>> Hoàn tất Warm-up!\n")

    # 2. Official Runs
    methods = [
        ("Baseline_GroupBy", lambda: aggregate_baseline(df_silver)),
        ("Uniform_Salting_S8", lambda: aggregate_uniform_salting(df_silver, num_salts=8)),
        ("Selective_Salting_S8", lambda: aggregate_selective_salting(df_silver, hotspot_pu_ids, num_salts=8)),
    ]
    orders = [[0, 1, 2], [1, 2, 0], [2, 0, 1]]

    all_run_records = []
    final_spark_results = {}

    for iteration in range(1, num_iterations + 1):
        cur_order = orders[(iteration - 1) % len(orders)]
        order_names = [methods[idx][0] for idx in cur_order]
        print(f">>> LƯỢT ĐO #{iteration}/{num_iterations} | Thứ tự: {' -> '.join(order_names)}")

        for idx in cur_order:
            method_name, method_func = methods[idx]
            job_group_id = f"nd2_cli_iter{iteration}_{method_name}"
            print(f"  -> Thực thi {method_name}...")
            collector.start_run(job_group_id, f"CLI Iter {iteration} {method_name}")

            res_df = method_func()
            collected = res_df.collect()

            m = collector.end_run(job_group_id)
            m["iteration"] = iteration
            m["method"] = method_name
            m["output_rows"] = len(collected)
            m["benchmark_mode"] = args.mode
            m["result_tier"] = tier_suffix
            m["master_url"] = spark.sparkContext.master
            m["spark_version"] = spark.version
            m["application_id"] = spark.sparkContext.applicationId
            m["run_timestamp_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            m["event_log_dir"] = paths["events_dir"]
            m["dataset_path"] = paths["silver_path"]
            m["dataset_manifest_sha256"] = silver_manifest
            m["hotspot_artifact"] = paths["hotspots_pu_path"]
            m["hotspot_pu_ids"] = json.dumps(hotspot_pu_ids)
            m["worker_evidence"] = json.dumps(
                verify_distributed_executors(spark, require_two_workers=require_standalone)
                if spark.sparkContext.master.startswith("spark://") else {}
            )
            plan_text = res_df._jdf.queryExecution().executedPlan().toString()
            plan_name = f"nd2_{method_name}_iter{iteration}_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.txt"
            plan_path = os.path.join(plans_dir, plan_name)
            with open(plan_path, "w", encoding="utf-8") as plan_file:
                plan_file.write(plan_text)
            m["executed_plan_path"] = plan_path
            m["spark.sql.adaptive.enabled"] = spark.conf.get("spark.sql.adaptive.enabled")
            m["spark.sql.shuffle.partitions"] = spark.conf.get("spark.sql.shuffle.partitions")
            m["salting_buckets"] = 8 if method_name != "Baseline_GroupBy" else None
            all_run_records.append(m)
            final_spark_results[method_name] = res_df

            shuffle_mb = m.get("shuffle_write_bytes")
            shuffle_text = f"{shuffle_mb / (1024 * 1024):.2f} MB" if shuffle_mb is not None else "unavailable"
            print(f"     Thời gian: {m['wall_clock_duration_s']:.3f}s | TSR: {m['task_skew_ratio_max']} | Shuffle Write: {shuffle_text}")

    # 3. Ground Truth Verification
    print("\n>>> Kiểm chứng độc lập DuckDB Ground Truth...")
    audit_dfs = []
    validation_by_method = {}
    for m_name, r_df in final_spark_results.items():
        passed, summary, df_audit = verify_aggregation_with_duckdb(r_df, paths["silver_path"])
        validation_by_method[m_name] = summary["status"]
        df_audit["method"] = m_name
        audit_dfs.append(df_audit)
        print(f"  - {m_name:25s}: {summary['status']} (Sai số đếm: {summary['count_mismatches']})")

    df_runs = pd.DataFrame(all_run_records)
    df_runs["validation_status"] = df_runs["method"].map(validation_by_method)
    valid_runs = df_runs[(df_runs["collector_status"] == "OK") & (df_runs["validation_status"] == "PASS")]
    excluded = len(df_runs) - len(valid_runs)
    if excluded:
        print(f">>> Excluding {excluded} run(s) with failed metrics collection or correctness validation from summary.")
    runs_csv = os.path.join(paths["benchmarks_dir"], f"benchmark_nd2_aggregation_runs_{tier_suffix}.csv")
    write_csv_preserving_history(df_runs, runs_csv)

    df_audit_all = pd.concat(audit_dfs, ignore_index=True)
    audit_csv = os.path.join(paths["benchmarks_dir"], f"benchmark_nd2_aggregation_audit_{tier_suffix}.csv")
    write_csv_preserving_history(df_audit_all, audit_csv)

    # 4. Summary Table
    if valid_runs.empty:
        spark.stop()
        raise RuntimeError("No ND2 runs passed both metrics collection and DuckDB correctness validation.")
    summary_rows = []
    for m_name, group in valid_runs.groupby("method"):
        durations = group["wall_clock_duration_s"]
        tsrs = group["task_skew_ratio_max"].dropna()
        psrs = group["partition_skew_ratio_max"].dropna()
        shuffles = group["shuffle_write_bytes"] / (1024 * 1024)
        summary_rows.append({
            "Phương án": m_name,
            "Số lượt đo": len(durations),
            "Thời gian Median (s)": round(durations.median(), 3),
            "Thời gian Mean (s)": round(durations.mean(), 3),
            "Thời gian Std (s)": round(durations.std() if len(durations) > 1 else 0.0, 3),
            "TSR Max Median": round(tsrs.median(), 2) if not tsrs.empty else "N/A",
            "PSR Median": round(psrs.median(), 2) if not psrs.empty else "N/A",
            "Shuffle Write Median (MB)": round(shuffles.median(), 2),
        })
    df_summary = pd.DataFrame(summary_rows)
    summary_csv = os.path.join(paths["benchmarks_dir"], f"benchmark_nd2_aggregation_summary_{tier_suffix}.csv")
    write_csv_preserving_history(df_summary, summary_csv)

    print(f"\n>>> BẢNG TỔNG HỢP HIỆU NĂNG NỘI DUNG 2:")
    print(df_summary.to_string(index=False))
    print(f"\n>>> Tệp đầu ra đã lưu: {runs_csv}, {summary_csv}, {audit_csv}")

    spark.stop()


if __name__ == "__main__":
    main()

