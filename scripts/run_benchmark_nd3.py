#!/usr/bin/env python3
"""CLI Runner for Nội dung 3: Join Skew Benchmark (NYC HVFHV 2026).

Usage:
  python3 scripts/run_benchmark_nd3.py [--mode {pilot,full}] [--master <url>] [--partitions <num>]
"""

import argparse
import os
import json
import sys
import time
import pandas as pd
from pyspark.sql import functions as F

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from scripts.benchmark_engine import (
    configure_environment,
    resolve_paths,
    dataset_manifest_fingerprint,
    get_spark_master_url,
    create_benchmark_spark_session,
    SparkMetricsCollector,
    load_hotspot_pu_ids,
    join_baseline_smj,
    join_aqe_skew,
    join_salting,
    verify_join_with_duckdb,
    verify_distributed_executors,
    write_csv_preserving_history,
)


def materialize_join_result(joined_df):
    return joined_df.groupBy("PU_Borough", "DO_Borough").agg(
        F.count("*").alias("cnt"),
        F.round(F.sum("trip_miles"), 2).alias("tot_miles"),
        F.round(F.sum("base_passenger_fare"), 2).alias("tot_fare")
    ).collect()


def main():
    parser = argparse.ArgumentParser(description="ND3 Join Skew Benchmark Runner")
    parser.add_argument("--mode", choices=["pilot", "full"], default="full", help="Benchmark execution mode")
    parser.add_argument("--master", default=None, help="Spark Master URL")
    parser.add_argument("--partitions", type=int, default=200, help="Shuffle partitions count")
    args = parser.parse_args()

    configure_environment()
    paths = resolve_paths()
    hotspot_pu_ids = load_hotspot_pu_ids(paths["hotspots_pu_path"])
    silver_manifest = dataset_manifest_fingerprint(paths["silver_path"])
    od_manifest = dataset_manifest_fingerprint(paths["od_matrix_path"])

    num_iterations = 1 if args.mode == "pilot" else 3
    master_url = args.master or get_spark_master_url()
    require_standalone = args.mode == "full"
    if require_standalone and not master_url.startswith("spark://"):
        parser.error("--mode full requires a Standalone master URL (spark://...). Use --mode pilot for local functional runs.")

    print(f"\n{'='*70}")
    print(f">>> STARTING NỘI DUNG 3 BENCHMARK: DISTRIBUTED JOIN SKEW")
    print(f"  - Mode: {args.mode.upper()} (Iterations: {num_iterations})")
    print(f"  - Master: {master_url}")
    print(f"  - Partitions: {args.partitions}")
    print(f"  - Hotspots: {len(hotspot_pu_ids)} zones")
    print(f"{'='*70}\n")

    spark = create_benchmark_spark_session(
        app_name="03_Benchmark_Join_Skew_CLI",
        master_url=master_url,
        shuffle_partitions=args.partitions,
        enable_aqe=False,
        require_standalone=require_standalone,
    )
    tier_suffix = "official_standalone" if require_standalone else (
        "local_functional" if spark.sparkContext.master.startswith("local") else "pilot_standalone"
    )
    plans_dir = os.path.join(paths["plans_dir"], tier_suffix)
    os.makedirs(plans_dir, exist_ok=True)
    collector = SparkMetricsCollector(spark)

    df_trips = spark.read.parquet(paths["silver_path"] + "/*.parquet")
    df_od = spark.read.parquet(paths["od_matrix_path"])

    # 1. Warm-up
    print(">>> Giai đoạn Warm-up...")
    collector.start_run("warmup_join_base", "Warmup Baseline SMJ")
    materialize_join_result(join_baseline_smj(spark, df_trips, df_od))
    collector.end_run("warmup_join_base")

    collector.start_run("warmup_join_aqe", "Warmup AQE")
    materialize_join_result(join_aqe_skew(spark, df_trips, df_od, skew_factor=2, skew_threshold_mb=16))
    collector.end_run("warmup_join_aqe")

    collector.start_run("warmup_join_salt", "Warmup Salting")
    materialize_join_result(join_salting(spark, df_trips, df_od, hotspot_pu_ids, num_salts=8))
    collector.end_run("warmup_join_salt")
    print(">>> Hoàn tất Warm-up!\n")

    # 2. Official Runs
    methods_join = [
        ("Baseline_SMJ", lambda: join_baseline_smj(spark, df_trips, df_od)),
        ("Spark_AQE_Skew_Join", lambda: join_aqe_skew(spark, df_trips, df_od, skew_factor=2, skew_threshold_mb=16)),
        ("Salting_Join_S8", lambda: join_salting(spark, df_trips, df_od, hotspot_pu_ids, num_salts=8)),
    ]
    orders_join = [[0, 1, 2], [1, 2, 0], [2, 0, 1]]

    all_join_runs = []
    final_join_results = {}

    for iteration in range(1, num_iterations + 1):
        cur_order = orders_join[(iteration - 1) % len(orders_join)]
        order_names = [methods_join[idx][0] for idx in cur_order]
        print(f">>> LƯỢT ĐO JOIN #{iteration}/{num_iterations} | Thứ tự: {' -> '.join(order_names)}")

        for idx in cur_order:
            method_name, method_func = methods_join[idx]
            job_group_id = f"nd3_cli_iter{iteration}_{method_name}"
            print(f"  -> Thực thi {method_name}...")
            collector.start_run(job_group_id, f"CLI Join Iter {iteration} {method_name}")

            joined_df = method_func()
            res_borough = materialize_join_result(joined_df)

            m = collector.end_run(job_group_id)
            m["iteration"] = iteration
            m["method"] = method_name
            m["borough_groups"] = len(res_borough)
            m["benchmark_mode"] = args.mode
            m["result_tier"] = tier_suffix
            m["master_url"] = spark.sparkContext.master
            m["spark_version"] = spark.version
            m["application_id"] = spark.sparkContext.applicationId
            m["run_timestamp_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            m["event_log_dir"] = paths["events_dir"]
            m["trip_dataset_path"] = paths["silver_path"]
            m["od_dataset_path"] = paths["od_matrix_path"]
            m["trip_dataset_manifest_sha256"] = silver_manifest
            m["od_dataset_manifest_sha256"] = od_manifest
            m["hotspot_artifact"] = paths["hotspots_pu_path"]
            m["hotspot_pu_ids"] = json.dumps(hotspot_pu_ids)
            m["worker_evidence"] = json.dumps(
                verify_distributed_executors(spark, require_two_workers=require_standalone)
                if spark.sparkContext.master.startswith("spark://") else {}
            )
            plan_text = joined_df._jdf.queryExecution().executedPlan().toString()
            plan_name = f"nd3_{method_name}_iter{iteration}_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.txt"
            plan_path = os.path.join(plans_dir, plan_name)
            with open(plan_path, "w", encoding="utf-8") as plan_file:
                plan_file.write(plan_text)
            m["executed_plan_path"] = plan_path
            m["spark.sql.adaptive.enabled"] = spark.conf.get("spark.sql.adaptive.enabled")
            m["spark.sql.adaptive.skewJoin.enabled"] = spark.conf.get("spark.sql.adaptive.skewJoin.enabled", "false")
            m["spark.sql.adaptive.skewJoin.skewedPartitionFactor"] = spark.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "2")
            m["spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes"] = spark.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "33554432")
            m["spark.sql.autoBroadcastJoinThreshold"] = spark.conf.get("spark.sql.autoBroadcastJoinThreshold", "10485760")
            all_join_runs.append(m)
            final_join_results[method_name] = joined_df

            read_mb = m.get("shuffle_read_bytes")
            spill_mb = m.get("memory_bytes_spilled")
            read_text = f"{read_mb / (1024 * 1024):.2f} MB" if read_mb is not None else "unavailable"
            spill_text = f"{spill_mb / (1024 * 1024):.2f} MB" if spill_mb is not None else "unavailable"
            print(f"     Thời gian: {m['wall_clock_duration_s']:.3f}s | TSR: {m['task_skew_ratio_max']} | Shuffle Read: {read_text} | Spill Mem: {spill_text}")

    # 3. Ground Truth Verification
    print("\n>>> Kiểm chứng độc lập DuckDB Ground Truth...")
    join_audit_dfs = []
    validation_by_method = {}
    for m_name, r_df in final_join_results.items():
        passed, summary, df_audit = verify_join_with_duckdb(
            r_df, paths["silver_path"], paths["od_matrix_path"], fare_tol=0.05, miles_tol=0.05
        )
        validation_by_method[m_name] = summary["status"]
        df_audit["method"] = m_name
        join_audit_dfs.append(df_audit)
        print(f"  - {m_name:25s}: {summary['status']} (Số dòng: {summary['total_trips_spark']:,} / {summary['total_trips_duck']:,})")

    df_join_runs = pd.DataFrame(all_join_runs)
    df_join_runs["validation_status"] = df_join_runs["method"].map(validation_by_method)
    valid_join_runs = df_join_runs[(df_join_runs["collector_status"] == "OK") & (df_join_runs["validation_status"] == "PASS")]
    excluded = len(df_join_runs) - len(valid_join_runs)
    if excluded:
        print(f">>> Excluding {excluded} run(s) with failed metrics collection or correctness validation from summary.")
    join_runs_csv = os.path.join(paths["benchmarks_dir"], f"benchmark_nd3_join_runs_{tier_suffix}.csv")
    write_csv_preserving_history(df_join_runs, join_runs_csv)

    df_join_audit_all = pd.concat(join_audit_dfs, ignore_index=True)
    join_audit_csv_path = os.path.join(paths["benchmarks_dir"], f"benchmark_nd3_join_audit_{tier_suffix}.csv")
    write_csv_preserving_history(df_join_audit_all, join_audit_csv_path)

    # 4. Summary Table
    if valid_join_runs.empty:
        spark.stop()
        raise RuntimeError("No ND3 runs passed both metrics collection and DuckDB correctness validation.")
    summary_join_rows = []
    for m_name, group in valid_join_runs.groupby("method"):
        durations = group["wall_clock_duration_s"]
        tsrs = group["task_skew_ratio_max"].dropna()
        psrs = group["partition_skew_ratio_max"].dropna()
        shuffles = group["shuffle_read_bytes"] / (1024 * 1024)
        spills = group["memory_bytes_spilled"] / (1024 * 1024)
        summary_join_rows.append({
            "Phương án": m_name,
            "Số lượt đo": len(durations),
            "Thời gian Median (s)": round(durations.median(), 3),
            "Thời gian Mean (s)": round(durations.mean(), 3),
            "Thời gian Std (s)": round(durations.std() if len(durations) > 1 else 0.0, 3),
            "TSR Max Median": round(tsrs.median(), 2) if not tsrs.empty else "N/A",
            "PSR Median": round(psrs.median(), 2) if not psrs.empty else "N/A",
            "Shuffle Read Median (MB)": round(shuffles.median(), 2),
            "Memory Spill Median (MB)": round(spills.median(), 2),
        })
    df_join_summary = pd.DataFrame(summary_join_rows)
    join_summary_csv = os.path.join(paths["benchmarks_dir"], f"benchmark_nd3_join_summary_{tier_suffix}.csv")
    write_csv_preserving_history(df_join_summary, join_summary_csv)

    print(f"\n>>> BẢNG TỔNG HỢP HIỆU NĂNG NỘI DUNG 3:")
    print(df_join_summary.to_string(index=False))
    print(f"\n>>> Tệp đầu ra đã lưu: {join_runs_csv}, {join_summary_csv}, {join_audit_csv_path}")

    spark.stop()


if __name__ == "__main__":
    main()

