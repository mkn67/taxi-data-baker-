#!/usr/bin/env python3
"""Run the frozen six-arm, two-workload Phase C mechanism confirmation."""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from pathlib import Path

import pandas as pd
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from scripts.chapter_03.benchmark_engine import (  # noqa: E402
    SparkMetricsCollector,
    configure_environment,
    create_benchmark_spark_session,
    dataset_manifest_fingerprint,
    verify_distributed_executors,
    verify_join_with_duckdb,
)

ARMS = {
    "A_SMJ": {"aqe": False, "coalesce": False, "skew": False, "salt": False},
    "A0_AQE_NO_COALESCE": {"aqe": True, "coalesce": False, "skew": False, "salt": False},
    "B_AQE_COALESCE": {"aqe": True, "coalesce": True, "skew": False, "salt": False},
    "C_AQE_COALESCE_SKEW": {"aqe": True, "coalesce": True, "skew": True, "salt": False},
    "D_SALTING_S8": {"aqe": False, "coalesce": False, "skew": False, "salt": True},
    "E_SALTING_S8_AQE_COALESCE": {"aqe": True, "coalesce": True, "skew": False, "salt": True},
}


def clear_group(sc):
    for key in ("spark.jobGroup.id", "spark.job.description", "spark.job.interruptOnCancel"):
        sc.setLocalProperty(key, None)


def parse_args():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--workloads", type=Path, required=True, help="JSON manifest with exactly two named workloads")
    ap.add_argument("--output-dir", type=Path, required=True, help="New, empty output directory")
    ap.add_argument("--blocks", type=int, default=8)
    ap.add_argument("--partitions", type=int, default=200)
    ap.add_argument("--skew-factor", type=int, default=2)
    ap.add_argument("--skew-threshold-mb", type=int, default=16)
    ap.add_argument("--order-seed", type=int, default=20261126)
    ap.add_argument("--master", default="spark://spark-master:7077")
    return ap.parse_args()


def read_workloads(path: Path):
    manifest = json.loads(path.read_text())
    workloads = manifest.get("workloads", [])
    if len(workloads) != 2 or len({w["workload_id"] for w in workloads}) != 2:
        raise ValueError("Manifest must contain exactly two uniquely named workloads.")
    for workload in workloads:
        for key in ("fact_path", "dimension_path", "hotspot_path"):
            if not Path(workload[key]).exists():
                raise FileNotFoundError(f"{workload['workload_id']} {key} missing: {workload[key]}")
    return manifest, workloads


def set_arm(spark, arm, partitions, factor, threshold_mb):
    cfg = ARMS[arm]
    values = {
        "spark.sql.adaptive.enabled": str(cfg["aqe"]).lower(),
        "spark.sql.adaptive.coalescePartitions.enabled": str(cfg["coalesce"]).lower(),
        "spark.sql.adaptive.skewJoin.enabled": str(cfg["skew"]).lower(),
        "spark.sql.adaptive.skewJoin.skewedPartitionFactor": str(factor),
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": str(threshold_mb * 1024 * 1024),
        "spark.sql.adaptive.autoBroadcastJoinThreshold": "-1",
        "spark.sql.autoBroadcastJoinThreshold": "-1",
        "spark.sql.shuffle.partitions": str(partitions),
    }
    for key, value in values.items():
        spark.conf.set(key, value)
    effective = {key: spark.conf.get(key) for key in values}
    for key in values:
        if effective[key].lower() != values[key].lower():
            raise RuntimeError(f"Spark configuration mismatch for {key}: requested {values[key]}, got {effective[key]}")
    return effective


def build_join(spark, trips, dimension, hotspots, arm):
    if not ARMS[arm]["salt"]:
        return trips.hint("merge").join(
            dimension.hint("merge"), ["PULocationID", "DOLocationID"], "inner"
        )
    trips_salted = trips.withColumn(
        "salt",
        F.when(
            F.col("PULocationID").isin(hotspots),
            F.pmod(F.abs(F.hash("pickup_datetime", "dropoff_datetime", "trip_miles")), F.lit(8)),
        ).otherwise(F.lit(0)),
    )
    salt_array = F.when(
        F.col("PULocationID").isin(hotspots), F.array([F.lit(i) for i in range(8)])
    ).otherwise(F.array([F.lit(0)]))
    dimension_salted = dimension.withColumn("salt", F.explode(salt_array))
    return trips_salted.hint("merge").join(
        dimension_salted.hint("merge"), ["PULocationID", "DOLocationID", "salt"], "inner"
    ).drop("salt")


def materialize(joined):
    return joined.groupBy("PU_Borough", "DO_Borough").agg(
        F.count("*").alias("cnt"),
        F.round(F.sum("trip_miles"), 2).alias("tot_miles"),
        F.round(F.sum("base_passenger_fare"), 2).alias("tot_fare"),
    )


def balanced_orders(seed, blocks):
    rng = random.Random(seed)
    base = list(ARMS)
    rng.shuffle(base)
    rotations = [base[i:] + base[:i] for i in range(len(base))]
    rng.shuffle(rotations)
    order = rotations + [list(rotations[i]) for i in range(blocks - len(rotations))]
    rng.shuffle(order)
    return order[:blocks]


def main():
    args = parse_args()
    if args.blocks != 8:
        raise SystemExit("Frozen confirmation protocol requires exactly 8 blocks per workload.")
    if args.partitions < 1 or args.skew_factor < 1 or args.skew_threshold_mb < 1:
        raise SystemExit("Partitions, skew factor, and skew threshold must be positive.")
    manifest, workloads = read_workloads(args.workloads)
    args.output_dir.mkdir(parents=True, exist_ok=False)
    plans_dir = args.output_dir / "plans"
    plans_dir.mkdir()
    protocol = {
        "protocol_id": manifest.get("protocol_id", "phase_c_supplement_ablation_v1"),
        "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "manifest_path": str(args.workloads.resolve()),
        "workloads": workloads,
        "arms": ARMS,
        "blocks_per_workload": args.blocks,
        "expected_measured_runs": len(workloads) * len(ARMS) * args.blocks,
        "partitions": args.partitions,
        "skew_factor": args.skew_factor,
        "skew_threshold_mb": args.skew_threshold_mb,
        "order_seed": args.order_seed,
        "master": args.master,
        "primary_metric": "wall_clock_duration_s for grouped materialized action",
        "randomization": "six cyclic rotations of one seeded random arm order, then two rotations repeated; block order randomized",
    }
    (args.output_dir / "protocol.json").write_text(json.dumps(protocol, indent=2))

    configure_environment()
    spark = create_benchmark_spark_session(
        app_name="PhaseC_Supplement_Ablation_Confirmation",
        master_url=args.master,
        shuffle_partitions=args.partitions,
        enable_aqe=False,
        require_standalone=True,
        extra_configs={"spark.sql.autoBroadcastJoinThreshold": "-1",
                      "spark.sql.adaptive.autoBroadcastJoinThreshold": "-1"},
    )
    collector = SparkMetricsCollector(spark)
    executor_evidence = verify_distributed_executors(spark, require_two_workers=True)
    all_runs, final_results = [], {}
    try:
        for w_index, workload in enumerate(workloads):
            wid = workload["workload_id"]
            print(f"\n=== Workload {wid}: {workload.get('purpose', '')} ===", flush=True)
            trips = spark.read.parquet(workload["fact_path"])
            dimension = spark.read.parquet(workload["dimension_path"])
            hotspots = json.loads(Path(workload["hotspot_path"]).read_text()) if workload["hotspot_path"].endswith(".json") else None
            if hotspots is None:
                import duckdb
                hotspots = duckdb.query(
                    f"SELECT DISTINCT PULocationID FROM read_parquet('{workload['hotspot_path']}') ORDER BY 1"
                ).df()["PULocationID"].astype(int).tolist()
            fact_fp = dataset_manifest_fingerprint(workload["fact_path"])
            dim_fp = dataset_manifest_fingerprint(workload["dimension_path"])
            # Unmeasured warm-up: one action for every arm and workload.
            for arm in ARMS:
                set_arm(spark, arm, args.partitions, args.skew_factor, args.skew_threshold_mb)
                spark.sparkContext.setJobGroup(f"warmup_{wid}_{arm}", f"Warm-up {wid} {arm}")
                joined = build_join(spark, trips, dimension, hotspots, arm)
                materialize(joined).collect()
                clear_group(spark.sparkContext)

            orders = balanced_orders(args.order_seed + w_index, args.blocks)
            for block, order in enumerate(orders, 1):
                print(f"Block {block}/{args.blocks}: {' -> '.join(order)}", flush=True)
                for position, arm in enumerate(order, 1):
                    effective = set_arm(spark, arm, args.partitions, args.skew_factor, args.skew_threshold_mb)
                    group = f"supp_{wid}_block{block}_{arm}"
                    collector.start_run(group, f"{wid} block {block} {arm}")
                    joined = build_join(spark, trips, dimension, hotspots, arm)
                    result = materialize(joined)
                    result.collect()
                    metrics = collector.end_run(group)
                    clear_group(spark.sparkContext)
                    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
                    plan_path = plans_dir / f"{wid}_{arm}_block{block}_{stamp}.txt"
                    plan_path.write_text(result._jdf.queryExecution().executedPlan().toString())
                    row = {
                        **metrics,
                        "workload_id": wid,
                        "block": block,
                        "order_position": position,
                        "method": arm,
                        "protocol_id": protocol["protocol_id"],
                        "spark_version": spark.version,
                        "application_id": spark.sparkContext.applicationId,
                        "master_url": spark.sparkContext.master,
                        "fact_manifest_sha256": fact_fp,
                        "dimension_manifest_sha256": dim_fp,
                        "fact_path": workload["fact_path"],
                        "dimension_path": workload["dimension_path"],
                        "hotspot_path": workload["hotspot_path"],
                        "hotspot_ids": json.dumps(hotspots),
                        "worker_evidence": json.dumps(executor_evidence),
                        "executed_plan_path": str(plan_path),
                        "spark.sql.adaptive.enabled": effective["spark.sql.adaptive.enabled"],
                        "spark.sql.adaptive.coalescePartitions.enabled": effective["spark.sql.adaptive.coalescePartitions.enabled"],
                        "spark.sql.adaptive.skewJoin.enabled": effective["spark.sql.adaptive.skewJoin.enabled"],
                        "spark.sql.adaptive.skewJoin.skewedPartitionFactor": effective["spark.sql.adaptive.skewJoin.skewedPartitionFactor"],
                        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": effective["spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes"],
                        "spark.sql.autoBroadcastJoinThreshold": effective["spark.sql.autoBroadcastJoinThreshold"],
                        "validation_status": "PENDING",
                    }
                    all_runs.append(row)
                    final_results[(wid, arm)] = (trips, dimension, hotspots)
                    print(f"  {arm}: {metrics.get('wall_clock_duration_s')} s; TSR={metrics.get('task_skew_ratio_max')}; collector={metrics.get('collector_status')}", flush=True)
                pd.DataFrame(all_runs).to_csv(args.output_dir / "runs.partial.csv", index=False)
                (args.output_dir / "progress.json").write_text(json.dumps({
                    "completed_workload": wid,
                    "completed_block": block,
                    "measured_rows_recorded": len(all_runs),
                    "expected_measured_rows": protocol["expected_measured_runs"],
                    "updated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                }, indent=2))

        audit_frames, validation = [], {}
        workload_by_id = {w["workload_id"]: w for w in workloads}
        for (wid, arm), (trips, dimension, hotspots) in final_results.items():
            set_arm(spark, arm, args.partitions, args.skew_factor, args.skew_threshold_mb)
            spark.sparkContext.setJobGroup(f"audit_{wid}_{arm}", f"DuckDB audit {wid} {arm}")
            joined = build_join(spark, trips, dimension, hotspots, arm)
            passed, summary, audit = verify_join_with_duckdb(
                joined, workload_by_id[wid]["fact_path"], workload_by_id[wid]["dimension_path"]
            )
            clear_group(spark.sparkContext)
            validation[(wid, arm)] = summary["status"]
            audit["workload_id"] = wid
            audit["method"] = arm
            audit_frames.append(audit)
            print(f"DuckDB {wid}/{arm}: {summary['status']} ({summary['total_trips_spark']:,}/{summary['total_trips_duck']:,})", flush=True)
        for row in all_runs:
            row["validation_status"] = validation[(row["workload_id"], row["method"])]

        runs = pd.DataFrame(all_runs)
        runs.to_csv(args.output_dir / "runs.csv", index=False)
        pd.concat(audit_frames, ignore_index=True).to_csv(args.output_dir / "duckdb_audit.csv", index=False)
        summaries = []
        for (wid, arm), group in runs.groupby(["workload_id", "method"]):
            summaries.append({
                "workload_id": wid, "method": arm, "n_blocks": len(group),
                "n_collector_ok": int((group["collector_status"] == "OK").sum()),
                "n_duckdb_pass": int((group["validation_status"] == "PASS").sum()),
                "wall_time_median_s": group["wall_clock_duration_s"].median(),
                "wall_time_mean_s": group["wall_clock_duration_s"].mean(),
                "wall_time_sd_s": group["wall_clock_duration_s"].std(),
                "TSR_median": pd.to_numeric(group["task_skew_ratio_max"], errors="coerce").median(),
                "shuffle_read_bytes_median": pd.to_numeric(group["shuffle_read_bytes"], errors="coerce").median(),
                "memory_spill_bytes_median": pd.to_numeric(group["memory_bytes_spilled"], errors="coerce").median(),
                "disk_spill_bytes_median": pd.to_numeric(group["disk_bytes_spilled"], errors="coerce").median(),
            })
        pd.DataFrame(summaries).to_csv(args.output_dir / "summary.csv", index=False)
        print(f"\nSaved {len(runs)} measured rows in {args.output_dir}", flush=True)
        if len(runs) != protocol["expected_measured_runs"]:
            raise RuntimeError(f"Expected {protocol['expected_measured_runs']} rows; got {len(runs)}")
        if (runs["collector_status"] != "OK").any() or (runs["validation_status"] != "PASS").any():
            raise RuntimeError("At least one collector or DuckDB correctness check failed; inspect artifacts.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
