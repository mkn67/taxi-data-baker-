#!/usr/bin/env python3
"""Diagnostic of repeated identical B plans; not a confirmation."""
import argparse
import csv
import hashlib
import json
import random
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from scripts.chapter_03.benchmark_engine import SparkMetricsCollector, create_benchmark_spark_session, load_hotspot_pu_ids, verify_distributed_executors
from scripts.chapter_03.run_phase_c_supplement_ablation import build_join, materialize, set_arm

ARM = "B_AQE_COALESCE"


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as source:
        for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def clear_group(sc):
    for key in ("spark.jobGroup.id", "spark.job.description", "spark.job.interruptOnCancel"):
        sc.setLocalProperty(key, None)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--blocks", type=int, choices=(2, 8), default=2)
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text())
    fact_path = manifest["fact_path"]
    hot_path = manifest["hotspots_path"]
    dim = manifest["dimensions"]["low"]
    for path, digest in ((fact_path, manifest["source_sha256"]["fact"]),
                         (hot_path, manifest["source_sha256"]["hotspots"]), (dim["path"], dim["sha256"])):
        if sha256(path) != digest:
            raise ValueError(f"Input hash mismatch: {path}")
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / "plans").mkdir()
    (args.output / "outputs").mkdir()
    orders = [["X", "Y"], ["Y", "X"]] * (args.blocks // 2)
    random.Random(20261128).shuffle(orders)
    protocol = {"purpose": "same-plan variation diagnostic; no universal noise-floor inference",
                "input_manifest": str(args.manifest.resolve()), "manifest_sha256": sha256(args.manifest),
                "runner_sha256": sha256(__file__), "arm": ARM, "labels": "X and Y invoke identical code and configuration",
                "dimension": "low", "seed": 20261128, "orders": orders, "warmup_actions": 2,
                "blocks": args.blocks, "measured_actions": args.blocks * 2, "shuffle_partitions": 200, "skew_factor": 2, "skew_threshold_mb": 16,
                "wall_time": "build DataFrame and materialized grouped collect; excludes output writing and DuckDB comparison",
                "analysis": "descriptive Y/X and abs(Y/X-1)*100 within block; runs share one application, no significance test or universal noise floor",
                "gate": "inspect event log, correctness, all final plans and task/collector scope before any further run"}
    (args.output / "protocol.json").write_text(json.dumps(protocol, indent=2))
    quoted_fact = "'" + fact_path.replace("'", "''") + "'"
    quoted_dim = "'" + dim["path"].replace("'", "''") + "'"
    db = duckdb.connect()
    reference_rows = db.execute(f"SELECT d.PU_Borough,d.DO_Borough,count(*) cnt,round(sum(f.trip_miles),2) tot_miles,round(sum(f.base_passenger_fare),2) tot_fare FROM read_parquet({quoted_fact}) f JOIN read_parquet({quoted_dim}) d USING (PULocationID,DOLocationID) GROUP BY 1,2").fetchall()
    db.close()
    reference = {(r[0], r[1]): r[2:] for r in reference_rows}
    (args.output / "outputs" / "duckdb_reference.json").write_text(json.dumps(reference_rows, indent=2))
    spark = create_benchmark_spark_session("PhaseC_SamePlan_Noise_Pilot", "spark://spark-master:7077", 200, False, require_standalone=True,
        extra_configs={"spark.sql.autoBroadcastJoinThreshold": "-1", "spark.sql.adaptive.autoBroadcastJoinThreshold": "-1"})
    try:
        collector = SparkMetricsCollector(spark)
        worker = verify_distributed_executors(spark, require_two_workers=True)
        fact = spark.read.parquet(fact_path)
        dimension = spark.read.parquet(dim["path"])
        hotspots = load_hotspot_pu_ids(hot_path)
        for i in range(2):
            set_arm(spark, ARM, 200, 2, 16)
            spark.sparkContext.setJobGroup(f"noise_warmup_{i}", "same plan warm-up")
            materialize(build_join(spark, fact, dimension, hotspots, ARM)).collect()
            clear_group(spark.sparkContext)
        rows = []
        for block, order in enumerate(orders, 1):
            for position, label in enumerate(order, 1):
                config = set_arm(spark, ARM, 200, 2, 16)
                group = f"noise_pilot_b{block}_{label}"
                collector.start_run(group, group)
                result = materialize(build_join(spark, fact, dimension, hotspots, ARM))
                measured = result.collect()
                metrics = collector.end_run(group)
                clear_group(spark.sparkContext)
                plan = result._jdf.queryExecution().executedPlan().toString()
                plan_path = args.output / "plans" / f"{group}.txt"
                plan_path.write_text(plan)
                payload = [x.asDict(recursive=True) for x in measured]
                output_path = args.output / "outputs" / f"{group}.json"
                output_path.write_text(json.dumps(payload, indent=2))
                observed = {(x["PU_Borough"], x["DO_Borough"]): (x["cnt"], x["tot_miles"], x["tot_fare"]) for x in payload}
                mismatch = [str(key) for key in set(reference) | set(observed) if key not in reference or key not in observed or
                    observed[key][0] != reference[key][0] or any(observed[key][i] is None or reference[key][i] is None or abs(observed[key][i] - reference[key][i]) > 0.05 for i in (1, 2))]
                lower = plan.lower()
                plan_ok = all(x in lower for x in ("sortmergejoin", "isfinalplan=true", "aqeshuffleread coalesced")) and "broadcasthashjoin" not in lower
                row = {**metrics, "block": block, "label": label, "method": ARM, "position": position,
                       "application_id": spark.sparkContext.applicationId, "config": json.dumps(config), "worker_evidence": json.dumps(worker),
                       "plan_path": str(plan_path), "output_path": str(output_path), "output_sha256": sha256(output_path),
                       "duckdb_status": "PASS" if not mismatch else "FAIL", "plan_marker_status": "PASS" if plan_ok else "FAIL"}
                rows.append(row)
                (args.output / "runs.json").write_text(json.dumps(rows, indent=2))
                print(json.dumps({k: row[k] for k in ("block", "label", "wall_clock_duration_s", "collector_status", "duckdb_status", "plan_marker_status")}), flush=True)
                if row["collector_status"] != "OK" or row["duckdb_status"] != "PASS" or not plan_ok:
                    raise RuntimeError("Pilot stopped; preserve partial evidence")
        with (args.output / "runs.csv").open("w", newline="") as out:
            writer = csv.DictWriter(out, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        print("Diagnostic complete; audit final plan equivalence and Event Log before drawing any conclusion.", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
