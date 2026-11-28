#!/usr/bin/env python3
"""Run four diagnostic actions, retaining and checking the measured outputs."""
import argparse
import csv
import json
import random
import sys
import time
from pathlib import Path

import duckdb
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from scripts.chapter_03.benchmark_engine import SparkMetricsCollector, create_benchmark_spark_session, load_hotspot_pu_ids, verify_distributed_executors
from scripts.chapter_03.run_phase_c_supplement_ablation import set_arm, build_join, materialize


def clear_group(sc):
    for key in ("spark.jobGroup.id", "spark.job.description", "spark.job.interruptOnCancel"):
        sc.setLocalProperty(key, None)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--manifest", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    ap.add_argument("--seed", type=int, default=20261128)
    a = ap.parse_args()
    manifest = json.loads(a.manifest.read_text())
    a.output.mkdir(parents=True, exist_ok=False)
    (a.output/"plans").mkdir()
    (a.output/"outputs").mkdir()
    rng = random.Random(a.seed)
    conditions = [(level, arm) for level in ("low", "high") for arm in ("B_AQE_COALESCE", "E_SALTING_S8_AQE_COALESCE")]
    rng.shuffle(conditions)
    protocol = {"purpose":"diagnostic pilot; 1 block; not confirmation", "manifest":str(a.manifest.resolve()), "seed":a.seed,
                "order":conditions,"shuffle_partitions":200,"skew_factor":2,"skew_threshold_mb":16,
                "wall_clock_scope":"build joined DataFrame, grouped materialized collect; excludes metrics, file write and DuckDB comparison",
                "relative_tolerance":"none", "sum_absolute_tolerance":0.05}
    (a.output/"pilot_protocol.json").write_text(json.dumps(protocol,indent=2))
    spark = create_benchmark_spark_session("PhaseC_Replication_Diagnostic_Pilot", "spark://spark-master:7077", 200, False, require_standalone=True,
        extra_configs={"spark.sql.autoBroadcastJoinThreshold":"-1","spark.sql.adaptive.autoBroadcastJoinThreshold":"-1"})
    collector = SparkMetricsCollector(spark)
    try:
        fact = spark.read.parquet(manifest["fact_path"])
        hotspots = load_hotspot_pu_ids(manifest["hotspots_path"])
        dims = {label:spark.read.parquet(manifest["dimensions"][label]["path"]) for label in ("low","high")}
        reference = {}
        db = duckdb.connect()
        fact_sql = "'" + manifest["fact_path"].replace("'","''") + "'"
        for label in ("low","high"):
            path = manifest["dimensions"][label]["path"].replace("'","''")
            ref = db.execute(f"SELECT d.PU_Borough,d.DO_Borough,count(*) cnt,round(sum(f.trip_miles),2) tot_miles,round(sum(f.base_passenger_fare),2) tot_fare FROM read_parquet({fact_sql}) f JOIN read_parquet('{path}') d USING (PULocationID,DOLocationID) GROUP BY 1,2").fetchall()
            reference[label] = {(x[0],x[1]):x[2:] for x in ref}
            hot = dims[label].filter(F.col("PULocationID").isin(hotspots)).count()
            array = F.when(F.col("PULocationID").isin(hotspots), F.array([F.lit(i) for i in range(8)])).otherwise(F.array(F.lit(0)))
            salted = dims[label].withColumn("salt", F.explode(array)).count()
            if hot != manifest["dimensions"][label]["hot_dimension_rows"] or salted != manifest["dimensions"][label]["rows"]+7*hot:
                raise RuntimeError(f"Replication mismatch {label}: {hot=} {salted=}")
        if set(reference["low"]) != set(reference["high"]) or any(
            reference["low"][k][0] != reference["high"][k][0] or
            any(abs(reference["low"][k][i]-reference["high"][k][i]) > 0.05 for i in (1,2))
            for k in reference["low"]
        ):
            raise RuntimeError("DuckDB ground truth differs between dimension levels")
        worker = verify_distributed_executors(spark, require_two_workers=True)
        all_runs=[]
        for level,arm in conditions:
            set_arm(spark,arm,200,2,16)
            spark.sparkContext.setJobGroup(f"replication_warmup_{level}_{arm}", "replication pilot warmup")
            materialize(build_join(spark,fact,dims[level],hotspots,arm)).collect()
            clear_group(spark.sparkContext)
        for position,(level,arm) in enumerate(conditions,1):
            config = set_arm(spark,arm,200,2,16)
            group=f"replication_pilot_{level}_{arm}"
            collector.start_run(group,group)
            result=materialize(build_join(spark,fact,dims[level],hotspots,arm))
            measured=result.collect()
            metrics=collector.end_run(group)
            clear_group(spark.sparkContext)
            plan=result._jdf.queryExecution().executedPlan().toString()
            plan_file=a.output/"plans"/f"{level}_{arm}.txt"
            plan_file.write_text(plan)
            output_file=a.output/"outputs"/f"{level}_{arm}.json"
            payload=[x.asDict(recursive=True) for x in measured]
            output_file.write_text(json.dumps(payload,indent=2))
            observed={(x['PU_Borough'],x['DO_Borough']):(x['cnt'],x['tot_miles'],x['tot_fare']) for x in payload}
            expected=reference[level]
            mismatch=[]
            for key in set(expected)|set(observed):
                x,y=observed.get(key),expected.get(key)
                if x is None or y is None or x[0]!=y[0] or any(x[i] is None or y[i] is None or abs(x[i]-y[i])>0.05 for i in (1,2)):
                    mismatch.append(str(key))
            plan_lower=plan.lower()
            plan_ok="sortmergejoin" in plan_lower and "broadcasthashjoin" not in plan_lower and "isfinalplan=true" in plan_lower and "aqeshuffleread coalesced" in plan_lower and (arm.startswith("B_") or "explode" in plan_lower)
            row={**metrics,"level":level,"method":arm,"position":position,"application_id":spark.sparkContext.applicationId,"worker_evidence":json.dumps(worker),"config":json.dumps(config),"plan_path":str(plan_file),"measured_output_path":str(output_file),"duckdb_status":"PASS" if not mismatch else "FAIL","mismatch_count":len(mismatch),"plan_status":"PASS" if plan_ok else "FAIL"}
            all_runs.append(row)
            (a.output/"runs.json").write_text(json.dumps(all_runs,indent=2))
            print(json.dumps({k:row[k] for k in ("level","method","wall_clock_duration_s","collector_status","duckdb_status","plan_status")}),flush=True)
        with (a.output/"runs.csv").open("w",newline="") as f:
            writer=csv.DictWriter(f,fieldnames=all_runs[0].keys());writer.writeheader();writer.writerows(all_runs)
        if any(x['duckdb_status']!='PASS' or x['collector_status']!='OK' or x['plan_status']!='PASS' for x in all_runs):
            raise RuntimeError("Pilot requires review: inspect output, Event Log and plans")
        print("Pilot actions finished; separately audit plan metrics for actual filler replication before gate PASS.",flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
