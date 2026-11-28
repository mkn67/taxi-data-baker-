#!/usr/bin/env python3
"""Paired block analysis of four ND3 methods with a frozen 5% practical threshold."""
from __future__ import annotations

import argparse
import itertools
import json
from pathlib import Path

import numpy as np
import pandas as pd


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--runs", type=Path, required=True)
    ap.add_argument("--task-work", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    runs = pd.read_csv(args.runs)
    work = pd.read_csv(args.task_work)
    if not runs.collector_status.eq("OK").all() or not runs.validation_status.eq("PASS").all():
        raise ValueError("All included runs must be OK/PASS")
    if runs.duplicated(["iteration", "method"]).any():
        raise ValueError("Duplicate paired key")
    pivot = runs.pivot(index="iteration", columns="method", values="wall_clock_duration_s").sort_index()
    if pivot.isna().any().any() or len(pivot) < 3:
        raise ValueError("Incomplete paired blocks")
    baseline = pivot["Baseline_SMJ"].to_numpy()
    rng = np.random.default_rng(20261123)
    comparisons = []
    for method in ("Spark_AQE_Skew_Join", "Salting_Join_S8", "Spark_AutoJoin_Reference"):
        target = pivot[method].to_numpy()
        ratios = target / baseline
        draws = rng.integers(0, len(ratios), size=(10000, len(ratios)))
        lo, hi = np.quantile(np.median(ratios[draws], axis=1), [.025, .975])
        differences = target - baseline
        observed = abs(float(np.mean(differences)))
        permuted = [abs(float(np.mean(differences * np.array(signs))))
                    for signs in itertools.product((-1, 1), repeat=len(differences))]
        p = sum(value >= observed - 1e-12 for value in permuted) / len(permuted)
        conclusion = "faster" if hi < .95 else "slower" if lo > 1.05 else "unresolved"
        comparisons.append({"method": method, "blocks": len(ratios),
                            "median_target_seconds": float(np.median(target)),
                            "median_paired_ratio": float(np.median(ratios)),
                            "ratio_ci95_low": float(lo), "ratio_ci95_high": float(hi),
                            "exact_sign_flip_p": p, "practical_5pct_result": conclusion})
    order = sorted(range(len(comparisons)), key=lambda i: comparisons[i]["exact_sign_flip_p"])
    adjusted = [0.0] * len(comparisons)
    running = 0.0
    for rank, index in enumerate(order):
        running = max(running, min(1.0, comparisons[index]["exact_sign_flip_p"] * (len(order) - rank)))
        adjusted[index] = running
    for item, p in zip(comparisons, adjusted):
        item["holm_adjusted_p"] = p
    joined = runs.merge(work, on=["iteration", "method", "application_id", "job_group"], validate="one_to_one")
    if len(joined) != len(runs) or not joined.all_jobs_succeeded.all():
        raise ValueError("Task-work reconciliation failed")
    resources = joined.groupby("method")[["executor_run_time_ms", "executor_cpu_time_ms", "jvm_gc_time_ms"]].median().to_dict("index")
    report = {"scope": "one S3 session, 10 randomized paired blocks; no cross-session pooling",
              "practical_delta": .05, "bootstrap_seed": 20261123,
              "manifest_count": int(runs.trip_dataset_manifest_sha256.nunique()),
              "runs": len(runs), "comparison_to_baseline": comparisons, "median_task_metrics": resources}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
