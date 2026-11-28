#!/usr/bin/env python3
"""Audit and summarize the frozen Phase C supplement confirmation outputs."""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import wilcoxon

ARMS = [
    "A_SMJ", "A0_AQE_NO_COALESCE", "B_AQE_COALESCE",
    "C_AQE_COALESCE_SKEW", "D_SALTING_S8", "E_SALTING_S8_AQE_COALESCE",
]
CONTRASTS = [
    ("A0_AQE_NO_COALESCE", "A_SMJ", "AQE enabled, without coalescing vs SMJ", False),
    ("B_AQE_COALESCE", "A0_AQE_NO_COALESCE", "Coalescing added while AQE stays enabled", True),
    ("C_AQE_COALESCE_SKEW", "B_AQE_COALESCE", "Skew optimization added on coalescing AQE", True),
    ("D_SALTING_S8", "A_SMJ", "Salting S8 vs SMJ", False),
    ("E_SALTING_S8_AQE_COALESCE", "B_AQE_COALESCE", "Salting added on coalescing AQE", False),
    ("E_SALTING_S8_AQE_COALESCE", "C_AQE_COALESCE_SKEW", "Salting + coalescing vs AQE skew join", True),
]


def holm_adjust(pvalues):
    order = np.argsort(pvalues)
    adjusted = np.empty(len(pvalues), dtype=float)
    running = 0.0
    total = len(pvalues)
    for rank, idx in enumerate(order):
        running = max(running, (total - rank) * float(pvalues[idx]))
        adjusted[idx] = min(1.0, running)
    return adjusted


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("results_dir", type=Path)
    ap.add_argument("--bootstrap", type=int, default=100_000)
    ap.add_argument("--seed", type=int, default=20261125)
    args = ap.parse_args()
    root = args.results_dir.resolve()
    protocol = json.loads((root / "protocol.json").read_text())
    runs = pd.read_csv(root / "runs.csv")
    expected = int(protocol["expected_measured_runs"])
    if len(runs) != expected or expected != 96:
        raise ValueError(f"Expected 96 measured rows, found {len(runs)}")
    if not (runs["collector_status"] == "OK").all():
        raise ValueError("At least one metrics collection failed.")
    if not (runs["validation_status"] == "PASS").all():
        raise ValueError("At least one run failed DuckDB validation.")

    group_counts = runs.groupby(["workload_id", "method"]).size()
    if len(group_counts) != 12 or not (group_counts == 8).all():
        raise ValueError("Each of the 12 workload/arm groups must contain exactly 8 blocks.")
    position_counts = runs.groupby(["workload_id", "method", "order_position"]).size().rename("n").reset_index()
    position_counts.to_csv(root / "position_balance_audit.csv", index=False)

    plan_rows = []
    for row in runs.to_dict("records"):
        text = Path(row["executed_plan_path"]).read_text(errors="replace")
        low = text.lower()
        plan_rows.append({
            "workload_id": row["workload_id"], "block": row["block"], "method": row["method"],
            "plan_path": row["executed_plan_path"],
            "has_final_adaptive_plan": "isfinalplan=true" in low,
            "has_coalesced_shuffle_reader": "aqeshuffleread coalesced" in low,
            "has_skew_split_marker": bool(re.search(r"sortmergejoin\(skew=true\)|aqeshuffleread[^\n]*skewed", low)),
            "has_sort_merge_join": "sortmergejoin" in low,
            "has_broadcast_join": "broadcast hash join" in low,
        })
    plans = pd.DataFrame(plan_rows)
    plans.to_csv(root / "plan_activation_audit.csv", index=False)
    activation = plans.groupby(["workload_id", "method"]).agg(
        n_plans=("plan_path", "size"),
        final_adaptive_plans=("has_final_adaptive_plan", "sum"),
        coalesced_plans=("has_coalesced_shuffle_reader", "sum"),
        skew_split_plans=("has_skew_split_marker", "sum"),
        sort_merge_plans=("has_sort_merge_join", "sum"),
        broadcast_plans=("has_broadcast_join", "sum"),
    ).reset_index()
    activation.to_csv(root / "plan_activation_summary.csv", index=False)

    summary = runs.groupby(["workload_id", "method"]).agg(
        n_blocks=("block", "nunique"),
        wall_time_median_s=("wall_clock_duration_s", "median"),
        wall_time_mean_s=("wall_clock_duration_s", "mean"),
        wall_time_sd_s=("wall_clock_duration_s", "std"),
        TSR_median=("task_skew_ratio_max", "median"),
        shuffle_read_median_bytes=("shuffle_read_bytes", "median"),
        memory_spill_median_bytes=("memory_bytes_spilled", "median"),
        disk_spill_median_bytes=("disk_bytes_spilled", "median"),
    ).reset_index()
    summary.to_csv(root / "summary_recomputed.csv", index=False)

    rng = np.random.default_rng(args.seed)
    paired_rows = []
    for wid, subset in runs.groupby("workload_id"):
        wide = subset.pivot(index="block", columns="method", values="wall_clock_duration_s").sort_index()
        for numerator, denominator, description, primary in CONTRASTS:
            ratios = (wide[numerator] / wide[denominator]).to_numpy(dtype=float)
            sampled = rng.choice(ratios, size=(args.bootstrap, len(ratios)), replace=True)
            boot_medians = np.median(sampled, axis=1)
            lo, hi = np.quantile(boot_medians, [0.025, 0.975])
            median_ratio = float(np.median(ratios))
            if hi < 0.95:
                margin = "numerator faster by more than 5%"
            elif lo > 1.05:
                margin = "numerator slower by more than 5%"
            elif lo >= 0.95 and hi <= 1.05:
                margin = "within the ±5% practical margin"
            else:
                margin = "direction/size not resolved at the ±5% margin"
            paired_rows.append({
                "workload_id": wid, "numerator": numerator, "denominator": denominator,
                "contrast": description, "n_paired_blocks": len(ratios),
                "primary_holm_family": primary,
                "wilcoxon_exact_p_raw": float(wilcoxon(ratios, np.ones_like(ratios), alternative="two-sided", method="exact").pvalue),
                "paired_ratio_median": median_ratio,
                "bootstrap_95_low": float(lo), "bootstrap_95_high": float(hi),
                "median_numerator_s": float(wide[numerator].median()),
                "median_denominator_s": float(wide[denominator].median()),
                "median_ratio_change_pct": (median_ratio - 1.0) * 100.0,
                "practical_margin_readout": margin,
            })
    paired = pd.DataFrame(paired_rows)
    primary_mask = paired["primary_holm_family"]
    paired["wilcoxon_holm_p"] = np.nan
    paired.loc[primary_mask, "wilcoxon_holm_p"] = holm_adjust(
        paired.loc[primary_mask, "wilcoxon_exact_p_raw"].to_numpy()
    )
    paired.to_csv(root / "paired_contrasts.csv", index=False)

    lines = [
        "# Phase C supplement confirmation — audited results",
        "",
        f"- Protocol: `{protocol['protocol_id']}`; Spark {runs['spark_version'].iloc[0]}; application `{runs['application_id'].iloc[0]}`.",
        f"- Measured actions: {len(runs)}/96; collector OK: {(runs['collector_status'] == 'OK').sum()}/96. DuckDB checks were rerun once per workload/arm; PASS is copied to measured rows, not verified separately per measured action.",
        "- Each workload/arm has 8 paired blocks. Ratios are numerator/denominator; values below 1 favor the numerator.",
        f"- Bootstrap: paired block ratios, unadjusted percentile 95% CI, {args.bootstrap:,} resamples, seed {args.seed}.",
        "- Exact paired Wilcoxon tests cover three planned contrasts per workload; the choice of this six-test Holm family was made after the run. Secondary contrasts are exploratory.",
        "- Practical interpretation uses the ±5% margin stated in the project plan; this synthetic evidence is not pooled with NYC S3 results.",
        "",
        "## Wall-time summary",
        "",
        summary.to_markdown(index=False, floatfmt=".4f"),
        "",
        "## Paired contrasts",
        "",
        paired.to_markdown(index=False, floatfmt=".4f"),
        "",
        "## Final-plan activation",
        "",
        activation.to_markdown(index=False),
        "",
    ]
    (root / "analysis_report.md").write_text("\n".join(lines))
    print(f"Audited {len(runs)} runs; wrote paired contrasts and plan audit under {root}")


if __name__ == "__main__":
    main()
