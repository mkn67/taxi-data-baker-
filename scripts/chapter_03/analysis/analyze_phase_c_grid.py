#!/usr/bin/env python3
"""Evaluate a frozen nearest-cell policy and held-out regret on the WP5 grid."""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


METHODS = ("Baseline_SMJ", "Spark_AQE_Skew_Join", "Salting_Join_S8", "Spark_AutoJoin_Reference")
COLORS = {"Baseline_SMJ": "#4e79a7", "Spark_AQE_Skew_Join": "#59a14f",
          "Salting_Join_S8": "#f28e2b", "Spark_AutoJoin_Reference": "#e15759",
          "unresolved": "#bab0ac"}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--manifest", type=Path, required=True)
    ap.add_argument("--grid", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    manifest = json.loads(args.manifest.read_text())
    cell_rows = []
    for cell in manifest["cells"]:
        directory = args.grid / cell["cell_id"]
        files = list(directory.glob("benchmark_nd3_join_runs_official_standalone_auto_strategy_comparison.csv"))
        if len(files) != 1:
            raise FileNotFoundError(f"Missing full run CSV for {cell['cell_id']}")
        runs = pd.read_csv(files[0])
        if len(runs) != 20 or set(runs.method) != set(METHODS) or not runs.collector_status.eq("OK").all() or not runs.validation_status.eq("PASS").all():
            raise ValueError(f"Incomplete or incorrect cell {cell['cell_id']}")
        if runs.duplicated(["iteration", "method"]).any() or runs.trip_dataset_manifest_sha256.nunique() != 1:
            raise ValueError(f"Key or manifest mismatch for {cell['cell_id']}")
        plans = list((directory / "nd3_plans").rglob("*.txt"))
        if len(plans) != 20:
            raise ValueError(f"Expected 20 executed plans for {cell['cell_id']}; got {len(plans)}")
        times = runs.pivot(index="iteration", columns="method", values="wall_clock_duration_s")
        medians = times.median().sort_values()
        best, runner_up = medians.index[:2]
        rng = np.random.default_rng(20261123)
        paired = (times[best] / times[runner_up]).to_numpy()
        draws = rng.integers(0, len(paired), size=(10000, len(paired)))
        ci_low, ci_high = np.quantile(np.median(paired[draws], axis=1), [.025, .975])
        confident = bool(ci_high < .95)
        row = {**cell, "zipf_exponent": manifest["skew_levels"][cell["skew"]]["zipf_exponent"],
               "oracle": best, "second": runner_up, "oracle_confident_5pct": confident,
               "best_vs_second_ratio_ci95_low": float(ci_low), "best_vs_second_ratio_ci95_high": float(ci_high)}
        for method in METHODS:
            row[f"median_seconds__{method}"] = float(medians[method])
        cell_rows.append(row)
    data = pd.DataFrame(cell_rows)
    development = data[data.split.eq("development")]
    heldout = data[data.split.eq("held_out")].copy()
    exponents = data.zipf_exponent.to_numpy()
    byte_logs = np.log2(data.dimension_parquet_bytes.to_numpy())
    exp_span = max(exponents) - min(exponents) or 1
    byte_span = max(byte_logs) - min(byte_logs) or 1
    for idx, cell in heldout.iterrows():
        distances = ((development.zipf_exponent - cell.zipf_exponent) / exp_span) ** 2 + (
            (np.log2(development.dimension_parquet_bytes) - math.log2(cell.dimension_parquet_bytes)) / byte_span) ** 2
        nearest = development.loc[distances.sort_values(kind="stable").index[0]]
        chosen = nearest.oracle
        heldout.loc[idx, "rule_choice"] = chosen
        heldout.loc[idx, "nearest_development_cell"] = nearest.cell_id
        heldout.loc[idx, "regret"] = (
            cell[f"median_seconds__{chosen}"] - cell[f"median_seconds__{cell.oracle}"]
        ) / cell[f"median_seconds__{cell.oracle}"]
    args.output.mkdir(parents=True, exist_ok=False)
    data.to_csv(args.output / "cell_results.csv", index=False)
    heldout.to_csv(args.output / "heldout_regret.csv", index=False)
    summary = {"development_cells": len(development), "held_out_cells": len(heldout),
               "median_regret": float(heldout.regret.median()), "worst_regret": float(heldout.regret.max()),
               "policy": "nearest development cell in normalized (Zipf exponent, log2 measured Parquet bytes)",
               "uncertain_oracle_cells": int((~data.oracle_confident_5pct).sum()),
               "interpretation": "Five paired blocks per cell; unresolved means the best/second bootstrap ratio CI does not exclude a 5% practical gap. Parquet bytes are a storage proxy, not broadcast-memory bytes."}
    (args.output / "decision_summary.json").write_text(json.dumps(summary, indent=2))
    fig, ax = plt.subplots(figsize=(9, 4))
    dim_labels = [x[0] for x in [("tiny", 0), ("small", 1), ("medium", 2), ("large", 3)]]
    skew_labels = ["uniform", "moderate", "strong"]
    for yi, skew in enumerate(skew_labels):
        for xi, dim in enumerate(dim_labels):
            row = data[(data["skew"] == skew) & (data["dimension_level"] == dim)].iloc[0]
            label = row.oracle if row.oracle_confident_5pct else "unresolved"
            ax.add_patch(plt.Rectangle((xi, yi), 1, 1, color=COLORS[label], alpha=.85))
            ax.text(xi + .5, yi + .5, label.replace("Spark_", "").replace("_Reference", ""),
                    ha="center", va="center", fontsize=8, color="white" if label != "unresolved" else "black")
            if row.split == "held_out":
                ax.text(xi + .9, yi + .12, "H", ha="right", fontsize=9, fontweight="bold")
    ax.set_xlim(0, 4); ax.set_ylim(3, 0)
    ax.set_xticks(np.arange(4) + .5, dim_labels)
    ax.set_yticks(np.arange(3) + .5, skew_labels)
    ax.set_xlabel("Dimension storage size level (measured Parquet bytes in CSV)")
    ax.set_ylabel("Zipf skew level")
    ax.set_title("Synthetic decision map; H = held-out cell; grey = unresolved")
    fig.tight_layout()
    fig.savefig(args.output / "synthetic_decision_map.png", dpi=180)
    plt.close(fig)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
