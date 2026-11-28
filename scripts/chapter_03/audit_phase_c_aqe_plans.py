#!/usr/bin/env python3
"""Audit saved Phase C AQE plans for final-plan, coalescing, and skew markers."""
from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path


SKEW_MARKER_PATTERNS = {
    "is_skew_true": re.compile(r"\bisSkew\s*=\s*true\b", re.IGNORECASE),
    "aqe_shuffle_read_skewed": re.compile(r"AQEShuffleRead\s+skewed", re.IGNORECASE),
    "partial_reducer_partition_spec": re.compile(r"PartialReducerPartitionSpec", re.IGNORECASE),
    "optimize_skewed_join": re.compile(r"OptimizeSkewedJoin", re.IGNORECASE),
}


def plan_sets(repo_root: Path) -> list[tuple[str, Path]]:
    phase_c = repo_root / "data/benchmarks/phase_c"
    return [
        ("synthetic_v3", phase_c / "synthetic_grid_v3_20261123"),
        ("sensitivity_v3", phase_c / "sensitivity_v3_20261123"),
        ("nyc_s3_retry_1500m", phase_c / "s3_10block_20261123/retry_1500m"),
    ]


def audit_plan(experiment: str, path: Path, repo_root: Path) -> dict[str, str]:
    plan = path.read_text(encoding="utf-8", errors="replace")
    found_markers = [
        name for name, pattern in SKEW_MARKER_PATTERNS.items() if pattern.search(plan)
    ]
    normalized_plan = plan.lower()
    return {
        "experiment": experiment,
        "plan_path": path.relative_to(repo_root).as_posix(),
        "final_plan": str("isfinalplan=true" in normalized_plan).lower(),
        "sort_merge_join": str("sortmergejoin" in normalized_plan).lower(),
        "aqe_coalesced": str("aqeshuffleread coalesced" in normalized_plan).lower(),
        "skew_split_markers": ",".join(found_markers),
        "skew_split_marker_found": str(bool(found_markers)).lower(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    default_root = Path(__file__).resolve().parents[2]
    parser.add_argument("--repo-root", type=Path, default=default_root)
    parser.add_argument(
        "--output",
        type=Path,
        default=default_root / "docs/refactor/evidence/phase_c_aqe_final_plan_audit.csv",
    )
    args = parser.parse_args()
    repo_root = args.repo_root.resolve()

    records = []
    for experiment, directory in plan_sets(repo_root):
        if not directory.is_dir():
            raise FileNotFoundError(f"Expected plan directory does not exist: {directory}")
        plans = sorted(directory.rglob("nd3_Spark_AQE_Skew_Join_iter*.txt"))
        if not plans:
            raise FileNotFoundError(f"No saved AQE plans found under {directory}")
        records.extend(audit_plan(experiment, plan, repo_root) for plan in plans)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "experiment",
        "plan_path",
        "final_plan",
        "sort_merge_join",
        "aqe_coalesced",
        "skew_split_markers",
        "skew_split_marker_found",
    ]
    with args.output.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(records)

    for experiment, _ in plan_sets(repo_root):
        subset = [row for row in records if row["experiment"] == experiment]
        print(
            f"{experiment}: {len(subset)} plans, "
            f"final={sum(row['final_plan'] == 'true' for row in subset)}, "
            f"SMJ={sum(row['sort_merge_join'] == 'true' for row in subset)}, "
            f"coalesced={sum(row['aqe_coalesced'] == 'true' for row in subset)}, "
            f"skew-marked={sum(row['skew_split_marker_found'] == 'true' for row in subset)}"
        )
    print(f"Wrote {len(records)} plan rows to {args.output}")


if __name__ == "__main__":
    main()
