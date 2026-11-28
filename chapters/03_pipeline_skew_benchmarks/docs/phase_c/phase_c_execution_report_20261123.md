# Phase C execution report — WP0–WP7

**Updated:** 23/11/2026  
**Purpose:** consolidate the implementation plan, completed run evidence and remaining limits. NYC and synthetic results are separate evidence streams.

## Executive summary

The frozen S3 ND3 comparison, P95 discovery measurement, resource-work extraction, synthetic skew × dimension sweep, configuration sensitivity and held-out decision-rule probe are present. The v3 synthetic sweep has finished; there is no running sweep process. This update completed WP4's missing hourly hotspot analysis. The implementation is sufficient for a bounded engineering report, but not for a general production rule or a publication novelty claim: the literature gate is open, only three synthetic cells are held out, the grid axis uses Parquet bytes and broadcast size is observed only for executed exchanges, and continuous host/cgroup telemetry was not captured for S3.

## WP results

| Work package | Work performed and evidence | Result and qualification |
|---|---|---|
| **WP0 — protocol** | Frozen in [`implementation_protocol_v1.md`](implementation_protocol_v1.md): δ=5%, randomized paired blocks, fixed run caps, paired bootstrap, Holm correction, infeasibility and exclusion rules. | Applied to the S3 10-block comparison and synthetic decision evaluation. Literature review remains open; no local “AQE or Salting?” draft was found, so do not make novelty claims from this pass. |
| **WP1 — P95 discovery cost** | Spark exact-P95 detection on S3, app `app-20261123045634-0001`; measurements are in `data/benchmarks/phase_c/s3_10block_20261123/retry_1500m/hotspot_detection_cost.json`. | First scan 3.953 s; subsequent scans 1.699 s and 1.277 s; all reproduced the stored 14 IDs. Adding 3.953 s to the 32.345 s Salting median gives an **estimate** of 36.298 s cold-start, not a directly measured combined run. Reuse changes that cost. |
| **WP2 — resource work** | Event Log task work joined to run job groups and timestamps; [`task_work_by_run.csv`](../../../../data/benchmarks/phase_c/s3_10block_20261123/retry_1500m/task_work_by_run.csv). | 40/40 timed runs reconciled. Task runtime, executor CPU, GC, shuffle, spill and task distributions are collected where available. No continuous host/cgroup telemetry in this S3 session; absent metrics remain unavailable, not zero. |
| **WP3 — S3 repeated runs** | Ten randomized paired blocks, four methods, same January–May cumulative Silver/OD manifests, two workers, 40 plans, DuckDB correctness. See [`s3_10block_20261123.md`](s3_10block_20261123.md). | 40/40 `OK/PASS`. Median seconds: SMJ 27.884; AQE 28.189; Salting 32.345; unhinted Spark auto 6.788. AQE vs SMJ is unresolved at δ=5%; Salting is slower in this session; auto broadcast is faster in this session. Do not pool with the incomplete 1,024 MiB run or earlier sessions. |
| **WP4 — hotspot stability** | Exact nearest-rank P95 calculated for each pickup zone by month and by pickup hour for Silver 2026-01 through 2026-05. Monthly source: `data/benchmarks/phase_c/hotspot_stability/`; hourly source: `data/benchmarks/phase_c/hotspot_stability_hourly/`. | Monthly adjacent Jaccard is 0.8667 and monthly hot-zone trip share is 15.78–16.31%. For 120 month-hour sets, hourly support is 256–260 zones. Median same-hour adjacent-month Jaccard is 0.857 (range 0.529–1.000); median within-month adjacent-hour Jaccard is 0.733 (range 0.444–1.000); hourly hotspot trip share is 12.92–26.07% (median 15.63%). The hourly set varies more across hours, but this is a diagnostic only: no zone-hour ND2 or join benchmark was run. |
| **WP5 — synthetic skew × dimension sweep** | v3 grid, fixed seed `20261123`, Zipf exponents 0/1/2, four dimension Parquet sizes (4,146; 5,182,230; 19,232,349; 48,093,999 bytes), nine development cells and three held-out cells. Five randomized blocks × four methods per cell. v2 is retained as diagnostic only due to an unintended fact-side broadcast plan. | 12/12 cells completed; 240/240 rows had collector `OK` and DuckDB `PASS`. Median of the 12 cell medians (seconds): SMJ 2.207, AQE 1.202, Salting 2.612, Spark auto 1.740. These figures describe only this synthetic generator and local cluster configuration. Dimension storage bytes are not in-memory broadcast bytes; no claim of physical infeasibility follows from these byte sizes. |
| **WP6 — configuration sensitivity** | Three shuffle partition counts (64/200/400) applied to every arm, plus AQE threshold values 4/64 MiB on fixed `moderate_medium` synthetic workload; artifacts in `data/benchmarks/phase_c/sensitivity_v3_20261123/`. | Five configurations × 12 runs = 60/60 collector `OK` and DuckDB `PASS`. AQE plans did not show a skew split. Medians vary with configuration; this sensitivity is exploratory and does not replace the primary configuration. |
| **WP7 — decision rule** | Nearest development cell in normalized `(Zipf exponent, log2 dimension Parquet bytes)`; evaluated on three cells designated as held-out in the manifest. Results in `data/benchmarks/phase_c/decision_v3_final_20261123/`. | Rule chose AQE for all three held-out cells; median and worst-cell regret are both 0.0 against the measured per-cell oracle. Only three synthetic cells were held out, and one has an unresolved best-vs-second comparison under δ=5%. Treat this as a small feasibility demonstration, not a validated general selector. |

## Follow-up analysis for the final manuscript

The archived Event Logs contain the SQL accumulator for `BroadcastExchange.data size`. Matching the measured job groups to SQL execution IDs gave 10,485,760 bytes and 63,572 rows for all ten S3 auto-strategy runs. The synthetic auto-strategy ran a broadcast in all 15 tiny and 15 small observations, with reported sizes of 1,056,768 and 83,886,080 bytes respectively. The 30 medium and large observations used SMJ. Spark's SQL metric describes the broadcast relation. It provides no measurement of network traffic or peak executor memory.

To reproduce the extraction from the repository root, use a new output path:

```bash
phase_c_dir=data/benchmarks/phase_c
python scripts/chapter_03/analysis/extract_phase_c_broadcast.py --logs logs/spark-events \
  --runs "$phase_c_dir"/s3_10block_20261123/retry_1500m/*_runs_*.csv \
         "$phase_c_dir"/synthetic_grid_v3_20261123/*/*_runs_*.csv \
  --output "$phase_c_dir"/decision_v3_final_20261123/broadcast_exchange_metrics_rerun.csv
```

All nine development cells have AQE as the lowest observed median. An always-AQE reference therefore achieves the same zero observed regret as the nearest-cell rule on the three held-out cells. Describe the split as recorded before measurement and the selector as exploratory. The available record does not establish a separate freeze of the implementation before access to held-out outcomes.

The sensitivity protocol JSON contains an old v2 dimension-path label. The observed run CSVs record the v3 medium dimension and the same v3 moderate facts across all five configurations. Use the run paths and fingerprints for provenance.

## Hourly hotspot analysis details

The added script is [`analyze_hotspot_hourly_stability.py`](../../../../scripts/chapter_03/analysis/analyze_hotspot_hourly_stability.py). It uses the timezone-naive hour component of Silver `pickup_datetime`, counts non-null `(PULocationID, pickup hour)` observations, and selects all zones at or above the exact nearest-rank P95 threshold separately for each month-hour. For every set it records zone support, threshold, trip share and IDs, plus Jaccard against the preceding hour and the same hour of the previous listed month.

Across the five months, each pickup hour has 256–260 zones with non-null trips. The hourly P95 set covers 12.92–26.07% of trips within an hour (median of monthly-hour values: 15.63%). The median within-month adjacent-hour Jaccard is 0.733 (range 0.444–1.000), while the median same-hour adjacent-month Jaccard is 0.857. This supports the practical warning that a static monthly list may not represent every hour equally. It does **not** show that hour-specific salting reduces runtime; that would require a separately designed and correctness-checked benchmark branch.

## Reproduction commands

```bash
./.venv/bin/python scripts/chapter_03/analysis/analyze_hotspot_hourly_stability.py
```

The command writes new files under `data/benchmarks/phase_c/hotspot_stability_hourly/` and will stop if that output directory already exists. Choose a new `--output` path to rerun without overwriting the saved evidence.

## Remaining gates and reporting rules

1. The local project tree contains no draft named “AQE or Salting?”. The official [Spark 3.5.9 performance tuning guide](https://spark.apache.org/docs/3.5.9/sql-performance-tuning.html) confirms that AQE skew-join detection requires a shuffled partition to exceed both the byte threshold and the configured factor times the median. This supports the implementation's distinction between coalescing and skew splitting; it is not a complete related-work or novelty review. Flow-Join++ metadata identifies the 2026 paper by Panduman, Ito and Onizuka, *Journal of Information Processing*, 34, 671–685, [DOI 10.2197/ipsjjip.34.671](https://doi.org/10.2197/ipsjjip.34.671). Revisit the supplied paper/draft and compare the exact method and experimental scope before making any novelty claim.
2. Do not state host-wide or cgroup causes for runtime variation in the S3 session; continuous telemetry was not collected.
3. Keep NYC S3 and generated synthetic evidence separate in tables and conclusions.
4. Describe dimension size as compressed Parquet bytes unless a compatible in-memory broadcast-size metric is added.
5. Keep held-out regret explicitly exploratory (three cells), and do not present the nearest-neighbor rule as a production-ready optimizer.
6. Hourly P95 analysis is diagnostic; no hourly ND2/ND3 performance experiment has been conducted.
