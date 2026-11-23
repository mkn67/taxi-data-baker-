# Phase C implementation plan — WP0–WP7

## Research question

Under which combinations of key skew and dimension-side size is salting beneficial, and when are baseline SMJ, AQE, or Spark-selected broadcast sufficient?

Real NYC results and synthetic sensitivity results are separate evidence streams. Existing S3 official evidence remains unchanged; no pilot/official pooling.

## WP0 — Freeze protocol and evidence rules

- Main arms: SMJ, standard AQE, Salting S=8, and unhinted Spark auto-strategy. Explicit broadcast hint is diagnostic only.
- Experimental unit: paired run block, same manifest/configuration, randomized method order within block.
- Primary endpoint: wall-time. Report paired median ratio/difference, bootstrap 95% CI and min–max. Resource endpoints: task time, executor CPU time when present, GC, shuffle, spill, TSR/PSR.
- Practical difference `δ = 5%` relative wall-time, frozen as a project decision threshold before new official runs. This is not a universal threshold.
- Primary comparisons: (1) S3 SMJ vs AQE vs Salting; (2) auto-strategy against those arms only after matching manifest, protocol and block; (3) held-out synthetic decision-rule regret.
- Use paired block bootstrap (10,000 resamples, fixed seed) for ratios/medians. Holm-adjust p-values for the three pairwise primary comparisons within each workload family. Treat CI crossing the ±δ practical-equivalence region as unresolved; n=3 remains descriptive.
- Exclusions: only predeclared environment failures. Method OOM/timeout under the resource budget is an infeasible outcome, never silently dropped.
- Literature gate: revisit “AQE or Salting?” and verify closest related work before novelty claims; Flow-Join++/CIBPartitioner are comparators/inspiration, not implementations.

## WP1 — Include hotspot discovery cost

Measure hotspot construction separately from the timed join and report (a) core runtime with stored P95 and (b) end-to-end runtime including detection. Also report reuse break-even: detection cost divided by per-run saving, only when saving is positive. Existing P95 artifact alone is not evidence that detection is free.

## WP2 — Add resource-work metrics

Extract `executorRunTime`, `executorCpuTime` when present, GC, shuffle read/write, spill and per-task distributions from event logs. `executorRunTime` is accumulated task time, not wall-time or CPU time; GC is a component and must not be added to it. Missing metrics are `NA`.

## WP3 — Pilot budget and repeated runs

Run a pilot to record per-run duration and variance. Then freeze a maximum of 10 measured blocks per method/tier, randomized paired order, plus one warm-up per method. Budget estimate is `10 × number_of_methods × number_of_tiers × pilot median duration`, plus warm-up and recovery allowance. Do not extend selectively after inspecting preferred outcomes. Stop at the cap; if uncertainty remains wider than δ, report inconclusive. OOM is reported as infeasible.

## WP4 — Hotspot stability

For each month and hour/period where sample support permits, report Jaccard of exact P95 pickup-ID sets **and** the fraction of trips those IDs cover. Since the set is small (about 14 IDs), Jaccard alone is insufficient. If temporal instability appears, evaluate ND2 `(zone, hour)` as a separate branch, not part of ND3.

## WP5 — Synthetic skew × dimension-size sweep

- Generate 3–4 Zipf skew levels (record exponent and fixed seed); optionally run lognormal as a separate sensitivity.
- Vary dimension bytes, not only row count. Record serialized/Parquet bytes and observed in-memory/broadcast size. Keep the dimension within the executor 1,500 MiB budget; include a safety margin and classify any method OOM as infeasible.
- Freeze complete held-out skew × size cells before performance inspection. Validate join correctness for every cell. Publish synthetic results separately from NYC.
- Lowering Spark's broadcast threshold is planner sensitivity only; it does not establish physical infeasibility.

## WP6 — Configuration sensitivity

At one fixed data tier, sweep shuffle partitions (64/200/400) across every Join arm. In a separate AQE-only sensitivity, vary skew thresholds. Keep primary configuration untouched; record final plans and distinguish coalescing from skew splitting.

## WP7 — Decision rule and held-out validation

Fit/freeze a rule using development synthetic cells only; evaluate once on held-out cells. Per-cell oracle is the fastest median among correct, feasible methods. Regret is `(rule median − oracle median) / oracle median`. Report median/worst-cell regret, feasible coverage and infeasible cells. Mark a cell unresolved when uncertainty overlaps the predeclared ±δ equivalence region; do not count near-ties as confident errors or wins. Build the decision map only after WP5 and WP7 validation.

## Execution gates and status — 23/11/2026

| WP | Status | Evidence |
|---|---|---|
| WP0 | Protocol frozen and used; literature recheck remains open before any novelty claim. | This plan and `implementation_protocol_v1.md`; δ=5%, randomized paired blocks, fixed 10 S3 blocks, bootstrap CI and Holm adjustment. No local “AQE or Salting?” draft was found in the project tree. |
| WP1 | Measured. | Spark exact P95 on S3, application `app-20261123045634-0001`; first scan 3.953 s, later scans 1.699/1.277 s, stored set reproduced. |
| WP2 | Extracted from Event Log for the S3 session. | 40 run rows reconciled with job groups and timestamps; task/CPU/GC in `task_work_by_run.csv`; no host/cgroup telemetry on this session. |
| WP3 | Completed for S3. | Ten paired blocks, four methods, 40/40 collector OK and DuckDB PASS, same manifest, two workers and 40 plans. |
| WP4 | Monthly and hourly exact-P95 stability analyses completed. | Monthly sets: adjacent Jaccard 0.8667; hotspot trip share 15.78–16.31%. Hourly sets across 5 × 24 month-hours (256–260 zones/hour): median same-hour adjacent-month Jaccard 0.857; median within-month adjacent-hour Jaccard 0.733; hourly hotspot trip share 12.92–26.07%. Outputs in `data/benchmarks/phase_c/hotspot_stability/` and `hotspot_stability_hourly/`. |
| WP5 | Completed as a controlled synthetic sweep, with v2 design rejected. | v3: 12 cells × five blocks × four methods; every run passes DuckDB, 12/12 cells exit 0. v2 partial artifacts are diagnostic only because auto plan broadcast the fact side. |
| WP6 | Completed on one fixed synthetic workload. | Three partition settings for all methods plus AQE threshold 4/64 MiB; all five configurations have 12/12 OK/PASS rows. No AQE skew split appeared. |
| WP7 | Exploratory held-out evaluation complete; insufficient to claim a general policy. | Nine development and three held-out cells; nearest development cell rule has zero median/worst regret on these three cells, while 5/12 cells have unresolved best-vs-second method under δ. The held-out count is small and synthetic. |

S3 report: `s3_10block_20261123.md`. Synthetic design note: `synthetic_sweep_design_20261123.md`. Sweep artifacts: `data/benchmarks/phase_c/synthetic_grid_v3_20261123/`; sensitivity artifacts: `data/benchmarks/phase_c/sensitivity_v3_20261123/`; decision outputs: `data/benchmarks/phase_c/decision_v3_final_20261123/`.

Interpretation limits: the decision rule is nearest-neighbour on only nine development cells, tested on three synthetic held-out cells. The design axis measures compressed Parquet bytes. The follow-up Event Log extraction reports SQL broadcast relation sizes for executed exchanges only. The always-AQE reference matches the nearest-cell rule on the three held-out cells. This sweep demonstrates behavior of the generated workload and does not estimate general NYC or production thresholds. The S3 repeated session did not capture continuous host/cgroup telemetry. The literature review gate remains before publication-oriented novelty claims. Hour-level exact P95 sets are descriptive diagnostics only; they do not replace the frozen P95 benchmark labels or establish that hourly salting improves a join.

Execution summary: see [`phase_c_execution_report_20261123.md`](phase_c_execution_report_20261123.md) for the consolidated WP0–WP7 results, the hourly hotspot extension and remaining evidence gaps.
