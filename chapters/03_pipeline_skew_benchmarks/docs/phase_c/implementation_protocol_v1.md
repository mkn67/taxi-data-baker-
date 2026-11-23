# Protocol v1 — Chương 3–4 benchmark extension

## Research question
Under which combinations of key skew and dimension-side size is salting beneficial, and when are baseline SMJ, AQE, or Spark-selected broadcast sufficient?

## Scope and evidence separation
- Real NYC workloads (S1/S2/S3) and synthetic workload sweeps are reported separately.
- Existing S3 official results remain the real-data reference; do not merge pilots or sessions with different protocols.
- Main comparison arms: baseline SMJ, standard AQE, Salting S=8, and unhinted Spark auto-strategy. Explicit broadcast hint is diagnostic only.

## Pre-registered primary comparisons
1. S3 official: baseline vs AQE vs Salting S=8; report the existing official run independently.
2. Auto-strategy comparison: report separately unless re-run under the same protocol and manifest as every main arm.
3. Synthetic held-out decision evaluation: select among methods using skew and dimension bytes; compare rule regret against the per-cell oracle.

## Statistical protocol to freeze before official runs
- Experimental unit: a run block on one fixed manifest/configuration; methods in each block use the same inputs and randomized order.
- Pilot estimates run-time variance and determines the fixed number of blocks and the maximum time budget before official data are examined.
- Primary summary: median wall-time and paired median ratio/difference by block; report bootstrap 95% confidence intervals and min–max.
- Practical threshold: set delta (δ) as the minimum worthwhile wall-time reduction before official runs. Treat differences within δ or whose interval does not resolve the comparison as practically unresolved.
- For multiple primary comparisons, use Holm adjustment on paired tests/interval claims; retain descriptive estimates regardless of adjusted result. Do not claim significance from n=3.
- Stop at the pre-set block/time cap. If uncertainty remains too wide, report inconclusive; do not extend selectively after seeing a preferred result.
- Exclusion rules are fixed before runs. Environment failures may be excluded with reason; method-specific OOM/failure is an infeasible outcome, not an excluded sample.

## Synthetic workload protocol
- Development cells: fixed combinations of 3–4 skew levels and dimension-side byte sizes.
- Generator: Zipf key-frequency generator, fixed and recorded seed, parameters and generated manifest. A lognormal alternative may be run as a separate sensitivity, not silently mixed.
- Hold out complete skew × dimension-size cells before inspecting performance; use only development cells to derive decision boundaries.
- Dimension size is reported in measured bytes. Keep generated dimension within the 1,500 MiB executor memory budget; record construction/storage size and observed broadcast behavior. Lowering the auto-broadcast threshold is a planner sensitivity, not proof of physical infeasibility.
- Correctness is checked per cell. Synthetic results are never pooled with NYC results.

## Decision rule and held-out evaluation
- Oracle per held-out cell: fastest median among methods that return correct output and complete within the recorded resource budget.
- Decision rule is fit only on development cells and frozen before held-out runs.
- Regret = (rule median time − oracle median time) / oracle median time.
- Report median and worst-cell regret, feasible-method coverage, and OOM/infeasible cells. Use δ and paired bootstrap uncertainty to mark near-ties as unresolved; do not count them as confident wins/losses.

## Metrics and provenance
Record wall-time, task executorRunTime, executorCpuTime when available, GC, shuffle read/write, spill, TSR/PSR, OD input/output rows, final physical plan, manifest, Spark configuration/version, application ID, worker evidence and DuckDB correctness. `executorRunTime` is task-time accumulation, not wall-time or CPU time; GC is a component metric and must not be added to runtime. Missing metrics are `NA`.

## Planned follow-up analyses
- Hotspot stability by month/hour: Jaccard plus the share of total trips covered by the selected hotspot set. Only evaluate zone-hour ND2 as a separate branch if temporal variation warrants it.
- AQE sensitivity: at one fixed data tier, vary shuffle partitions across all join arms; separately vary AQE skew thresholds. Keep the primary configuration unchanged and distinguish coalescing from skew splitting.
- Selective broadcast is the first optional proposed method, opened only if the synthetic sweep identifies a useful region where the dimension is large but the hot-key subset is small. Bucketing and per-key skew prediction remain deferred.

## Literature review gate
Revisit the draft “AQE or Salting?” and search/verify the closest recent work before claiming novelty. Treat Flow-Join++ as a conceptual cost-rule comparator only when its rule can be operationalized from available measurements. CIBPartitioner concerns spatial joins; any transfer to key-based OD joins must be described as inspiration, not implementation.
