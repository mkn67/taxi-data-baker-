# Synthetic sweep design audit — 23/11/2026

The first generated grid (`synthetic_v2`) is **diagnostic only**. Although its dimension Parquet files ranged from 4 KiB to 48.1 MB, Spark's unhinted strategy chose `BroadcastHashJoin BuildLeft` on the fact side for a medium-dimension cell. The fact's projected representation was small enough for that choice. The v2 orchestrator was stopped after three complete cells; its partial fourth cell and artifacts are retained, excluded from decision-rule fitting and held-out evaluation.

The revised grid (`synthetic_v3`) keeps the same frozen Zipf exponents, seed, dimension sizes and development/held-out split. It changes row-level `trip_miles` and `base_passenger_fare` to high-entropy values that the benchmark materializes, increasing each fact Parquet file to about 43–44 MB. The new `moderate_medium` pilot (2 million facts, 19.2 MB dimension Parquet) passed DuckDB for all four methods. Its unhinted final plan is `SortMergeJoin`, establishing that the planner no longer broadcasts the fact in this probe. The pilot remains separate from the five-block grid.

Measured dimension storage bytes are 4,146; 5,182,230; 19,232,349; and 48,093,999. These are compressed Parquet sizes, not Spark's in-memory broadcast relation sizes. The sweep must report the actual final join plan per cell. A dimension above 10 MiB on disk is not by itself proof that Broadcast is infeasible.

`synthetic_grid_v3_20261123` runs the 12 cells sequentially with five randomized blocks per cell and four join arms. The sensitivity job waits for all 12 cells to complete successfully before starting. Any failed cell remains visible in `grid_status.json`; no decision map is published until all cells pass correctness and plan gates.
