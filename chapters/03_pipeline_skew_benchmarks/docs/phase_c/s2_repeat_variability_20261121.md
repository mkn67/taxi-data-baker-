# Phase C — S2 repeat session and variability check

## Run provenance

This is a separate `pilot_standalone` session, not a continuation or replacement of the earlier S2 pilot and not part of the five-month official benchmark. It reused the same cumulative S2 inputs and the same four ND3 methods, with three measured iterations per method and one action warm-up per method.

| Item | Evidence |
|---|---|
| Spark application | `app-20261121092615-0007`, `FINISHED` |
| Input | S2 Silver, 60,133,386 rows; OD, 62,223 keys; P95 hotspot artifact rebuilt from S2 |
| Silver / OD run fingerprints | `d388d7e52aa55fd63dce11026a0dbd6609b7926118aad0122bc239fc16445075` / `a56baf9e34dd3d97dfa2eac3e49a8374e6bfe7a3d3e6e583c1c1ae3a0237ae3b` |
| Copied input-manifest SHA-256 | `bbb7d3988ea3f6c6617f7be1d4456317824ffafe89382aefc9e1aaf95cba0d56` |
| Configuration | Spark 3.5.9 Standalone; two workers on one host; one executor core and 1,500 MiB per executor; four total cores; 200 shuffle partitions |
| Result tier | `pilot_standalone`; output isolated under `data/benchmarks/scaling/s2_jan_mar/nd3_repeat_20261121T1625/` |

All **12/12** measured runs have collector `OK`, DuckDB `PASS`, and evidence from both workers. The run CSV records the application, configuration, input fingerprints, worker evidence, selected stage, correctness and executed-plan paths for every iteration. Twelve final plans were saved. All four methods reproduced the expected 60,133,386 trips; no measured run was excluded.

## Repeat results

Times are seconds; the three iteration values are shown in execution order. Shuffle is the selected stage's median shuffle read, and both memory and disk spill were zero in every run.

| Method | Iteration times | Median [min–max] | TSR by iteration | Shuffle Read (MiB) |
|---|---|---:|---|---:|
| Baseline SMJ | 43.333 / 42.101 / 43.309 | 43.309 [42.101–43.333] | 1.94 / 1.78 / 1.76 | 888.10 |
| Spark AQE | 41.712 / 40.368 / 44.282 | 41.712 [40.368–44.282] | 1.15 / 1.14 / 1.25 | 887.63 |
| Salting Join S=8 | 48.006 / 49.825 / 47.749 | 48.006 [47.749–49.825] | 1.58 / 1.64 / 1.66 | 979.51 |
| Broadcast Hash Join reference | 9.726 / 10.573 / 10.048 | 10.048 [9.726–10.573] | 1.18 / 1.27 / 1.29 | 0.04 |

### Comparison with the first S2 session

The ratio is the repeat-session median divided by the first S2-session median. The range width divided by the median is a descriptive summary only, not a CV acceptance threshold.

| Method | First S2 median [min–max] (s) | Repeat median [min–max] (s) | Repeat / first | Range / median, first → repeat |
|---|---:|---:|---:|---:|
| Baseline SMJ | 28.298 [25.507–40.980] | 43.309 [42.101–43.333] | 1.530 | 54.7% → 2.8% |
| Spark AQE | 25.801 [25.502–41.974] | 41.712 [40.368–44.282] | 1.617 | 63.8% → 9.4% |
| Salting Join S=8 | 29.668 [29.587–35.816] | 48.006 [47.749–49.825] | 1.618 | 21.0% → 4.3% |
| Broadcast reference | 6.114 [5.878–10.505] | 10.048 [9.726–10.573] | 1.644 | 75.7% → 8.4% |

The repeat did not reproduce a single last-iteration spike: its three times are closer within each method. However, every repeat-session median is materially higher than the first S2 median. Therefore, the earlier within-session outlier is not enough to explain the cross-session shift, and this one repeat does not establish stable performance.

## Task and resource evidence

Task metrics were re-extracted from successful `SparkListenerTaskEnd` events for the selected stage and attempt in each run CSV. The combined task CSV contains **3,795 successful task records** across S1, first S2, and the S2 repeat. It includes task runtime, executor CPU time, JVM GC time, input/shuffle bytes and records, spills, executor, host, and partition identifiers.

For Baseline, first-S2 per-iteration selected-stage executor CPU totals were **31.7 / 33.6 / 50.8 s**; the repeat was **52.1 / 51.8 / 51.6 s**. For AQE, they were **33.8 / 33.8 / 53.9 s** and **52.8 / 52.5 / 59.0 s**, respectively. The repeat's task runtimes resemble the slower third iteration of the earlier session, but do so across all three iterations. Shuffle read stayed constant between sessions for each method. This is consistent with the wall-time change being reflected in task execution, not just driver-side wall time.

Across the selected-stage task records, JVM GC was under **1.7%** of summed executor run time (maximum **1.63%** in S1 Broadcast iteration 2); in the two S2 sessions it stayed under **1.1%**. Memory and disk spill were zero. These observations make GC and spill unlikely to explain the large first-session outliers in those stages. They do **not** isolate host CPU contention, CPU throttling, storage wait, or background workload. Per-task executor CPU time is not host CPU utilization.

Podman cgroup snapshots were collected approximately every two seconds for the master, both workers, driver, and Airflow. Per-measured-run windows are in the resource CSV; raw snapshots remain alongside it. Worker-window median `CPUPerc` values ranged roughly **47.6–54.1%**, with observed per-window maxima up to **54.4%**; maximum sampled worker memory was **1.702 GB**. The worker containers had a two-CPU quota, while each executor was configured for one core. These container-level samples do not reveal host-wide load or CPU throttling. Block/network I/O are retained as cumulative start/end snapshots, not application-exclusive storage-wait measurements.

## AQE final-plan intervention

The table summarizes all nine AQE iterations using the saved final plans and the selected-stage task counts. Each plan is final, retains Sort-Merge Join, and shows three coalesced shuffle reads. No skew-split marker was found.

| Input level/session | Iterations | Selected-stage tasks | Final join | Coalesced shuffle reads per plan | Skew split |
|---|---:|---:|---|---:|---|
| S1 | 1–3 | 5 each | Sort-Merge Join | 3 | Not detected |
| S2 first session | 1–3 | 16 each | Sort-Merge Join | 3 | Not detected |
| S2 repeat | 1–3 | 16 each | Sort-Merge Join | 3 | Not detected |

This is evidence of AQE coalescing, not skew-partition splitting. Broadcast remains an explicitly hinted reference (`F.broadcast`); this session did not test Spark's automatic join choice. The compressed OD Parquet size is not the in-memory broadcast relation size, so broadcast memory feasibility remains unmeasured.

## Artifacts and decision

- Run CSV, summary, DuckDB audit, session context, copied S2 manifest, Spark-submit log, and raw cgroup snapshots: `data/benchmarks/scaling/s2_jan_mar/nd3_repeat_20261121T1625/`.
- Task-level records: `nd3_task_records_s1_s2_repeat.csv` in that directory.
- AQE final-plan table: `nd3_aqe_interventions_s1_s2_repeat.csv` in that directory.
- Per-run container resources: `nd3_container_resources_s2_repeat.csv` in that directory.
- Application Event Log: `logs/spark-events/app-20261121092615-0007`.
- Reproduction/analysis code: `scripts/chapter_03/analysis/analyze_nd3_scaling_evidence.py`.

**Decision:** keep S3 blocked for now. The repeat narrows within-session ranges but leaves a large shift between S2 sessions, with no host-level CPU/throttling or storage-wait evidence to attribute it. Do not combine the two sessions into one median or infer stable scaling. A further experiment should first control or measure host-level conditions; no multi-factor ablation is opened by this result. No DOCX was changed.

**Cập nhật sau phiên thứ ba:** Đã chạy thêm S2 với lấy mẫu host/cgroup; kết quả, giới hạn lấy mẫu và quyết định cổng S3 ở [s2_host_checked_20261121.md](s2_host_checked_20261121.md). Biên bản này tiếp tục mô tả riêng phiên repeat, không thay bằng số của phiên mới.

**Cập nhật sau phiên S2 kiểm soát đầy đủ:** `app-20261121171337-0001` có 12/12 lượt OK/PASS, 12/12 cửa sổ host/cgroup, run provenance, plans, task metrics và DuckDB audit. Xem [s2_controlled_20261122.md](s2_controlled_20261122.md). Phiên repeat chậm vẫn được giữ lại và nguyên nhân chưa xác định; quyết định mới chỉ cho mở S3 pilot thăm dò có telemetry, chưa mở benchmark S3 official.
