# DETOUR — Kế hoạch tổng hợp và trạng thái thực hiện

**Cập nhật:** 28/11/2026. Đây là đầu mối kế hoạch của đồ án và paper, gộp các kế hoạch Phase C, Chương 4 và thực nghiệm bổ sung. Phần đầu phản ánh trạng thái mới nhất; các bản cũ được giữ nguyên nội dung ở phụ lục để truy vết, không coi checklist lịch sử là công việc cần chạy lại.

## 1. Mục tiêu và nguyên tắc

- Hoàn thiện báo cáo IS405 5 chương về pipeline Spark, lệch tải aggregation/join, Airflow và phân tích Manhattan; bảo toàn evidence, mã và artifact đã có.
- Chuẩn bị bản thảo paper dựa trên kết quả đã kiểm toán; chỉ khẳng định đóng góp trong phạm vi workload và tài nguyên đã đo.
- Không thay thuật toán, gộp các phiên khác giao thức, tự mở rộng sweep hoặc tạo thêm câu hỏi chỉ vì kết quả thú vị. Mọi phép chạy bổ sung cần câu hỏi, đối chứng, tài nguyên, protocol và tiêu chí dừng riêng.
- Không thay số thiếu bằng 0. Mỗi claim định lượng cần nguồn dữ liệu, application ID, plan, protocol, phương pháp tính và correctness phù hợp.

Lộ trình Research Consolidation ban đầu dự kiến 10 ngày làm việc, tùy khả năng tìm lại artifacts: (1) khóa scope và lập sổ thực nghiệm; (2) audit dữ liệu, execution và thống kê; (3) đồng bộ Chương 1–2; (4) tái cấu trúc Chương 3; (5) viết Chương 4 theo findings; (6) hoàn thiện hình thức; (7) review, đóng băng và chuẩn bị paper. Đây là ước lượng lịch sử, không phải cam kết thời gian hay bằng chứng mọi checklist đã hoàn thành.

Sổ thực nghiệm phân biệt tám nhóm: E1 official tháng 01, E2 các tháng 01–05 độc lập, E3 S1–S3 tích lũy, E4 S3 Phase C 10 paired blocks, E5 synthetic Zipf × dimension, E6 sensitivity/hotspot/P95, E7 ND4 Airflow tháng 03 và E8 Gold Manhattan. Các phiên bổ sung 96, 32 và 16 action có application và protocol riêng. Không gộp ND3 official/ND4 trước khi runner sửa warm-up với các phiên có action warm-up được xác nhận thành cùng một mẫu.

## 2. Lộ trình từ đầu đến hiện tại

| Giai đoạn | Nội dung | Trạng thái và bằng chứng |
| --- | --- | --- |
| Dữ liệu, pipeline và benchmark gốc | Raw → Silver, hotspot P95, ND2 aggregation, ND3 OD join, DuckDB và ND4 Airflow; Gold Manhattan. | Đã triển khai; mỗi phiên có phạm vi và giao thức riêng. Xem [sổ thực nghiệm](01_experiment_registry.md) và [audit](02_evidence_audit.md). |
| Phase C WP0–WP7 | Khóa protocol, chi phí P95, công việc task, S3 10 block, ổn định hotspot, lưới synthetic, độ nhạy và đánh giá quy tắc trên ô giữ lại. | Các gói đã thực hiện theo phạm vi trong [báo cáo Phase C](../../chapters/03_pipeline_skew_benchmarks/docs/phase_c/phase_c_execution_report_20261123.md). Quy tắc chưa thiết lập vùng Salting thắng AQE. |
| Research Consolidation | Đồng bộ báo cáo, kiểm tra số liệu, provenance, hình, phương pháp và findings. | Có bảng audit và bản thảo Chương 3–4; vẫn cần rà toàn bộ báo cáo, mục lục và phiên bản Word/PDF trước khi nộp. |
| Kiểm chứng AQE | Hai workload × 6 nhánh × 8 block = 96 action trong application `app-20261125142144-0001`. | Audit I/O 96/96; DuckDB là 12 phép kiểm cấp workload/phương án, không phải 96 output độc lập. 10M có split ở C 8/8 plan; 2M chưa thấy marker 8/8. Không quy khác biệt chỉ cho số dòng. Xem [audit 96 lượt](10_confirmation_evidence_audit.md). |
| Chi phí nhân bản | Hai dimension × B/E × 8 block = 32 action trong `app-20261128072824-0002`. | Output đo 32/32 PASS DuckDB; Generate tăng 700.000 dòng; median ratio-of-ratios 0,9972, CI 95% [0,9381; 1,0632], chưa phân định theo ±5%. Thay unmatched filler, không đo matched-key fanout, không so E/C trực tiếp. [Nhật ký](11_replication_experiment_execution_20261128.md). |
| Dao động cùng plan | Pilot 4 action và phiên độc lập 16 action trong `app-20261128075949-0004`. | Audit 16/16 output/plan và 64 stage I/O; trung vị abs(Y/X−1) = 4,63%, 4/8 cặp vượt 5%. Chỉ mô tả một workload, một application và một host; không phải noise floor chung. [Audit 16 lượt](19_same_plan_diagnostic_8blocks_20261128.md). |
| Đối chiếu Malviya và liên quan | Đã xem paper và repo commit `b62cc0ecfc275681a1052e3aa30f6a6778698d36`. | Trùng đáng kể AQE–Salting và ablation; chưa xác lập novelty, chưa có raw của họ để kiểm chứng số. [Bảng so sánh](15_malviya_comparison_20261128.md). |
| Bản Word 5 chương | Đã tạo bản đầy đủ bổ sung Chương 3–4 và bản tách hai chương. | Mục lục bản đầy đủ là tĩnh, số trang cần cập nhật; các chương ngoài 3–4 chưa được rà toàn diện trong đợt xuất. Không gọi là bản nộp cuối khi chưa kiểm tra. |

## 3. Việc còn làm, theo thứ tự

1. **Rà bản Word đầy đủ:** đối chiếu claim, bảng, hình và chú thích Chương 1–5 với artifacts và giữa các chương. Kiểm đặc biệt tài nguyên executor, phiên warm-up cũ/mới, phạm vi DuckDB, số lượng dataset, hình Gold và kết luận Chương 5.
2. **Đồng bộ tài liệu tham khảo:** rà trích dẫn Malviya và các nghiên cứu liên quan theo bản/nguồn thực tế. Nêu khác biệt workload và thiết kế như đặc điểm của DETOUR; chưa khẳng định ưu thế hay độ mới tổng quát.
3. **Hoàn thiện phát hành:** sửa mục lục và danh mục bảng/hình, trích dẫn, đánh số, đường dẫn, phân trang; xuất PDF mới và kiểm tra hiển thị. Lưu phiên bản kèm changelog.
4. **Paper riêng:** chọn storyline dựa trên evidence, viết related work, phương pháp, kết quả và giới hạn. Nếu đóng góp chưa đủ khác biệt, cân nhắc case study hoặc một thiết kế nghiên cứu mới có kiểm soát; không chọn thí nghiệm để ép ra chiến thắng.

## 4. Các tùy chọn ngoài phạm vi hiện tại

- E/C cùng block trên input khác hoặc thay matched hot-key fanout: chưa chạy; cần protocol, correctness, preflight cardinality và ngân sách riêng.
- Nhiều application/host: chưa chạy; không dùng 16 lượt cùng plan làm bằng chứng khái quát qua hệ thống.
- [Đề xuất MLlib dự đoán hotspot](../../chapters/05_conclusion_future_work/plan_mllib_du_doan_hotspot.md): hướng riêng, chưa triển khai; không tự đưa thành kết quả hoặc vào phạm vi benchmark hiện hành.

## 5. Tài liệu làm việc chính

- [Bản thảo Chương 3](../../chapters/03_pipeline_skew_benchmarks/manuscript/chapter3_complete.md) · [Chương 4](../../chapters/04_results_tradeoffs/manuscript/chapter4_results.md) · [nhật ký thay đổi](CHANGELOG.md).
- [Protocol Phase C lịch sử](../../chapters/03_pipeline_skew_benchmarks/docs/phase_c/implementation_protocol_v1.md) · [nhật ký bổ sung](09_paper_supplement_execution_log.md) · [audit 96](10_confirmation_evidence_audit.md) · [audit replication](11_replication_experiment_execution_20261128.md) · [audit cùng plan](19_same_plan_diagnostic_8blocks_20261128.md).
- [Dàn ý paper](07_paper_outline.md) · [bản nháp phương pháp/kết quả](14_paper_draft_methods_results.md) · [đối chiếu nghiên cứu](12_related_work_comparison_20261128.md).

---

## Phụ lục — Nội dung các kế hoạch trước khi gộp

**Lưu vết lịch sử:** bốn phần sau được giữ nội dung của các kế hoạch cũ để không mất thiết kế và lệnh. Thời điểm, trạng thái, các checklist chưa tick và khuyến nghị chạy lại trong các phần này có thể đã cũ; ưu tiên phần 1–5 ở đầu tài liệu. Đây không phải bốn kế hoạch đang cần thực hiện song song.

### Bản lưu: Kế hoạch Phase C WP0–WP7

Nguồn lịch sử: `chapters/03_pipeline_skew_benchmarks/docs/phase_c/implementation_plan_wp0_wp7.md` (đã gộp vào tài liệu này).

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

Execution summary: see [`phase_c_execution_report_20261123.md`](../../chapters/03_pipeline_skew_benchmarks/docs/phase_c/phase_c_execution_report_20261123.md) for the consolidated WP0–WP7 results, the hourly hotspot extension and remaining evidence gaps.


### Bản lưu: Kế hoạch và trạng thái Chương 4

Nguồn lịch sử: `chapters/04_results_tradeoffs/planning/plan_chuong_4.md` (đã gộp vào tài liệu này).

# Kế hoạch và trạng thái Chương 4

## Phạm vi

Chương 4 báo cáo kết quả đo được và phân tích đánh đổi, còn định nghĩa phương pháp nằm ở Chương 3. Bản thảo chính là [`chapter4_results.md`](../../chapters/04_results_tradeoffs/manuscript/chapter4_results.md), gồm các mục 4.1–4.8. Số liệu tháng 01 và lần chạy Airflow tháng 03 được trình bày tách biệt. Benchmark tháng 01–05 là các input độc lập; ngoài ra ND3 đã có tier tích lũy S1/S2/S3, trong đó S1/S2 là pilot và S3 là official. Không gộp hai kiểu phạm vi này.

## Trạng thái theo mục

| Mục | Nội dung đã có | Nguồn bằng chứng / giới hạn |
|---|---|---|
| **4.1 Thiết lập và đo lường** | Cấu hình Podman Compose, Master/2 Worker/Driver, biến kiểm soát, warm-up, ba lượt đo, REST API/Event Log, manifest và cách đọc TSR/PSR. | S3 official có host/cgroup telemetry lấy mẫu đủ 9/9 cửa sổ; các lượt cũ không có bộ telemetry đồng nhất. Không có CPU/I/O attribution theo stage, SparkListener tùy biến hay network throughput vật lý. |
| **4.2 ND2 Aggregation** | Baseline, Uniform Salting và Selective Salting; thời gian trung vị và khoảng nhỏ nhất–lớn nhất, TSR/PSR, shuffle/spill và diễn giải P95. | Bảng/Hình 4.2 dùng official tháng 01, application `app-20261103153557-0001`. Kết quả ND4 tháng 03 được trình bày riêng ở Bảng 4.12. |
| **4.3 ND3 Shuffle Join** | Baseline SMJ, AQE, Salting Join; task metrics, final plan, spill, OD replication và tier S3 tích lũy. FlowJoin++ gợi ý thêm một đối chứng vận hành Spark tự chọn chiến lược join, không hint. | Tháng 01 dùng application `app-20261103153727-0002`; S3 official dùng `app-20261121180448-0005`, 9/9 lượt OK/PASS. AQE coalesce nhưng không split skew. Phiên auto-strategy `app-20261122074817-0000` bổ sung Bảng 4.16/Hình 4.8: 12/12 OK/PASS, hai Worker, cùng S3 manifest; ba auto plans đều BroadcastHashJoin, telemetry đủ 12/12 cửa sổ. ND4 tháng 03 ở Bảng 4.12. |
| **4.4 Kiểm chứng** | DuckDB audit ND2/ND3, preflight tính duy nhất khóa OD, cách diễn giải 90 dòng metadata theo 5 tháng. | `PASS` của 3 iteration cùng phương án là trạng thái validation cấp phương án được gắn vào metadata, không phải ba phép audit độc lập. |
| **4.5 So sánh phạm vi dữ liệu** | Raw/Silver tách riêng; kết quả 5 tháng độc lập và tier tích lũy S1/S2/S3 được phân biệt. | Các tháng riêng lẻ có dải Silver hẹp; S1/S2 là pilot, S3 là official. Kết quả mô tả ba tier, chưa chứng minh quy luật scaling tổng quát. |
| **4.6 Phân tích không gian** | Gold tháng 01, so sánh south/north trong cùng khung giờ, bản đồ tĩnh và Kepler.gl, đối chiếu bối cảnh và giới hạn mô tả. | Nhóm toàn Gold: 41 phía Nam, 23 phía Bắc, 5 mơ hồ, 194 ngoài Manhattan. Đây là nhóm phân tích theo đường xấp xỉ, không phải nhãn CRZ chính thức; không suy luận nhân quả. |
| **4.7 Trade-offs và Phase C** | Đối chứng S3 ngày 22/11, phiên ghép cặp 10 block ngày 23/11, chi phí P95, độ ổn định hotspot, lưới tổng hợp, độ nhạy và quy tắc chọn. | S3 40/40 OK/PASS; lưới v3 240/240; độ nhạy 60/60. Quy tắc chọn có ba ô giữ lại, cho kết quả như mốc luôn chọn AQE. |
| **4.8 Tổng kết** | Tổng hợp kết quả, điều kiện thí nghiệm và giới hạn suy rộng. | LightGBM/ML chưa triển khai, chỉ có thể nêu là hướng phát triển ở Chương 5. |

## Đối chiếu bản thảo với WP0–WP7

| Gói công việc | Vị trí trong bản thảo | Bằng chứng được trình bày |
|---|---|---|
| WP0 | 4.1.4 | Giao thức block ghép cặp, ngưỡng 5%, bootstrap và Holm. |
| WP1–WP3 | 4.4.3, 4.7.2 | Chi phí quét P95, Event Log theo job group, phiên 10 block và audit theo phương án. |
| WP4 | 4.7.3 | Jaccard và tỷ trọng chuyến thuộc hotspot theo tháng/giờ. |
| WP5 | 4.7.4–4.7.5 | Lưới v3, bản đồ kết quả, byte Parquet và kích thước BroadcastExchange. |
| WP6 | 4.7.4 | Độ nhạy theo số phân vùng và ngưỡng skew AQE. |
| WP7 | 4.7.6–4.7.7 | Regret từng ô giữ lại, mốc luôn chọn AQE và ma trận kiểm tra trên workload đích. |

Xem [plan WP0–WP7](00_plan_tong_hop_detour.md) và [báo cáo thực thi](../../chapters/03_pipeline_skew_benchmarks/docs/phase_c/phase_c_execution_report_20261123.md) để đối chiếu trạng thái từng gói.

## ND4 — Airflow benchmark tháng 03/2026

Một lượt xác nhận sạch có run ID `nd4-march-clean-20261120`, hoàn tất ngày 20/11/2026. DAG `chapter4_nd4_march_2026_benchmark` chạy tuần tự ND2 rồi ND3, sau đó kiểm định và đóng gói kết quả. Cả ba task Airflow đều có trạng thái `success`. ND2 dùng application `app-20261120074816-0006`; ND3 dùng `app-20261120074935-0007`. ND2 có action warm-up; ND3 cũ không tạo job warm-up (xác nhận bằng Event Log, runner đã sửa ngày 21/11/2026). Mỗi phương án có ba lượt đo; 18 dòng run đều có collector `OK`, DuckDB validation `PASS`, Spark 3.5.9 và bằng chứng hai Worker.

Artifacts của lượt xác nhận nằm tại `data/benchmarks/nd4_airflow/2026_03/nd4-march-clean-20261120/`: run CSV, summary/audit CSV, hình thời gian và provenance JSON. Một lượt Airflow trước đó cần chạy lại thủ công task đóng gói do container cũ thiếu Matplotlib; lượt xác nhận sạch sau khi dựng image mới chạy toàn DAG, không chạy task thủ công.

Hai phiên tái lập ngày 22/11/2026 dùng runner có action warm-up ND3, cùng manifest Silver tháng 03: `nd4-march-repeat-20261122` và `nd4-march-repeat2-20261122`. Cả 6 task thành công và 36/36 dòng đo `OK/PASS`. Kết quả và giới hạn dao động nằm trong `../evidence/nd4_reproducibility_20261122.md`; lượt 20/11 được báo riêng vì giao thức ND3 khác.

## Lệnh tái lập

```bash
podman compose up -d --build spark-master spark-worker-1 spark-worker-2 airflow-standalone
podman exec airflow-standalone airflow dags unpause chapter4_nd4_march_2026_benchmark
podman exec airflow-standalone airflow dags trigger chapter4_nd4_march_2026_benchmark
podman exec airflow-standalone airflow dags list-runs --dag-id chapter4_nd4_march_2026_benchmark
```

Airflow UI: `http://localhost:8085`; Spark Master UI: `http://localhost:8080`. Đóng gói lại một thư mục run có sẵn:

```bash
python scripts/chapter_04/build_nd4_march_artifacts.py \
  --airflow-run-id <dag-run-id> \
  --run-dir data/benchmarks/nd4_airflow/2026_03/<dag-run-id>
```

Các script `build_chapter4_evidence.py`, `build_chapter4_figures.py` và `build_chapter4_remaining.py` chỉ tổng hợp evidence đã lưu, không khởi chạy benchmark Spark.

## Giới hạn cần giữ trong bản nộp

- Cụm benchmark gồm nhiều container trên cùng host, không đại diện trực tiếp cho cụm nhiều máy qua mạng vật lý.
- Giới hạn CPU/RAM Compose là cấu hình, không phải usage. S3 official và phiên auto-strategy ngày 22/11 có host/cgroup telemetry. Phiên 10 block ngày 23/11 có số đo công việc task từ Event Log và thiếu telemetry host/cgroup liên tục.
- Không có SparkListener tùy biến; metric hiện dùng Spark REST API và Event Log.
- Năm tháng Silver độc lập có quy mô gần nhau. Các tier S1–S3 tích lũy đã có artifact nhưng khác phiên/pilot/official; chưa xác lập quy luật scaling tổng quát.
- Các lượt n=3 dùng median và min–max để mô tả, không kết luận ý nghĩa thống kê.
- Gold dùng dữ liệu tháng 01 và đường tham chiếu 60th Street xấp xỉ; kết quả không xác định tác động nhân quả của chính sách thu phí.
- Không tuyên bố đã triển khai LightGBM, CIBPartitioner, MFTune, hay ML skew detection.
- Flow-Join++ là nghiên cứu tham khảo về dự đoán heavy hitter/cardinality và tối ưu distributed join. Project không triển khai Scardina hoặc Heavy Hitter Push-Down; nhánh Spark auto-strategy đã chạy là đối chứng triển khai, được báo riêng với benchmark ba nhánh official trước đó.

## Checklist bàn giao

- [x] Các mục 4.1–4.8 có nội dung trong bản thảo Chương 4.
- [x] Bảng/hình ND2 và ND3 tháng 01 được tạo từ official run artifacts.
- [x] So sánh ổn định theo tháng dùng run-level CSV, tách Raw và Silver, nêu rõ giới hạn quy mô.
- [x] DuckDB audits, worker evidence, plans và manifest được giữ cùng nguồn kết quả.
- [x] ND4 tháng 03 có một lần Airflow end-to-end sạch; bảng, biểu đồ và provenance được tạo tự động.
- [x] Lặp lại DAG ND4 hai phiên ngày 22/11 trên cùng manifest với runner có action warm-up cho ND2 và ND3: 6/6 task success, 36/36 dòng OK/PASS, hai Worker. Biên bản `../evidence/nd4_reproducibility_20261122.md` ghi riêng median từng phiên và các PSR `NA`.
- [x] Bảng spatial và ma trận quyết định có số liệu/giới hạn rõ ràng.
- [x] Các giới hạn về telemetry, Listener, scaling, địa lý và suy luận nhân quả được nêu trong bản thảo.
- [x] Kết quả S3 ND3 official và OD replication đã được đưa vào bản thảo Chương 4.
- [x] Đã chạy đối chứng Spark auto-strategy S3 `app-20261122074817-0000`: bốn nhánh × ba lượt sau warm-up, 12/12 OK/PASS, đúng manifest, hai Worker, 12 plans, đủ telemetry. Bảng 4.16 và Hình 4.8 dùng riêng phiên này; xem biên bản `flowjoin_auto_strategy_20261122.md`.

- [x] Bổ sung kiểm toán tần suất/fanout theo cặp OD sau benchmark; phân biệt pickup P95 với P95 OD chẩn đoán và với mô hình heavy hitter của Flow-Join++.


### Bản lưu: Kế hoạch bổ sung paper ban đầu

Nguồn lịch sử: `docs/refactor/08_paper_supplement_plan.md` (đã gộp vào tài liệu này).

# DETOUR — Kế hoạch thực nghiệm bổ sung cho paper

> **Cập nhật 28/11:** Phiên ablation 96 lượt và replication 32 lượt đã hoàn tất. Repo Malviya đã được kiểm tra tại commit `b62cc0ecfc275681a1052e3aa30f6a6778698d36`; xem [đối chiếu chi tiết](15_malviya_comparison_20261128.md). Các bảng dưới đây mô tả kế hoạch lịch sử; không chạy lại mặc định. Câu hỏi AQE–Salting, ablation coalescing, final plan và replication có trùng lặp đáng kể với Malviya; chưa tuyên bố novelty. Ưu tiên rà claim, provenance, phạm vi noise và viết lại paper. Mọi nghiên cứu E/C matched, fanout có output được kiểm soát hoặc nhiều host cần protocol và ngân sách riêng trước khi chạy.

Phạm vi là ND3 trên Spark 3.5.9. Bản IS405 đã có kết quả quan sát theo phạm vi từng phiên. Kế hoạch này bổ sung bằng chứng cho bản thảo nghiên cứu về coalescing, skew splitting và chi phí Selective Salting. Mỗi lượt mới dùng thư mục output riêng cùng giao thức, manifest và mã phiên.

## 1. Câu hỏi và dấu hiệu cần kiểm chứng

1. **RQ-P1.** Trên cùng workload OD, AQE coalescing thay đổi wall-time bao nhiêu? Khi final plan xác nhận có skew splitting, bật cơ chế này đem lại lợi ích bổ sung bao nhiêu?
2. **RQ-P2.** Khi hai phương án đều được phép coalescing, Selective Salting S=8 so với AQE có skew splitting thế nào? Chi phí của Salting thay đổi ra sao khi số hàng chiều thuộc khóa được salt tăng?

Chứng cứ hiện có giúp chọn thí nghiệm tiếp theo. Trên 12 ô synthetic v3, nhánh AQE có median thấp nhất và cả 60 final plan có coalescing. Lưới chưa ghi nhận skew split. Trên NYC S3, phân tích hậu nghiệm của 10 block cho tỷ số thời gian ghép cặp Salting/AQE khoảng 1,160, KTC bootstrap 95% [1,121; 1,187]. Ở ô synthetic `strong_large`, bảng chiều có 5.000.256 hàng nhưng Salting chỉ nhân thêm 91 hàng. Các kết quả này là mốc tham chiếu theo workload, cấu hình và phạm vi đo đã có.

Nguồn: [Chương 4](../../chapters/04_results_tradeoffs/manuscript/chapter4_results.md), [kiểm toán bằng chứng](02_evidence_audit.md), [lưới v3](evidence/cell_results.csv), [bộ sinh synthetic v3](../../scripts/chapter_03/generate_phase_c_synthetic.py).

## 2. Công việc theo thứ tự

| Giai đoạn | Loại | Công việc | Điều kiện để đi tiếp |
| --- | --- | --- | --- |
| 0. Rà tài liệu và khóa protocol | DOC + AUDIT | Đọc bản Malviya khi có toàn văn, ghi rõ thân bài và phiên bản. Lập bảng đối chiếu với Flow-Join++ và nghiên cứu gần nhất theo câu hỏi, workload, nhánh đối chứng, kiểm chứng final plan, chi phí và bằng chứng thực nghiệm. Chốt cấu hình, block, seed, metric, quy tắc bỏ lượt trước phiên xác nhận. | Có câu hỏi có thể kiểm chứng và tuyên bố đóng góp dự kiến có phạm vi cụ thể. Nếu thiếu Malviya, pilot kỹ thuật vẫn có thể tiến hành. Tuyên bố độ mới chờ đối chiếu toàn văn. |
| 1. Pilot xác nhận cơ chế | CODE + AUDIT | Thêm chế độ ablation riêng. Dùng một input nhỏ từ lưới v3 làm đối chứng coalescing, kiểm tra thêm một input có hot key tạo partition đủ lớn và có dữ liệu từ nhiều shuffle map task. Dùng tối đa 3 cấu hình pilot, tối đa 2 block/cấu hình. | Có một workload mà AQE chạy với coalescing, final plan giữ SMJ và chưa split, cùng một workload có skew split xác nhận được. Nếu hết ngân sách mà chưa split, dừng nhánh xác nhận skew splitting và báo điều kiện/tín hiệu đã đo. |
| 2. Đo xác nhận | CODE + AUDIT | Dùng hai workload đã khóa, sáu nhánh, tám block ghép cặp/workload. Chạy đủ các nhánh trong mỗi block theo thứ tự ngẫu nhiên đã lập trước, kiểm tra phân bố vị trí chạy giữa các nhánh. Tổng **96 lượt đo** cộng warm-up riêng. | Kết quả đúng theo DuckDB, có provenance cho toàn bộ block. Báo tỷ lệ final plan kích hoạt cơ chế theo nhánh/workload. Hiệu ứng của skew splitting cần đi cùng bằng chứng cơ chế trong contrast tương ứng. |
| 3. Kiểm tra chi phí nhân bản | BOTH | Sau khi đóng xong đối chứng cơ chế, giữ một workload có split, thay đổi lượng hàng chiều thuộc hot pickup được salt. Hai mức × hai nhánh chính × tám block, tối đa **32 lượt đo** nếu cả hai mức được đo trong đợt này. | Giữ output join như nhau, chứng minh số hàng nhân thêm thay đổi và ghi nhận mức thay đổi đủ để diễn giải shuffle, CPU và wall-time. Nếu chọn ra điều kiện chiến thắng hoặc ngưỡng thực hành, xác nhận trên seed hoặc workload đã giữ lại trước khi đưa ra quy tắc chọn. |
| 4. Viết phần bổ sung cho paper | DOC | Trình bày bảng paired ratios, final plan, biểu đồ phân vị task và chi phí nhân bản. Bản đồ phương án nhanh nhất có thể để phụ lục. | Các câu kết luận truy ngược được về phép đối chiếu và kết quả ghép cặp. Kết luận độ mới có bảng đối chiếu với các bài liên quan. |

Pilot và phiên xác nhận dùng các đầu ra tách riêng. Chọn workload pilot theo dấu hiệu kích hoạt cơ chế và khả năng hoàn tất trong ngân sách cụm. Kết quả thời gian pilot chỉ dùng chẩn đoán. Các đối chiếu xác nhận dùng dữ liệu thu sau khi đã khóa giao thức. Ngân sách mặc định là **96 lượt xác nhận cơ chế** rồi quyết định có thực hiện thêm **tối đa 32 lượt nhân bản** hay không. Lượt kiểm tra trên workload giữ lại chỉ phát sinh nếu paper muốn công bố một quy tắc chọn phương án.

## 3. Ma trận đối chứng

Mỗi nhánh dùng cùng input, cấu hình tài nguyên, số shuffle partitions ban đầu, pipeline aggregate, cơ chế materialize và đơn vị đo. Giữ SMJ nhất quán, kiểm tra theo final plan. Đặt ngưỡng broadcast tĩnh và thích ứng bằng `-1`. Giữ các cơ chế AQE khác cố định trong nhóm nhánh thích ứng. Ghi cấu hình hiệu lực theo lượt để bắt cấu hình còn sót từ nhánh trước.

| Nhánh | AQE | Coalescing | Skew optimization | Salting | Phép đối chiếu chính |
| --- | --- | --- | --- | --- | --- |
| A. SMJ hiện hành | Tắt | Tắt | Tắt | Tắt | Mốc tương thích với benchmark DETOUR cũ |
| A0. AQE mở, coalescing và skew tắt | Bật | Tắt | Tắt | Tắt | Mốc giữ AQE cố định để đo riêng tác động của việc bật coalescing |
| B. AQE coalescing | Bật | Bật | Tắt | Tắt | B/A0 đo phần thay đổi khi chỉ bật coalescing |
| C. AQE coalescing + skew | Bật | Bật | Bật | Tắt | C/B đo lợi ích bổ sung khi bật skew optimization trên nền coalescing |
| D. Salting S=8 hiện hành | Tắt | Tắt | Tắt | Bật | D/A đo Salting khi hai nhánh cùng chạy với AQE tắt |
| E. Salting S=8 + coalescing | Bật | Bật | Tắt | Bật | E/B đo phần tăng thêm do Salting và E/C so hai phương án trên cùng nền coalescing |

B/A mô tả hiệu ứng của gói cấu hình vì cả AQE và coalescing thay đổi. B/A0 giữ trạng thái AQE cố định. C/B đo skew optimization khi coalescing đang bật. Thiết kế này chưa ước lượng riêng tương tác giữa hai cơ chế ở trạng thái coalescing tắt.

**Gate skew split.** Kiểm tra cả ngưỡng byte tuyệt đối và ngưỡng tương đối theo median partition của từng phía, căn cứ thống kê Spark dùng ở shuffle map stage. Ưu tiên số đo trực tiếp theo phía và partition. Nếu collector hiện tại chỉ có shuffle-read tổng từ task, ghi đó là số liệu chẩn đoán có giới hạn và dựa vào final adaptive plan hoặc SQL metrics xác nhận việc chia partition. Ghi số partition split và số split thực hiện nếu có. Cờ cấu hình, tổng shuffle bytes, Zipf exponent và số task giảm đều đo những đại lượng riêng. Khi hết pilot mà final plan vẫn chưa split, kết quả B/C chỉ cho phép kết luận về hiệu ứng khi bật cấu hình trong phạm vi đó.

**Khóa yếu tố đối chiếu.** Cố định cả số executor và core/heap thực cấp, input Parquet, cách chia input file/map task, số partition ban đầu, skew factor, byte threshold, advisory partition size, cấu hình coalescing và warm-up. Dùng cùng danh sách hotspot bất biến và cùng S=8 cho D/E. Kiểm tra join type, key, phía shuffle và kết quả tổng hợp theo DuckDB. Xác nhận số partition split ở hai workload bằng final plan. Nếu plan thực thi khác giao thức ở một số block, công bố tỷ lệ và nguyên nhân. Phân tích theo cấu hình đã chỉ định giữ các lượt hợp lệ theo quy tắc đã khóa. Phân tích riêng các lượt có split, nếu làm, ghi rõ là chẩn đoán.

Nguồn cấu hình và điều kiện Spark: [Spark 3.5.9 — SQL Performance Tuning](https://spark.apache.org/docs/3.5.9/sql-performance-tuning.html).

## 4. Thiết kế thí nghiệm nhân bản

- Giữ nguyên fact, tập dimension key khớp fact và kết quả join. Cố định tổng số hàng chiều. Dịch chuyển một phần hàng không khớp của dimension giữa hai loại pickup: thuộc và ngoài danh sách salted. Dùng cặp OD duy nhất và giữ schema, kiểu dữ liệu, độ rộng payload và kiểu phân vùng file tương đương.
- Đo `dim_hot_rows`, số hàng chiều sau nhân bản và shuffle/CPU. Với một hàng chiều cho mỗi cặp OD, mức tăng dự kiến là `(S − 1) × dim_hot_rows`. Đối chiếu theo dữ liệu thực tế sau khi tạo cấu hình.
- Báo độc lập kích thước Parquet và khối lượng shuffle. Dù tổng số hàng chiều giống nhau, thay key có thể đổi phân bố hash partition hoặc dung lượng nén. So sánh lại các đại lượng này để giới hạn lời giải thích. Can thiệp này đo **chi phí nhân bản các hàng chiều** khi giữ kết quả join cố định. Nếu nghiên cứu thêm fanout của các khóa khớp, dùng một thiết kế riêng có kiểm soát cardinality đầu ra.

## 5. Phân tích và báo cáo

- **Primary metric** là wall-time của action đã materialize. Chốt trước các tỷ số theo block: B/A0, C/B và E/C là các đối chiếu chính, D/A và E/B giải thích phần chênh lệch. Báo median tỷ số và khoảng tin cậy bootstrap 95% bằng lấy mẫu lại cả block trong từng workload. Dùng biên thực hành ±5% từ Phase C. KTC nằm hoàn toàn dưới 0,95 hoặc trên 1,05 cho thấy vượt biên theo hướng tương ứng. KTC nằm trong [0,95; 1,05] cho phép mô tả chênh lệch nhỏ ở độ phân giải đã đặt. Các trường hợp KTC giao ranh giới được báo là chưa phân định theo biên này. Nếu dùng kiểm định nhiều contrast, chốt nhóm kiểm định và hiệu chỉnh trước khi chạy.
- **Metric cơ chế** gồm số task tại stage join tương ứng, median/p95/max task duration, CPU executor, partition size, shuffle read/write, disk/memory spill, số split, số hàng chiều nhân thêm và độ phủ metric. Truy nguyên stage bằng kế hoạch thực thi và Event Log. Giữ thiếu dữ liệu ở trạng thái thiếu. TSR/PSR từ các stage khác vai trò dùng để mô tả.
- **Chi phí hotspot** có hai chế độ sử dụng. Danh sách đã lưu dùng để so thời gian join thường xuyên. Nếu muốn tuyên bố lợi ích với lần chạy đầu, đo thêm chính pipeline phát hiện hotspot rồi chạy join trong cùng giao thức. Nếu chỉ có hai số đo riêng, báo tổng ước lượng hoặc chi phí phân bổ theo số lần tái dùng.
- **Kết luận âm tính** có giá trị nếu phép thử xác nhận được cơ chế và phạm vi. Nếu AQE có split và vẫn nhanh hơn, báo chênh lệch cùng mức skew, số hàng nhân bản và tài nguyên đã đo. Nếu Salting thắng ở một điều kiện, chỉ mở rộng thành quy tắc sau kiểm tra độc lập. Nếu C và B gần nhau dù có split, báo khoảng bất định và chi phí của việc chia partition ở điều kiện đó.

## 6. File nào sửa khi triển khai

| File hoặc đường dẫn | Loại | Thay đổi dự kiến |
| --- | --- | --- |
| [`benchmark_engine.py`](../../scripts/chapter_03/benchmark_engine.py) và [`run_benchmark_nd3.py`](../../scripts/chapter_03/run_benchmark_nd3.py) | CODE | Thêm chế độ riêng cho ma trận ablation, set tường minh cờ theo nhánh, log cấu hình hiệu lực. Hành vi mặc định của các lệnh cũ tiếp tục phục vụ benchmark IS405. |
| [`generate_phase_c_synthetic.py`](../../scripts/chapter_03/generate_phase_c_synthetic.py) | CODE | Mở rộng bộ sinh cho workload kiểm chứng split và can thiệp số hàng chiều được salt. Ghi bộ dữ liệu mới theo manifest riêng có checksum và thông số sinh. |
| [`analyze_phase_c_paired.py`](../../scripts/chapter_03/analysis/analyze_phase_c_paired.py) | CODE + AUDIT | Tái sử dụng phân tích ghép cặp cho bộ contrast mới. Kiểm tra alignment theo block, phạm vi stage, plan và cấu hình trước khi xuất bảng. |
| `data/benchmarks/phase_c/` | AUDIT | Lưu thư mục mới theo phiên với protocol version, seed, raw CSV, final plans, Event Logs hoặc đường dẫn truy nguyên và DuckDB audit. |
| [Sổ thực nghiệm](01_experiment_registry.md), [kiểm toán](02_evidence_audit.md), [dàn ý paper](07_paper_outline.md) | DOC | Chỉ cập nhật số liệu sau nghiệm thu phiên xác nhận. Gắn từng claim với contrast và workload. Bản Word/PDF IS405 đã phát hành tiếp tục là mốc của đợt consolidation. |

## Đầu ra tối thiểu

- Biên bản pilot kèm bằng chứng một workload có skew split và một workload chỉ thấy coalescing, hoặc kết luận pilot chưa kích hoạt được split trong ngân sách.
- Bảng đối chứng ghép cặp có cấu hình, final plan và tỷ số wall-time. Nếu qua giai đoạn 3, thêm bảng số hàng chiều thực sự nhân bản và chi phí.
- Bảng đối chiếu nghiên cứu liên quan, với mục Malviya chỉ đánh dấu đã đối chiếu sau khi có bản thảo và phiên bản xác định. [Flow-Join++](https://doi.org/10.2197/ipsjjip.34.671) là mốc về nhận diện heavy hitter dựa trên cardinality.

Trạng thái triển khai và các gate còn mở được ghi trong [`09_paper_supplement_execution_log.md`](09_paper_supplement_execution_log.md).


### Bản lưu: Kế hoạch cập nhật sau đối chiếu Malviya

Nguồn lịch sử: `docs/refactor/16_plan_after_malviya_20261128.md` (đã gộp vào tài liệu này).

# DETOUR — Kế hoạch kiểm toán và thực nghiệm bổ sung

**Phiên bản:** 2.0 · 28/11/2026  
**Mục đích:** Tài liệu làm việc cho tác giả và Codex; dùng để kiểm toán bằng chứng, đánh giá chi phí nhân bản của Selective Salting và chuẩn bị viết báo cáo/paper.

## Cập nhật sau phiên 28/11 và paper Malviya

**Cập nhật tiếp sau đối chứng cùng plan:** pilot 4 action và diagnostic độc lập 16 action đã hoàn tất, audit PASS trong phạm vi nêu tại [báo cáo 8 block](19_same_plan_diagnostic_8blocks_20261128.md). Ở diagnostic 16 action, trung vị `abs(Y/X − 1)` là 4,63%, 4/8 block vượt 5%; đây chỉ là mô tả cho workload/application/host đó, không phải noise floor chung hoặc CI cho các phiên khác. Chương 3–4 Markdown và bản thảo paper đã bổ sung giới hạn; Word/PDF chưa cập nhật. Các bước còn lại vẫn là rà claim, related work và quyết định hướng bài; không tự mở sweep mới.

**Trạng thái:** 96 lượt cũ đã được audit I/O; pilot 4 action và confirmation replication 32 action đã hoàn tất. Không chạy lại các bước này như công việc còn tồn. Repo Malviya đã được đối chiếu ở commit `b62cc0ecfc275681a1052e3aa30f6a6778698d36`; đã đọc README, cấu hình core, generator, arms và audit ledger. Đây là kiểm tra mã và tài liệu, chưa chạy tái lập hoặc xác minh kết quả gốc của họ.

- Cả hai workload 2M và 10M của confirmation 96 lượt thuộc cùng application `app-20261125142144-0001`; input và đặc điểm workload khác nhau. Không nêu rằng hai workload này thuộc hai phiên confirmation khác nhau hoặc kết luận chỉ đổi số dòng đã gây split.
- Paper Malviya có AQE vs Salting, đối chứng coalescing, final plan, noise-floor control và probe bật skew split. Không nhận các yếu tố này là novelty tổng quát của DETOUR. Cần đối chiếu cụ thể workload NYC thực, join hai khóa, thiết kế 96/32 lượt, correctness, điều kiện hardware và các giới hạn trước khi xác định đóng góp.
- Audit 96 lượt: 384 stage, 96/96 lượt có shuffle read/write và memory/disk spill khớp task Event Log trong phạm vi collector; DuckDB kiểm 12 kết quả workload × phương án, không phải từng action.
- Replication confirmation `app-20261128072824-0002`: 32/32 output action PASS DuckDB, Generate low/high tương ứng 5.000.347/5.700.347 dòng ở từng lượt E, I/O 32/32 khớp Event Log. Median ratio-of-ratios 0,9972, CI 95% [0,9381; 1,0632] cắt ±5%, chưa phân định. Hai mức thay unmatched filler, khác tăng fanout ở các khóa dimension khớp fact như Malviya khảo sát; không so trực tiếp với AQE skew join C hay kết luận replication miễn phí.
- Cần đánh giá noise floor một cách có kiểm soát: dùng các block/đối chứng có sẵn trước; chỉ đề xuất đo thêm sau khi đối chiếu repo và chốt hypothesis, phạm vi, ngân sách. Nếu không có identical-plan control thì ghi **chưa đo**, không tự đặt ±5% là noise floor đo được. Khi so TSR, báo thêm số task và max task duration; mức parallelism/stage khác nhau khiến max/median có thể thay đổi do mẫu số.
- Ưu tiên hiện tại: kiểm tra provenance và những raw artifacts còn thiếu, đối chiếu paper với code đã pin; audit lại các claim; chỉnh paper và tài liệu. Không tự chạy 32/48 lượt mới hoặc triển khai thuật toán mới chỉ vì paper liên quan có sweep.

### Điều chỉnh dựa trên repo Malviya tại commit đã kiểm

1. **Chọn nguồn hiện hành.** README phân biệt paper 13 trang hiện tại với phần overview cũ. Phần cũ nói có crossover, nhưng ledger của repo giải thích kết luận này bị ảnh hưởng bởi thứ tự baseline/JVM lạnh và nói kết quả mới không ủng hộ crossover đó. Không trích bảng cũ làm finding mới; trích phiên bản, trang và commit chính xác.
2. **Phân biệt code với dữ liệu raw.** README nêu generated data, raw results và Event Logs bị loại khỏi Git; không có kho raw hay DOI được dẫn. Vì vậy có thể so code/config và claim được công bố, nhưng chưa kiểm chứng độc lập các số của họ. `docs/REPRODUCIBILITY.md` và README có thông tin lịch sử khác nhau; báo rõ nguồn, không mặc định mọi chỉ dẫn đều áp dụng cho phiên mới nhất.
3. **Provenance 2M:** repo `config/v3_core.yaml` đặt seed 20260818, 2.000 engines, theta 1,2, dimension 20.000 và schema telemetry; `src/skewbench/datagen.py` sinh fact/dimension bằng NumPy/pandas. DETOUR `generate_phase_c_synthetic.py` dùng schema OD, 256 pickup keys, seed 20261123 và dimension khác. Các mô tả generator/config tại hai commit khác nhau rõ rệt; sự trùng số 2M không đủ kết luận chia sẻ dữ liệu hay sao chép. Chưa có raw phía kia để so checksum trực tiếp; không tuyên bố quyền tác giả hoặc độc lập tuyệt đối.
4. **Chỉnh phát biểu advisory:** PDF có probe bật split khi thay advisory và threshold; audit ledger H4 tự ghi phép thử một biến còn thiếu. Vì vậy không viết rằng họ đã chứng minh riêng advisory gây split bằng đối chứng một biến. Không tự thiết kế một nhánh mới cho DETOUR nếu không có hypothesis và ngân sách được chốt.
5. **Không xem TSR là metric quyết định đơn lẻ.** Đối chiếu số task, max/median executor runtime, plan, shuffle records/bytes và wall-time theo cùng execution/stage. Max/mean cũng thay đổi theo phân hoạch và workload; không gọi bất kỳ ratio nào là hoàn toàn bất biến theo phần cứng hay coalescing. Nếu thiếu cùng-stage metric thì báo unavailable.
6. **Ưu tiên evidence thật và giới hạn theo phiên:** NYC S3 cho workload thực nhưng chưa thấy split; synthetic 10M cho C/B có split 8/8 với CI chạm biên thực hành; replication 32 lượt B/E cho unmatched filler, chưa kiểm P của khóa khớp như bên họ. Chưa có vùng E thắng C. Ba kết quả phục vụ các câu hỏi khác nhau và không gộp thành một mẫu.
7. **Tiêu chí chọn hướng paper:** trước hết viết đóng góp ứng viên và ma trận so với Malviya, Flow-Join++ cùng literature gần nhất. Nếu không chỉ ra được câu hỏi/điều kiện và evidence khác biệt đủ rõ, báo paper cần thu hẹp thành báo cáo/case study hoặc nghiên cứu thêm có kiểm soát; không khẳng định novelty do khác tên dữ liệu hay khác số lượng nhánh.
8. **Tùy chọn tương lai (chưa phê duyệt/chưa chạy):** đối chứng matched hot-key fanout có kiểm soát cardinality, hoặc E/C cùng block, hoặc xác nhận trên nhiều application/host. Mỗi hướng cần câu hỏi, tài nguyên, validity gate và budget riêng trước khi triển khai. Không so E 32 lượt với C 96 lượt như kết quả ghép cặp.

### Việc cần làm tiếp theo — theo thứ tự

| Ưu tiên | Việc cần làm | Đầu ra và cổng hoàn thành |
|---|---|---|
| 1 | Rà Chương 3–4 với registry, plan, Event Log và audit 96/32; đối chiếu nguồn, phạm vi metric, số task và correctness. | Bảng claim → artifact → trạng thái; correction được ghi changelog. Không điều chỉnh số khi thiếu evidence. |
| 2 | Đọc đối chiếu bản PDF với repo đã pin và lập bảng Malviya/DETOUR: dữ liệu, join, AQE, kiểm soát, thiết kế, metric và giới hạn. | Trích trang/đường dẫn và commit; phân biệt code, claim trong paper và kết quả raw chưa có. |
| 3 | Sửa lập luận và dàn ý paper theo evidence đã có: NYC S3, 96 lượt ablation, 32 lượt replication, mỗi phiên với giới hạn riêng. | Không khẳng định thắng C, causal effect riêng advisory, hay novelty tổng quát; giữ khác biệt thực nghiệm như đóng góp ứng viên cần review. |
| 4 | Review độ sẵn sàng. Nếu câu hỏi paper vẫn chưa đủ khác biệt, lựa chọn báo cáo case study hoặc thiết kế bổ sung theo gate riêng. | Chỉ đề xuất thử nghiệm có hypothesis, control, thông số, tài nguyên và tiêu chí dừng đã khóa; chưa tự chạy. |

Hoàn tất khi claim định lượng trong bản thảo có evidence tương ứng, phần so sánh nghiên cứu có nguồn, và mọi giới hạn chính được nêu. Trạng thái xuất Word/PDF phải được kiểm tra riêng; Markdown cập nhật không đồng nghĩa bản phát hành đã cập nhật.

## 1. Mục tiêu và giới hạn

1. Hoàn tất kiểm toán provenance và metric của 96 lượt confirmation hiện có, nhất là shuffle/spill.
2. Đo tác động của việc tăng số dòng dimension thuộc hotspot tới chi phí Salting S=8 trên cùng workload synthetic 10M.
3. Đối chiếu tài liệu tham khảo, ghi chính xác đóng góp và giới hạn của DETOUR.
4. Cập nhật Chương 3–4 và tư liệu paper sau khi nghiệm thu evidence; giữ bản phát hành hiện có làm mốc.

Không thay thuật toán, không tạo research question mới và không chạy toàn bộ benchmark cũ. Không chọn kết quả bằng cách thử lặp đến khi Salting thắng. Những nhánh ngoài ngân sách và thiết kế dưới đây chỉ là lựa chọn tương lai, không tự động triển khai. Thời gian trong plan là ước lượng, phụ thuộc môi trường và thời gian audit.

## 2. Trạng thái đã có và phản hồi nhận xét trong ảnh

Nhận xét cho rằng không thấy ma trận sáu nhánh và run 10M trong `DETOUR_BigData_2026-11-25.docx` là hợp lý đối với bản Word được xem; việc không thấy trong bản Word đó không tự nó chứng minh số liệu của người khác. Thực tế workspace hiện có các artifact sau; cần kiểm tra chéo nội dung và nguồn gốc trước khi dùng làm claim:

| Bằng chứng | Trạng thái hiện có | Phạm vi được phép diễn giải |
|---|---|---|
| Pilot `supplement_pilot_20261125_OQ8HCM` | Có manifest, fact 10M và final plan pilot. | Một block chẩn đoán; không dùng kết luận ổn định. |
| Confirmation `app-20261125142144-0001` | Có Event Log, protocol, 96 dòng đo, 96 plan và audit. | 2 workload × 6 nhánh × 8 block, một Spark application trên một host. |
| AQE C, synthetic 10M | Audit final plan báo split 8/8. | Chỉ phiên và cấu hình này; đối chiếu plan và Event Log trước khi trích dẫn. |
| AQE C, synthetic 2M | Audit plan báo không thấy split 8/8. | Không quan sát marker trong phạm vi phiên; không suy ra AQE không bao giờ split. |
| DuckDB | 12 phép kiểm tra chạy lại, 2 workload × 6 phương án; PASS được gán cho các dòng tương ứng. | Không mô tả là 96 output đo được kiểm chứng độc lập. |
| Stage và TSR | Event Log ghép SQL execution ID và join codegen, tính lại TSR khớp 96/96. | Stage chứa join có thể chứa thêm operator; TSR không trực tiếp đếm split. |
| Shuffle/spill | Audit theo task Event Log khớp 96/96 lượt trong phạm vi collector; xem `10_confirmation_evidence_audit.md`. | Không quy spill toàn action cho riêng join. |
| Nguồn 2M và Malviya | Fact 2M có manifest và generator trong project; đã đối chiếu generator/config của repo Malviya tại commit đã pin. | Schema và cấu hình được kiểm khác nhau; chưa có raw phía Malviya để so checksum. Trùng số dòng không đủ chứng minh quan hệ nguồn gốc. |

Chương 3 và 4 Markdown đã có mục bổ sung; Word/PDF phát hành có thể chưa chứa các mục đó. Không gộp kết quả 2M/10M với NYC S3, không kết luận thay đổi quy mô fact là nguyên nhân duy nhất làm split xuất hiện. Ghi mọi claim bằng artifact và phiên cụ thể.

### Đường dẫn truy nguyên hiện có

- Event Log confirmation: `logs/spark-events/app-20261125142144-0001`
- Thư mục confirmation: `data/benchmarks/phase_c/supplement_pilot_20261125_OQ8HCM/results/phase_c_supplement_confirmation_20261125T142141Z/`
- Trong thư mục confirmation: `protocol.json`, `runs.csv`, `duckdb_audit.csv`, `paired_contrasts.csv`, `stage_execution_audit.csv`, `plan_activation_audit.csv`, `plans/`.
- Pilot manifest: `data/benchmarks/phase_c/supplement_pilot_20261125_OQ8HCM/pilot_manifest.json`.
- Fact 2M: `data/benchmarks/phase_c/synthetic_v3/synthetic_manifest.json` và script `scripts/chapter_03/generate_phase_c_synthetic.py`.
- Nhật ký và audit hiện có: `docs/refactor/09_paper_supplement_execution_log.md`, `docs/refactor/10_confirmation_evidence_audit.md`.
- Bản thảo đang biên tập: `chapters/03_pipeline_skew_benchmarks/manuscript/chapter3_complete.md`, `chapters/04_results_tradeoffs/manuscript/chapter4_results.md`.

## 3. Thứ tự triển khai gốc (lưu vết lịch sử)

Các giai đoạn 0–D dưới đây là kế hoạch trước khi chạy; trạng thái và evidence hiện tại ở phần cập nhật đầu tài liệu. Giai đoạn E và việc đối chiếu repo vẫn còn tiếp tục.

| Giai đoạn | Công việc | Ước lượng | Gate |
|---|---|---:|---|
| 0 | Kiểm tra workspace, Compose, hai Worker, dung lượng và file | 15–30 phút | Đủ hạ tầng và input mới mở pilot |
| A | Xác minh provenance + audit 96 lượt | 1–3 giờ | Claim gắn được với evidence, metric chưa rõ được gắn nhãn |
| B | Thiết kế và sinh dimension hai mức | 1–3 giờ | Invariants, baseline ground truth và checksum đạt |
| C | Pilot 4 điều kiện | 30–90 phút + runtime | Plan, output, metric và tài nguyên đạt |
| D | Confirmation: 8 block × 4 điều kiện | runtime tùy cụm | Đủ dữ liệu hoặc ghi phiên không hoàn tất |
| E | Phân tích, đối chiếu nghiên cứu, cập nhật Chương 3–4 | 1–3 giờ hoặc hơn | Có bảng evidence và mô tả giới hạn |

Không cam kết hoàn thành trong một ngày. Nếu ở A còn sai sót có thể ảnh hưởng đến tính hợp lệ thì sửa/ghi rõ trước khi dùng kết luận. B–D chỉ thực hiện sau khi thiết kế được khóa và pilot qua gate.

## 4. Giai đoạn 0 — Khởi động và kiểm tra

Tại root repo trên Fedora:

```bash
cd ~/SEM5/Bigdata/taxi-data-baker-
podman compose up -d
podman ps
curl -fsS http://127.0.0.1:8080/json/
df -h .
```

Xác nhận `spark-master`, `spark-driver`, hai worker ALIVE; đọc số core và memory thực cấp, kiểm tra container nào đang chạy công việc khác, ổ đĩa và đường mount. Nếu lệnh `podman compose` không khả dụng, dùng lệnh compose hiện hành của máy. Không đo khi cụm đang chạy workload khác. Công cụ Codex có thể không truy cập được rootless Podman của Fedora; khi đó người dùng chạy lệnh và gửi output, không giả vờ job đã chạy.

## 5. Giai đoạn A — Xác minh evidence và audit shuffle/spill

### A1. Provenance 10M và 2M

1. Đọc manifest, input, generator, seed, tạo ngày nào và app ID. Kiểm SHA-256 nội dung hiện tại với SHA-256 được ghi trong manifest, nếu có; fingerprint tên/kích thước/mtime không phải SHA-256 nội dung.
2. Nối từng dòng đo với application ID, job group, SQL execution ID, final plan, Event Log, config và DuckDB audit. Giữ nguyên file gốc; đầu ra audit ghi thư mục riêng.
3. Với 10M: kiểm 8 plan C có `isFinalPlan=true`, `SortMergeJoin(skew=true)` hoặc marker shuffle skew; 8 plan B không split. Với 2M: kiểm các plan C không có marker. Ghi số plan, đường dẫn và kết quả.
4. Kiểm tra ngày tạo và checksum, lịch sử nguồn generator và tài liệu tham khảo. Đối chiếu Malviya khi xác định được đúng bản thảo/README. Trạng thái nguồn gốc chưa xác định khi thiếu tài liệu; sự trùng 2M không đủ kết luận.
5. Mỗi claim đánh dấu **Verified / Needs correction / Unavailable**. Nếu thiếu plan split, không tuyên bố split cho run đó. Các artifact có sẵn được kiểm toán, không tự động giả định sai chỉ vì Word cũ chưa nêu.

### A2. Audit metric theo task

- Truy execution đầu của từng group tương ứng action đo; lọc các action warm-up/audit bị thừa job group theo SQL execution ID và thời gian.
- Lập bảng theo `(workload, block, arm, application_id, sql_execution_id, job_ids, stage_id, attempt_id)`; so với `stages_detail`, CSV collector và plan.
- Lấy các task thành công theo stage attempt, ghi nhận số task, retry/speculation nếu có. Tính memory/disk spill, shuffle read/write và kiểm tra trường thiếu. So tổng theo phạm vi collector; không cộng hai lần stage chia sẻ hoặc attempt.
- Khác biệt do phạm vi REST/attempt/task phải điều tra; số không đủ evidence để là `Unavailable`, không đổi NA thành 0. Ghi sai khác byte, tỷ lệ và nguyên nhân, giữ nguyên CSV gốc.
- Lưu `audit_96_runs.csv`, `audit_stage_task.csv`, `audit_summary.md`, script tái lập và version/code hash. Không gán spill toàn action cho riêng join.

**Gate A:** đối chiếu được 96 lượt hoặc nêu rõ các lượt không thể đối chiếu. Chỉ dùng số liệu Verified khi viết kết luận định lượng tương ứng; các phát hiện khác ghi là chưa xác minh.

## 6. Giai đoạn B — Thiết kế nhân bản dimension

### B1. Câu hỏi và can thiệp

Trên fact 10M, khi tổng số dòng dimension và kết quả join được giữ cố định, thay đổi số dòng dimension có pickup thuộc tập hotspot nhưng không khớp fact có làm thay đổi chi phí tương đối của E so với B hay không?

- B = AQE bật, coalescing bật, skew optimization tắt, không salting.
- E = cùng nền AQE/coalescing, skew optimization tắt, Selective Salting S=8.
- Hai mức dimension: **thấp** và **cao**. Chọn số dòng hotspot cụ thể dựa trên preflight RAM/ổ đĩa rồi ghi vào protocol trước pilot/confirmation. Số ví dụ 13, 10.000 hoặc 100.000 không phải thông số đã khóa.
- Giữ fact, danh sách hotspot, schema/kiểu dữ liệu, payload, tổng dòng dimension, keys và giá trị khớp fact, số file/row groups và cấu hình Spark tương đương. Dịch chuyển một phần khóa filler không khớp fact giữa pickup thuộc và không thuộc hotspot, bảo đảm composite OD key duy nhất.
- Ghi Parquet bytes, kích thước dòng, phân bố hash và số input splits/map tasks; thay khóa vẫn có thể làm đổi nén, partition, optimizer hoặc I/O. Đây là yếu tố gây nhiễu cần báo cáo, không tự xem như được kiểm soát hoàn hảo.
- Dự kiến dòng tăng thêm `(S - 1) × dim_hot_rows`; kiểm tra cả số đếm dự kiến và sau materialize transformation. Xác nhận input đi qua `Generate/explode` và shuffle bằng plan/metric phù hợp; nếu optimizer loại bỏ filler, dừng/diễn giải lại.
- Đây là phép đo chi phí replication khi giữ output join, không đo fanout thay đổi ở các khóa khớp và không tự xác định vùng E thắng AQE skew join.

### B2. Khóa protocol trước confirmation

Lưu một file `protocol.json` mới có: ID phiên, thời gian, phiên bản code/git và working-tree status, input paths + SHA-256 nội dung, generator version/seed, số dòng và số byte mỗi input, danh sách hotspot, số dòng nhân bản, nhánh, Spark version/config hiệu lực, tài nguyên thực cấp, warm-up, action đo, 8 block, seed và danh sách thứ tự bốn điều kiện, metric, contrast, điều kiện sai/lỗi và phương án phân tích. Không ghi đè pilot/confirmation cũ. Thông số được chọn sau pilot phải ghi rõ và khóa trước confirmation.

## 7. Giai đoạn C — Pilot và cổng nghiệm thu

Pilot: 2 mức × B/E × 1 block = **4 action đo**, cộng warm-up và audit. Kết quả chỉ để kiểm tra kỹ thuật, không đưa vào confirmation.

Checklist đạt:

- Hai dimension có cùng tổng số dòng/schema; OD unique, non-null; tập khóa và giá trị khớp fact giống nhau; số dòng hotspot và số replication thực tế đúng theo manifest.
- Bốn output của action đo được lưu/đối chiếu với DuckDB reference sau khi dừng đồng hồ; coverage, counts và sums thỏa dung sai đã khóa. Không chạy action khác rồi nói đã kiểm chứng chính action đo.
- Plan đúng nhánh, không broadcast, coalescing theo cấu hình; E có salt/explode và quan sát được dữ liệu filler được xử lý. Ghi input split, task, shuffle, spill; missing giữ nguyên.
- Spark thu đủ executor/worker evidence; không OOM, thiếu disk, mất executor hay trùng workload; output/plan/event log được lưu.
- Chênh lệch số dòng replication đủ lớn và khả năng hoàn tất được xác nhận. Nếu chưa đạt, sửa nguyên nhân hoặc dừng; mọi điều chỉnh được ghi trước khi khóa confirmation.

## 8. Giai đoạn D — Confirmation tối đa 32 lượt

Mỗi block gồm **B thấp, E thấp, B cao, E cao**; cố định input và ngẫu nhiên hóa thứ tự bốn điều kiện bằng seed, kiểm tra cân bằng thứ tự. Chạy 8 block = 32 action đo, ngoài warm-up, preflight, DuckDB và audit. Chỉ chạy sau pilot PASS. Ghi checkpoint từng block; khi lỗi không tự chạy bù hoặc xóa lượt. Phân loại nguyên nhân và áp dụng quy tắc thất bại đã khóa, báo số block hợp lệ theo từng contrast.

Ghi per run: block, thứ tự, condition, app/job group/SQL execution IDs, dataset content hashes, timestamp, config `spark.conf.get`, executor thực cấp, plan sau action, thời gian action, metrics, output reference/hash và kết quả đối soát trực tiếp của action. Correctness, parse plan, ghi file và DuckDB diễn ra ngoài đồng hồ wall-time. Không thay NA thành 0.

### Phân tích khóa trước

Với block i, tính `R_low = E_low / B_low`, `R_high = E_high / B_high` và `R_effect = R_high / R_low`. Mẫu số phải dương, chỉ ghép trong cùng block hợp lệ. Báo median tỷ số theo block; bootstrap percentile 95% lấy mẫu lại toàn bộ block (bao gồm cả bốn điều kiện), seed và số lần lấy mẫu được ghi trước. Đặt ngưỡng thực hành ±5% cho từng tỷ số; CI giao ngưỡng được coi là chưa phân định, CI chưa hiệu chỉnh đồng thời phải nêu rõ. Nếu dùng kiểm định nhiều contrast, định nghĩa trước family, phép kiểm định và điều chỉnh; không chọn sau khi xem kết quả.

Báo thêm số dòng dimension hot, dòng phát sinh, Parquet bytes, shuffle, memory/disk spill, task metrics và coverage theo nhánh. Ghi rõ spill là stage/action nào. Không suy ra nguyên nhân chỉ từ tương quan hoặc median; đánh giá alternative explanations như nén, hash, map split, optimizer và overhead E. Không so ghép E mới với C của phiên 96 lượt vì khác input/thời điểm.

## 9. Cổng quyết định và hướng bài

- **Nếu E/B cao tăng rõ so với E/B thấp:** báo bằng chứng chi phí tương đối tăng trong hai mức và cấu hình đo, kèm bất định và yếu tố gây nhiễu.
- **Nếu không khác rõ:** báo kết quả chưa phân định hoặc khác biệt nhỏ theo CI; không kết luận replication không có chi phí.
- **Nếu E/B < 1:** báo Salting nhanh hơn B trong điều kiện đó sau kiểm chứng, không gọi là thắng AQE skew join C.
- **Nếu muốn so trực tiếp E/C:** thiết kế thêm nhánh C cùng input và block; 3 nhánh × 2 mức × 8 block = 48 lượt, vượt ngân sách mặc định 32. Đây là lựa chọn sau phân tích, không tự mở rộng.
- **Nếu muốn tuyên bố vùng chiến thắng hoặc ngưỡng:** cần workload/seed giữ lại và xác nhận độc lập trước khi đưa ra quy tắc thực hành. Không chọn điều kiện dựa vào một pilot.

## 10. Nghiên cứu liên quan, viết bài và đầu ra nghề nghiệp

Lập bảng cho từng bài: câu hỏi; operator/khóa; dataset; baseline; AQE on/off; hạ tầng; phát hiện; hạn chế; mối liên hệ DETOUR; số trang và phiên bản nguồn. Bốn tài liệu đã đọc: Flow-Join++ (AQE tắt trong cấu hình báo cáo), CIBPartitioner (spatial join và computational intensity), MFTune (tuning và giới hạn proxy quy mô nhỏ; file arXiv v1 có metadata placeholder), Zhang–Sang–Wu (thiết kế chính sách trước/sau và nhóm đối chứng). Không nhận triển khai hoặc vượt phương pháp của họ khi chưa so trực tiếp. Malviya: repo và bản paper trong repo đã định danh; còn cần đối chiếu phiên bản PDF đã nhận và chưa có raw artifacts để xác minh số của họ.

Sau gate audit, sửa Chương 3: workload, B/E, protocol, correctness, thời gian đo. Sửa Chương 4: paired ratios, plan, audit, kết quả âm tính và giới hạn; tách NYC và synthetic; ghi changelog, application ID, đường dẫn evidence. Giữ Word/PDF cũ làm mốc, chỉ xuất bản cập nhật sau khi rà liên kết, hình bảng và phân trang.

Hồ sơ Grab/Xanh SM (tùy chọn): viết 1 trang mô tả bài toán, vai trò, dữ liệu công khai, công nghệ, quyết định dựa trên evidence, kết quả đúng phạm vi, dashboard/hình và link repo đã loại bí mật. Nêu đây là project nghiên cứu sử dụng dữ liệu NYC; không tuyên bố quan hệ doanh nghiệp hay kết quả vận hành của Grab/Xanh SM. Không đưa địa chỉ, credential hay dữ liệu riêng tư vào hồ sơ.

## 11. Checklist hoàn thành

- [ ] Môi trường và tài nguyên đã kiểm tra; không chạy cùng benchmark khác.
- [x] Kiểm manifest/checksum khả dụng của input 2M/10M và định danh repo Malviya; chưa so được raw của họ.
- [x] Audit shuffle/spill 96 lượt theo phạm vi collector.
- [x] Protocol nhân bản, hai mức và seed được khóa trước confirmation.
- [x] Pilot 4 điều kiện qua gate; giữ artifact riêng.
- [x] Confirmation 32 lượt hoàn tất.
- [x] Output từng action của confirmation replication được đối chiếu DuckDB ngoài thời gian đo.
- [x] Contrast cùng block, CI và phạm vi kiểm định được ghi trong nhật ký replication.
- [ ] Báo cáo tách synthetic với NYC, nêu một host và giới hạn suy rộng.
- [ ] Tài liệu tham khảo, novelty và claims có nguồn; cập nhật changelog.

**Bước tiếp theo:** thực hiện bốn ưu tiên ở đầu tài liệu; các runner và kết quả 32 lượt đã có. Không khởi động lại benchmark chỉ vì hướng dẫn lịch sử ở mục 4–8 còn được giữ để truy vết.

