# DETOUR — Nhật ký triển khai kế hoạch bổ sung

Ngày rà soát: 25/11/2026  
Phạm vi: ND3, Spark 3.5.9, audit artifacts, pilot và confirmation trên Fedora Standalone.

## Trạng thái

| Hạng mục | Trạng thái | Ghi chú |
| --- | --- | --- |
| Chốt phạm vi | Hoàn tất | Giữ phạm vi thực nghiệm ND3 bổ sung theo plan: kiểm toán, pilot, confirmation và đánh giá chi phí nhân bản có điều kiện. |
| Kiểm tra artifacts AQE | Hoàn tất | Đọc lại các final plan và phân biệt thư mục pilot/retry với lượt xác nhận. |
| Đối chiếu nghiên cứu liên quan | Một phần | Đã đối chiếu PDF và repo Malviya tại commit `b62cc0ecfc275681a1052e3aa30f6a6778698d36`; xem `15_malviya_comparison_20261128.md`. Raw kết quả phía họ không có trong repo; novelty toàn diện chưa xác minh. |
| Chạy pilot cơ chế | Hoàn tất trên Fedora Standalone | Pilot 10M dòng đã chạy trên Spark 3.5.9; cả ba phương án PASS DuckDB. Final plan AQE có `SortMergeJoin(skew=true)` và `AQEShuffleRead coalesced and skewed`. Chỉ một block; thời gian pilot không dùng làm kết luận. |
| Bổ sung runner ablation | Hoàn tất trong repo | Thêm runner sáu nhánh và launcher hai workload, tám block/nhánh/workload, lưu config, Event Log provenance, final plans, DuckDB audit và tiến độ từng block. Syntax Python/Bash đã kiểm tra. |
| Ablation xác nhận | Hoàn tất, cần xét giới hạn kiểm chứng | 96/96 lượt; collector 96/96 OK; DuckDB PASS ở 12 kết quả chạy lại theo workload/phương án, trạng thái được gán cho các dòng đo tương ứng; đủ 96 final plans. |
| Phân tích ghép cặp và plan | Hoàn tất bước đầu | Đã lưu bootstrap CI 95%, paired contrast, audit activation final plan và kiểm tra cân bằng vị trí chạy; xem giới hạn trong audit confirmation. |
| Audit confirmation | Stage và TSR đã đối chiếu | Event Log xác nhận 96 target stage thuộc execution đo và chứa join codegen, TSR khớp 96/96; chi tiết và giới hạn spill/DuckDB trong `10_confirmation_evidence_audit.md`. |
| Bản thảo Chương 3–4 | Đã cập nhật Markdown | Đã thêm phương pháp và kết quả 96 và 32 lượt; Word/PDF đã phát hành chưa cập nhật. PDF Malviya có trùng lặp đáng kể về câu hỏi và phương pháp; chưa khẳng định novelty. |

## Kiểm toán final plans của các phiên trước confirmation

| Tập plan | Plan thuộc phiên được kiểm tra | Có final plan | Có AQE coalescing | Có dấu hiệu skew split |
| --- | ---: | ---: | ---: | ---: |
| Synthetic v3 | 60 | 60 | 60 | 0 |
| Sensitivity v3 | 15 | 15 | 15 | 0 |
| NYC S3, `retry_1500m` | 10 | 10 | 10 | 0 |

Các plan được kiểm tra đều cho thấy Sort-Merge Join. Hai plan AQE exploratory nằm ngoài thư mục `retry_1500m` không được cộng vào bộ 10 block xác nhận S3. Trong các phiên cũ này, AQE coalescing được xác nhận qua plan, còn skew splitting chưa được quan sát. Phiên confirmation bổ sung phía dưới đã quan sát split trên workload 10M.

## Tổng quan nghiên cứu và ranh giới tuyên bố

Spark SQL Performance Tuning 3.5.9 mô tả post-shuffle coalescing và skew join optimization thành các cơ chế riêng. Skew join cần Adaptive Query Execution và `spark.sql.adaptive.skewJoin.enabled`. Marker `AQEShuffleRead coalesced` trong plan xác nhận coalescing. Cần bằng chứng split riêng trong final plan để xác nhận skew splitting.

| Công trình | Vấn đề và cách tiếp cận | Quan hệ với DETOUR |
| --- | --- | --- |
| Flow-Join++ (Panduman, Ito và Onizuka, 2026) | Bộ ước lượng cardinality dự đoán fanout và heavy hitters, sau đó planner dùng Heavy Hitter Push-Down cho workload nhiều phép join. Bài đánh giá trên TPC-H có skew injection và Phone Bill. | DETOUR dùng một equi-join OD, Spark 3.5.9, AQE và Selective Salting S=8. Phạm vi đối chiếu hiện giới hạn ở câu hỏi skew-aware join và chi phí thực thi do thiết kế, workload và phương pháp khác nhau. |
| Malviya draft | Đã nhận PDF *Adaptive Query Execution or Salting? A Cost Model and Decision Procedure for Skewed Joins in Apache Spark*. | Trùng câu hỏi, ablation coalescing và xem xét chi phí salting; khác biệt và giới hạn được liệt kê tại `15_malviya_comparison_20261128.md`. |

Flow-Join++ là mốc liên quan gần về heavy-hitter prediction và skew-aware distributed joins. So sánh runtime trực tiếp chưa có cơ sở do thiết kế và workload khác nhau. Độ mới của DETOUR so với bài này và Malviya draft vẫn cần được đối chiếu.

Đã nhận PDF `main.pdf` 13 trang của Aditya Malviya và đối chiếu repo tại commit đã ghi ở trên. Phiên bản PDF, peer review, raw artifact và nguồn dữ liệu đầy đủ vẫn cần xác minh; chưa khẳng định novelty hay mối quan hệ nguồn gốc dữ liệu chỉ dựa trên số dòng.

## Pilot cơ chế đã thực thi trên Fedora

Pilot `supplement_pilot_20261125_OQ8HCM` đã chạy từ Fedora terminal trên Spark Standalone 3.5.9 với hai worker, mỗi worker cấp 2 cores; dimension có 5.000.256 hàng; fact có 10.000.000 hàng và hotspot trip share 0,9573251. Cấu hình AQE dùng 200 shuffle partitions, skew factor 2, threshold 16 MiB, broadcast tắt.

| Phương án | Wall-time pilot | TSR | Spill memory | Spill disk | DuckDB |
| --- | ---: | ---: | ---: | ---: | --- |
| Baseline SMJ | 6,895 s | 141,43 | 120,00 MiB | 49,16 MiB | PASS |
| AQE skew join | 4,075 s | 1,18 | 568,00 MiB | 172,46 MiB | PASS |
| Salting S8 | 7,389 s | 23,16 | 0 MiB | 0 MiB | PASS |

Đây là một block chẩn đoán, không phải kết quả xác nhận. AQE plan có `AdaptiveSparkPlan isFinalPlan=true`, `SortMergeJoin(skew=true)` và `AQEShuffleRead coalesced and skewed`, nên gate pilot về hiện diện skew split đã đạt. Output: `data/benchmarks/phase_c/supplement_pilot_20261125_OQ8HCM/results/`.

## Confirmation ablation đã hoàn tất

[`run_phase_c_supplement_ablation.py`](../../scripts/chapter_03/run_phase_c_supplement_ablation.py) cài đặt sáu nhánh A/A0/B/C/D/E theo ma trận trong plan. [`run_phase_c_supplement_confirmation.sh`](../../scripts/chapter_03/run_phase_c_supplement_confirmation.sh) khóa hai workload và chạy 8 block mỗi workload: strong-large v3 (2M fact rows; năm AQE plans trước đó coalescing-only) và pilot strong 10M (skew split đã xác nhận). Hai workload dùng chung dimension lớn 5.000.256 hàng. Do quy mô fact khác nhau, các contrast được diễn giải riêng trong từng workload, không gộp thành so sánh trực tiếp giữa workload.

Confirmation đã chạy trên Fedora Standalone. Có đủ 96/96 dòng đo, 8 block trong mỗi nhóm workload/nhánh, 96/96 collector OK và 96 final plans. DuckDB PASS cho 12 phép kiểm tra chạy lại theo workload và phương án; trạng thái này được chép lên 96 dòng đo, không kiểm chứng riêng mỗi lượt đo. Cả hai workload giữ Sort-Merge Join, không có broadcast. Workload 2M có 8/8 final plans coalescing-only ở B/C/E và 0/8 skew split. Workload 10M có 8/8 skew split ở C; B và E đều coalescing-only. Runner tạo `runs.partial.csv` theo block cùng `runs.csv`, `summary.csv`, `duckdb_audit.csv`, `progress.json` và plans trong `data/benchmarks/phase_c/supplement_pilot_20261125_OQ8HCM/results/phase_c_supplement_confirmation_20261125T142141Z/`.

### Kết quả paired chính

Tỷ số là numerator/denominator theo cùng block; dưới 1 nghĩa là numerator nhanh hơn. Khoảng tin cậy bootstrap 95% là percentile CI chưa hiệu chỉnh, lấy mẫu lại tám paired block. Kiểm định Wilcoxon paired exact áp dụng cho ba contrast chính trong mỗi workload; Holm hiệu chỉnh trên sáu kiểm định. Việc chọn sáu phép kiểm định thành một nhóm Holm được quyết định sau khi chạy. Ngưỡng thực tiễn ±5% đã nêu trong plan.

| Workload | Contrast | Median ratio (95% CI) | Diễn giải có giới hạn |
| --- | --- | ---: | --- |
| strong-large v3, 2M | B/A0: thêm coalescing | 0,642 (0,546–0,690) | Coalescing nhanh hơn 35,8% theo median; Holm p=0,0469; CI vượt biên 5%. |
| strong-large v3, 2M | C/B: bật skew optimization | 1,022 (1,002–1,080) | Không có split; khác biệt nhỏ, chưa phân định theo biên ±5%; Holm p=0,0547. |
| strong-10M | B/A0: thêm coalescing | 0,821 (0,787–0,888) | Coalescing nhanh hơn 17,9% theo median; Holm p=0,0469; CI vượt biên 5%. |
| strong-10M | C/B: bật skew optimization | 0,918 (0,851–0,989) | Final plan split 8/8; nhanh hơn 8,2% theo median và Holm p=0,0469. Tuy vậy, CI chưa xác lập chắc mức cải thiện trên 5%. TSR median 1,81 xuống 1,21; disk spill median tăng khoảng 156,7 MiB lên 179,6 MiB. |
| strong-10M | E/C: Salting + coalescing so với AQE skew join | 1,148 (1,065–1,200) | E chậm hơn 14,8%; Holm p=0,0469; CI nằm ngoài biên 5% theo hướng chậm hơn. |
| strong-10M | D/A: Salting S8 so với SMJ | 0,935 (0,895–0,957) | Median nghiêng nhanh hơn 6,5%, nhưng CI cắt biên 5%; không đủ để xác lập lợi thế thực tiễn >5%. |
| strong-large v3, 2M | D/A: Salting S8 so với SMJ | 1,089 (1,005–1,158) | Median nghiêng chậm hơn 8,9%; độ bất định chưa xác lập mức chênh >5%. |

Nhận xét giới hạn: trong các điều kiện đã đo, bật AQE coalescing đem lại mức giảm wall-time rõ nhất. AQE skew optimization có dấu hiệu cải thiện thêm trên workload 10M khi final plan split; cùng lúc disk spill cao hơn. Salting không thắng nhất quán giữa hai workload, và ở contrast E/C chậm hơn AQE skew join. Kết quả này chưa xác định ngưỡng tổng quát để chọn salting. TSR là metric của stage được collector chọn theo shuffle activity lớn nhất, không phải phép đếm trực tiếp số partition được split; bằng chứng split đến từ final plan.

Phân tích có thể tái tạo bằng [`analyze_phase_c_supplement_confirmation.py`](../../scripts/chapter_03/supplement_audit/analyze_phase_c_supplement_confirmation.py). Các đầu ra chính là `analysis_report.md`, `paired_contrasts.csv`, `plan_activation_summary.csv`, `plan_activation_audit.csv`, `position_balance_audit.csv` và `summary_recomputed.csv` trong thư mục confirmation nêu trên. Bootstrap CI chưa hiệu chỉnh đồng thời; nhóm sáu p-value Holm được chọn sau chạy. Đã cập nhật các bản thảo Markdown; bản Word/PDF đã phát hành chưa cập nhật. Cần rà claim đối chiếu với PDF Malviya.

## Gate trước khi tiếp tục

1. Spill/shuffle đã đối chiếu theo Event Log; không quy spill tổng cho riêng join.
2. Pilot và confirmation chi phí nhân bản đã hoàn tất; xem `11_replication_experiment_execution_20261128.md`.
3. Đã đọc PDF và repo Malviya; cần xác minh version, raw artifact cùng provenance 2M, xem `15_malviya_comparison_20261128.md`.
4. Reviewer rà final plans, thống kê và manuscript; ghi rõ đây là synthetic confirmation, tách khỏi NYC S3.

## Tệp liên quan

- Kế hoạch: [kế hoạch tổng hợp DETOUR](00_plan_tong_hop_detour.md), có lưu nội dung kế hoạch bổ sung ban đầu trong phụ lục.
- Kết quả hiện có: [`chapter4_results.md`](../../chapters/04_results_tradeoffs/manuscript/chapter4_results.md)
- Kiểm toán bằng chứng: [`02_evidence_audit.md`](02_evidence_audit.md)
- Audit confirmation, stage và giới hạn: [`10_confirmation_evidence_audit.md`](10_confirmation_evidence_audit.md)
- Audit stage theo SQL execution: [`audit_supplement_stage_events.py`](../../scripts/chapter_03/supplement_audit/audit_supplement_stage_events.py)
- Script audit plan: [`audit_phase_c_aqe_plans.py`](../../scripts/chapter_03/audit_phase_c_aqe_plans.py)
- Script pilot: [`run_phase_c_supplement_pilot.sh`](../../scripts/chapter_03/run_phase_c_supplement_pilot.sh)
- Bảng audit 85 plan: [`phase_c_aqe_final_plan_audit.csv`](evidence/phase_c_aqe_final_plan_audit.csv)
- Final plans: `data/benchmarks/phase_c/synthetic_grid_v3_20261123/`, `data/benchmarks/phase_c/sensitivity_v3_20261123/` và `data/benchmarks/phase_c/s3_10block_20261123/retry_1500m/`

## Tài liệu tham chiếu

- Apache Spark 3.5.9, [SQL Performance Tuning](https://spark.apache.org/docs/3.5.9/sql-performance-tuning.html)
- Panduman, Ito và Onizuka, [Flow-Join++](https://doi.org/10.2197/ipsjjip.34.671), *Journal of Information Processing*, 34, 671–685 (2026)
