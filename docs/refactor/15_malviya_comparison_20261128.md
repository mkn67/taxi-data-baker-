# Đối chiếu DETOUR với bản thảo Malviya

Ngày 28/11/2026. Tác giả cung cấp `main.pdf`, 13 trang: Aditya Malviya, *Adaptive Query Execution or Salting? A Cost Model and Decision Procedure for Skewed Joins in Apache Spark*. Repo được cung cấp: `https://github.com/adityamalviya04692/Reseach_Paper-AQE-vs-Salting`, commit `b62cc0ecfc275681a1052e3aa30f6a6778698d36` (commit timestamp 2026-10-16T10:23:45+05:30). Tình trạng peer review và quan hệ thời gian ưu tiên chưa được xác minh; không tự gán DOI. Hai PDF trong repo có cùng SHA-256 `f067de0d017a11f95e615356b66b7ea35ecda04797251bd46e1f96c40ff16b09`; chưa so byte với file người dùng gửi.

## Kiểm tra repo tại commit được chốt

- README đánh dấu bảng crossover trong phần overview là lịch sử và yêu cầu dùng paper 13 trang hiện hành. `docs/AUDIT_LEDGER.md` ghi crossover cũ bị ảnh hưởng bởi baseline chạy trên JVM lạnh; không dùng bảng cũ để diễn giải kết quả paper hiện tại.
- README ghi data sinh, raw JSONL/CSV và Spark Event Logs bị loại khỏi repo; chưa tìm thấy raw-results archive hoặc DOI được dẫn. Do đó audit này so sánh code/config và claim công bố, chưa xác minh lại số liệu của tác giả.
- `config/v3_core.yaml`: fact 2M, dimension 20.000, 2.000 engines, Zipf 1,2, seed 20260818, 16 shuffle partitions, Spark `local[2]`, 5 repetitions; probe multiplier 1/10/32. `src/skewbench/datagen.py` sinh telemetry bằng NumPy/pandas. DETOUR `generate_phase_c_synthetic.py` dùng OD và 256 pickup keys, seed 20261123 trong synthetic v3. Schema và mô tả sinh khác; không có raw phía họ để so checksum trực tiếp, không kết luận nguồn gốc chỉ từ số dòng.
- `docs/AUDIT_LEDGER.md` mục H4 nêu phép thử advisory-only còn thiếu và các probe thay advisory cùng threshold; vì thế trình bày đây là kết quả probe theo cấu hình, chưa xác định ảnh hưởng độc lập của chỉ advisory.
- Repo xác nhận `salt_selective(k=1)` trả plain join trong `src/skewbench/arms.py` và runner tính control discrepancy. DETOUR chưa có measurement-floor control tương đương đã kiểm chứng; ±5% của DETOUR là ngưỡng thực hành đặt trước, không phải noise floor đo được. Một thiết kế control tương đương chỉ nên mở sau khi khóa hypothesis, điều kiện plan và ngân sách.
- README/reproducibility mô tả một số metric như độc lập phần cứng; đó là claim của repo, không dùng như chân lý chung. Actual plan, partition, cấu hình, tài nguyên và spill có thể thay đổi giữa hệ thống.

## Đối chiếu phương pháp theo bản PDF

| Khía cạnh | Malviya | DETOUR | Đánh giá |
| --- | --- | --- | --- |
| Câu hỏi | Chọn AQE, salting, hay kết hợp; mô hình chi phí và salt cardinality. PDF tr. 1–2 | Đánh giá AQE, salting và ảnh hưởng execution plan, chi phí và tài nguyên | Trùng câu hỏi nghiên cứu đáng kể. Không xem so sánh AQE–salting trên Spark là mới chỉ vì đã thực hiện. |
| Thiết kế | SKEWBENCH, sáu chiến lược gồm baseline, AQE, uniform/selective salting, AQE + salting, broadcast; kiểm soát plan. PDF tr. 6–8 | Các nhánh tương tự trong các phiên khác nhau; ablation A/A0/B/C/D/E. | Sáu nhánh không phải điểm mới tự thân; các ma trận không hoàn toàn đồng nhất. |
| Tách coalescing | Đối chứng AQE bật/tắt coalescing; kiểm final adaptive plan. PDF tr. 7–8 | B/A0 tách coalescing, C/B so skew enabled/disabled trên nền coalescing. | Phân biệt coalescing không thể nhận là mới nói chung. Phiên DETOUR có C split 8/8 trên workload 10M; cần định vị chính xác kết quả bổ sung và so với probe kích hoạt split của tác giả. |
| Skew split | Ở ba probe thickness chính không thấy split, sau đó báo probe hạ advisory size để kích hoạt. PDF tr. 8 | Confirmation 10M có split 8/8 ở C, so B trong cùng block; 2M không thấy split ở C. | Malviya không chỉ quan sát non-split: họ cũng có phép thử kích hoạt split. DETOUR có bằng chứng paired C/B tại cấu hình đã đo, nhưng chưa chứng minh tính mới so với bài hoặc literature khác. |
| Input | 2M fact synthetic Zipf, schema engine telemetry C-MAPSS; dimension có thể nhiều hàng cho một join key, khảo sát probe hot-key P = 10, 100, 320. PDF tr. 6–7 | Synthetic OD 2M/10M, join hai khóa, dimension OD unique; thêm workload NYC thực S3. | Cùng số 2M và Zipf không đủ chứng minh cùng dữ liệu hay độc lập. DETOUR 32 lượt đổi **unmatched filler** thuộc hotspot, không đổi fanout khớp; khác sweep P của bài. |
| Chi phí salting | Mô hình T(k)=aH/k+bP(k−1)+c, sweep k, so replication selective/uniform; tác giả báo hệ số replication theo thời gian không xác định ở cả ba fit. PDF tr. 1, 9–10 | S=8; đo hai mức số dòng dimension thuộc hotspot, phát ra thêm 700.000 dòng trong mức cao. | Cả hai không phân định rõ chi phí wall-time trong thí nghiệm tương ứng. Không mô tả kết quả âm tính này là mới hay bác bỏ mô hình của họ. |
| Hạ tầng và thống kê | Spark 3.5.3 local single-node, hai core; 5 repeats/cell, noise-floor control và median/IQR; PDF tr. 7, 11 | Spark 3.5.9 Standalone, 2 worker trên cùng host; 8 paired blocks trong một application mỗi phiên bổ sung; bootstrap CI. | Khác cấu hình, nhưng không chứng minh tốt hơn; không quy wall-time/shuffle/spill là hardware-independent tuyệt đối. |
| Validation | Plan và Event Log là artifact họ mô tả. PDF tr. 6–7 | 96-run DuckDB theo workload/phương án; 32-run so chính output từng action | Đây là kiểm chứng correctness của DETOUR. Không khẳng định paper kia thiếu correctness validation nếu chưa kiểm toàn bộ artifact. |

## Kết quả và giới hạn khi viết

- Bản PDF có ví dụ salting nhanh hơn baseline trong một số probe thickness, nhưng khác input, join, thời gian và cấu hình. Không so trực tiếp con số giữa hai paper.
- Phiên replication DETOUR 32 lượt chỉ so E với B, không so AQE skew join C. Median ratio-of-ratios 0,9972, CI [0,9381; 1,0632] cắt ngưỡng ±5%; không khẳng định replication miễn phí.
- Tác giả Malviya cũng cảnh báo TSR max/median có thể gây hiểu nhầm khi coalescing thay đổi số task (PDF tr. 10–11). Đây không phải điểm mới riêng của DETOUR. Báo số task và max task duration, cùng plan, wall-time, shuffle và spill; không suy từ TSR sang hiệu quả toàn phần.
- Hai paper có trùng lặp đáng kể về so sánh AQE/salting, ablation coalescing và chi phí replication. Khả năng khác biệt có thể đến từ workload NYC thực, join hai khóa, các phiên và đối chứng có split, kiểm chứng correctness; đây là các đặc điểm phải đặt trong bảng related work và khảo sát thêm, chưa đủ để tuyên bố novelty.
- Lưu ý phản biện: PDF đưa nhận định một số metric là độc lập phần cứng; không mặc định chấp nhận toàn bộ. Thời gian task, spill, shuffle và execution plan có thể phụ thuộc cấu hình/tài nguyên; cần kiểm chứng theo metric và phạm vi.

## Provenance 2M — chưa kết luận

Project DETOUR có `generate_phase_c_synthetic.py`, manifest `data/benchmarks/phase_c/synthetic_v3/synthetic_manifest.json` và fact 2M SHA-256 `7c34f674466fb214a622f5de59bbe8881afb54e3d4e8610f77360512565d7771` khớp manifest. Repo Malviya đã có generator và seed khác như trên, nhưng không có raw data để so checksum; cần nguồn đầu vào và lịch sử đầy đủ trước khi kết luận nguồn gốc/chronology. Một PDF và con số 2M không chứng minh mượn hay độc lập.
