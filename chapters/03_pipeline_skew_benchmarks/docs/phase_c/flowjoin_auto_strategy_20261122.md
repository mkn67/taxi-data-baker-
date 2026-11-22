# Đối chứng ND3 S3 liên hệ Flow-Join++ — 22/11/2026

## Nghiên cứu tham khảo và câu hỏi của đồ án

Yohanes Yohanie Fridelin Panduman, Ryuichi Ito và Makoto Onizuka, *Flow-Join++: Skew-aware Distributed Joins with Cardinality-based Heavy Hitter Prediction*, Journal of Information Processing, Vol.34, 671–685 (August 2026), DOI: [10.2197/ipsjjip.34.671](https://doi.org/10.2197/ipsjjip.34.671). Thông tin và phương pháp được đọc từ PDF người dùng cung cấp.

| Nội dung | Flow-Join++ | Phạm vi đồ án |
|---|---|---|
| Bài toán | Equi-join hai bảng và nhiều bảng, trọng tâm FK–PK | Một inner join trips–OD trên composite key; OD duy nhất |
| Nhận diện heavy hitter | Scardina dự đoán fanout/cardinality theo khóa | Pickup P95 chính xác; kiểm toán tần suất OD sau đo |
| Tổ chức xử lý | Heavy Hitter Push-Down và Broadcast Fast-Path theo cost model | Các nhánh SMJ/AQE/Salting, thêm Spark tự chọn join với threshold tường minh |
| Thực nghiệm | Phone Bill/TPC-H, gồm skew kiểm soát, trên EMR nhiều node | NYC HVFHV S3, hai Worker container trên cùng host |
| Mức kế thừa | Nghiên cứu tham khảo về heavy hitter và chọn chiến lược join | Đối chứng vận hành có liên hệ; chưa triển khai mô hình/planner của bài |

Câu hỏi thực nghiệm bổ sung: khi dimension OD có thể broadcast, nhánh Spark tự chọn chiến lược có thay đổi đánh đổi runtime/shuffle so với các nhánh ép SMJ trong workload taxi hay không? Bài báo giúp định vị câu hỏi này; phiên chạy không đánh giá mô hình dự đoán hay Heavy Hitter Push-Down của Flow-Join++, không chứng minh novelty tổng quát. Ngưỡng Spark 10 MiB không tương đương ngưỡng cost/size-ratio Broadcast Fast-Path của bài.

## Thiết kế đã chạy

- Application `app-20261122074817-0000`; Spark 3.5.9 Standalone; hai Worker cùng host, hai executor một core/1.500 MiB được cấp. `spark.cores.max=4` là mức trần yêu cầu, không phải bốn core đã cấp.
- Silver S3 tích lũy 01–05/2026: 101.263.728 dòng; OD và pickup P95 dựng từ cùng phạm vi.
- Cùng fingerprint Silver `2eb4649c39a223b414b31851a7b6058058e230df93e7fe890ad4e858a3e11aac` và OD `b8fc86635e1fe71fa6ab13a04c139e17da7175c611df188d11c1c5f286b7ba04` với S3 official cũ.
- Mỗi phương án có một action warm-up và ba lượt đo. Dùng ba phép xoay đầu của chu kỳ bốn phương án; các vị trí chưa được cân bằng hoàn toàn trên đủ bốn vòng.
- Nhánh auto không hint, AQE tắt, threshold 10.485.760 byte; ba nhánh core giữ cấu hình cũ, 200 shuffle partitions. Đây là lựa chọn tĩnh theo cấu hình Spark, không đại diện mọi cấu hình mặc định của Spark/AQE.
- Không loại lượt vì runtime cao. Hàm audit tính lại kết quả từ DataFrame logic cuối của mỗi phương án để đối soát DuckDB; PASS được gán cho ba dòng iteration tương ứng. Đây không phải ba audit độc lập cho ba output đã materialize.

## Kết quả

| Phương án | Median [min–max] (giây) | TSR | PSR | Shuffle Read (MiB) | Shuffle Write (MiB) | Memory/Disk Spill (MiB) |
|---|---:|---:|---:|---:|---:|---:|
| Baseline SMJ | 46,207 [45,727–47,886] | 1,90 | 1,95 | 1.496,69 | 1.496,69 | 0,00 / 0,00 |
| Spark AQE | 46,392 [46,331–48,342] | 1,18 | 1,07 | 1.496,24 | 1.496,24 | 0,00 / 0,00 |
| Salting S=8 | 55,367 [54,833–58,184] | 1,79 | 1,55 | 1.650,64 | 1.650,64 | 0,00 / 0,00 |
| Spark tự chọn (AQE tắt) | 12,384 [11,578–12,654] | 1,31 | NA | 0,06 | 0,06 | 0,00 / 0,00 |

Trong cùng phiên, Spark tự chọn `BroadcastHashJoin` với OD ở phía build; cả ba plan đều xác nhận lựa chọn này và mã nguồn nhánh không có hint. Median thấp hơn Baseline SMJ 73,2% (tỷ số thời gian Baseline/auto 3,73 lần). Đây là chênh lệch quan sát từ ba lượt sau warm-up trong một phiên, chưa xác lập ý nghĩa thống kê hay hiệu quả trên cụm khác. Các final plan AQE vẫn là SMJ có coalesce, không có skew split. Shuffle Read đo trong job group không bao gồm toàn bộ chi phí broadcast, không phải số byte NIC; không suy ra tốc độ mạng hay FinOps.

PSR của nhánh Spark tự chọn được giữ NA: ở stage được collector chọn (20 task), trường chung được ưu tiên là shuffle-read records và trung vị của trường này bằng 0. Đây là mẫu số 0 theo quy tắc đã chốt ở Chương 3, không phải PSR bằng 0 hoặc phép đo cân bằng hoàn hảo.

TSR/PSR được tính theo stage do collector chọn. Khi auto đổi sang broadcast, stage được chọn không nhất thiết có vai trò giống stage shuffle-join của nhánh SMJ; không dùng riêng tỷ số skew để khẳng định thời gian giảm do cân bằng task.

## Kiểm toán đơn vị khóa sau benchmark

OD có 63.572 khóa duy nhất; 101.263.728 chuyến đều khớp, không có khóa OD null/trùng hoặc chuyến không khớp. Mỗi chuyến khớp đúng một hàng dimension; số kết quả join theo một khóa OD bằng tần suất chuyến của khóa đó. Hotspot pickup P95 có 14 vùng, chọn 3.608 khóa OD và 16,02% chuyến. P95 tần suất theo cặp OD (chỉ để chẩn đoán) chọn 3.179 khóa, bao phủ 61,95% chuyến; chỉ 535 trong số đó thuộc các pickup đã chọn. Hai quy tắc sử dụng đơn vị khóa khác nhau; chênh lệch tập khóa không tự chứng minh pickup P95 sai hoặc task bị skew. P95 OD không phải ngưỡng chi phí heavy hitter của Flow-Join++ và không được dùng để thay H trong benchmark.

Phép kiểm toán DuckDB này chạy sau khi ứng dụng Spark đã dừng; nó không nằm trong thời gian đo, không phải chi phí online của một mô hình hay predictor. Output chi tiết là `join_key_profile/od_key_frequency_fanout.csv`.

## Cổng và artifacts

- [x] 12/12 dòng run collector OK, validation cấp phương án PASS; không có lượt bị loại.
- [x] Fingerprint Silver/OD giữ nguyên S3 official.
- [x] Hai Worker, 12 final plans; ba auto plans đều BroadcastHashJoin không hint trong mã nguồn.
- [x] Event Log có đủ bốn job group warm-up thành công trước các lượt đo.
- [x] Host/cgroup lấy mẫu xuyên phiên, 12/12 cửa sổ đầy đủ theo cách ghép timestamp ước tính.
- [x] Task records, AQE intervention, shuffle/spill và OD replication được lưu; metric thiếu giữ NA.
- [x] OD materialized sau salting: 63.572 → 88.828 dòng; tăng 25.256.
- [ ] Chưa tái triển khai Flow-Join++ hoặc so sánh trực tiếp với hệ thống của bài.
- [ ] Chưa lặp phiên auto-reference trên ngày khác để kiểm tra độ ổn định giữa phiên.

Artifact: `data/benchmarks/scaling/s3_jan_may/flowjoin_auto_strategy_20261122T074810Z`. Xem `session_context.json` cho câu lệnh, cấu hình yêu cầu, thời gian và SHA-256 mã nguồn; `comparison_analysis.json`, CSV run/audit/summary, `physical_plan_strategies.csv`, `nd3_task_records.csv`, `nd3_host_cgroup_by_run.csv`, `nd3_aqe_interventions.csv`, `spark_submit.log`, manifest và figure. Event Log: `logs/spark-events/app-20261122074817-0000`. Bộ ba nhánh S3 official trước được giữ riêng.
