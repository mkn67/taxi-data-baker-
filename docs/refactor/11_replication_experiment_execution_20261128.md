# DETOUR — Audit I/O và pilot chi phí nhân bản 28/11/2026

## Môi trường

Theo output từ terminal Fedora, Spark Standalone có hai worker ALIVE, mỗi worker 2 core và 2.048 MiB worker memory, không có application hoạt động tại thời điểm kiểm tra. Dung lượng trống của phân vùng project khoảng 72 GiB tại lúc preflight. Đây là tài nguyên khả dụng, không phải số đo sử dụng thực tế của tác vụ.

## Audit 96 lượt confirmation

Phiên `app-20261125142144-0001`: `audit_supplement_io_events.py` đối chiếu task thành công của các stage attempt thuộc SQL execution đo và `stages_detail` do collector ghi. 96/96 lượt có scope và metric Verified; 384 stage có delta bằng 0 cho shuffle read/write và memory/disk spill. Không quan sát task fail/killed, speculative hay duplicate success partition trong các stage audit. Shuffle read được tính local bytes + remote bytes; số liệu là tổng phạm vi stage collector, không gán riêng cho join.

Đầu ra không ghi đè CSV gốc: `data/benchmarks/phase_c/supplement_pilot_20261125_OQ8HCM/audit_io_20261128/audit_96_runs.csv`, `audit_stage_task.csv`, `audit_summary.json`. Script SHA-256 ghi trong `audit_summary.json`.

## Checksum và nguồn gốc input

SHA-256 nội dung fact 10M `23c178f1c3040c6613c5ce91cf7fb90fa0cdc796034c615be22a2820b08238bf`, dimension lớn `039ee1c16d10aebe0fa96e26afcc7c2dccdd5b58d62f9bdea953cbe4acd86441`, fact 2M `7c34f674466fb214a622f5de59bbe8881afb54e3d4e8610f77360512565d7771` đều khớp manifest của project. Điều này xác nhận tính toàn vẹn so với manifest, không tự chứng minh quyền tác giả hay quan hệ với Malviya; đối chiếu tài liệu gốc vẫn chưa hoàn tất.

## Hai input pilot dimension

Hai dimension được tạo bằng `scripts/chapter_03/supplement_audit/prepare_supplement_replication.py`, nguồn và output hashes trong `data/benchmarks/phase_c/replication_pilot_20261128/manifest.json`. Mỗi mức có 5.000.256 dòng, 41 Parquet row groups, không null/duplicate keys; 256 dòng khớp fact. Đối chiếu hai chiều matched rows bằng DuckDB `EXCEPT ALL` không phát hiện chênh lệch.

| Mức | Dòng dimension thuộc hotspot | Dòng thêm dự kiến với S=8 | Parquet bytes |
| --- | ---: | ---: | ---: |
| Thấp | 13 | 91 | 44.614.629 |
| Cao | 100.013 | 700.091 | 44.696.118 |

Đây là hai mức pilot được chọn sau preflight, chưa là thông số khóa confirmation. Các dòng thêm vào tập pickup hotspot đều là filler OD không khớp fact; hash partition, khả năng nén và I/O có thể thay đổi, cần báo như yếu tố gây nhiễu. Cần dùng plan/SQL metrics để xác minh filler thực sự đi qua phép nhân bản trong action đo, không dựa riêng trên preflight count.

## Pilot Spark

Lần thử `app-20261128072316-0000` dừng ở warm-up vì runner dùng `SparkContext.clearJobGroup()` không có trong PySpark 3.5; không có lượt đo. Đã sửa runner xóa local properties bằng API có sẵn. Lần chạy lại tạo output riêng; chưa xác nhận gate cho đến khi đọc kết quả/plan/Event Log và đối chiếu sự nhân bản của filler.

Lần chạy lại `app-20261128072418-0001` đã hoàn tất 4/4 action pilot. Collector, final plan và so sánh DuckDB đối với chính output đã đo đều PASS 4/4. Kết quả count theo hai dimension đối chiếu hai chiều giống nhau. Event Log SQL execution 8 (E thấp) ghi metric `Generate.number of output rows` 5.000.347; execution 11 (E cao) ghi 5.700.347, khớp expected sau S=8. Cả hai executor chạy, không có lỗi reported trong runner. Gate pilot cho phép chạy confirmation, không dùng wall-time một block làm kết luận.

Confirmation khóa cùng hai dimension pilot và fact/hotspots checksum trong manifest; 8 block × 4 điều kiện B thấp/E thấp/B cao/E cao, seed 20261128, thiết kế bốn hoán vị vòng lặp hai lần, xáo trộn thứ tự block. CI bootstrap percentile 95% 100.000 lần, seed 20261128, lấy mẫu block cùng bốn điều kiện, không hiệu chỉnh đồng thời; không định sử dụng kiểm định p-value. Dừng và giữ partial nếu correctness/plan/collector lỗi, không tự chạy bù. `confirmation_protocol.json` phải được ghi trước warm-up/đo, cùng mã hash và thứ tự block.

Lần gọi confirmation đầu tiên dừng trước khi tạo Spark application do image driver không có lệnh `git`; không phát sinh lượt đo. Đã đổi wrapper lấy git revision và working-tree status trên Fedora, ghi thành file riêng trước khi gọi container. Lần chạy lại `app-20261128072824-0002` hoàn tất 32/32 lượt với collector, plan và đối soát output action đo PASS. Output tại `confirmation_20261128T072821Z_GDPsIA/results/`.

## Kết quả confirmation

| Contrast trong cùng block | Median tỷ số | Bootstrap percentile 95% CI | Đọc theo biên ±5% |
| --- | ---: | ---: | --- |
| E/B, mức thấp | 1,0096 | [0,9852; 1,0424] | CI trong [0,95; 1,05] |
| E/B, mức cao | 1,0063 | [0,9243; 1,0477] | CI cắt biên dưới; chưa phân định theo biên 5% |
| (E/B cao) / (E/B thấp) | 0,9972 | [0,9381; 1,0632] | CI cắt cả hai biên; chưa phân định mức thay đổi thực hành |

CI chưa hiệu chỉnh đồng thời, tám block trong một Spark application và một host. Không tính p-value. Không suy ra replication không có chi phí, hay Salting thắng/thua AQE skew join C. Ở mức thấp/cao, E có median shuffle read khoảng 243.240.946/253.021.722 byte, còn B khoảng 239.376.671/239.271.194 byte. Đây là tổng các stage của action, không phải lưu lượng mạng vật lý hay riêng operator join. Những thay đổi về khóa filler, phân bố hash và nén là yếu tố nhiễu.

Audit `audit_io/` ghép SQL execution, stage attempts và task: 32/32 lượt Verified, 128/128 stage khớp collector về shuffle read/write và memory/disk spill; Event Log `app-20261128072824-0002`. `generate_metric_audit.csv` kiểm 16 lượt E: 8/8 mức thấp phát ra 5.000.347 dòng và 8/8 mức cao phát ra 5.700.347 dòng. `paired_block_ratios.csv` và `paired_analysis.csv` do `analyze_supplement_replication.py` tạo. Bốn điều kiện xuất hiện đúng 2 lần ở mỗi vị trí trong tám block. Kết quả output 32 action đã đo so khớp DuckDB, dung sai số thực 0,05.
