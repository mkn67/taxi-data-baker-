# DETOUR — Audit pilot cùng plan, 28/11/2026

## Phạm vi

Application `app-20261128075314-0003`: Spark 3.5.9, hai worker trên cùng host, hai executor được cấp mỗi executor 2 core và 1024 MiB heap. Fact 10M và dimension low của phiên replication; nhánh B, AQE và coalescing bật, skew optimization và broadcast tắt. Hai nhãn X/Y gọi cùng code, dữ liệu và cấu hình. Hai warm-up, 2 block × 2 action, thứ tự XY rồi YX; output được giữ và so với DuckDB ngoài đồng hồ đo. Pilot dùng kiểm tra kỹ thuật, không đủ để kết luận noise floor.

## Kết quả và audit

| Block | Thứ tự | X (s) | Y (s) | Y/X |
| --- | --- | ---: | ---: | ---: |
| 1 | X → Y | 4,3457 | 3,9284 | 0,9040 |
| 2 | Y → X | 4,0088 | 4,2002 | 1,0477 |

Hai cặp chênh lệch thời gian, nhưng mới có hai block; không ước lượng tỷ lệ biến động bao phủ 95% hoặc suy ra chênh lệch giữa phương pháp. Trong cả hai block lượt đầu chậm hơn lượt sau; chỉ hai block nên chưa thể xác định đây là hiệu ứng thứ tự.

| Kiểm tra | Kết quả | Giới hạn |
| --- | --- | --- |
| Output từng action | 4/4 DuckDB PASS | So count chính xác và hai tổng với sai số tuyệt đối 0,05 theo runner. |
| Final plan | 4/4 final SMJ, coalesced; không thấy skew/broadcast marker | Sau chuẩn hóa expression ID theo thứ tự xuất hiện và plan_id, final-plan text có cùng SHA-256 `7f7e6ca1f6294e8f193eedcfedd106aef48f168be5882ab667f959e9f261f68c`. Điều này không chứng minh runtime hay partition sizes giống hệt. |
| Event Log | ApplicationEnd; mỗi group đo một SQL execution, tất cả 16 measured jobs thành công; cả warm-up và đo có 84 task success | `logs/spark-events/app-20261128075314-0003`. |
| Target stage | 4/4 stage có codegen join; 4 task/stage; TSR tính lại khớp collector | Thuộc stage chứa join, có thể gồm operator khác. |
| Cấu hình thực cấp | Stage submission 4/4 khớp AQE=true, coalesce=true, skew=false, 200 shuffle partitions, auto broadcast=-1 | Chỉ xác nhận các trường vừa nêu. |
| Shuffle/spill | 4/4 action, 16/16 stage khớp collector và Event Log; không có delta | `audit_io/audit_summary.json`; tổng stage collector, không quy riêng operator. |

Artifacts: `data/benchmarks/phase_c/same_plan_noise_pilot/run_20261128T075311Z_DS4gnF/` gồm protocol, outputs, plans, runs và audit I/O. Pilot runner hash trong protocol xác định phiên bản script lúc chạy; script sau đó được mở tùy chọn số block, không thay artifact cũ.

## Bước kế tiếp đã khóa

Diagnostic 8 block × X/Y = 16 action đo, cùng nhánh B và input low; hai warm-up; 4 block XY và 4 block YX được xáo thứ tự với seed 20261128. Đây là phiên độc lập với pilot; không gộp pilot thành confirmation. Trước khi phân tích, yêu cầu output từng action, final plan và Event Log đạt audit như trên. Báo Y/X, chênh lệch tuyệt đối theo block, median và khoảng quan sát; phân tích thăm dò thứ tự nếu cần, không dùng kiểm định ý nghĩa trên 8 block để khẳng định không có hiệu ứng. Phép đo chỉ mô tả biến động trong một application, một workload và một host; không áp dụng như noise floor phổ quát cho các phiên khác. Nếu lỗi hoặc sai correctness, dừng và giữ partial, không tự chạy lại.
