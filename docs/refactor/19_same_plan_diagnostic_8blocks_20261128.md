# DETOUR — Audit đối chứng cùng plan: 8 block

Phiên `app-20261128075949-0004`, ngày 28/11/2026: 16 action đo, 8 block, sau hai warm-up, tách khỏi pilot `app-20261128075314-0003`. Hai nhãn X/Y cùng nhánh B và cùng code, fact 10M, dimension low, cấu hình và action; 4 block XY và 4 YX, thứ tự block theo seed 20261128. Hai worker cùng một host, một Spark application. Protocol, input SHA-256 và runner hash được lưu trước đo.

## Audit

| Hạng mục | Kết quả | Phạm vi |
| --- | --- | --- |
| Output | 16/16 khớp DuckDB; checksum output khớp CSV | Count chính xác, tổng sai số tuyệt đối tối đa 0,05. |
| Final plan | 16/16 final SMJ + coalescing, không thấy broadcast join/skew marker; cùng hash text đã chuẩn hóa | Expression ID được ánh xạ theo thứ tự xuất hiện, plan_id bỏ trị số. Cùng plan text không chứng minh phân bố task hay trạng thái runtime giống nhau. |
| Execution và target stage | 16/16 mỗi group có một SQL execution; jobs thành công; stage thuộc execution và có codegen của join | Stage có thể chứa operator khác; mỗi target stage có 4 task thành công; 16/16 TSR tính lại khớp CSV. |
| Cấu hình stage | 16/16 khớp AQE=true, coalescing=true, skew=false, shuffle partitions=200 và hai broadcast thresholds=-1 | Chỉ xác nhận những trường đã liệt kê; application có ApplicationEnd. |
| Shuffle/spill | 16/16 lượt và 64/64 stage khớp task Event Log với collector | Audit toàn bộ phạm vi collector, không gán riêng join. |

Tái lập: `scripts/chapter_03/supplement_audit/audit_same_plan_diagnostic.py` và `scripts/chapter_03/supplement_audit/audit_supplement_io_events.py`; kết quả trong `data/benchmarks/phase_c/same_plan_noise_pilot/diagnostic8_20261128T075946Z_eErmdB/audit_plans_and_pairs/` và `audit_io/`, Event Log `logs/spark-events/app-20261128075949-0004`.

## Mô tả chênh lệch theo block

Định nghĩa đã khóa: `abs(Y/X − 1) × 100%`. X và Y là nhãn kỹ thuật, không đại diện cho hai phương pháp.

| Block | Thứ tự | X (s) | Y (s) | Y/X | Chênh lệch tuyệt đối |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 | YX | 4,5438 | 4,2999 | 0,9463 | 5,37% |
| 2 | YX | 4,6599 | 4,5918 | 0,9854 | 1,46% |
| 3 | XY | 4,2745 | 4,4409 | 1,0389 | 3,89% |
| 4 | XY | 4,2732 | 4,7349 | 1,1080 | 10,80% |
| 5 | XY | 4,0205 | 3,7092 | 0,9226 | 7,74% |
| 6 | YX | 3,6066 | 3,6034 | 0,9991 | 0,09% |
| 7 | XY | 3,5944 | 3,6848 | 1,0252 | 2,52% |
| 8 | YX | 4,1835 | 3,7935 | 0,9068 | 9,32% |

Trung vị chênh lệch tuyệt đối 4,63%, khoảng quan sát 0,09–10,80%; 4/8 cặp vượt 5%. Trong 7/8 block lượt thứ hai chậm hơn lượt đầu, trái xu hướng trong pilot 2 block; dữ liệu ít và theo thứ tự thời gian nên chỉ mô tả, không khẳng định cơ chế thứ tự. Median của 8 action ở block 1–4 là 4,4924 s, ở block 5–8 là 3,6970 s; đây là quan sát thăm dò theo thời gian, không chứng minh nguyên nhân.

## Diễn giải và quyết định

Đối chứng cho thấy chênh lệch trên 5% có thể xuất hiện giữa hai action cùng nhánh tại workload này. Kết quả củng cố việc báo CI, thứ tự và dao động thay vì coi ±5% là noise floor; không thể áp ngưỡng/biến động phiên này trực tiếp cho 96 hoặc 32 action ở application khác. Nó không bác bỏ hay chứng minh lại kết quả trước, không phải so sánh AQE với salting và không xác định cơ chế gây biến động.

Không mở thêm sweep tự động từ kết quả này. Tiếp tục ưu tiên tổng hợp bằng chứng, ghi giới hạn trong Chương 3–4 và paper. Muốn đo tính ổn định giữa application/host hay so E/C ở fanout khác cần protocol và ngân sách riêng; không dùng lại kết quả của diagnostic này làm ghép cặp.
