# Phase A — kiểm tra artifact hiện có (21/11/2026)

Phạm vi: chỉ đọc Gold/Silver, CSV benchmark, plan và Spark event logs đã có. Không chạy benchmark mới. `PASS` nghĩa là artifact đủ để trả lời câu hỏi tiền kiểm, không có nghĩa phương pháp tối ưu đã thắng.

| Kiểm tra | Kết quả quan sát | Trạng thái |
|---|---|---|
| Hotspot P95 qua tháng | Năm artifact `data/silver/hotspots_p95_pu_2026_MM` đều có 14 ID. Jaccard 10 cặp tháng: 0,750–1,000; trung vị 0,867. Tháng 01 và 03 trùng tập. | PASS cho độ ổn định tập ID; chưa đo tỷ lệ trips thuộc H |
| AQE intervention | Final executed plans của ứng dụng ND3 tháng 01 `app-20261103153727-0002` và Bảng 4.7 thể hiện `AQEShuffleRead coalesced`, không có node skew split. CSV ghi 5 task ở stage được chọn, so với 200 ở Baseline/Salting. | PASS cho mô tả coalescing; kết quả skew split âm tính |
| Record so với task time | Tính Spearman trên successful tasks của đúng `target_stage_id` từ run CSV, dùng `Shuffle Read Metrics.Total Records Read` và `Executor Run Time` trong event logs. ND2: Baseline 0,281–0,311; Selective 0,159–0,314; Uniform 0,007–0,044 (mỗi stage 200 task). ND3: Baseline 0,422–0,516; Salting 0,248–0,356 (200 task); AQE 0,600–0,700 (5 task). | PASS cho thống kê mô tả; không suy ra nguyên nhân hay so AQE 5 task với 200 task như các mẫu tương đương |
| Biến thiên wall time | Official tháng 01 mỗi phương pháp có 3 measured runs, collector `OK`, validation `PASS`. CV mẫu ND2: Baseline 14,2%, Selective 15,6%, Uniform 9,3%; ND3: Baseline 63,6%, AQE 26,9%, Salting 16,2%. | INCONCLUSIVE cho ưu thế hiệu năng ổn định; nhất là ND3 Baseline có một lượt 22,316 s |
| Broadcast | Có `BroadcastHashJoin` trong plan ablation **development**. Official ND3 tắt broadcast để cô lập SMJ; chưa có đối chứng Broadcast cùng input/config/run chính thức hay đo kích thước projected OD theo byte. | INCONCLUSIVE cho khuyến nghị triển khai |
| Môi trường | CSV official ghi Spark 3.5.9 và hai worker hosts; Compose khai báo CPU/RAM. Host hiện tại có Intel i5-1340P, 16 logical CPUs, 15 GiB RAM. Chưa có cgroup usage per application. | PASS cho cấu hình khai báo; INCONCLUSIVE cho mức sử dụng thực tế |
| Công trình gần nhất | Ba PDF do người dùng cung cấp đã được đối chiếu trong plan. Bản thảo “AQE or Salting?” chưa có URL và chưa được xác minh trong lượt này. | INCONCLUSIVE cho tuyên bố novelty |

**Nguồn chính:** `data/benchmarks/benchmark_nd{2,3}_*_runs_official_standalone.csv`, `data/benchmarks/nd3_plans/official_standalone/`, `data/benchmarks/nd3_plans_dev/`, `logs/spark-events/app-20261103153557-0001`, `logs/spark-events/app-20261103153727-0002`, và năm artifact hotspot PU. Event log được đọc theo từng dòng JSON bằng `scripts/chapter_04/analyze_task_load.py`; bảng per-run tái lập được lưu tại `nd2_task_load.csv` và `nd3_task_load.csv` trong thư mục này. Hệ số Spearman tính riêng từng stage, không gộp qua các run.

**Giới hạn:** ba lượt đo là ít; không có CPU utilization, memory peak, NIC throughput hoặc critical path đã dựng đầy đủ. Một stage có tương quan record/time thấp không chứng minh dữ liệu không skew; một stage có tương quan cao cũng không định danh hot key gây chậm.
