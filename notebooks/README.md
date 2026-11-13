# Notebook theo nội dung đồ án

| Nội dung | Notebook hiện có | Mục đích |
|---|---|---|
| **1 — Môi trường/khảo sát** | Chưa có | Quy trình xử lý và khảo sát nằm trong `scripts/chapter_02/`. |
| **2 — Aggregation** | `chapter_03/02_benchmark_aggregation_salting.ipynb` | Đọc và giải thích ND2. |
| **3 — Join** | `chapter_03/03_benchmark_join_skew.ipynb` | Đọc và giải thích ND3. |
| **3 — Kiểm tra artifact** | `chapter_03/04_chapter3_artifact_checks.ipynb` | Kiểm tra artifacts Chương 3. |
| **4 — Đánh giá** | Chưa có | Tổng hợp và vẽ bằng scripts trong `scripts/chapter_04/`. |

Notebook dùng runner và dữ liệu trong `scripts/chapter_03/` và `data/`. Mở/chạy từ môi trường project. `notebooks/logs/` giữ Spark event logs đã được cấu hình cho notebook; không phải notebook hoặc dữ liệu đầu vào.

Hai notebook benchmark đã được đặt mặc định ở chế độ pilot; các output đã lưu từ lần chạy cũ được xóa để tránh nhầm đường dẫn/kết quả local cũ với artifacts hiện tại. Khi chạy, notebook ghi riêng dưới `data/benchmarks/notebook_runs/`; runner CLI chính thức vẫn dùng các tệp tier ở `data/benchmarks/`.

Xem [bản đồ mã nguồn và artifact theo Nội dung 1–4](../docs/project_map_content_1_to_4.md) để biết trạng thái triển khai và đầu ra tương ứng.
