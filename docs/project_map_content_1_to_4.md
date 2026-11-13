# Bản đồ mã nguồn và artifact theo Nội dung 1–4

Tài liệu này nối từng yêu cầu trong đề cương với mã nguồn, notebook, đầu vào và kết quả hiện có. Các thư mục dữ liệu không được di chuyển; đường dẫn bên dưới là vị trí hiện hành trong project.

## Nội dung 1 — Môi trường thực nghiệm và khảo sát dữ liệu

| Thành phần | Đường dẫn | Vai trò/trạng thái |
|---|---|---|
| Cấu hình Spark Standalone và Airflow | `docker-compose.yml`, `Dockerfile.spark`, `requirements-spark.txt` | Khai báo Master, hai Worker, Driver và Airflow. Có cấu hình service Airflow; chưa có DAG benchmark trong `dags/`. |
| Tiền xử lý Raw → Silver | `scripts/chapter_02/02_preprocess_and_audit.py` | Tạo Silver, audit lọc tuần tự và hotspot P95. |
| Audit zone/phụ phí | `scripts/chapter_02/02b_zone_boundary_check.py`, `02c_audit_filter_duckdb.py`, `audit_cbd_congestion_fee.py`, `build_table_2_6.py` | Kiểm tra zone, lọc và phí CBD. |
| Hình khảo sát | `scripts/chapter_02/plot_figure_2_4.py`–`plot_figure_2_8.py` | Sinh hình vào `data/zones/`. Hình 2.4 dùng Raw để mô tả phí; Hình 2.8 dùng Silver và kiểm tra khớp hotspot P95 đã lưu. |
| Đầu vào/kết quả | `data/raw/`, `data/silver/`, `data/zones/` | Giữ nguyên cấu trúc và vị trí hiện có. |
| Notebook | — | Chưa có notebook Chương 1/2; quy trình có thể tái lập hiện đặt trong scripts. |

**Điểm vào:** đọc hướng dẫn đầu script, xác nhận biến đường dẫn và chạy từ thư mục gốc project. Dữ liệu hiện diện không đồng nghĩa với việc mọi nguồn đã được tải tự động bằng script.

## Nội dung 2 — Aggregation và Selective Salting

| Thành phần | Đường dẫn | Vai trò/trạng thái |
|---|---|---|
| Runner ND2 | `scripts/chapter_03/run_benchmark_nd2.py` | Baseline, Uniform Salting S=8, Selective Salting S=8. |
| Engine chung | `scripts/chapter_03/benchmark_engine.py` | Chạy workload, thu thập metrics, lưu run metadata và audit. |
| Notebook | `notebooks/chapter_03/02_benchmark_aggregation_salting.ipynb` | Đọc/giải thích benchmark; runner script là đường chạy tái lập chính. |
| Kết quả | `data/benchmarks/benchmark_nd2_aggregation_*.csv`, `nd2_plans/`, `figure_nd2_*.png` | Run, summary, audit, plan và hình. Các tháng độc lập nằm trong `data/benchmarks/scaling/`. |

**Điểm vào:** `python scripts/chapter_03/run_benchmark_nd2.py --mode pilot` (pilot dùng cùng Silver, giảm số lượt đo).

## Nội dung 3 — Shuffle Join

| Thành phần | Đường dẫn | Vai trò/trạng thái |
|---|---|---|
| Runner ND3 | `scripts/chapter_03/run_benchmark_nd3.py` | Baseline Sort-Merge Join, AQE và Salting Join S=8. |
| OD dimension/preflight | `scripts/chapter_03/build_monthly_od_matrix.py`, `audit_od_matrix_keys.py` | Tạo OD matrix theo tháng và kiểm tra khóa. |
| Notebook | `notebooks/chapter_03/03_benchmark_join_skew.ipynb` | Giải thích workload và kế hoạch join. |
| Kết quả | `data/benchmarks/benchmark_nd3_join_*.csv`, `nd3_plans/`, `od_matrix_key_audit.csv` | Run, summary, audit, execution plan và preflight OD. |

**Điểm vào:** `python scripts/chapter_03/run_benchmark_nd3.py --mode pilot`.

## Nội dung 4 — Tự động hóa và đánh giá hiệu năng

| Thành phần | Đường dẫn | Vai trò/trạng thái |
|---|---|---|
| Tổng hợp bằng chứng Chương 4 | `scripts/chapter_04/build_chapter4_evidence.py` | Đọc CSV đã lưu, tính lại bảng và correctness summaries; không khởi chạy Spark. |
| Vẽ kết quả | `scripts/chapter_04/build_chapter4_figures.py` | Sinh hình ND2/ND3 vào `data/benchmarks/` và bản dùng trong báo cáo vào `docs/assets/chapter4/`. |
| Bản thảo | `chapters/04_results_tradeoffs/sections_4_1_to_4_4.md` | Diễn giải kết quả thực nghiệm hiện có. |
| Airflow | `dags/` và service `airflow-standalone` trong Compose | Service được khai báo nhưng chưa có DAG benchmark. |
| Tháng 03/2026 | `data/benchmarks/scaling/2026_03/` | Có artifacts tháng 03 đã lưu; việc có artifacts không chứng minh pipeline Airflow tự động chạy. |

**Phạm vi cần giữ rõ trong báo cáo:** chưa ghi rằng Airflow đã điều phối benchmark hoặc REST API/Listener đã thu telemetry của một lượt chạy tháng 03 nếu chưa có DAG và provenance tương ứng. Script hiện tại tổng hợp artifact, không thay thế bằng chứng chạy mới.

## Quy ước vị trí ảnh và dữ liệu

- `docs/assets/readme/`: hình minh họa cho README.
- `docs/assets/chapter4/`: bản hình đã chuẩn bị để chèn vào báo cáo Chương 4.
- `data/zones/`: bảng kiểm tra vùng và hình khảo sát Chương 2.
- `data/benchmarks/`: run/summary/audit, execution plans và hình sinh từ kết quả benchmark hoặc spatial analysis.
- `data/gold/`: GeoParquet, CSV và Kepler.gl Gold.
- `data/raw/`, `data/silver/`: dữ liệu nguồn và dữ liệu đã tiền xử lý.

Không đổi tên hoặc di chuyển artifact dữ liệu trong đợt sắp xếp này để bảo toàn đường dẫn được cấu hình trong runner, báo cáo và notebook.
