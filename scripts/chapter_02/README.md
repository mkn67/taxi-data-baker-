# Scripts Chương 2 — Tiền xử lý và khảo sát dữ liệu

Chạy từ project root. Các script được chia theo công việc để phân biệt bước tạo dữ liệu, kiểm toán và trình bày:

| Công việc | Script | Đầu ra chính |
|---|---|---|
| Raw → Silver, audit lọc tuần tự, hotspot P95 | `02_preprocess_and_audit.py` | `data/silver/`, CSV audit trong `data/benchmarks/` |
| Audit zone ứng viên quanh 60th Street | `02b_zone_boundary_check.py` | `data/zones/manhattan_60th_boundary_audit.csv` |
| Audit lọc độc lập bằng DuckDB | `02c_audit_filter_duckdb.py` | Audit trong `data/benchmarks/` |
| Audit phí CBD | `audit_cbd_congestion_fee.py` | In bảng Markdown ra terminal |
| Tạo bảng 2.6 | `build_table_2_6.py` | In bảng Markdown ra terminal từ `data/zones/manhattan_60th_boundary_audit.csv` |
| Tạo hình 2.4–2.8 | `plot_figure_2_4.py`–`plot_figure_2_8.py` | `data/zones/`; Hình 2.8 lấy phân bố từ Silver và đối chiếu với hotspot P95 đã lưu |

Điểm bắt đầu của quy trình dữ liệu là `02_preprocess_and_audit.py`. Kiểm tra cấu hình đầu vào và output trong từng script trước khi chạy lại vì một số script dùng đường dẫn mặc định theo container hoặc thư mục làm việc hiện tại; các hình được lưu trong `data/zones/`.
