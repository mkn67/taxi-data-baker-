# Kiểm kê nguồn DETOUR

## Phạm vi và nguồn chỉnh sửa

Tệp người dùng cung cấp `/home/kedora/Bản tải về/BigData.docx` là nguồn của bản báo cáo hợp nhất. Tệp có bìa, năm chương, 39 bảng và 29 hình nội tuyến khi nhận. `DETOUR (3).pdf` không có trong repo hay thư mục tải về đã rà. Bản PDF cũ không được dùng thay thế Word mới. Tạo bản Word/PDF mới trong `docs/refactor/release/` để giữ bản người dùng gửi. Kế hoạch nhắc báo cáo bốn chương. Bản nguồn thực tế đã có Chương 5 và ảnh ND5, nên giữ cả chương đó. Outline paper được lưu riêng.

Repo tại `/home/kedora/SEM5/Bigdata/taxi-data-baker-`. Trước lượt này, Git đã có thay đổi ở Markdown Chương 3–4, README Chương 5 và `scripts/chapter_02/02c_audit_filter_duckdb.py`, cùng các thư mục ảnh Chương 2/3/5, Chương 5, script ND5 và sổ thực nghiệm chưa theo dõi. Dùng các phần đã có làm đầu vào.

| Đường dẫn | Chức năng | Quyết định |
|---|---|---|
| `docker-compose.yml`, `Dockerfile.spark`, `Dockerfile.airflow` | Hạn mức container và cấu hình Spark/Airflow | AUDIT. Tách hạn mức khai báo với executor thực cấp từ log |
| `scripts/chapter_02/02_preprocess_and_audit.py`, `scripts/chapter_02/02c_audit_filter_duckdb.py` | ETL và audit lọc độc lập | AUDIT. Chạy phép đếm đọc dữ liệu, dựa vào bản audit hiện tại |
| `scripts/chapter_03/benchmark_engine.py`, `run_benchmark_nd2.py`, `run_benchmark_nd3.py` trong cùng thư mục | Runner ND2/ND3 và collector | AUDIT. So code với CSV, final plan và Event Log |
| `dags/chapter4_march_benchmark.py` | Điều phối ND4 | AUDIT. Đối chiếu clean và hai repeat |
| `scripts/chapter_03/build_spatial_gold.py`, `scripts/chapter_05/build_nd5_explorer.py` | Gold và tra cứu Kepler.gl | DOC/AUDIT. Ảnh 3D thật đã có trong Chương 5 |
| `chapters/03_pipeline_skew_benchmarks/manuscript/chapter3_complete.md` | Chương 3 đã được chỉnh theo hướng phương pháp | DOC. Dùng phần đã biên tập mới để đồng bộ bản Word |
| `chapters/04_results_tradeoffs/manuscript/chapter4_results.md` | Bản nội dung Chương 4 trong repo | DOC. Đồng bộ các chỉnh sửa về bằng chứng và các phiên |
| `data/raw/`, `data/silver/`, `data/gold/`, `data/benchmarks/`, `logs/spark-events/` | Đầu vào, CSV, manifest, audit, plans, Event Logs | AUDIT, đọc tại chỗ |
| `docs/assets/chapter2/` đến `docs/assets/chapter5/` | Ảnh báo cáo | AUDIT/DOC. Đối chiếu mục, phiên và chú thích |
| `docs/detour-consolidation/experiment_registry.md` | Sổ ứng dụng đã có | AUDIT. Kiểm tra và bổ sung metadata từ artifact |
| `docs/refactor/07_paper_outline.md` | Dàn ý trích xuất bài báo | PAPER-LATER |

Thông điệp báo cáo: giảm chênh lệch tải công việc không bảo đảm giảm thời gian thực thi toàn phần. Kết quả phụ thuộc vào workload, chiến lược vật lý và chi phí chuẩn bị.

Thứ tự xử lý: inventory, registry, kiểm toán claim, Word Chương 1–2, đồng bộ phương pháp Chương 3, findings Chương 4, kiểm tra bố cục và outline. Hai rủi ro chính là gộp sai phiên warm-up và diễn giải nhầm heap/CPU hoặc bộ đếm BroadcastExchange. Chỉ phân tích artifacts có sẵn.
