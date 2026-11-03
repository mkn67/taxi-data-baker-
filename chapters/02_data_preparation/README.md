# Chương 2 — Tiền xử lý và chuẩn bị dữ liệu thực nghiệm

Thư mục này là chỉ mục đầu ra cho các mục 2.1–2.7. Dữ liệu và mã nguồn vẫn nằm ở các thư mục chung của pipeline.

## Nguồn hiện có

- Tiền xử lý tuần tự, Silver và audit: `scripts/chapter_02/02_preprocess_and_audit.py`, `scripts/chapter_02/02c_audit_filter_duckdb.py`.
- Kiểm tra vùng biên và phân loại Taxi Zone: `scripts/chapter_02/02b_zone_boundary_check.py`.
- Thống kê quy mô raw: `scripts/chapter_02/02d_scaling_test.py`, `data/benchmarks/figure_3_4_data_scale.csv`.
- Kiểm tra phụ phí và hình liên quan: `scripts/chapter_02/02e_cbd_congestion_fee2.5.py`, `scripts/chapter_02/02_Bang_2.6.py`, `scripts/chapter_02/02_plot_figure_2_6.py`, `data/benchmarks/`.
- Silver và hotspot theo tháng: `data/silver/`.

## Trạng thái

Theo mục lục người dùng cung cấp, Chương 2 được đánh dấu hoàn tất. Bản thảo nội dung Chương 2 chưa có trong repository này; thư mục hiện đóng vai trò mục lục artifact.

Đối chiếu thuật ngữ mục 2.5 với MFTune: xem [research_alignment_notes.md](../research_alignment_notes.md#chương-2--tiền-xử-lý-và-thiết-kế-quy-mô). Các tháng 01–05 hiện là benchmark trên input độc lập gần cùng kích thước, chưa phải fidelity levels có kiểm tra proxy/full-fidelity.
