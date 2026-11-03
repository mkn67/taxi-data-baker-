# Tổ chức project theo chương luận văn

Các thư mục `01`–`05` gom bản thảo và chỉ mục bằng chứng theo mục lục. Mã chạy và notebook được chia theo chương trong `scripts/chapter_02/`, `scripts/chapter_03/` và `notebooks/chapter_03/`. Dữ liệu vẫn giữ nguyên trong `data/`; Docker Compose tiếp tục mount nguyên thư mục `scripts/` và `notebooks/`. README trong từng chương chỉ ra các nguồn tương ứng; không phải nội dung luận văn hoàn chỉnh.

| Chương | Thư mục | Nội dung |
|---|---|---|
| 1 | [01_overview_scientific_basis](01_overview_scientific_basis/README.md) | Tổng quan, cơ sở khoa học, khảo sát dữ liệu |
| 2 | [02_data_preparation](02_data_preparation/README.md) | Tiền xử lý, kiểm định, dữ liệu Silver và hotspot |
| 3 | [03_pipeline_skew_benchmarks](03_pipeline_skew_benchmarks/README.md) | Pipeline, ND2/ND3, benchmark và phương pháp không gian |
| 4 | [04_results_tradeoffs](04_results_tradeoffs/README.md) | Kết quả thực nghiệm, scaling và đánh đổi |
| 5 | [05_conclusion_future_work](05_conclusion_future_work/README.md) | Kết luận, đóng góp, hạn chế và hướng phát triển |

## Thư mục dùng chung

- `scripts/chapter_02/`, `scripts/chapter_03/`: mã theo chương; tiện ích notebook chung còn ở `scripts/` gốc.
- `notebooks/chapter_03/`: notebook benchmark và kiểm tra artifact Chương 3.
- `data/raw/`, `data/silver/`, `data/gold/`: các tầng dữ liệu.
- `data/benchmarks/`: audit, summary, plan, hình và kết quả theo từng run.
- `data/zones/`: lookup và hình học Taxi Zone.
- `dags/`: Airflow DAG; `logs/`: log thực thi.

Không di chuyển các đường dẫn dữ liệu/script trên khi chưa cập nhật cấu hình trong mã và compose.
