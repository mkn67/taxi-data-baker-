# Tổ chức project theo chương luận văn

Các thư mục `01`–`05` gom bản thảo và chỉ mục bằng chứng theo mục lục. Scripts Chương 1–3 nằm tương ứng trong `scripts/chapter_01/`, `scripts/chapter_02/` và `scripts/chapter_03/`; notebook Chương 3 nằm trong `notebooks/chapter_03/`. Dữ liệu vẫn giữ nguyên trong `data/`; Compose mount nguyên thư mục `scripts/` và `notebooks/`. README từng chương chỉ ra nội dung và artifact tương ứng.

| Chương | Thư mục | Nội dung |
|---|---|---|
| 1 | [01_overview_scientific_basis](01_overview_scientific_basis/README.md) | Tổng quan, cơ sở khoa học, khảo sát dữ liệu |
| 2 | [02_data_preparation](02_data_preparation/README.md) | Tiền xử lý, kiểm định, dữ liệu Silver và hotspot |
| 3 | [03_pipeline_skew_benchmarks](03_pipeline_skew_benchmarks/README.md) | Pipeline, ND2/ND3, benchmark và phương pháp không gian |
| 4 | [04_results_tradeoffs](04_results_tradeoffs/README.md) | Kết quả thực nghiệm, scaling và đánh đổi |
| 5 | [05_conclusion_future_work](05_conclusion_future_work/README.md) | Kết luận, đóng góp, hạn chế và hướng phát triển |

## Thư mục dùng chung

- `scripts/chapter_01/`–`scripts/chapter_03/`: scripts được nhóm theo đúng chương; tiện ích notebook Chương 3 nằm trong `chapter_03/`.
- `notebooks/chapter_03/`: notebook benchmark và kiểm tra artifact Chương 3.
- `data/raw/`, `data/silver/`, `data/gold/`: các tầng dữ liệu.
- `data/benchmarks/`: audit, summary, plan, hình và kết quả theo từng run.
- `data/zones/`: lookup và hình học Taxi Zone.
- `dags/`: Airflow DAG; `logs/`: log thực thi.

Không di chuyển các đường dẫn dữ liệu/script trên khi chưa cập nhật cấu hình trong mã và compose.
