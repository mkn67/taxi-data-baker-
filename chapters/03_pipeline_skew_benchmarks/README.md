# Chương 3 — Pipeline, tối ưu hóa lệch tải và thiết kế thực nghiệm

## Bản thảo

- [Chương 3 hoàn chỉnh, từ mục 3.1 đến 3.7](manuscript/chapter3_complete.md)
- [Mục 3.1](manuscript/chapter3_section_3_1.md)
- [Mục 3.2–3.7](manuscript/chapter3_sections_3_2_to_3_5.md)
- [Phương pháp Chương 3](manuscript/chapter3_methodology.md)

## Audit và bằng chứng nghiên cứu

- [Ghi chú nghiên cứu không gian và tài liệu tham khảo](docs/chapter3_spatial_research_notes.md)
- [Checklist artifact](docs/chapter3_artifact_audit.md)
- [Báo cáo thực hiện Phase C WP0–WP7](docs/phase_c/phase_c_execution_report_20261123.md)
- Các biên bản theo phase nằm trong `docs/phase_a/`, `docs/phase_b/` và `docs/phase_c/`.

## Mã nguồn và notebook dùng chung

- ND2/ND3: `scripts/chapter_03/benchmark_engine.py`, `scripts/chapter_03/run_benchmark_nd2.py`, `scripts/chapter_03/run_benchmark_nd3.py`.
- Scaling: `scripts/chapter_03/run_monthly_scaling_benchmarks.sh`, `scripts/chapter_03/aggregate_scaling_benchmarks.py`, `scripts/chapter_03/build_monthly_od_matrix.py`.
- Hình 3.4 quy mô Raw theo tháng: `scripts/chapter_03/plot_figure_3_4_data_scale.py`.
- Spatial/Kepler: `scripts/chapter_03/build_spatial_gold.py`, `scripts/chapter_03/build_kepler_spatial_artifact.py`.
- Phân tích ổn định hotspot theo giờ: `scripts/chapter_03/analyze_hotspot_hourly_stability.py`.
- Notebook: `notebooks/chapter_03/02_benchmark_aggregation_salting.ipynb`, `notebooks/chapter_03/03_benchmark_join_skew.ipynb`, `notebooks/chapter_03/04_chapter3_artifact_checks.ipynb`.

## Artifact chính

Run CSV, summary, audit, execution plans và hình ở `data/benchmarks/`; Gold GeoParquet/CSV và Kepler `.gl` ở `data/gold/`. Xem checklist để biết trạng thái và giới hạn bằng chứng.

Đối chiếu phạm vi CIBPartitioner, MFTune và nghiên cứu chính sách Zhang et al.: [research alignment](../shared/research_alignment_notes.md#chương-3--phương-pháp) và [spatial research notes](docs/chapter3_spatial_research_notes.md).

Artifacts tạo từ dữ liệu ở `data/benchmarks/` và `data/gold/`; hình trình bày ở `docs/assets/`. Biên bản Phase C mới nhất và trạng thái nâng cấp nằm trong `docs/phase_c/`.

### Chạy nhanh

Từ project root, sau khi Spark Standalone và Silver tháng 01/2026 đã sẵn sàng:

```bash
python scripts/chapter_03/run_benchmark_nd2.py --mode pilot
python scripts/chapter_03/run_benchmark_nd3.py --mode pilot
python scripts/chapter_03/run_benchmark_nd2.py --mode full --master spark://spark-master:7077
python scripts/chapter_03/run_benchmark_nd3.py --mode full --master spark://spark-master:7077
```

Scaling nhiều tháng dùng `bash scripts/chapter_03/run_monthly_scaling_benchmarks.sh`. Xem [README gốc](../../README.md) và checklist artifact trước khi chạy chính thức. Pilot/full là chế độ chạy trên cùng input Silver, không phải mức dữ liệu khác nhau.

Hai benchmark notebook mặc định ở pilot và ghi vào `data/benchmarks/notebook_runs/` để không ghi đè CSV/plan official của runner CLI. Dùng runner script làm nguồn chính cho kết quả benchmark được trích dẫn.
