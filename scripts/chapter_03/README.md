# Scripts Chương 3 — Benchmark và phân tích không gian

Chạy từ project root. Nhóm script theo vai trò:

| Vai trò | Script |
|---|---|
| Engine chung ND2/ND3 | `benchmark_engine.py` |
| Aggregation (Nội dung 2) | `run_benchmark_nd2.py` |
| Shuffle Join (Nội dung 3) | `run_benchmark_nd3.py` |
| OD theo tháng và preflight khóa | `build_monthly_od_matrix.py`, `audit_od_matrix_keys.py` |
| Tổng hợp benchmark tháng độc lập | `run_monthly_scaling_benchmarks.sh`, `aggregate_scaling_benchmarks.py` |
| Spatial Gold và Kepler | `build_spatial_gold.py`, `build_kepler_spatial_artifact.py` |
| Hình và notebook | `plot_*.py`, `generate_notebooks.py`, `test_notebook_execution.py` |

Artifacts được ghi vào `data/benchmarks/`, `data/silver/` và `data/gold/`; không gom output sinh ra vào cùng thư mục mã nguồn. Điểm vào benchmark là hai runner ND2/ND3. Xem [bản đồ Nội dung 1–4](../../docs/project_map_content_1_to_4.md) để biết đầu vào, output và giới hạn từng phần.
