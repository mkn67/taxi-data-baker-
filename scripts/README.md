# Scripts theo chương

Chạy lệnh từ project root. Thư mục `chapter_01`–`chapter_03` khớp trực tiếp với các chương; dữ liệu đầu vào/đầu ra vẫn dùng chung trong `data/`.

| Thư mục | Vai trò | Bắt đầu từ |
|---|---|---|
| `chapter_01/` | Khảo sát sơ bộ dữ liệu cho Chương 1 | `README.md` (ghi chú script khảo sát cũ) |
| `chapter_02/` | Làm sạch, Silver, audit, boundary và hình Chương 2 | `02_preprocess_and_audit.py` |
| `chapter_03/` | ND2/ND3, benchmark nhiều tháng độc lập, OD, spatial Gold/Kepler và hình Chương 3 | `run_benchmark_nd2.py`, `run_benchmark_nd3.py` |

## Chương 1 — Khảo sát ban đầu

- `chapter_01/01_explore_and_hotspot.py`: script khảo sát ban đầu, giữ làm tư liệu; đường dẫn runtime còn theo cấu hình cũ và không dùng làm nguồn hotspot benchmark chính thức.
- Hotspot P95 tái lập cho benchmark được tạo trong pipeline Chương 2 bằng `chapter_02/02_preprocess_and_audit.py`.

## Chương 2 — Chuẩn bị dữ liệu

- `chapter_02/02_preprocess_and_audit.py`: lọc Raw tháng 01/2026, ghi Silver, audit tuần tự và hotspot P95.
- `chapter_02/02b_zone_boundary_check.py`: audit zone ứng viên quanh đường 60th Street.
- `chapter_02/02c_audit_filter_duckdb.py`: audit độc lập bằng DuckDB.
- Các script `plot_*.py`: tạo hình kiểm toán và hình minh họa Chương 2.

Ví dụ: `python scripts/chapter_02/02_preprocess_and_audit.py`.

## Chương 3 — Benchmark và phân tích spatial

- ND2: `chapter_03/run_benchmark_nd2.py` (Baseline, Uniform Salting, Selective Salting).
- ND3: `chapter_03/run_benchmark_nd3.py` (Baseline SMJ, AQE, Salting Join).
- Engine dùng chung: `chapter_03/benchmark_engine.py`.
- Scaling tháng độc lập: `chapter_03/run_monthly_scaling_benchmarks.sh`.
- Hình quy mô input Raw theo tháng: `chapter_03/plot_figure_3_4_data_scale.py`.
- Spatial Gold và Kepler: `chapter_03/03_build_gold_spatial_analysis.py`, `chapter_03/build_kepler_spatial_artifact.py`.
- Tạo/kiểm tra notebook: `chapter_03/generate_notebooks.py`, `chapter_03/test_notebook_execution.py`.

Ví dụ chạy thử:

```bash
python scripts/chapter_03/run_benchmark_nd2.py --mode pilot
python scripts/chapter_03/run_benchmark_nd3.py --mode pilot
```

Benchmark đầy đủ yêu cầu Spark Standalone:

```bash
python scripts/chapter_03/run_benchmark_nd2.py --mode full --master spark://spark-master:7077
python scripts/chapter_03/run_benchmark_nd3.py --mode full --master spark://spark-master:7077
```

`pilot` và `full` sử dụng cùng tập Silver theo runner; khác nhau ở số lượt đo. Không xem pilot là tập con ngày/tuần. Trước khi chạy script ghi dữ liệu, xác nhận đường dẫn và sao lưu đầu ra cần giữ.

## Notebook

Notebook nằm trong `notebooks/chapter_03/`. Chúng dùng để đọc, chạy và kiểm tra luồng benchmark; script runner là đường chạy tái lập chính.
