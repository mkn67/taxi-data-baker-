# Scripts

Chạy lệnh từ project root. Scripts được chia theo chương luận văn; `data/` vẫn là thư mục dữ liệu dùng chung.

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
- Spatial Gold và Kepler: `chapter_03/03_build_gold_spatial_analysis.py`, `chapter_03/build_kepler_spatial_artifact.py`.

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
