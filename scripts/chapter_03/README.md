# Scripts Chương 3 — Benchmark và phân tích không gian

Chạy từ project root. Nhóm script theo vai trò:

| Vai trò | Script |
|---|---|
| Engine chung ND2/ND3 | `benchmark_engine.py` |
| Aggregation (Nội dung 2) | `run_benchmark_nd2.py` |
| Shuffle Join (Nội dung 3) | `run_benchmark_nd3.py` |
| Task, AQE plan, and container-resource evidence | `analyze_nd3_scaling_evidence.py` |
| OD theo tháng và preflight khóa | `build_monthly_od_matrix.py`, `audit_od_matrix_keys.py` |
| Tổng hợp benchmark tháng độc lập | `run_monthly_scaling_benchmarks.sh`, `aggregate_scaling_benchmarks.py` |
| Spatial Gold và Kepler | `build_spatial_gold.py`, `build_kepler_spatial_artifact.py` |
| Hình và notebook | `plot_*.py`, `generate_notebooks.py`, `test_notebook_execution.py` |

Artifacts được ghi vào `data/benchmarks/`, `data/silver/` và `data/gold/`; không gom output sinh ra vào cùng thư mục mã nguồn. Điểm vào benchmark là hai runner ND2/ND3. Xem [bản đồ Nội dung 1–4](../../docs/project_map_content_1_to_4.md) để biết đầu vào, output và giới hạn từng phần.

Runner ND3 hiện gọi action cho từng warm-up. Để chạy pilot có Broadcast Hash Join tham chiếu và ba lượt đo xoay thứ tự trên cụm Compose, dùng `--mode pilot --include-broadcast --pilot-iterations 3`; đặt `BENCHMARKS_DIR` riêng cho mỗi phiên. Xem [kết quả pilot và lệnh đầy đủ](../../chapters/03_pipeline_skew_benchmarks/docs/phase_b/pilot_nd3_broadcast_20261121.md). Các lượt ND3 official trước ngày 21/11/2026 không có job warm-up trong Event Log; cần phân biệt khi so sánh thời gian.

Mức S2 tích lũy tháng 01–03 được chuẩn bị bằng `prepare_cumulative_s2.py`: script tạo Silver mới, OD và P95 cùng phạm vi, kèm `s2_input_manifest.json`; không chỉnh dữ liệu Silver từng tháng. [Biên bản S2](../../chapters/03_pipeline_skew_benchmarks/docs/phase_b/s2_cumulative_nd3_pilot_20261121.md) ghi kết quả pilot ND3 và giới hạn. Các lượt S2 không thuộc bảng năm tháng độc lập.

Runner ND2 cũng hỗ trợ `--pilot-iterations 3`. `compare_s1_s2_pilots.py` đối chiếu CSV pilot ND2/ND3 S1–S2, từ chối run thiếu audit/telemetry, thiếu hai Worker hoặc lẫn manifest/application. Bảng đã tạo nằm ở `data/benchmarks/scaling/s2_jan_mar/s1_s2_pilot_comparison.csv`.

`analyze_nd3_scaling_evidence.py` nhận nhiều bộ run CSV/Event Log, kiểm tra trạng thái correctness/collector và xuất task records, AQE final-plan interventions cùng resource windows từ log `podman stats`. Cách dùng và giới hạn diễn giải được ghi trong [biên bản Phase C S2 repeat](../../chapters/03_pipeline_skew_benchmarks/docs/phase_c/s2_repeat_variability_20261121.md).
