# Đối chiếu bảng và hình trong báo cáo

ID phiên tra tại [sổ thực nghiệm](01_experiment_registry.csv). Hình minh họa do tác giả soạn được ghi là sơ đồ. Ảnh giữ từ bản Word gốc có nguồn dữ liệu kiểm tra được, nhưng một số ảnh không còn tệp dựng chính xác, nên không dùng chúng để phục hồi số đo.

| Bảng, hình | Nguồn | Tổng hợp / hình thức | Tệp tạo hoặc nguồn ảnh |
|---|---|---|---|
| B1.1 | Raw NYC TLC tháng 01 | Schema 25 trường và bản ghi minh họa | Word đầu vào, dữ liệu `data/raw/fhvhv_tripdata_2026-01.parquet` |
| B1.2, H1.1 | `docker-compose.yml`, E1 và E4.01 | Hạn mức Compose và sơ đồ môi trường | Bản Word đầu vào |
| B2.1–2.3, H2.1–2.2 | Raw và Silver tháng 01, `02_preprocess_and_audit.py` | Lược đồ 8+2, quy mô, trình tự và điều kiện lọc | Bản Word đầu vào |
| B2.4, H2.3 | D02 | Số hàng còn lại sau từng bộ lọc | `02c_audit_filter_duckdb.py`, ảnh H2.3 nhúng trong Word |
| B2.5, H2.4 | Raw tháng 01 | Phân bố phí CBD và giá trị thiếu | `audit_cbd_congestion_fee.py`, `plot_figure_2_4.py` |
| H2.5 | `data/zones/shp/taxi_zones.shp` | Đa giác Taxi Zone | `plot_figure_2_5.py` |
| B2.6, H2.6 | `data/zones/manhattan_60th_boundary_audit.csv` | 16 zone ứng viên trong phạm vi 1.000 m, nhóm hình học | `02b_zone_boundary_check.py`, `build_table_2_6.py`, `plot_figure_2_6.py` |
| B2.7, H2.7 | E1.01–02, E3.01–02 | Thiết kế và warm-up pilot/official trên cùng Silver tháng 01 | `plot_figure_2_7.py` |
| B2.8, H2.8 | `data/zones/hotspots_p95.csv`, Silver và artifact hotspot tháng 01 | Đếm theo zone, P95 nearest-rank | `02_preprocess_and_audit.py`, `plot_figure_2_8.py` |
| H2.9 | Silver, OD, hotspot, shapefile và audit | Sơ đồ kết quả chuẩn bị đầu vào | Word đầu vào |
| H3.1–3.2 | Luồng ETL/ND2/ND3/Gold và Compose | Sơ đồ kiến trúc, cấu hình Spark Standalone | `scripts/chapter_03/plot_chapter3_architecture.py`, PNG trong `docs/assets/chapter3/` |
| H3.3 | Silver tháng 01, artifact P95 | Tần suất chuyến xếp theo zone, ngưỡng P95 | `plot_figure_3_3_pickup_p95.py`, `data/benchmarks/figure_3_3_pickup_p95_hotspots.png` |
| B3.1–3.2 | Runner ND2/ND3, cấu hình, E1/E3 | Ma trận phương pháp và thiết kế đo | `run_benchmark_nd2.py`, `run_benchmark_nd3.py`, `benchmark_engine.py` |
| H3.4 | E8.GOLD | Bản đồ tĩnh hai khung giờ, thang màu P95 gộp | `build_spatial_gold.py`; `docs/assets/chapter3/figure_3_4_manhattan_density.png` |
| B4.A | F1–F8 trong [kiểm toán](02_evidence_audit.md) | Tóm tắt bằng chứng và phạm vi suy luận | Word và bản Markdown Chương 4 |
| B4.1, H4.1 | Compose, E1, E3.08–09, E4.01 | Hạn mức khai báo, số core thực cấp và heap theo phiên | H4.1 từ `docs/assets/chapter4/figure_4_1_spark_topology.png` |
| B4.2 | E1, E3 và mã runner | Ma trận biến kiểm soát, một hàng một nhánh | `benchmark_engine.py` và hai runner |
| B4.3, H4.2 | E1.01, `chapter4_evidence/nd2_january_summary.csv` | Median và min–max 3 lượt, giây, tỷ số và MiB | `build_chapter4_evidence.py`, `build_chapter4_figures.py` |
| B4.4–4.6, H4.3 | E1.02, E3.09 và `chapter4_evidence/nd3_january_summary.csv` | Jan và S3 ba nhánh, tách phiên. H4.3 dùng Jan, min–max 3 lượt | Hai script Chương 4 như dòng trên |
| B4.7–4.8 | E1.02/E3.08–09, E4.01 và `chapter4_evidence/od_key_preflight.csv` | Cơ chế đọc từ physical plan; hàng OD trước/sau explode | Runner ND3 và audit OD |
| B4.9–4.10 | `chapter4_evidence/duckdb_january_correctness.csv`, `official_run_validity_jan_may.csv` | Sai khác trên nhóm trong audit cấp phương án; số dòng run collector/audit hợp lệ | `build_chapter4_evidence.py` |
| B4.11 | Parquet Raw/Silver tháng 01–05 | Số hàng, MiB và ngày theo scope | Hình 3.4: `notebooks/chapter_03/04_chapter3_artifact_checks.ipynb`; bảng: `build_chapter4_remaining.py`. Hình lịch sử được tạo bởi `plot_figure_3_4_data_scale.py` trước khi chuyển vào notebook. |
| B4.12, H4.4 | E7.03–04 | Clean Airflow, median và min–max ba lượt, giây | `build_nd4_march_artifacts.py`, ảnh tập trung `docs/assets/chapter4/` |
| B4.13 | E7.05–08 | Hai repeat Airflow, median và min–max từng phiên | CSV cấp lượt và `02_canonical_method_summary.csv` |
| H4.5–4.6 | E1 và E2.01–08 | Median, min–max 3 lượt/tháng/phương án, giây. Tháng 01 chạy lại 03/11, tháng 02–05 đo 30/10 | `build_chapter4_remaining.py` |
| B4.14 | E3.02, E3.05–07, E3.09 | Pilot S1/S2 và official S3. Median riêng từng phiên | `analyze_nd3_scaling_evidence.py` và sổ thực nghiệm |
| B4.15, H4.7 | E8.GOLD, `chapter4_evidence/spatial_group_density_summary.csv` | Mật độ trung vị theo zone và khung giờ; H4.7 là biểu đồ cột | `build_chapter4_remaining.py` |
| B4.16, H4.8 | E3.08 | Cùng phiên S3 4 nhánh, median và min–max ba lượt | `docs/assets/chapter4/figure_4_8_s3_auto_strategy.png` và nguồn phiên trong registry |
| B4.17 | E4.01, `task_work_by_run.csv` | Median/min–max theo phương án, tỷ số ghép cặp/KTC, tổng công việc theo lượt | `analyze_phase_c_paired.py`, `extract_phase_c_task_work.py` |
| H4.9 | E5.01–12 | Ô Zipf × dung lượng Parquet, khoảng bootstrap với biên 5%, ký hiệu ô giữ lại | `analyze_phase_c_grid.py` |
| B4.18 | E6.01–05 | Median ba lượt một phương án theo cấu hình | CSV cấp lượt và `02_canonical_method_summary.csv` |
| B4.19 | E4.01 và E5.01–12 | Byte theo loại số đo, hàng, kế hoạch và accumulator | `extract_phase_c_broadcast.py` |
| B4.20 | E5, manifest v3 và `evidence/heldout_regret.csv` | Median oracle trong mỗi ô, tỷ số, regret | `analyze_phase_c_grid.py` |
| B4.21 | F1–F7 | Ma trận lựa chọn theo tín hiệu workload | Tổng hợp trong báo cáo |
| B5.1, H5.1 | E8.GOLD | Ví dụ đối chiếu hai zone, ảnh Kepler.gl 3D do tác giả cung cấp | `scripts/chapter_05/build_nd5_explorer.py`; `docs/assets/chapter5/figure_5_1_kepler_manhattan_3d.png` |
