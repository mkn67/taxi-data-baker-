# Scripts Chương 4 — Benchmark và tổng hợp kết quả

Thư mục gồm cả DAG orchestration và các script tổng hợp.

## ND4 Airflow benchmark tháng 03/2026

DAG `dags/chapter4_march_benchmark.py` chạy ND2 rồi ND3 tuần tự trên Spark Standalone. Mỗi runner dùng một warm-up, ba lượt đo và đối soát DuckDB. Lượt xác nhận sạch `nd4-march-clean-20261120` chạy thành công cả ba task ngày 20/11/2026; kết quả nằm riêng ở `data/benchmarks/nd4_airflow/2026_03/nd4-march-clean-20261120/`.

Hai phiên lặp cùng giao thức warm-up ngày 22/11/2026 là `nd4-march-repeat-20261122` và `nd4-march-repeat2-20261122`; xem `chapters/04_results_tradeoffs/nd4_reproducibility_20261122.md`. Lượt 20/11 dùng runner ND3 cũ chưa thực hiện action warm-up, nên được báo riêng.

Có thể đóng gói lại bảng, hình và provenance cho một run đã hoàn thành bằng:

```bash
python scripts/chapter_04/build_nd4_march_artifacts.py --airflow-run-id <dag-run-id> --run-dir data/benchmarks/nd4_airflow/2026_03/<dag-run-id>
```

Script xác nhận đủ 9 dòng mỗi bài toán, ba lượt/phương án, collector `OK`, validation `PASS`, Spark 3.5.9 và hai Worker trước khi tạo các tệp tổng hợp.
Hình của từng phiên được giữ trong thư mục run. Chỉ thêm `--publish-figure` khi muốn chủ động thay Hình 4.4 của bản thảo bằng hình của phiên đó.

## Tổng hợp các kết quả official đã lưu

```bash
python scripts/chapter_04/build_chapter4_evidence.py
python scripts/chapter_04/build_chapter4_figures.py
python scripts/chapter_04/build_chapter4_remaining.py
```

- `build_chapter4_evidence.py`: bảng ND2/ND3 tháng 01, correctness DuckDB, OD key preflight và trạng thái benchmark theo tháng.
- `build_chapter4_figures.py`: Hình 4.2 và 4.3 từ CSV cấp lượt đo tháng 01.
- `build_chapter4_remaining.py`: kiểm tra benchmark official theo tháng, bảng ổn định và kết quả không gian.
- Bảng tổng hợp Chương 4 nằm ở `data/benchmarks/chapter4_evidence/`; hình bản thảo ở `docs/assets/chapter4/`.
- Bản thảo: `chapters/04_results_tradeoffs/chapter4_results.md`.

Các script `build_chapter4_*` chỉ xử lý evidence đã lưu; chúng không thay thế một lần Spark benchmark mới.

## Chạy cụm

```bash
podman compose up -d --build spark-master spark-worker-1 spark-worker-2 airflow-standalone
podman exec airflow-standalone airflow dags unpause chapter4_nd4_march_2026_benchmark
podman exec airflow-standalone airflow dags trigger chapter4_nd4_march_2026_benchmark
```

Cụm host fallback có tại `scripts/chapter_04/start_spark_host_fallback.sh`, nhưng không áp giới hạn cgroups Compose và không được gắn nhãn official.
