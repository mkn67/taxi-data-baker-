# Airflow DAGs

`chapter4_march_benchmark.py` triển khai Nội dung 4 cho tháng 03/2026. DAG có lượt xác nhận sạch ngày 20/11/2026 với run ID `nd4-march-clean-20261120`; ba task ND2, ND3 và đóng gói đều ở trạng thái `success`. ND2 và ND3 chạy tuần tự để tránh tranh tài nguyên; mỗi runner có một warm-up, ba lượt đo, REST/Event Log telemetry, run metadata và DuckDB correctness audit.

Run CSV, summary CSV, audit, execution plans, bảng tổng hợp, biểu đồ và provenance JSON được lưu tách biệt tại `data/benchmarks/nd4_airflow/2026_03/nd4-march-clean-20261120/`. Một lượt chạy trước khi image được làm mới đã phải chạy lại thủ công bước đóng gói do thiếu Matplotlib. Lượt `nd4-march-clean-20261120` sau đó chạy cả DAG tự động, không cần chạy task thủ công.

Airflow dùng image tùy chỉnh trong `Dockerfile.airflow`, với Java 17 và PySpark 3.5.9; volume scripts là read-only, data và Spark event logs được chia sẻ. Lượt xác nhận sạch sau khi cập nhật image đã hoàn thành đủ ba task tự động.
