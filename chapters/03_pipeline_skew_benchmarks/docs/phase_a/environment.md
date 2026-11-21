# Phase A — môi trường quan sát

| Thành phần | Bằng chứng / giá trị |
|---|---|
| Repo Spark | `/home/kedora/SEM5/Bigdata/taxi-data-baker-` |
| Python trong `.venv` | Có PySpark 3.5.9 và DuckDB 1.5.6; import thành công khi kiểm tra |
| Python hệ thống | Không có `pyspark`/`duckdb`; vì vậy phải dùng `.venv` hoặc image dự án |
| Host CPU/RAM | Intel Core i5-1340P; 16 logical CPUs; `free -h` báo 15 GiB RAM tổng |
| Host Java | OpenJDK 25.0.5; không dùng thông số host này để suy ra Java trong container Spark |
| Compose | Image `localhost/taxi-spark:3.5.9-py310`; Master 1 CPU/1024 MiB; Worker, Driver và Airflow có các giới hạn riêng trong `docker-compose.yml` |
| Benchmark chính | CSV ND2/ND3 tháng 01 ghi Spark 3.5.9, application IDs `app-20261103153557-0001` và `app-20261103153727-0002`, collector OK và audit PASS |
| Telemetry chưa có | Không có chuỗi `cpu.stat`/`memory.peak` gắn từng application; giới hạn Compose không phải số đo sử dụng |

Các giá trị host là trạng thái tại thời điểm kiểm tra; bảng tài nguyên báo cáo Chương 4 lấy từ cấu hình Compose và metadata của đúng lượt chạy, không lấy từ trạng thái host hiện tại.
