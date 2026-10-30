# README --- Chạy Spark Cluster + Airflow trên Fedora (Podman)

Tài liệu ghi lại cách khởi động, kiểm tra và chạy smoke test cho project
`taxi-data-baker`.

> **Trạng thái đã xác nhận:** Spark 3.5.9 Standalone Master + 2
> Workers + Driver chạy được; bài test `pi.py` đã phân phối task tới cả
> `spark-worker-1` và `spark-worker-2`, hoàn thành với exit code 0.\
> **Lưu ý:** Đây mới là kiểm tra hạ tầng, chưa chứng minh benchmark dữ
> liệu taxi chạy đúng hoặc kết quả benchmark đã hợp lệ.

## 1. Thông tin môi trường

-   Hệ điều hành: Fedora Linux
-   Container runtime: Podman
-   Compose file: `docker-compose.yml`
-   Spark: `3.5.9-scala2.12-java17-python3-ubuntu`
-   Spark Master URL: `spark://spark-master:7077`
-   Spark Master UI: `http://localhost:8080`
-   Worker 1 UI: `http://localhost:8081`
-   Worker 2 UI: `http://localhost:8082`
-   Spark Driver UI mặc định: `http://localhost:4040` (chỉ hiện khi ứng
    dụng Spark đang chạy)
-   Airflow UI: `http://localhost:8085`

Thư mục project hiện dùng:

``` bash
cd ~/SEM5/Bigdata/taxi-data-baker
```

Nếu chuyển project sang vị trí khác, thay đường dẫn trên bằng đường dẫn
thật.

## 2. Khởi động project

Chạy các lệnh tại thư mục có file `docker-compose.yml`.

``` bash
cd ~/SEM5/Bigdata/taxi-data-baker
podman-compose up -d
```

Kiểm tra các container:

``` bash
podman ps -a
```

Xem trạng thái health của Master:

``` bash
podman inspect spark-master --format '{{json .State.Health}}'
```

Xem log Master và Workers:

``` bash
podman logs --tail 100 spark-master
podman logs --tail 100 spark-worker-1
podman logs --tail 100 spark-worker-2
```

Trong log của từng Worker, cần thấy thông báo tương tự:

``` text
Successfully registered with master spark://spark-master:7077
```

Mở Spark Master UI tại `http://localhost:8080`. Kiểm tra Master đang
sống và có **2 Workers** đã đăng ký. Mỗi Worker hiện được cấu hình 2
cores và khoảng 2 GiB RAM theo log đã kiểm tra.

## 3. Chạy smoke test phân tán (Pi example)

Chạy từ container Driver:

``` bash
podman exec spark-driver /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-driver \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=512m \
  --conf spark.cores.max=2 \
  /opt/spark/examples/src/main/python/pi.py 10
```

Kết quả mong đợi: - Spark báo phiên bản `3.5.9`. - Ứng dụng có
application ID. - Executor được cấp trên cả `spark-worker-1` và
`spark-worker-2`. - Các task hoàn tất và cuối output có dòng gần như
`Pi is roughly 3.14...`. - SparkContext dừng bình thường, exit code 0.

Nếu chỉ thấy job chạy ở Driver hoặc Worker chưa đăng ký, xem log
Master/Workers và kiểm tra Master UI trước khi tiếp tục.

## 4. Kiểm tra các thư mục được mount

Compose đang mount các thư mục project vào container như sau:

  Thư mục trên host       Đường dẫn trong container
  ----------------------- -----------------------------------------
  `./data`                `/opt/spark/data`
  `./scripts`             `/opt/spark/scripts`
  `./notebooks`           `/opt/spark/notebooks`
  `./logs/spark-events`   `/opt/spark/work-dir/logs/spark-events`
  `./dags`                `/opt/airflow/dags`
  `./logs/airflow`        `/opt/airflow/logs`

Kiểm tra mount từ Driver:

``` bash
podman exec spark-driver ls -la /opt/spark/data
podman exec spark-driver ls -la /opt/spark/scripts
podman exec spark-driver ls -la /opt/spark/notebooks
```

Nếu cần kiểm tra Airflow DAGs:

``` bash
podman exec airflow-standalone ls -la /opt/airflow/dags
```

Tên thư mục hoặc file bên trong có thể khác tùy trạng thái repo thực tế.

## 5. Kiểm tra Python dependencies trong Driver

Chạy:

``` bash
podman exec spark-driver python3 -c "import pyspark; print('pyspark:', pyspark.__version__)"
podman exec spark-driver python3 -c "import pandas, numpy, duckdb; print('pandas/numpy/duckdb: OK')"
```

Lệnh thứ hai chỉ thành công nếu cả ba package đã được cài trong image.
Nếu thiếu package, **đừng chỉ cài thủ công trong container đang chạy**
nếu muốn môi trường tái lập được. Hãy bổ sung dependencies vào
image/build definition hoặc quy trình setup có version pin, rồi rebuild
và khởi động lại.

## 6. Xem log và dừng project

Theo dõi log:

``` bash
podman-compose logs -f spark-master
podman-compose logs -f spark-worker-1
podman-compose logs -f spark-worker-2
podman-compose logs -f spark-driver
podman-compose logs -f airflow-standalone
```

Dừng các service nhưng giữ container/volume theo cấu hình Compose:

``` bash
podman-compose stop
```

Khởi động lại:

``` bash
podman-compose start
```

Tắt và xóa các container/network do Compose quản lý:

``` bash
podman-compose down
```

`down` không đồng nghĩa xóa thư mục dữ liệu trên host. Tuy nhiên, trước
khi dùng các lệnh xóa volume hoặc dọn dữ liệu, hãy kiểm tra Compose file
và backup dữ liệu cần giữ.

## 7. Thứ tự làm việc đề xuất cho benchmark taxi

1.  Khởi động cluster và xác nhận Master có 2 Workers.
2.  Chạy Pi smoke test để xác nhận Spark phân phối task.
3.  Kiểm tra dependencies và các mount dữ liệu/script.
4.  Xác nhận event logging được bật và thư mục `logs/spark-events` có
    thể ghi.
5.  Chạy benchmark-engine smoke test với dữ liệu nhỏ.
6.  Chạy aggregation benchmark: Baseline, Static Salting và
    Heuristic/Selective Salting.
7.  Chạy join benchmark: Baseline Sort-Merge Join, AQE và Salting Join.
8.  Kiểm tra tính đúng đắn: đối chiếu row count và tổng các trường
    tiền/fare với Baseline (ví dụ bằng DuckDB theo pipeline của
    project).
9.  Sau khi correctness đạt mới so sánh duration, task skew ratio,
    shuffle read/write và disk spill.

**Không được kết luận salting/AQE tốt hơn chỉ từ một lần chạy hoặc từ Pi
smoke test.** Ghi lại cấu hình, input size, event logs và metrics cho
từng lần benchmark.

## 8. Troubleshooting nhanh

### Master chưa healthy

``` bash
podman logs --tail 200 spark-master
podman inspect spark-master --format '{{json .State.Health}}'
```

### Worker không đăng ký được

``` bash
podman logs --tail 200 spark-worker-1
podman logs --tail 200 spark-worker-2
```

Kiểm tra Worker dùng đúng master URL `spark://spark-master:7077` và các
service cùng network Compose.

### Spark submit không kết nối được Driver

Giữ các cấu hình mạng đã dùng trong smoke test:

``` text
spark.driver.host=spark-driver
spark.driver.bindAddress=0.0.0.0
```

Đồng thời kiểm tra container `spark-driver` đang chạy và các service
phân giải được hostname trong Compose network.

### Cổng host bị chiếm

Kiểm tra:

``` bash
podman ps --format '{{.Names}}  {{.Ports}}'
```

Nếu thay port mapping trong `docker-compose.yml`, cập nhật URL UI trong
README này cho khớp.

------------------------------------------------------------------------

## Checklist trước khi bắt đầu phiên làm việc

-   [ ] Đang ở đúng thư mục `taxi-data-baker`
-   [ ] `podman-compose up -d` thành công
-   [ ] `spark-master` healthy
-   [ ] Cả hai Workers đã đăng ký trong Master UI
-   [ ] Pi smoke test chạy thành công trên cả hai Workers
-   [ ] Dữ liệu và scripts hiện diện trong Driver
-   [ ] Dependencies cần thiết đã được xác nhận
-   [ ] Event logging và quyền ghi log đã được kiểm tra
-   [ ] Chỉ bắt đầu benchmark chính thức sau khi test correctness
