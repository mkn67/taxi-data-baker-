# Taxi Data Baker — Pipeline dữ liệu và benchmark Spark

Project xây dựng pipeline xử lý dữ liệu chuyến xe HVFHV của New York, đánh giá một số cách xử lý lệch tải trên Apache Spark, và tạo dữ liệu không gian mô tả điểm trả khách ở Manhattan. README này tóm tắt phạm vi ba chương đầu, trạng thái artifact và cách chạy các thành phần chính.

## Tổng quan đề tài

Đề tài tập trung vào hai nhóm công việc:

1. **Benchmark lệch tải trên Spark:** ND2 tổng hợp chuyến theo vùng đón (`PULocationID`); ND3 nối dữ liệu chuyến với OD dimension theo cặp vùng đón–trả. Các phương án được so sánh gồm Baseline, Selective Salting, Uniform Salting cho ND2, và Baseline Sort-Merge Join, Spark AQE, Salting Join cho ND3.
2. **Phân tích mật độ trả khách:** tổng hợp điểm trả theo Taxi Zone và khung giờ Ngày/Đêm, tính chuyến/km² từ hình học vùng và xuất Gold cùng bản đồ Kepler.gl.

Pipeline tiền xử lý tạo Silver Parquet từ dữ liệu Raw. Spark thực hiện các benchmark phân tán; DuckDB đối soát đầu ra độc lập; Spark Event Logs và REST API cung cấp task/stage metrics và executor evidence. Dữ liệu chuyến cấp dòng không được gom về driver để tính kết quả benchmark; chỉ các bảng tổng hợp nhỏ được dùng cho đối chiếu.

Airflow service có trong Compose nhưng thư mục `dags/` hiện chưa có DAG; các pipeline và benchmark hiện được gọi bằng scripts/CLI.

### Phạm vi dữ liệu và kết quả

- Benchmark chính thức dùng Spark 3.5.9 Standalone trên Podman Compose, gồm một Master và hai Worker. Mỗi phương án có một warm-up và ba lượt đo; cấu hình chính dùng 200 shuffle partitions.
- Benchmark ND2/ND3 tháng 01–05/2026 chạy trên **năm tập Silver tháng độc lập**, không phải dữ liệu tích lũy. Quy mô Silver trong các tháng khoảng 19,0–21,1 triệu dòng, nên đây là phép so sánh giữa các input tháng gần kích thước, không phải bằng chứng scaling trên dải quy mô lớn.
- Run CSV chính thức hiện có 90/90 lượt ND2/ND3 với collector `OK` và DuckDB validation `PASS`: mỗi benchmark có 3 phương án × 3 lượt × 5 tháng = 45 lượt, tổng hai benchmark là 90. Xem run-level CSV và summary trong `data/benchmarks/` để đối chiếu theo tháng/phương pháp.
- Kết quả ND2 cho thấy salting có thể giảm Shuffle Write so với Uniform Salting nhưng các phương án salting mất thêm thời gian so với baseline trong workload đã đo. Với ND3, AQE giảm TSR trong các run nhưng final plan không cho thấy skew-specific partition split; các run AQE có spill. Riêng benchmark tháng 01, Salting Join materialize OD dimension từ 58.747 lên 83.646 hàng, tăng 24.899 hàng.
- Spatial Gold dùng Silver tháng 01/2026. Toàn bộ 263 zone được phân loại thành 41 zone phía Nam, 23 phía Bắc, 5 vùng mơ hồ và 194 ngoài Manhattan. Đây là nhóm phân tích theo đường 60th Street xấp xỉ, không phải lớp phân loại CRZ pháp lý và không chứng minh quan hệ nhân quả.

Artifact kết quả chính:

- Summary/run/audit benchmark: `data/benchmarks/` và `data/benchmarks/scaling/`.
- Spatial Gold: `data/gold/dropoff_density_by_zone_period.parquet` và `.csv`.
- Kepler.gl: `data/gold/dropoff_density_by_zone_period.gl`.
- Hình density: `data/benchmarks/figure_3_1_dropoff_density.png`.

Các kết luận benchmark chỉ áp dụng cho workload, dữ liệu, cấu hình và môi trường đã đo. Project chưa có mô hình LightGBM, SparkListener tùy biến hay collector đo mức sử dụng CPU/RAM cgroup theo từng run; không xem các mục này là kết quả đã triển khai.

### Tình trạng nội dung theo chương

| Chương | Nội dung | Trạng thái trong project |
|---|---|---|
| 1 | Tổng quan đề tài, cơ sở khoa học và khảo sát dữ liệu | Có chỉ mục artifact; bản thảo hiện nằm trong DOCX tổng hợp, chưa được lưu trong repository. |
| 2 | Tiền xử lý, kiểm định dữ liệu, Silver, hotspot và phạm vi benchmark tháng 01 | Có scripts/data/audit; bản thảo hiện nằm trong DOCX tổng hợp. Pilot là chế độ chạy thử trên Silver tháng 01, không phải tập 7 ngày. |
| 3 | Pipeline, ND2/ND3, phương pháp benchmark và spatial methodology | Có bản thảo Markdown, notebooks, run artifacts, Gold và Kepler artifact. |
| 4 | Kết quả thực nghiệm và phân tích đánh đổi | Có [kế hoạch viết](chapters/04_results_tradeoffs/plan_chuong_4.md); chưa phải chương hoàn chỉnh. |
| 5 | Kết luận và hướng phát triển | Có README chỉ mục; chưa phải chương hoàn chỉnh. |

## Tài liệu và mã nguồn theo chương

Thư mục `chapters/` chia tài liệu theo Chương 1–5. Mã và notebook được xếp
trong `scripts/chapter_02/`, `scripts/chapter_03/` và `notebooks/chapter_03/`;
dữ liệu vẫn giữ nguyên trong `data/`. Compose mount nguyên thư mục `scripts/`
và `notebooks/`, nên các lệnh chạy dùng đường dẫn lồng theo chương. Bắt đầu từ
[chapters/README.md](chapters/README.md), [scripts/README.md](scripts/README.md)
và [notebooks/README.md](notebooks/README.md).

> **Trạng thái đã xác nhận:** Spark 3.5.9 Standalone Master + 2 Workers + Driver đã chạy được. Ngoài smoke test hạ tầng, các benchmark ND2/ND3 official cũng có worker evidence cho cả hai Worker và DuckDB validation `PASS`. Các kết luận cụ thể phải được truy về run CSV/audit, không suy từ smoke test.

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
cd ~/SEM5/Bigdata/taxi-data-baker-
```

Nếu chuyển project sang vị trí khác, thay đường dẫn trên bằng đường dẫn
thật.

## 2. Khởi động project

Chạy các lệnh tại thư mục có file `docker-compose.yml`.

``` bash
cd ~/SEM5/Bigdata/taxi-data-baker-
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
5.  Chạy pilot ND2/ND3 trên Silver tháng 01 để kiểm tra chức năng; pilot
    chỉ giảm số lượt đo, không dùng tập dữ liệu nhỏ hơn.
6.  Chạy aggregation benchmark: Baseline GroupBy, Uniform Salting S=8
    và Selective Salting S=8.
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

-   [ ] Đang ở đúng thư mục `taxi-data-baker-`
-   [ ] `podman-compose up -d` thành công
-   [ ] `spark-master` healthy
-   [ ] Cả hai Workers đã đăng ký trong Master UI
-   [ ] Pi smoke test chạy thành công trên cả hai Workers
-   [ ] Dữ liệu và scripts hiện diện trong Driver
-   [ ] Dependencies cần thiết đã được xác nhận
-   [ ] Event logging và quyền ghi log đã được kiểm tra
-   [ ] Chỉ bắt đầu benchmark chính thức sau khi test correctness

## 9. Cấu trúc chương và cách chạy pipeline

Các hướng dẫn chi tiết theo thư mục:

- [Chỉ mục tài liệu Chương 1–5](chapters/README.md)
- [Scripts và lệnh chạy](scripts/README.md)
- [Notebook](notebooks/README.md)
- [Chương 3: nội dung, nguồn hình và artifact](chapters/03_pipeline_skew_benchmarks/README.md)

### Tiền xử lý dữ liệu tháng 01/2026

Chạy từ project root trong môi trường đã cài dependencies Spark:

```bash
python scripts/chapter_02/02_preprocess_and_audit.py
```

Script đọc Raw tháng 01, áp dụng bộ lọc, ghi Silver và tạo hotspot P95. Trước khi chạy, kiểm tra đường dẫn đầu vào/đầu ra và cấu hình Spark trong script hoặc biến môi trường mà script hỗ trợ. Không chạy lại trên thư mục kết quả cần giữ nếu chưa sao lưu.

### Benchmark ND2 và ND3

Chạy thử chức năng (một lượt đo):

```bash
python scripts/chapter_03/run_benchmark_nd2.py --mode pilot
python scripts/chapter_03/run_benchmark_nd3.py --mode pilot
```

Chạy benchmark đầy đủ trên Spark Standalone (một warm-up và ba lượt đo):

```bash
python scripts/chapter_03/run_benchmark_nd2.py --mode full --master spark://spark-master:7077
python scripts/chapter_03/run_benchmark_nd3.py --mode full --master spark://spark-master:7077
```

Trong container Driver, dùng các đường dẫn `/opt/spark/scripts/...` và đảm bảo Silver, hotspot/OD dimension được mount tại `/opt/spark/data`. Pilot/full là chế độ số lượt chạy, không phải hai kích thước dữ liệu. Chạy lệnh từ container chỉ khi dependencies, volume mount và Spark network đã được kiểm tra.

### Benchmark qua các tháng độc lập

Các benchmark tháng 02–05 được điều phối riêng; tháng 01 là benchmark official chính. Mỗi tháng là một input độc lập, không gộp thành tập tích lũy và không gọi đây là scaling trên dải quy mô lớn:

```bash
bash scripts/chapter_03/run_monthly_scaling_benchmarks.sh
```

Đọc README của Chương 3 và kiểm tra tham số/script trước khi chạy vì bước này tạo dữ liệu và benchmark cho nhiều tháng, tốn thời gian và tài nguyên. Chỉ chạy lại khi cần tái tạo artifact; các tháng là input độc lập, không diễn giải như một tập dữ liệu tích lũy hay mức tăng quy mô lớn.

### Tạo hình Chương 3 và spatial Gold

```bash
python scripts/chapter_03/plot_chapter3_architecture.py
python scripts/chapter_03/plot_figure_3_3_pickup_p95.py
python scripts/chapter_03/03_build_gold_spatial_analysis.py
python scripts/chapter_03/build_kepler_spatial_artifact.py
```

Kiểm tra biến môi trường/đường dẫn trong từng script trước khi chạy. Spatial builder ghi Gold và audit; Kepler builder tạo file `.gl` từ Gold hiện có.
