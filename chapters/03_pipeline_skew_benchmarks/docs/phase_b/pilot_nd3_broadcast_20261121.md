# Pilot ND3 trên Spark Standalone — 21/11/2026

## Phạm vi và thay đổi mã

Đây là bước thử tiếp theo sau quyết định `GO thu hẹp` của Phase A. Cụm Podman Compose đã chạy với một Spark Master, hai Worker, một driver và Airflow. Master REST báo hai Worker sống và bốn core. Runner ND3 được sửa để mỗi warm-up **thực sự gọi action `collect()`**; trước đó nó chỉ dựng DataFrame nên không thực hiện warm-up. Event Log ND3 official tháng 01–05 và ND4 tháng 03 cũ có 0 job warm-up; Event Log pilot mới có job warm-up cho cả bốn nhánh. Một nhánh `Broadcast_Hash_Join_Reference` được thêm bằng `F.broadcast(df_od)` và chỉ bật qua `--include-broadcast` trong chế độ pilot. Ba nhánh official giữ nguyên định nghĩa và không bị ghi đè.

Lệnh chạy pilot bốn nhánh với ba lượt đo luân phiên:

```bash
podman exec spark-driver env \
  BENCHMARKS_DIR=/opt/spark/data/benchmarks/phase_b_broadcast_rotated_20261121 \
  SPARK_MASTER_URL=spark://spark-master:7077 \
  SPARK_EXPECTED_VERSION=3.5.9 \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --conf spark.driver.host=spark-driver \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=1500m \
  --conf spark.cores.max=4 \
  /opt/spark/scripts/chapter_03/run_benchmark_nd3.py \
  --mode pilot --master spark://spark-master:7077 \
  --partitions 200 --include-broadcast --pilot-iterations 3
```

## Đầu vào và provenance

- Silver tháng 01/2026: `data/silver/fhvhv_2026_01_clean.parquet/`; manifest SHA-256 `273460456aa6e443af5ee4b29b6bad5783898264bee5eebfcce7fe0a082b1242`.
- OD cùng tháng: `data/silver/od_matrix_2026_01.parquet`; manifest SHA-256 `9f19e90ba8b12f9826ca3eb0caa582d7b37dafb5fa2510ad94cdae0e7eb2b2cc`. Parquet nén trên đĩa là 873.323 byte; đây **không** phải số byte bộ nhớ sau broadcast.
- Spark 3.5.9; ứng dụng `app-20261121084242-0002`; một action warm-up và ba lượt đo cho mỗi phương án. Event Log có job trong cả bốn nhóm warm-up. Run CSV ghi executor ở cả `spark-worker-1` và `spark-worker-2` cho 12 lượt. Đây là các container trên cùng một host.
- Đầu ra nằm riêng tại `data/benchmarks/phase_b_broadcast_rotated_20261121/`: run CSV, summary CSV, DuckDB audit CSV và `nd3_plans/pilot_standalone/`. Lượt pilot một vòng trước đó nằm ở `data/benchmarks/phase_b_broadcast_pilot_20261121/` (ứng dụng `app-20261121083430-0001`); không gộp hai phiên để tính trung vị. Các tệp `official_standalone` không được thay thế.

## Kết quả quan sát

| Phương án | Wall-time trung vị [nhỏ nhất–lớn nhất] (giây) | TSR trung vị | PSR trung vị | Shuffle Read trung vị (MiB) | Disk Spill (MiB) | Task của stage được chọn |
|---|---:|---:|---:|---:|---:|---:|
| Baseline SMJ | 9,658 [8,961–9,802] | 2,19 | 2,06 | 295,72 | 0 | 200 |
| Spark AQE | 9,639 [9,455–9,680] | 1,09 | 1,00 | 295,22 | 0 | 5 |
| Salting Join S=8 | 10,781 [10,622–10,942] | 1,53 | 1,60 | 326,47 | 0 | 200 |
| Broadcast Hash Join tham chiếu | 2,403 [2,368–2,710] | 1,07 | NA | 0,01 | 0 | 4 |

Nguồn: `benchmark_nd3_join_runs_pilot_standalone.csv` trong thư mục pilot ba lượt. `collector_status=OK` và `validation_status=PASS` cho cả 12 lượt; 12 final plan được lưu. DuckDB đối soát mỗi phương án với 19.993.097 chuyến; audit lưu 32 cặp borough cho mỗi phương án. OD có 58.747 hàng trước salting và 83.646 hàng sau explode, thêm 24.899 hàng; số đếm sau materialize khớp kỳ vọng.

Final plan của nhánh mới chứa `BroadcastHashJoin ... BuildRight` và `BroadcastExchange`; ba nhánh kia lần lượt dùng SMJ, SMJ với AQE và SMJ theo khóa có salt. Broadcast được **ép bằng hint**; CSV vẫn ghi `spark.sql.autoBroadcastJoinThreshold=-1` do nhánh đối chứng trước đó. Vì vậy, phép đo chứng minh Broadcast Hash Join chạy đúng và nhanh trong pilot này, **không** chứng minh Spark sẽ tự chọn Broadcast theo cấu hình mặc định.

## Diễn giải và giới hạn

Trung vị 2,403 giây của Broadcast thấp hơn 9,639–10,781 giây của các nhánh shuffle trong phiên pilot này. Baseline SMJ là đối chứng được ép để nghiên cứu lệch tải, chưa phải mốc triển khai nhanh nhất cho OD nhỏ của tháng 01. Ba lượt được xoay thứ tự theo vòng bốn vị trí, nhưng còn ít và cùng một phiên chạy; chưa có khoảng tin cậy đáng tin cậy hay bằng chứng độ bền qua nhiều phiên. Số task của stage được chọn khác nhau mạnh giữa các phương án, nên không dùng TSR/PSR để xếp hạng cân bằng tải giữa Broadcast, AQE và SMJ. Shuffle Read của Broadcast vẫn có một phần nhỏ từ bước tổng hợp kết quả theo borough. Không có đo CPU, mức dùng bộ nhớ thực tế của broadcast hay cgroup gắn từng job.

**Bước tiếp theo theo plan:** đo byte và bộ nhớ phía OD sau projection đủ để đánh giá mức khả thi của Broadcast; lặp ở phiên khác nếu cần xác nhận độ ổn định, rồi mới quyết định có mở S2 tích lũy 01–03 hay không. Silver của cả ba tháng đã có, nhưng runner hiện đọc một thư mục Silver và kiểm tra nhãn tháng trên đường dẫn. Không thể dùng OD/hotspot tháng 01 cho S2: cần tạo đầu vào tích lũy có manifest, dựng OD và phát hiện P95 từ chính tập 01–03, rồi đối soát số dòng trước khi benchmark. Chưa đưa các số pilot này vào bảng chính thức Chương 4.
