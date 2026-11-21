# S2 tích lũy 01–03/2026 — đầu vào và pilot ND3

## Tạo đầu vào đúng phạm vi

`scripts/chapter_03/prepare_cumulative_s2.py` đọc ba Silver tháng 01, 02, 03, ghi một Silver tích lũy **mới** ở `data/benchmarks/scaling/s2_jan_mar/silver/`, rồi dựng OD Matrix và hotspot pickup P95 nearest-rank từ chính tập tích lũy đó. Ba thư mục Silver nguồn không bị thay đổi. `s2_input_manifest.json` ghi danh sách Parquet nguồn/đích, kích thước, thời điểm sửa, số dòng, Spark application ID, ngưỡng P95 và tập hotspot. Manifest này nhận diện tệp, không phải checksum nội dung từng tệp.

Đối soát nguồn: tháng 01 **19.993.097**, tháng 02 **19.045.927**, tháng 03 **21.094.362** dòng Silver; tổng S2 **60.133.386** dòng, bằng số dòng đọc lại từ Silver tích lũy. OD S2 có **62.223** khóa và **0** nhóm khóa null/trùng. P95 có hạng 248 trên 261 pickup zone, ngưỡng **580.504** chuyến và 14 hotspot. Ứng dụng chuẩn bị dữ liệu: `app-20261121085258-0003`.

## Benchmark pilot

Runner ND3 đọc ba đường dẫn S2 riêng (`SILVER_PATH`, `OD_MATRIX_PATH`, `HOTSPOTS_PU_PATH`) và ghi vào `data/benchmarks/scaling/s2_jan_mar/nd3_pilot/`. Cấu hình: Spark 3.5.9 Standalone, hai Worker cùng host, 200 shuffle partitions, một action warm-up và ba lượt đo xoay thứ tự cho bốn phương án. Ứng dụng `app-20261121085410-0004`. Manifest SHA-256 trong run CSV: Silver `d388d7e52aa55fd63dce11026a0dbd6609b7926118aad0122bc239fc16445075`; OD `a56baf9e34dd3d97dfa2eac3e49a8374e6bfe7a3d3e6e583c1c1ae3a0237ae3b`.

| Phương án | Wall-time trung vị [nhỏ nhất–lớn nhất] (giây) | Shuffle Read trung vị (MiB) | TSR ba lượt | Disk Spill ba lượt (MiB) |
|---|---:|---:|---|---|
| Baseline SMJ | 28,298 [25,507–40,980] | 888,10 | 1,92 / 2,23 / 2,01 | 0 / 0 / 0 |
| Spark AQE | 25,801 [25,502–41,974] | 887,63 | 1,14 / 1,16 / 1,20 | 0 / 0 / 0 |
| Salting Join S=8 | 29,668 [29,587–35,816] | 979,51 | 1,48 / 1,93 / 6,05 | 0 / 0 / 0 |
| Broadcast Hash Join tham chiếu | 6,114 [5,878–10,505] | 0,04 | 1,17 / 1,29 / 1,38 | 0 / 0 / 0 |

Nguồn: `benchmark_nd3_join_runs_pilot_standalone.csv`. **12/12** lượt có `collector_status=OK`, `validation_status=PASS`, executor ở cả `spark-worker-1`/`spark-worker-2`; 12 final plans được lưu. DuckDB đối soát mỗi phương án với **60.133.386** chuyến và 33 cặp borough. OD sau salting có 87.409 hàng, tăng 25.186 so với 62.223 hàng gốc; số đếm sau explode khớp kỳ vọng. Final plan AQE cho thấy coalescing, chưa thấy skew-specific split. Broadcast được ép bằng hint, nên không chứng minh Spark tự chọn chiến lược này.

## Giới hạn so sánh S1–S2

S1 pilot ba lượt có thứ tự trung vị Broadcast < AQE ≈ Baseline < Salting; S2 pilot cũng giữ thứ tự đó, nhưng khoảng thời gian Baseline/AQE trên S2 rộng và lượt ba chậm hơn nhiều. Salting S2 có một lượt TSR 6,05. Các lượt này được giữ vì telemetry và correctness đạt; chưa xác định nguyên nhân từ số liệu CPU/I/O/cgroup. S1 và S2 chạy ở **hai ứng dụng khác nhau trong một phiên trên cùng host**; ba lượt chưa đủ để ước lượng quy luật scaling hay kiểm định ưu thế ổn định. ND2 trên S2 đã chạy sau ND3, được trình bày ở bảng đối chiếu dưới đây; S3 chưa tạo. Không gộp pilot S2 vào chuỗi benchmark năm tháng độc lập trong Bảng 3.2.

## Đối chiếu pilot S1–S2 cho ND2 và ND3

ND2 S1 được chạy lại với runner hiện tại (`app-20261121091217-0006`), một action warm-up và ba lượt đo/phương án; ND2 S2 dùng ứng dụng `app-20261121090936-0005` với cùng cấu hình. Cả 9/9 lượt ND2 S2 có collector `OK`, DuckDB `PASS` và hai Worker; S1 cũng đạt audit tương ứng. Bảng dưới được tạo bằng `scripts/chapter_03/compare_s1_s2_pilots.py`, đầu ra `data/benchmarks/scaling/s2_jan_mar/s1_s2_pilot_comparison.csv`. Script yêu cầu mỗi phương án có đúng ba iteration, một application và một manifest ở mỗi mức, tier `pilot_standalone`, tất cả run `OK/PASS` và hai Worker.

| Bài toán | Phương án | S1 trung vị [min–max] (s) | S2 trung vị [min–max] (s) | S2/S1 trung vị |
|---|---|---:|---:|---:|
| ND2 | Baseline | 2,394 [1,996–2,539] | 3,646 [3,592–4,816] | 1,523 |
| ND2 | Selective S=8 | 5,284 [5,058–5,390] | 7,832 [7,492–8,493] | 1,482 |
| ND2 | Uniform S=8 | 5,098 [4,299–5,381] | 8,542 [8,283–9,181] | 1,676 |
| ND3 | Baseline SMJ | 9,658 [8,961–9,802] | 28,298 [25,507–40,980] | 2,930 |
| ND3 | AQE | 9,639 [9,455–9,680] | 25,801 [25,502–41,974] | 2,677 |
| ND3 | Salting S=8 | 10,781 [10,622–10,942] | 29,668 [29,587–35,816] | 2,752 |
| ND3 | Broadcast tham chiếu | 2,403 [2,368–2,710] | 6,114 [5,878–10,505] | 2,544 |

Tỷ số S2/S1 là tỷ số của **hai trung vị thời gian ở hai ứng dụng riêng**, chỉ mô tả các lượt này; không là độ phức tạp thuật toán hay hệ số scaling phổ quát. ND2 giữ Baseline nhanh nhất, nhưng thứ tự Selective/Uniform đổi giữa S1 và S2; hai khoảng S1 của chúng chồng lấp. ND3 giữ thứ tự trung vị Broadcast < AQE ≈ Baseline < Salting, song khoảng Baseline/AQE ở S2 rộng. Một phiên chạy trên cùng host và ba lượt/mức chưa đủ cho kết luận ổn định qua phiên.

Phân tích bổ sung dùng successful tasks của **stage được chọn trong từng lượt**, không gộp stage: Spearman giữa `shuffle read records` và `executorRunTime` ở S2 là 0,952/0,837/0,939 cho Baseline, 0,958/0,940/0,624 cho Salting và 0,688/0,656/0,615 cho AQE. Baseline/Salting có 200 task ở stage đó, AQE có 16; Broadcast chỉ 12 task và số record bằng nhau nên hệ số không xác định. File nguồn: `nd3_task_load.csv` trong thư mục `nd3_pilot`; S1 tương ứng có `nd3_task_load.csv` trong thư mục pilot Broadcast. Tương quan này không xác định riêng nguyên nhân của lượt wall-time chậm: CPU, GC, I/O và chờ tài nguyên chưa được tách.

**Quyết định sau pilot:** chưa mở S3 hoặc ma trận salt/AQE lớn. S2 đã đủ làm mức tích lũy đầu tiên cho cả ND2 và ND3, nhưng độ dao động các lượt ND3 cuối cần kiểm tra qua một phiên chạy khác hoặc telemetry tài nguyên trước khi dùng S1 để dự đoán S3. Đây là điểm dừng theo cổng Phase B/C của plan, không phải kết luận phương pháp nào thắng một cách ổn định.

## Cập nhật sau phiên S2 lặp — 21/11/2026

Đã chạy một ứng dụng S2 riêng (`app-20261121092615-0007`) với cùng Silver/OD/hotspot manifest, bốn phương án × ba lượt, warm-up có action, telemetry cgroup theo container và đối soát DuckDB. Cả 12/12 lượt đạt `OK/PASS` và có hai Worker; kết quả không ghi đè pilot S2 ban đầu. Phân tích biến thiên, task CPU/GC, AQE final plan và giới hạn telemetry được ghi tại [biên bản Phase C S2 repeat](../phase_c/s2_repeat_variability_20261121.md). Trong phiên lặp các khoảng từng phương án hẹp hơn, nhưng median cao hơn 1,53–1,64 lần so với phiên S2 trước; nguyên nhân chênh lệch giữa phiên chưa được định danh. **Giữ S3 ở trạng thái chưa mở** cho tới khi điều kiện host/CPU được kiểm soát hoặc đo đủ để giải thích dịch chuyển này.
