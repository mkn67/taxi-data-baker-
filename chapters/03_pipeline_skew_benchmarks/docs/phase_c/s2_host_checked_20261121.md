# Phase C — S2 ND3: phiên đo bổ sung có telemetry host

## Phạm vi và nguồn bằng chứng

Đây là phiên `pilot_standalone` thứ ba trên S2 tích lũy tháng 01–03/2026, không phải benchmark `official_standalone` và không thay thế hai phiên trước. Cả ba phiên dùng cùng Silver và OD manifest (`d388d7e52aa55fd63dce11026a0dbd6609b7926118aad0122bc239fc16445075` và `a56baf9e34dd3d97dfa2eac3e49a8374e6bfe7a3d3e6e583c1c1ae3a0237ae3b`). Phiên mới là Spark application `app-20261121165056-0000`, Spark 3.5.9 Standalone, hai Worker trên cùng host, mỗi executor một core/1.500 MiB, tối đa bốn core và 200 shuffle partitions. Mỗi phương án có một warm-up và ba lượt đo, thứ tự xoay vòng.

Run CSV xác nhận **12/12 lượt `collector_status=OK`, `validation_status=PASS`**, đủ bằng chứng hai Worker; DuckDB đối soát 60.133.386 chuyến cho cả bốn phương án. Đã lưu 12 executed plans. Event Log trích ra 1.284 task records thành công ở các stage được chọn; ba final plan AQE đều là Sort-Merge Join, có coalesced shuffle reads và **không có skew split**. Salting materialize 87.409 hàng OD từ 62.223 hàng, tăng 25.186 hàng; số này thuộc S2, không dùng số OD tháng 01.

## Ba phiên S2

Đơn vị thời gian là giây. Đây là median của ba lượt trong **từng phiên**, không gộp chín lượt thành một mẫu; dấu ngoặc là min–max của phiên mới.

| Phương án | S2 đầu | S2 repeat chậm | S2 có host telemetry |
|---|---:|---:|---:|
| Baseline SMJ | 28,298 | 43,309 | 26,027 [25,297–26,924] |
| Spark AQE | 25,801 | 41,712 | 26,704 [26,501–27,643] |
| Salting Join S=8 | 29,668 | 48,006 | 31,337 [29,928–32,390] |
| Broadcast tham chiếu | 6,114 | 10,048 | 7,345 [6,212–7,571] |

Phiên thứ ba gần phiên đầu hơn phiên repeat chậm: tỷ số median phiên thứ ba/phiên đầu là 0,920 (Baseline), 1,035 (AQE), 1,056 (Salting), 1,201 (Broadcast). Cả bốn phương án trong phiên repeat vẫn chậm hơn phiên thứ ba 1,368–1,664 lần. Việc hai phiên nhanh và một phiên chậm cùng dùng manifest không chứng minh hiệu năng giữa các phiên ổn định. Thứ hạng Broadcast nhanh nhất và Salting chậm hơn hai nhánh SMJ giữ nguyên trong ba phiên; chênh lệch Baseline–AQE nhỏ và không nên diễn giải như hiệu quả AQE skew split.

## Kiểm tra tải host và giới hạn CPU

Script `sample_host_cgroup.py` lấy mẫu host load, CPU/IO Pressure Stall Information (PSI) và `cpu.stat` của hai Worker cùng driver mỗi khoảng hai giây. Script `analyze_s2_host_telemetry.py` ghép cửa sổ lấy mẫu với mốc kết thúc và wall time trong run CSV; mốc bắt đầu là ước tính, nên các delta cgroup là số đo **xấp xỉ theo cửa sổ**, không phải chi phí riêng của Spark action. Có **175 mẫu từ 16:50:40 đến 16:56:28 UTC**, bao trùm 9/12 lượt đầu. Bộ lấy mẫu dừng sớm (exit 143); ba lượt cuối không có cửa sổ hoàn chỉnh. Không điền 0 cho các lượt thiếu.

Trong 9 cửa sổ có mẫu đầy đủ, host load 1 phút tối đa theo từng lượt nằm trong 3,27–4,43; CPU PSI `some avg10` tối đa 0,54%, IO PSI `some avg10` tối đa 0,33%. Delta `throttled_usec` lớn nhất theo một Worker trong một cửa sổ là 413.849 µs (khoảng 0,414 s), và một số cửa sổ không có lần throttling nào. Các giá trị này không cho thấy sự nghẽn host/cgroup kéo dài trong **phần phiên nhanh được quan sát**; chúng không giải thích phiên repeat chậm vì phiên đó không có cùng loại telemetry host. `cpu.stat` cũng không thay thế số đo storage wait riêng của ứng dụng.

Trong selected stages của phiên mới, tổng executor CPU time khoảng 32,05–32,48 s (Baseline), 34,27–35,80 s (AQE), 35,01–38,51 s (Salting); JVM GC tối đa 1,02% tổng executor runtime ở các stage này. Memory và disk spill đều bằng 0. Đây là số liệu selected stage, không phải tổng CPU của toàn application.

## Quyết định cổng S3

**S2 pilot hoàn tất về tính đúng đắn, provenance, hai Worker, task metrics và final plans; S2 chưa đạt điều kiện ổn định giữa các phiên để dùng median thời gian làm cơ sở suy rộng. Giữ S3 ở trạng thái `BLOCKED` đối với benchmark hiệu năng chính thức.** Chưa có bằng chứng nguyên nhân của phiên chậm. Không gộp median ba phiên, không chọn riêng phiên nhanh để tuyên bố tốc độ, và không suy luận AQE đã chia partition lệch.

Để mở S3, cần một phiên đối chứng có telemetry host/cgroup bao phủ **toàn bộ** warm-up và 12 lượt, đồng thời ghi trạng thái host trước khi chạy; nếu lại xuất hiện phiên chậm, so CPU PSI, throttling và IO PSI với phiên nhanh trên cùng cấu hình. Nếu các chỉ số này không giải thích được, báo S2 như ba phiên riêng và giới hạn kết luận scaling ở tính đúng đắn/thứ hạng quan sát, hoặc bổ sung phép đo storage/host process trước khi đưa ra khẳng định tốc độ. Không hạ ngưỡng AQE chỉ để tạo skew split.

Artifact: `data/benchmarks/scaling/s2_jan_mar/nd3_host_checked_20261121/` gồm run/summary/audit CSV, 12 final plans, `host_cgroup_samples.csv`, `nd3_host_cgroup_by_run.csv`, `nd3_task_records.csv`, `nd3_aqe_interventions.csv`, `spark_submit.log`; Event Log `logs/spark-events/app-20261121165056-0000`.
