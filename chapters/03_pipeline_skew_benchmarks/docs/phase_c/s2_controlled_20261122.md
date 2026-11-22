# S2 ND3 controlled repeat — 22/11/2026

## Thiết kế được chốt trước khi chạy

Mục tiêu là kiểm tra độ biến động giữa phiên trước khi dùng S2 làm căn cứ mở benchmark S3. Ngưỡng khác biệt nhỏ nhất có ý nghĩa thực tế được chốt trước khi xem kết quả là **10% wall time tương đối** cho so sánh cặp phương án. Đây là mức quyết định riêng của đồ án để phản ánh khác biệt đủ đáng cân nhắc khi vận hành; không phải chuẩn chung của Spark. Với ba lượt/phương án/phiên, báo cáo median và min–max mô tả; không tuyên bố ý nghĩa thống kê.

Không loại lượt theo thời gian. Chỉ lượt collector lỗi hoặc correctness không đạt mới không được dùng trong summary; metric thiếu được giữ là NA. Dùng cùng S2 Silver, OD Matrix, hotspot P95 và manifest của các phiên trước; cùng Spark 3.5.9, hai Worker, executor một core/1.500 MiB, tối đa bốn core, 200 shuffle partitions; một warm-up/nhánh, ba lượt đo/nhánh, thứ tự xoay vòng. Chạy `Baseline_SMJ`, `Spark_AQE_Skew_Join`, `Salting_Join_S8` và Broadcast tham chiếu có hint.

## Kết quả phiên

Application `app-20261121171337-0001`. Có **12/12 lượt collector OK và DuckDB PASS**, hai Worker tham gia; mỗi phương án cho 60.133.386 chuyến. Run CSV mang cùng fingerprint Silver/OD như hai phiên S2 trước. Đã lưu 12 plan, DuckDB audit cấp phương án, 1.284 successful selected-stage task records và ba final AQE plan. AQE giữ Sort-Merge Join/coalesce, không có skew split. Spill memory và disk bằng 0 ở cả 12 lượt. Salting materialize 87.409 hàng OD từ 62.223 hàng đầu vào, tức tăng 25.186 hàng.

| Phương án | Ba lượt (giây) | Median [min–max] |
|---|---:|---:|
| Baseline SMJ | 27,541 / 27,434 / 27,685 | 27,541 [27,434–27,685] |
| Spark AQE | 26,739 / 27,217 / 27,072 | 27,072 [26,739–27,217] |
| Salting Join S=8 | 33,049 / 32,829 / 32,157 | 32,829 [32,157–33,049] |
| Broadcast tham chiếu | 7,134 / 7,193 / 6,739 | 7,134 [6,739–7,193] |

## Host và cgroup

Host load, top process, CPU/IO PSI, CPU throttling, `memory.current` và `memory.peak` được lấy mẫu mỗi hai giây từ trước warm-up đến sau 12 lượt. Ghép theo timestamp với thời lượng mỗi action cho kết quả **12/12 cửa sổ đầy đủ**. Trong các cửa sổ này, CPU PSI `some avg10` tối đa 0,68%; IO PSI `some avg10` tối đa 1,18%; load 1 phút tối đa 4,78. Delta throttled time tối đa trong một cửa sổ là 0,686 giây ở Worker 2; mức sử dụng/peak bộ nhớ từng container được giữ trong raw samples. Host có hoạt động nền và có throttling ngắn; do đó không mô tả môi trường là hoàn toàn không tranh chấp. Tuy nhiên, không có lượt nào có wall time tăng đột biến tương tự phiên repeat chậm trước đó.

Độ rộng min–max/median trong phiên là 0,91% Baseline, 1,77% AQE, 2,72% Salting và 6,36% Broadcast. So với phiên S2 đầu và phiên có telemetry trước đó, median của phiên này lệch lần lượt khoảng: Baseline −2,7%/+5,8%; AQE +4,9%/+1,4%; Salting +10,7%/+4,8%; Broadcast +16,7%/−2,9%. Ba phiên không phải các block phần cứng độc lập; đây vẫn là lặp trên cùng host.

## Kết luận về cổng S3

Phiên này đáp ứng đầy đủ telemetry và correctness. Ba phiên có wall time tương đối nhất quán cho Baseline/AQE/Salting trong ngưỡng 10% giữa các median, ngoại trừ so sánh Salting với phiên S2 đầu ở mức +10,7% (sát ngưỡng); Broadcast dao động hơn. Phiên repeat `app-20261121092615-0007` vẫn chậm hơn các phiên khác khoảng 35–54% và không có telemetry host cùng thời điểm. Không có cơ sở để xóa hoặc loại nó, cũng chưa xác định được nguyên nhân. Vì vậy **không gộp các phiên và chưa dùng median tổng hợp để khẳng định tốc độ S2**.

**Quyết định:** Cho phép mở **S3 pilot thăm dò có telemetry đầy đủ**, nhưng chưa coi đó là benchmark scaling chính thức và chưa dùng nó để khẳng định quy luật tốc độ. S3 pilot phải giữ bốn phương án, 12 lượt, cùng manifest/plan/audit/worker evidence, host/cgroup sampling phủ toàn phiên và báo riêng mọi session; không mở thêm salt-factor/AQE ablation. Cổng S3 official chỉ được xem xét sau khi pilot S3 đạt correctness/provenance và không lặp lại biến động chưa giải thích, hoặc khi có đủ telemetry để quy thuộc điều kiện phiên. Mọi báo cáo giữ phiên S2 chậm như một quan sát hợp lệ.

## Artifact

Thư mục `data/benchmarks/scaling/s2_jan_mar/nd3_s2_controlled_20261122/` gồm run/summary/audit CSV, manifest, session context đã chốt trước, 12 plan, Spark submit log, host/cgroup raw samples, bảng ghép telemetry theo run, task records và AQE interventions. Event Log: `logs/spark-events/app-20261121171337-0001`.
