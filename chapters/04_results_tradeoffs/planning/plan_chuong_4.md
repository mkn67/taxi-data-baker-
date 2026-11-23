# Kế hoạch và trạng thái Chương 4

## Phạm vi

Chương 4 báo cáo kết quả đo được và phân tích đánh đổi, còn định nghĩa phương pháp nằm ở Chương 3. Bản thảo chính là [`chapter4_results.md`](../manuscript/chapter4_results.md), gồm các mục 4.1–4.8. Số liệu tháng 01 và lần chạy Airflow tháng 03 được trình bày tách biệt. Benchmark tháng 01–05 là các input độc lập; ngoài ra ND3 đã có tier tích lũy S1/S2/S3, trong đó S1/S2 là pilot và S3 là official. Không gộp hai kiểu phạm vi này.

## Trạng thái theo mục

| Mục | Nội dung đã có | Nguồn bằng chứng / giới hạn |
|---|---|---|
| **4.1 Thiết lập và đo lường** | Cấu hình Podman Compose, Master/2 Worker/Driver, biến kiểm soát, warm-up, ba lượt đo, REST API/Event Log, manifest và cách đọc TSR/PSR. | S3 official có host/cgroup telemetry lấy mẫu đủ 9/9 cửa sổ; các lượt cũ không có bộ telemetry đồng nhất. Không có CPU/I/O attribution theo stage, SparkListener tùy biến hay network throughput vật lý. |
| **4.2 ND2 Aggregation** | Baseline, Uniform Salting và Selective Salting; thời gian trung vị và khoảng nhỏ nhất–lớn nhất, TSR/PSR, shuffle/spill và diễn giải P95. | Bảng/Hình 4.2 dùng official tháng 01, application `app-20261103153557-0001`. Kết quả ND4 tháng 03 được trình bày riêng ở Bảng 4.12. |
| **4.3 ND3 Shuffle Join** | Baseline SMJ, AQE, Salting Join; task metrics, final plan, spill, OD replication và tier S3 tích lũy. FlowJoin++ gợi ý thêm một đối chứng vận hành Spark tự chọn chiến lược join, không hint. | Tháng 01 dùng application `app-20261103153727-0002`; S3 official dùng `app-20261121180448-0005`, 9/9 lượt OK/PASS. AQE coalesce nhưng không split skew. Phiên auto-strategy `app-20261122074817-0000` bổ sung Bảng 4.16/Hình 4.8: 12/12 OK/PASS, hai Worker, cùng S3 manifest; ba auto plans đều BroadcastHashJoin, telemetry đủ 12/12 cửa sổ. ND4 tháng 03 ở Bảng 4.12. |
| **4.4 Kiểm chứng** | DuckDB audit ND2/ND3, preflight tính duy nhất khóa OD, cách diễn giải 90 dòng metadata theo 5 tháng. | `PASS` của 3 iteration cùng phương án là trạng thái validation cấp phương án được gắn vào metadata, không phải ba phép audit độc lập. |
| **4.5 So sánh phạm vi dữ liệu** | Raw/Silver tách riêng; kết quả 5 tháng độc lập và tier tích lũy S1/S2/S3 được phân biệt. | Các tháng riêng lẻ có dải Silver hẹp; S1/S2 là pilot, S3 là official. Kết quả mô tả ba tier, chưa chứng minh quy luật scaling tổng quát. |
| **4.6 Phân tích không gian** | Gold tháng 01, so sánh south/north trong cùng khung giờ, bản đồ tĩnh và Kepler.gl, đối chiếu bối cảnh và giới hạn mô tả. | Nhóm toàn Gold: 41 phía Nam, 23 phía Bắc, 5 mơ hồ, 194 ngoài Manhattan. Đây là nhóm phân tích theo đường xấp xỉ, không phải nhãn CRZ chính thức; không suy luận nhân quả. |
| **4.7 Trade-offs và Phase C** | Đối chứng S3 ngày 22/11, phiên ghép cặp 10 block ngày 23/11, chi phí P95, độ ổn định hotspot, lưới tổng hợp, độ nhạy và quy tắc chọn. | S3 40/40 OK/PASS; lưới v3 240/240; độ nhạy 60/60. Quy tắc chọn có ba ô giữ lại, cho kết quả như mốc luôn chọn AQE. |
| **4.8 Tổng kết** | Tổng hợp kết quả, điều kiện thí nghiệm và giới hạn suy rộng. | LightGBM/ML chưa triển khai, chỉ có thể nêu là hướng phát triển ở Chương 5. |

## Đối chiếu bản thảo với WP0–WP7

| Gói công việc | Vị trí trong bản thảo | Bằng chứng được trình bày |
|---|---|---|
| WP0 | 4.1.4 | Giao thức block ghép cặp, ngưỡng 5%, bootstrap và Holm. |
| WP1–WP3 | 4.4.3, 4.7.2 | Chi phí quét P95, Event Log theo job group, phiên 10 block và audit theo phương án. |
| WP4 | 4.7.3 | Jaccard và tỷ trọng chuyến thuộc hotspot theo tháng/giờ. |
| WP5 | 4.7.4–4.7.5 | Lưới v3, bản đồ kết quả, byte Parquet và kích thước BroadcastExchange. |
| WP6 | 4.7.4 | Độ nhạy theo số phân vùng và ngưỡng skew AQE. |
| WP7 | 4.7.6–4.7.7 | Regret từng ô giữ lại, mốc luôn chọn AQE và ma trận kiểm tra trên workload đích. |

Xem [plan WP0–WP7](../../03_pipeline_skew_benchmarks/docs/phase_c/implementation_plan_wp0_wp7.md) và [báo cáo thực thi](../../03_pipeline_skew_benchmarks/docs/phase_c/phase_c_execution_report_20261123.md) để đối chiếu trạng thái từng gói.

## ND4 — Airflow benchmark tháng 03/2026

Một lượt xác nhận sạch có run ID `nd4-march-clean-20261120`, hoàn tất ngày 20/11/2026. DAG `chapter4_nd4_march_2026_benchmark` chạy tuần tự ND2 rồi ND3, sau đó kiểm định và đóng gói kết quả. Cả ba task Airflow đều có trạng thái `success`. ND2 dùng application `app-20261120074816-0006`; ND3 dùng `app-20261120074935-0007`. ND2 có action warm-up; ND3 cũ không tạo job warm-up (xác nhận bằng Event Log, runner đã sửa ngày 21/11/2026). Mỗi phương án có ba lượt đo; 18 dòng run đều có collector `OK`, DuckDB validation `PASS`, Spark 3.5.9 và bằng chứng hai Worker.

Artifacts của lượt xác nhận nằm tại `data/benchmarks/nd4_airflow/2026_03/nd4-march-clean-20261120/`: run CSV, summary/audit CSV, hình thời gian và provenance JSON. Một lượt Airflow trước đó cần chạy lại thủ công task đóng gói do container cũ thiếu Matplotlib; lượt xác nhận sạch sau khi dựng image mới chạy toàn DAG, không chạy task thủ công.

Hai phiên tái lập ngày 22/11/2026 dùng runner có action warm-up ND3, cùng manifest Silver tháng 03: `nd4-march-repeat-20261122` và `nd4-march-repeat2-20261122`. Cả 6 task thành công và 36/36 dòng đo `OK/PASS`. Kết quả và giới hạn dao động nằm trong `../evidence/nd4_reproducibility_20261122.md`; lượt 20/11 được báo riêng vì giao thức ND3 khác.

## Lệnh tái lập

```bash
podman compose up -d --build spark-master spark-worker-1 spark-worker-2 airflow-standalone
podman exec airflow-standalone airflow dags unpause chapter4_nd4_march_2026_benchmark
podman exec airflow-standalone airflow dags trigger chapter4_nd4_march_2026_benchmark
podman exec airflow-standalone airflow dags list-runs --dag-id chapter4_nd4_march_2026_benchmark
```

Airflow UI: `http://localhost:8085`; Spark Master UI: `http://localhost:8080`. Đóng gói lại một thư mục run có sẵn:

```bash
python scripts/chapter_04/build_nd4_march_artifacts.py \
  --airflow-run-id <dag-run-id> \
  --run-dir data/benchmarks/nd4_airflow/2026_03/<dag-run-id>
```

Các script `build_chapter4_evidence.py`, `build_chapter4_figures.py` và `build_chapter4_remaining.py` chỉ tổng hợp evidence đã lưu, không khởi chạy benchmark Spark.

## Giới hạn cần giữ trong bản nộp

- Cụm benchmark gồm nhiều container trên cùng host, không đại diện trực tiếp cho cụm nhiều máy qua mạng vật lý.
- Giới hạn CPU/RAM Compose là cấu hình, không phải usage. S3 official và phiên auto-strategy ngày 22/11 có host/cgroup telemetry. Phiên 10 block ngày 23/11 có số đo công việc task từ Event Log và thiếu telemetry host/cgroup liên tục.
- Không có SparkListener tùy biến; metric hiện dùng Spark REST API và Event Log.
- Năm tháng Silver độc lập có quy mô gần nhau. Các tier S1–S3 tích lũy đã có artifact nhưng khác phiên/pilot/official; chưa xác lập quy luật scaling tổng quát.
- Các lượt n=3 dùng median và min–max để mô tả, không kết luận ý nghĩa thống kê.
- Gold dùng dữ liệu tháng 01 và đường tham chiếu 60th Street xấp xỉ; kết quả không xác định tác động nhân quả của chính sách thu phí.
- Không tuyên bố đã triển khai LightGBM, CIBPartitioner, MFTune, hay ML skew detection.
- Flow-Join++ là nghiên cứu tham khảo về dự đoán heavy hitter/cardinality và tối ưu distributed join. Project không triển khai Scardina hoặc Heavy Hitter Push-Down; nhánh Spark auto-strategy đã chạy là đối chứng triển khai, được báo riêng với benchmark ba nhánh official trước đó.

## Checklist bàn giao

- [x] Các mục 4.1–4.8 có nội dung trong bản thảo Chương 4.
- [x] Bảng/hình ND2 và ND3 tháng 01 được tạo từ official run artifacts.
- [x] So sánh ổn định theo tháng dùng run-level CSV, tách Raw và Silver, nêu rõ giới hạn quy mô.
- [x] DuckDB audits, worker evidence, plans và manifest được giữ cùng nguồn kết quả.
- [x] ND4 tháng 03 có một lần Airflow end-to-end sạch; bảng, biểu đồ và provenance được tạo tự động.
- [x] Lặp lại DAG ND4 hai phiên ngày 22/11 trên cùng manifest với runner có action warm-up cho ND2 và ND3: 6/6 task success, 36/36 dòng OK/PASS, hai Worker. Biên bản `../evidence/nd4_reproducibility_20261122.md` ghi riêng median từng phiên và các PSR `NA`.
- [x] Bảng spatial và ma trận quyết định có số liệu/giới hạn rõ ràng.
- [x] Các giới hạn về telemetry, Listener, scaling, địa lý và suy luận nhân quả được nêu trong bản thảo.
- [x] Kết quả S3 ND3 official và OD replication đã được đưa vào bản thảo Chương 4.
- [x] Đã chạy đối chứng Spark auto-strategy S3 `app-20261122074817-0000`: bốn nhánh × ba lượt sau warm-up, 12/12 OK/PASS, đúng manifest, hai Worker, 12 plans, đủ telemetry. Bảng 4.16 và Hình 4.8 dùng riêng phiên này; xem biên bản `flowjoin_auto_strategy_20261122.md`.

- [x] Bổ sung kiểm toán tần suất/fanout theo cặp OD sau benchmark; phân biệt pickup P95 với P95 OD chẩn đoán và với mô hình heavy hitter của Flow-Join++.
