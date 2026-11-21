# Kế hoạch và trạng thái Chương 4

## Phạm vi

Chương 4 báo cáo kết quả đo được và phân tích đánh đổi, còn định nghĩa phương pháp nằm ở Chương 3. Bản thảo chính là [`chapter4_results.md`](chapter4_results.md), gồm các mục 4.1–4.8. Số liệu tháng 01 và lần chạy Airflow tháng 03 được trình bày tách biệt; các tháng 01–05 là input độc lập, không phải benchmark dữ liệu tích lũy.

## Trạng thái theo mục

| Mục | Nội dung đã có | Nguồn bằng chứng / giới hạn |
|---|---|---|
| **4.1 Thiết lập và đo lường** | Cấu hình Podman Compose, Master/2 Worker/Driver, biến kiểm soát, warm-up, ba lượt đo, REST API/Event Log, manifest và cách đọc TSR/PSR. | Cấu hình CPU/RAM là giới hạn khai báo; không có phép đo `cpu.stat`, memory peak/current theo application, SparkListener tùy biến hay network throughput. |
| **4.2 ND2 Aggregation** | Baseline, Uniform Salting và Selective Salting; thời gian trung vị và khoảng nhỏ nhất–lớn nhất, TSR/PSR, shuffle/spill và diễn giải P95. | Bảng/Hình 4.2 dùng official tháng 01, application `app-20261103153557-0001`. Kết quả ND4 tháng 03 được trình bày riêng ở Bảng 4.12. |
| **4.3 ND3 Shuffle Join** | Baseline SMJ, AQE, Salting Join; task metrics, final plan, spill và OD replication overhead. | Bảng 4.4, Hình 4.3 và các kế hoạch thực thi dùng official tháng 01, application `app-20261103153727-0002`. AQE có coalescing nhưng không có bằng chứng skew-specific split trong plan đã lưu. ND4 tháng 03 ở Bảng 4.12. |
| **4.4 Kiểm chứng** | DuckDB audit ND2/ND3, preflight tính duy nhất khóa OD, cách diễn giải 90 dòng metadata theo 5 tháng. | `PASS` của 3 iteration cùng phương án là trạng thái validation cấp phương án được gắn vào metadata, không phải ba phép audit độc lập. |
| **4.5 Tính ổn định theo tháng** | Raw/Silver tách riêng; trung vị và khoảng nhỏ nhất–lớn nhất 5 tháng và giới hạn suy rộng. | Silver khoảng 19,05–21,09 triệu dòng (dải hẹp); tháng là các bộ riêng, thời điểm chạy khác nhau. Không gọi là scaling test. |
| **4.6 Phân tích không gian** | Gold tháng 01, so sánh south/north trong cùng khung giờ, bản đồ tĩnh và Kepler.gl, đối chiếu bối cảnh và giới hạn mô tả. | Nhóm toàn Gold: 41 phía Nam, 23 phía Bắc, 5 mơ hồ, 194 ngoài Manhattan. Đây là nhóm phân tích theo đường xấp xỉ, không phải nhãn CRZ chính thức; không suy luận nhân quả. |
| **4.7 Trade-offs** | Bảng ma trận quyết định cho Baseline, Salting, AQE và Salting Join; tách bằng chứng khỏi giả thuyết CPU tax/GC/FinOps. | Không kết luận CPU, GC, network hay chi phí cloud khi chưa có telemetry tương ứng. |
| **4.8 Tổng kết** | Tổng hợp kết quả, điều kiện thí nghiệm và giới hạn suy rộng. | LightGBM/ML chưa triển khai, chỉ có thể nêu là hướng phát triển ở Chương 5. |

## ND4 — Airflow benchmark tháng 03/2026

Lượt xác nhận sạch có run ID `nd4-march-clean-20261120`, hoàn tất ngày 20/11/2026. DAG `chapter4_nd4_march_2026_benchmark` chạy tuần tự ND2 rồi ND3, sau đó kiểm định và đóng gói kết quả. Cả ba task Airflow đều có trạng thái `success`. ND2 dùng application `app-20261120074816-0006`; ND3 dùng `app-20261120074935-0007`. ND2 có action warm-up; ND3 cũ không tạo job warm-up (xác nhận bằng Event Log, runner đã sửa ngày 21/11/2026). Mỗi phương án có ba lượt đo; 18 dòng run đều có collector `OK`, DuckDB validation `PASS`, Spark 3.5.9 và bằng chứng hai Worker.

Artifacts của lượt xác nhận nằm tại `data/benchmarks/nd4_airflow/2026_03/nd4-march-clean-20261120/`: run CSV, summary/audit CSV, hình thời gian và provenance JSON. Một lượt Airflow trước đó cần chạy lại thủ công task đóng gói do container cũ thiếu Matplotlib; lượt xác nhận sạch sau khi dựng image mới chạy toàn DAG, không chạy task thủ công.

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
- Giới hạn CPU/RAM cấu hình trong Compose không phải usage thực tế; chưa có cgroup telemetry theo application.
- Không có SparkListener tùy biến; metric hiện dùng Spark REST API và Event Log.
- Năm tháng Silver có quy mô gần nhau; chưa có benchmark scaling ở mức tăng nhiều lần hoặc trên tập tích lũy.
- Các lượt n=3 dùng median và min–max để mô tả, không kết luận ý nghĩa thống kê.
- Gold dùng dữ liệu tháng 01 và đường tham chiếu 60th Street xấp xỉ; kết quả không xác định tác động nhân quả của chính sách thu phí.
- Không tuyên bố đã triển khai LightGBM, CIBPartitioner, MFTune, hay ML skew detection.

## Checklist bàn giao

- [x] Các mục 4.1–4.8 có nội dung trong bản thảo Chương 4.
- [x] Bảng/hình ND2 và ND3 tháng 01 được tạo từ official run artifacts.
- [x] So sánh ổn định theo tháng dùng run-level CSV, tách Raw và Silver, nêu rõ giới hạn quy mô.
- [x] DuckDB audits, worker evidence, plans và manifest được giữ cùng nguồn kết quả.
- [x] ND4 tháng 03 chạy Airflow end-to-end sạch; bảng, biểu đồ và provenance được tạo tự động.
- [x] Bảng spatial và ma trận quyết định có số liệu/giới hạn rõ ràng.
- [x] Các giới hạn về cgroup, Listener, scaling, địa lý và suy luận nhân quả được nêu trong bản thảo.
