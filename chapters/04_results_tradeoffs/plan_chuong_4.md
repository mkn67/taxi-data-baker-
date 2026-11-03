# Kế hoạch hoàn thiện Chương 4

## Mục tiêu và nguyên tắc viết

Chương 4 trình bày **kết quả đo được và phân tích đánh đổi**, còn quy trình, định nghĩa metric và cấu hình phương pháp đã nằm ở Chương 3. Mọi bảng/hình cần trỏ về một nguồn duy nhất trong `data/benchmarks/` hoặc `data/gold/`; ghi rõ tier, tháng, số lượt, application ID khi phù hợp. Không coi giá trị metric bị thiếu là 0.

**Trạng thái hiện tại**

- **Có kết quả để phân tích:** ND2, ND3, đối soát DuckDB, benchmark theo tháng độc lập, spatial Gold và bản đồ Kepler.gl.
- **Có cấu hình nhưng thiếu phép đo runtime:** giới hạn CPU/RAM đã khai báo trong Compose; chưa thấy bảng thu thập `cpu.stat`, `memory.current/peak` hay số liệu cgroup theo từng application.
- **Chưa triển khai:** mô hình LightGBM/ML nhận diện khóa gây skew; không đưa thành mục kết quả thực nghiệm Chương 4, chuyển thành hướng phát triển Chương 5 nếu cần.
- **Chưa có SparkListener tùy biến:** runner hiện có Event Log, thu metrics qua Spark REST API và lưu plan; không mô tả là đã dùng Listener nếu chưa bổ sung.

## Kế hoạch theo mục lục

**Điều chỉnh tiêu đề trước khi soạn bản chương:** 4.1.1 ghi Podman Compose; 4.1.2 dùng “Khai báo và áp giới hạn tài nguyên bằng cgroups”; 4.1.3 đổi “Listener” thành “Spark Event Logs và REST API”. 4.5 là “Đánh giá tính ổn định của kết quả qua năm tháng dữ liệu độc lập”; đổi tên cả các tiểu mục theo bảng dưới đây. Bỏ mục ML khỏi Chương 4 và chuyển spatial thành 4.6, trade-offs thành 4.7, tổng kết thành 4.8. Đặt 4.6.3 là “Đối chiếu với nghiên cứu của Zhang, Sang & Wu” và bỏ năm ở tiêu đề; ghi SSRN 5549128/preprint trong nội dung. Mục 4.5.1 dẫn lại định nghĩa Pilot ở Chương 2/3, tránh lặp phương pháp.

### 4.1. Thiết lập môi trường và quy trình đo lường thực nghiệm

| Mục | Nội dung cần viết | Bằng chứng hiện có | Việc cần hoàn tất |
|---|---|---|---|
| **4.1.1** | Cụm Spark Standalone chạy bằng Podman Compose; vai trò Master, hai Worker và Driver; tài nguyên được cấp cho từng container. Official runs dùng Spark 3.5.9. | README/benchmark artifact audit ghi Podman; `docker-compose.yml`; cột `spark_version` và `worker_evidence` trong CSV official. Cả 90 dòng run-level ND2/ND3 đều ghi Spark 3.5.9 và hai worker. | Ghi Podman Compose và Spark 3.5.9 cho cụm benchmark chính thức. Notebook local-functional Spark 3.5.6 là bằng chứng chạy local riêng, không nhầm với run Standalone. Phân biệt tài nguyên container với Spark worker/executor. |
| **4.1.2** | **Khai báo và áp giới hạn tài nguyên bằng cgroups**; phân biệt hạn mức Docker/Podman với heap Spark. | Compose khai báo Master 1 CPU/1 GiB; mỗi Worker 2 CPU, Spark worker memory 2 GiB, giới hạn container 2,5 GiB; Driver 2 CPU/2,5 GiB. | Chưa có phép đo usage runtime qua `cpu.stat`, `memory.current/peak` hay `memory.events`; không gọi đây là số liệu sử dụng và không kết luận mức dùng thực tế. Ghi đúng runtime thực sự dùng trong run official theo metadata/nhật ký, không suy từ tên Compose. |
| **4.1.3** | Quy trình warm-up, ba lượt đo xoay thứ tự; thu thời gian, task/stage metrics, execution plan và executor evidence qua Spark Event Logs và REST API. | Run CSV, event logs, Spark REST API task/stage metrics, final plans và metadata. | Đổi tiêu đề thành **“Quy trình Warm-up, đo lường lặp lại và thu thập chỉ số qua Spark Event Logs và REST API”**. Project chưa có SparkListener tùy biến; chỉ nêu Listener nếu bổ sung và đối chiếu được. |

**Sản phẩm:** Bảng 4.1 cấu hình cụm/tài nguyên; Hình 4.1 topology; sơ đồ hoặc bảng quy trình đo; chú thích phạm vi phép đo cgroup.

### 4.2. Kết quả thực nghiệm trên phép toán tổng hợp dữ liệu (Aggregation Benchmark)

| Mục | Cách trình bày | Nguồn và trạng thái |
|---|---|---|
| **4.2.1** | So sánh median wall time Baseline, Selective S=8; Uniform S=8 làm đối chứng bổ sung. Tách kết quả tháng 01 khỏi xu hướng 5 tháng. | `benchmark_nd2_aggregation_summary_official_standalone.csv`; run CSV; Hình ND2 được tái tạo từ summary official. Có đủ ba lượt/phương án, correctness PASS. |
| **4.2.2** | TSR/PSR và straggler: giải thích stage/task scope, nêu trường hợp metric không khả dụng; tránh gọi phân bố khóa P95 là task skew. | Summary/run CSV ND2. PSR Baseline tháng 01 là N/A; không thay bằng 0. |
| **4.2.3** | Phân tích Shuffle Write, cùng memory/disk spill nếu metric có; so sánh mức giảm shuffle với tổng thời gian tăng. | Official summary và task metrics. Spill rỗng/null phải ghi “không khả dụng”; chỉ ghi 0 nếu nguồn ghi nhận 0. |
| **4.2.4** | Đánh giá P95 như heuristic chọn khóa: tác động phân phối và chi phí salting; không khẳng định mô hình phát hiện skew tối ưu. | Artifact hotspot P95 từng tháng, summary ND2, notebook/runner. |

**Thông điệp cần kiểm chứng bằng số liệu:** trong các tháng đã đo, Selective Salting giảm Shuffle Write so với Uniform Salting nhưng wall time vẫn cao hơn Baseline; kết luận giới hạn ở workload/cấu hình đã chạy.

**Sản phẩm:** Bảng 4.2 median và min–max của ba lượt, TSR/PSR, shuffle và spill; Hình 4.2 so sánh ND2 tháng 01; Hình 4.3 xu hướng ND2 qua năm tháng. Không dùng SD trên n=3 để hàm ý ý nghĩa thống kê.

### 4.3. Kết quả thực nghiệm trên phép kết nối phân tán (Shuffle Join Benchmark)

| Mục | Cách trình bày | Nguồn và trạng thái |
|---|---|---|
| **4.3.1** | So sánh Baseline Sort-Merge, AQE và Salting S=8 bằng median, min–max của ba lượt; không diễn giải ý nghĩa thống kê từ cỡ mẫu n=3. | `benchmark_nd3_join_summary_official_standalone.csv` và run-level CSV; đủ tháng 01–05. |
| **4.3.2** | Đối chiếu TSR và PSR trên stage đã chọn; giải thích AQE có thể giảm tỷ số mà vẫn có spill. | Summary/run CSV và metric collector. |
| **4.3.3** | Trích final adaptive physical plan cùng tier/run; chỉ ra `SortMergeJoin`, `AQEShuffleRead`, coalescing và việc không có skew-specific split trong các run được báo cáo. | `data/benchmarks/nd3_plans/official_standalone/`; gắn plan với application/lượt tương ứng. Không dùng plan dev thay cho official. |
| **4.3.4** | So sánh Shuffle Read, memory/disk spill; định lượng OD dimension materialized từ 58.747 lên 83.646 hàng (+24.899) cho tháng 01. Nêu rõ đây là số hàng, không phải phần trăm byte. | ND3 official summary/run CSV, preflight OD key audit và count sau explode. |

**Sản phẩm:** Bảng 4.3 ND3 metrics; Hình 4.4 so sánh tháng 01; Hình 4.5 xu hướng 5 tháng; trích đoạn plan AQE/SMJ và bảng replication overhead.

### 4.4. Kiểm chứng tính đúng đắn và nhất quán dữ liệu bằng DuckDB

- **4.4.1:** ND2 đối chiếu count, average fare, total miles theo `PULocationID` giữa ba phương án và DuckDB.
- **4.4.2:** ND3 đối chiếu theo borough pair: trip count, fare, miles, distinct pickup zones và pair coverage.
- **4.4.3:** giải thích OD composite-key uniqueness/null preflight, manifest, PASS/FAIL và điều kiện loại run (collector phải OK, validation PASS).
- **Nguồn:** `*_audit_official_standalone.csv`, run/summary CSV, `od_matrix_key_audit.csv`; monthly run/audit trong `data/benchmarks/scaling/2026_02`–`2026_05`.
- **Sản phẩm:** Bảng 4.4 tóm tắt số lượt hợp lệ/bị loại theo từng phương pháp. Đã đọc cả 10 CSV run-level chính thức (ND2 và ND3, tháng 01–05): mỗi phương pháp-tháng có 3 lượt, 3 `PASS`, 3 collector `OK`; tổng 90/90 lượt hợp lệ, không có lượt bị loại trong các CSV này. Bảng kết quả cần thể hiện số lượt theo phương pháp, không chỉ ghi tổng.

| Benchmark | Phương pháp | Tổng lượt | Hợp lệ | Bị loại |
|---|---|---:|---:|---:|
| ND2 | Baseline GroupBy | 15 | 15 | 0 |
| ND2 | Selective Salting S=8 | 15 | 15 | 0 |
| ND2 | Uniform Salting S=8 | 15 | 15 | 0 |
| ND3 | Baseline Sort-Merge Join | 15 | 15 | 0 |
| ND3 | AQE Skew Join | 15 | 15 | 0 |
| ND3 | Salting Join S=8 | 15 | 15 | 0 |

### 4.5. Đánh giá tính ổn định của kết quả qua năm tháng dữ liệu độc lập

| Mục | Cách trình bày | Giới hạn phải ghi |
|---|---|---|
| **4.5.1 — Phạm vi dữ liệu theo tháng và định nghĩa Pilot/Primary** | Trình bày phạm vi dữ liệu đo theo tháng; dẫn về Chương 2/3 cho pipeline và phân biệt Raw/Silver. Pilot là chế độ chạy thử local trên cùng Silver tháng 01, giảm số lượt đo; không phải tập dữ liệu 7 ngày. Primary benchmark là official Standalone tháng 01; các run tháng 01–05 là input tháng độc lập. | Bản báo cáo đầy đủ hiện có Chương 1–3, không có Chương 4–5 và không nhắc LightGBM trong phần Chương 1. Phần 2.5.1/2.5.3 và chú thích Hình 2.7 đã mô tả đúng Pilot/Primary; nhưng còn câu cũ ở phần tổng quan nghiên cứu Chương 1 và đoạn mô tả quy mô nêu Pilot ~4,5 triệu/1 tuần, Scaling Test ~80–100 triệu. Cần đồng bộ các câu cũ này trong DOCX trước khi viết kết quả. Không có Silver 7 ngày dùng cho benchmark. |
| **4.5.2 — So sánh kết quả giữa các tháng** | So sánh kết quả giữa năm tháng độc lập như kiểm tra độ ổn định qua các input, không gọi là scaling theo quy mô. Báo cáo median và min–max theo ba lượt; không diễn giải kiểm định ý nghĩa thống kê. | Silver từ 19.045.927 đến 21.094.362 dòng, chênh max/min khoảng 10,8% (xấp xỉ ±5% quanh trung điểm); dải quy mô hẹp. |
| **4.5.3 — Xu hướng của các phương pháp qua các tháng** | Mô tả xu hướng phương pháp và biến động qua các tháng trong phạm vi workload/cấu hình đã đo. | Không suy ra hiệu quả khi quy mô tăng mạnh hoặc luật độ phức tạp từ năm input có quy mô gần nhau. |
| **4.5.4 — Giới hạn suy rộng** | Giới hạn suy rộng theo số tháng, dải quy mô hẹp, hai Worker, Spark 3.5.9, 200 shuffle partitions và ba lượt đo. | Đây là kiểm tra qua input tháng độc lập, không phải scaling benchmark; MFTune chỉ là tài liệu tham khảo, project chưa triển khai multi-fidelity tuning. |

**Nguồn:** `data/benchmarks/scaling/monthly_benchmark_summary.csv` và summary/run CSV từng tháng. Đây là phần có thể viết với bằng chứng hiện tại; không cần gọi thêm là “benchmark tích lũy”.

### Quyết định phạm vi ML: chuyển thành hướng phát triển Chương 5

Project chưa có feature dataset, nhãn, model, notebook, dự đoán hay benchmark dùng danh sách khóa do ML chọn. Vì vậy bỏ mục ML khỏi Chương 4 để tiêu đề không hàm ý đã có kết quả thực nghiệm. Nếu đưa vào Chương 5, trình bày như hướng mở rộng; cần định nghĩa nhãn độc lập với heuristic P95 (nhãn `count ≥ P95` sẽ chỉ dạy mô hình lặp lại chính heuristic), kiểm tra rò rỉ theo tháng, đánh giá trên tháng giữ lại, tích hợp salting, rồi đối soát correctness và chi phí runtime trước khi tuyên bố hiệu quả. Với khoảng 261 zone/tháng và nhãn dương quanh 5% nếu lấy ngưỡng P95, số mẫu dương qua năm tháng chỉ ở mức vài chục (các ties quanh ngưỡng có thể làm số này tăng); leave-one-month-out vì vậy dễ biến động và chưa đủ để quảng cáo khả năng tổng quát hóa ML. CIBPartitioner có thể làm cơ sở thảo luận về khó khăn của dự báo tải, sau khi đối chiếu đúng đoạn nguồn; không gán phương pháp của bài cho project.

**Ràng buộc với Chương 1/đề cương:** file báo cáo đầy đủ được cung cấp gồm nội dung Chương 1–3; tìm trong nội dung Chương 1 không thấy LightGBM, Machine Learning hay học máy. Vì vậy bản thảo hiện tại không cam kết LightGBM trong mục tiêu/đóng góp Chương 1. Tuy nhiên Chương 4–5 và đề cương gốc chưa có trong file; trước khi chốt mục lục cuối cùng, đối chiếu đề cương hoặc xác nhận với giảng viên nếu LightGBM từng được giao như một mục tiêu riêng.

### 4.6. Phân tích mật độ trả khách quanh ranh giới thu phí Manhattan

**Ghi chú phạm vi nhóm:** Gold citywide sau phân loại có 41 zone `SOUTH_OF_APPROX_60TH`, 23 `NORTH_OF_APPROX_60TH`, 5 `BOUNDARY_AMBIGUOUS` và 194 `OUTSIDE_MANHATTAN`. Bảng ứng viên gần ranh giới là tập con khác: 8 ứng viên phía Nam, 3 phía Bắc và 5 mơ hồ. Không dùng số 8/3 để mô tả toàn bộ Gold.

- **4.6.1:** Tóm tắt Gold tháng 01/2026: nhóm theo `DOLocationID × time_period`, tính chuyến/km² trên polygon EPSG:32618.
- **4.6.2:** So sánh phía Nam và phía Bắc riêng trong từng khung giờ. Daytime dài 16 giờ (05:00–20:59), Nighttime dài 8 giờ (21:00–04:59); không so trực tiếp tổng ngày với tổng đêm hoặc mật độ từng kỳ như thể thời lượng bằng nhau. Nếu cần so giữa Ngày/Đêm, chuẩn hóa thành chuyến/giờ/km². Báo median theo zone và số zone của từng nhóm.
- **4.6.3:** Đối chiếu Zhang, Sang & Wu như bối cảnh và khác biệt thiết kế. Ghi nguồn là SSRN preprint 5549128, chưa peer review; chưa xác minh trực tiếp ngày/năm đăng nên không ghi năm trong nội dung cho đến khi xác nhận bản ghi thư mục. Không chuyển ước lượng nhân quả của nghiên cứu sang project.
- **4.6.4:** Chèn bản đồ Kepler.gl và ảnh PNG; hướng dẫn mở `data/gold/dropoff_density_by_zone_period.gl`. Ảnh tĩnh dùng chung P95 scale để so bản đồ Ngày/Đêm.
- **4.6.5:** Gọi south/north là nhóm phân tích theo đường 60th Street xấp xỉ, không phải phân loại CRZ chính thức. Một tháng hậu chính sách, Taxi Zone thay vì vị trí chuyến cụ thể và không có đối chứng nên kết quả là mô tả, không suy luận nhân quả.

**Nguồn:** `data/gold/dropoff_density_by_zone_period.csv/.parquet/.gl`, `data/benchmarks/spatial_analysis_audit.csv`, `figure_3_1_dropoff_density.png`, `docs/chapter3_spatial_research_notes.md`.

### 4.7. Thảo luận và tổng hợp các đánh đổi kỹ thuật (Trade-offs)

- **4.7.1:** Đặt thời gian, TSR/PSR, shuffle, spill trên cùng workload; nhấn mạnh giảm skew ratio không tự động làm giảm end-to-end duration.
- **4.7.2:** Baseline: ít overhead; Selective Salting: thêm hash và two-stage merge; AQE: thích ứng runtime/coalescing nhưng không có skew-specific split trong final plan ở cấu hình đã chạy; Salting Join: nhân bản OD và tăng shuffle. Mô tả AQE không kích hoạt skew split là kết quả âm tính trong cấu hình đã đo. Không so tổng Shuffle Read với ngưỡng skew 16 MiB: AQE xét kích thước từng partition và điều kiện lệch tương đối, không phải tổng bytes của stage.
- **4.7.3:** Đề xuất tiêu chí chọn theo bằng chứng: kích thước/độ lệch partition sau shuffle, skew stage, khả năng broadcast, chi phí replication, bộ nhớ và yêu cầu thời gian. Ghi đây là hướng dẫn rút từ workload đã thử, không phải định luật tổng quát.
- **4.7.4:** Nối kết quả với thiết kế pipeline: đo đúng scope, correctness trước performance, giữ provenance và kiểm tra tài nguyên thực tế khi scale.

**Sản phẩm:** Bảng trade-off có hàng cho Baseline, Selective/Uniform Salting, AQE và Salting Join; không cần benchmark mới nếu các số ở 4.2–4.3 đã được kiểm tra đồng bộ.

### 4.8. Tổng kết chương

Tóm tắt phát hiện chính, điều kiện thí nghiệm, phương pháp nào cải thiện metric nào, chi phí phát sinh, kết quả spatial mô tả và giới hạn suy rộng. Nêu LightGBM là hướng phát triển ở Chương 5, không phải kết quả đã thực nghiệm.

## Thứ tự thực hiện đề xuất

1. **Khóa nguồn số liệu:** audit file official, application IDs, months, worker evidence, correctness PASS; cập nhật các bảng/hình nếu số liệu nguồn đổi.
2. **Viết 4.1 và 4.4:** thiết lập thực nghiệm và điều kiện hợp lệ; mô tả giới hạn cgroups khai báo cùng Event Logs/REST API hiện có, không chờ collector cgroup/Listener.
3. **Viết 4.2 và 4.3:** phân tích benchmark ND2/ND3; dùng hình đã tái tạo từ official summary, final plan và replication count.
4. **Viết 4.5:** mô tả năm input tháng độc lập như kiểm tra độ ổn định; tách Raw/Silver và nêu rõ không phải scaling theo quy mô.
5. **Viết 4.6:** chuyển số Gold/Kepler từ phương pháp Chương 3 sang kết quả; giới hạn so sánh thời lượng Ngày/Đêm.
6. **Viết 4.7–4.8:** tổng hợp trade-off và kết luận sau khi khóa bảng/hình.
7. **Soát cuối:** đơn vị, tên tier, tháng, caption, citation year, null-vs-zero, source path; mọi kết luận đều truy ngược được về artifact.

## Danh sách đầu ra Chương 4

- [ ] Bảng cấu hình môi trường và ngân sách tài nguyên; xác định rõ giới hạn khai báo hay usage đo được.
- [ ] Bảng/biểu đồ benchmark ND2, bao gồm median, độ phân tán, TSR/PSR và shuffle/spill có sẵn.
- [ ] Bảng/biểu đồ benchmark ND3, final physical plan AQE, spill và OD replication overhead.
- [ ] Bảng correctness DuckDB và số run được tính từ official run files.
- [ ] Bảng/biểu đồ so sánh tính ổn định qua 5 tháng input độc lập; tách Raw khỏi Silver và không gọi đây là scaling theo quy mô.
- [ ] Bản đồ/biểu đồ spatial cùng diễn giải không nhân quả và ghi đúng nhóm phân tích.
- [x] Loại ML khỏi kết quả Chương 4; chuyển LightGBM thành hướng phát triển Chương 5.
- [ ] Bảng tổng hợp trade-off và kết luận chương.
