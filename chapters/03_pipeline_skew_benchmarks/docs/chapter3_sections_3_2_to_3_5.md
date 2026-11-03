# Nội dung Chương 3 — các mục 3.2 đến 3.5

> **Ghi chú biên tập:** ND2/ND3 official Standalone đã chạy cho các tháng độc lập 01–05/2026. Run metadata, hai-worker evidence, manifests, task metrics, plans và DuckDB audits được lưu theo tier official; phạm vi suy rộng còn hẹp vì các tháng có kích thước tương đương và không phải tập tích lũy.

## 3.2. Tối ưu hóa lệch tải trong phép toán tổng hợp (Aggregation) bằng Selective Salting

### 3.2.1. Phương pháp Baseline và cơ chế tổng hợp dữ liệu phân tán bằng groupBy

Phép tổng hợp ND2 nhóm dữ liệu chuyến theo `PULocationID`. Với mỗi vùng đón, hệ thống tính số chuyến, giá vé cơ sở trung bình (`base_passenger_fare`) và tổng quãng đường (`trip_miles`). Phương án Baseline áp dụng trực tiếp `groupBy("PULocationID")` và các phép tổng hợp tương ứng trên DataFrame Spark.

Trong thực thi phân tán, phép nhóm theo khóa yêu cầu các bản ghi cùng khóa được đưa về cùng nhóm tổng hợp. Nếu một số khóa có tần suất lớn, khối lượng công việc của các nhóm/partition liên quan có thể cao hơn. Baseline được dùng làm mốc để so sánh với các biến thể salting trên cùng dữ liệu và cùng đại lượng đầu ra. Không suy luận rằng khóa có nhiều chuyến chắc chắn gây task skew nếu chưa quan sát metric task ở Spark.

### 3.2.2. Nhận diện khóa nóng dựa trên phân vị P95 và lựa chọn khóa cần tối ưu

Số chuyến theo từng `PULocationID` được tính trước để xác định ngưỡng P95. Quy tắc nearest-rank sắp các số đếm tăng dần, lấy phần tử hạng `ceil(0,95 × n)` làm ngưỡng và chọn toàn bộ khóa có số đếm lớn hơn hoặc bằng ngưỡng. Cách lấy `>=` giữ lại cả các khóa đồng hạng tại ngưỡng. Đây là tập H được chia sẻ bởi Selective Salting ND2 và Salting Join ND3.

Với Silver tháng 01/2026 hiện có, phép kiểm tra độc lập cho 261 vùng pickup khác null, hạng P95 là 248, ngưỡng 194.522 chuyến và 14 khóa H: 7, 37, 61, 76, 79, 112, 132, 138, 161, 181, 230, 231, 234 và 255. Artifact lưu tại `data/silver/hotspots_p95_pu_2026_01/`. Trước khi báo cáo benchmark, cần gắn danh sách/threshold này với manifest dữ liệu của cùng lần chạy. Loader chính ưu tiên artifact đã lưu; nhánh fallback trong mã dùng `QUANTILE_DISC`, do đó nếu chạy mà thiếu artifact cần kiểm tra sự tương đương của tập được tái tạo với quy tắc nearest-rank đã công bố.

> **[CHÈN HÌNH 3.3 TẠI ĐÂY]** Phân bố số chuyến theo thứ hạng pickup và ngưỡng P95; hình được tạo bởi `scripts/chapter_03/plot_figure_3_3_pickup_p95.py` tại `data/benchmarks/figure_3_3_pickup_p95_hotspots.png`.

### 3.2.3. Cơ chế Two-Stage Aggregation và gán Salt có chọn lọc (S=8)

Hai phương án salting đều thực hiện tổng hợp hai giai đoạn. Ở giai đoạn đầu, mỗi chuyến được gán `salt` từ 0 đến 7 bằng hàm băm xác định trên các thuộc tính chuyến (`pickup_datetime`, `dropoff_datetime`, `trip_miles`), sau đó nhóm theo `(PULocationID, salt)`. Với Uniform Salting, mọi vùng được chia thành các bucket. Với Selective Salting, chỉ chuyến có `PULocationID` thuộc H được chia; các vùng còn lại nhận salt 0.

Giai đoạn hai gộp các kết quả từng bucket theo `PULocationID`. Số chuyến và tổng quãng đường được cộng từ partial aggregates. Giá vé trung bình được tính từ tổng fare chia cho số fare không null, nhờ đó kết quả trung bình được tái dựng đúng từ các bucket thay vì lấy trung bình không trọng số của các trung bình từng bucket. Mục tiêu của Selective Salting là giảm tập khóa phải xử lý bằng khóa trung gian, trong khi vẫn giữ nguyên các khóa thông thường ở một bucket.

### 3.2.4. Hiện thực hóa trên Apache Spark và đánh giá tác động đến Shuffle

Ba nhánh ND2 được triển khai trong `scripts/chapter_03/benchmark_engine.py` và gọi bởi `scripts/chapter_03/run_benchmark_nd2.py`; notebook `notebooks/chapter_03/02_benchmark_aggregation_salting.ipynb` chứa luồng chạy và đầu ra notebook đã lưu. Runner full có warm-up riêng cho mỗi phương án, sau đó đo ba lượt và xoay thứ tự chạy giữa các lượt. Mặc định `spark.sql.shuffle.partitions` là 200, có thể thay đổi bằng `--partitions`.

Bộ thu thập lưu thời gian wall-clock của job group, TSR, PSR khi metric task phù hợp khả dụng, shuffle read/write và memory/disk spill. Audit DuckDB theo từng `PULocationID` so sánh số chuyến, giá vé trung bình và tổng miles giữa Spark với phép tính độc lập. Official runs ND2 theo tháng đều có manifest, worker evidence và audit PASS; các lần đo cho thấy lợi ích shuffle write của selective so với uniform salting nhưng không giảm wall time so với baseline trên dải input đo.

Median wall time baseline/selective/uniform của ND2 theo tháng 01–05 (giây): 1,214/3,064/3,638; 1,411/2,943/3,377; 1,410/2,959/3,417; 1,410/2,775/3,400; 1,114/2,545/3,092. Baseline nhanh nhất trong các tháng đo; selective có shuffle write median 0,10 MB so với 0,45 MB của uniform, nhưng cả hai nhánh salting chậm hơn baseline trên input/config này do overhead tổng hợp hai giai đoạn. Đây không phải khẳng định salting không có ích ở quy mô hoặc phân bố khác.

> **[CHÈN BẢNG KẾT QUẢ 3.2 TẠI ĐÂY]** Bảng hợp nhất ở `data/benchmarks/scaling/monthly_benchmark_summary.csv`; run-level metrics/provenance theo từng tháng ở `data/benchmarks/scaling/2026_02` … `2026_05` và official tier gốc cho 2026-01.

## 3.3. Xử lý lệch tải trong phép kết nối phân tán (Join) bằng Spark AQE

### 3.3.1. Đặc điểm phép nối OD theo khóa và sự khác biệt với Spatial Join dựa trên quan hệ hình học

ND3 nối chuyến đi với bảng chiều OD bằng cặp khóa `(PULocationID, DOLocationID)` theo inner join. Bảng OD gắn thuộc tính vùng nguồn/đích cho từng cặp; sau join, kết quả được tổng hợp theo `PU_Borough` và `DO_Borough`. Đây là equi-join trên ID vùng, không phải spatial join: phép nối không kiểm tra giao nhau, chứa trong hoặc khoảng cách giữa các hình học đa giác.

Phân tích đa giác Taxi Zone ở mục 3.6 là một nhánh địa lý khác. Việc hai vùng có quan hệ không gian không phải điều kiện của ND3; join OD dựa trên khóa đã có trong dữ liệu. Nêu rõ khác biệt này giúp tránh gọi phép OD key join là “spatial join”.

Sự phân biệt này cũng quan trọng khi đối chiếu Yang et al. (2026): CIBPartitioner ước lượng computational intensity trên grid histogram để cân bằng phân hoạch cho spatial join hình học. ND3 của đồ án không dùng polygon predicate hay spatial partitioner; đây là phép nối theo composite OD key. Vì vậy CIBPartitioner phù hợp làm nghiên cứu liên quan về mục tiêu cân bằng công việc, không phải phương pháp được hiện thực hoặc đối chứng trực tiếp trong ND3.

### 3.3.2. Cơ chế nhận diện và chia nhỏ phân vùng lệch tải trong AQE Skew Join

Adaptive Query Execution (AQE) cho phép Spark điều chỉnh một số quyết định thực thi dựa trên thống kê runtime sau các stage shuffle. Trong nhánh thí nghiệm này, AQE và `spark.sql.adaptive.skewJoin.enabled` được bật; ngưỡng skew factor đặt bằng 2 và ngưỡng kích thước partition là 16 MiB. Partition chỉ được xử lý như skew nếu thỏa điều kiện của Spark theo thống kê partition và cấu hình thực tế. Bật cấu hình không đồng nghĩa chắc chắn có partition nào được tách.

Do đó, sự kiện AQE skew split phải được chứng minh bằng final adaptive physical plan/Spark UI của đúng lượt đo sau khi action đã materialize. Plan phải thể hiện skew join/shuffle partition splitting tương ứng. Nếu plan cuối không có dấu hiệu này, kết luận phù hợp là AQE được bật nhưng lần chạy đó không chứng minh được skew split; không viết rằng AQE đã xử lý skew chỉ dựa trên config.

### 3.3.3. Cấu hình AQE và kiểm soát kế hoạch thực thi Sort-Merge Join

Baseline ND3 tắt `spark.sql.adaptive.enabled`, tắt skew join và đặt `spark.sql.autoBroadcastJoinThreshold=-1` để loại broadcast join khỏi đối chứng. Nhánh AQE bật adaptive execution và skew join, đặt factor 2, threshold 16 MiB, đồng thời vẫn tắt broadcast. Cả hai dùng cùng Silver, OD dimension, composite join key và inner semantics. Runner mặc định dùng 200 shuffle partitions, nhưng giá trị có thể được override; khi báo cáo cần lấy config thực tế trong run CSV.

Runner áp dụng `.hint("merge")` ở cả hai đầu join để yêu cầu Sort-Merge Join, đồng thời lưu physical plan sau khi materialize kết quả, gắn với iteration/application ID và config.

### 3.3.4. Giới hạn của AQE và các điều kiện đối chứng trong thực nghiệm

AQE có thể không kích hoạt skew splitting nếu partition không vượt ngưỡng, skew factor không đạt, hoặc phép join/plan không thuộc trường hợp Spark có thể tối ưu. Kết quả cũng phụ thuộc vào thống kê sau shuffle và điều kiện runtime. AQE không thay thế correctness audit, không bảo đảm mọi dạng lệch dữ liệu được xử lý, và chi phí điều chỉnh có thể ảnh hưởng thời gian.

Đối chứng giữ nguyên dữ liệu, join key, join type, số shuffle partitions và hành động materialize; chỉ thay đổi AQE/skew settings theo ma trận. Các official final plan theo tháng thể hiện `isFinalPlan=true`, `SortMergeJoin` và AQE coalescing nhưng không có skew-specific partition splitting. Vì vậy kết quả hiện xác nhận AQE chạy, song skew split không được kích hoạt/không được chứng minh trên các input và cấu hình đã đo.

> **Kết quả Standalone theo tháng:** mọi ND3 method/month đều PASS DuckDB (3 lượt/phương án). Các final AQE plans có `isFinalPlan=true`, `SortMergeJoin`, `AQEShuffleRead coalesced`, không có skew-specific partition splitting. Vì vậy benchmark xác nhận AQE chạy/coalescing, nhưng không xác nhận skew split trong tập/config đã đo.

> **[CHÈN HÌNH 3.3.x TẠI ĐÂY]** Trích đoạn physical plan Baseline và AQE từ cùng tier/lần chạy; chú thích vị trí `SortMergeJoin`, `Exchange` và dấu hiệu skew split nếu xuất hiện. Không dùng plan dev như bằng chứng chính thức.

## 3.4. Xử lý lệch tải trong phép kết nối phân tán (Join) bằng Salting Join

### 3.4.1. Nguyên lý biến đổi khóa, gán Salt và nhân bản dữ liệu phía kết nối

Salting Join thêm thành phần `salt` vào khóa join để chia các bản ghi chuyến thuộc vùng pickup nóng thành nhiều nhóm con. Với H và S=8, mỗi chuyến có pickup trong H nhận salt xác định bằng `pmod(abs(hash(pickup_datetime, dropoff_datetime, trip_miles)), 8)`. Các chuyến ngoài H nhận salt 0.

Để giữ nguyên tính khớp, hàng OD có pickup thuộc H được bung thành tám bản sao với salt 0–7; hàng OD không nóng chỉ có salt 0. Join sau đó dùng `(PULocationID, DOLocationID, salt)`. Bằng cách này, chuyến nóng được phân tán qua các khóa mở rộng, còn bản ghi OD tương ứng được nhân bản để mỗi chuyến vẫn khớp đúng một bản sao theo salt. Đây là đánh đổi giữa khả năng phân chia tải và lượng dữ liệu dimension được nhân thêm.

### 3.4.2. Hiện thực hóa Salting Sort-Merge Join trên ma trận hành trình Đi–Đến (OD Matrix)

Hàm `join_salting` trong `scripts/chapter_03/benchmark_engine.py` tạo salt cho DataFrame chuyến, dùng `explode` trên mảng salt để nhân các hàng OD liên quan đến H, rồi inner join theo ba trường khóa. AQE skew join và broadcast được tắt trong nhánh này; hai phía join có `MERGE` hint để yêu cầu Sort-Merge Join. Physical plan vẫn được lưu để kiểm chứng cách thực thi.

Chi phí nhân bản OD không bằng tám lần toàn bộ bảng: chỉ các hàng OD có pickup thuộc H mới được nhân thành tám bản; các hàng khác giữ một bản. Official run mới materialize/count phép `explode` trước giai đoạn đo: 58.747 hàng gốc thành 83.646 hàng, tăng thực tế 24.899 hàng; count này khớp số dự kiến. Đây là overhead theo số hàng, không phải dung lượng bytes; shuffle read của Salting là 326,47 MB trong cùng run.

### 3.4.3. Kiểm chứng tính đúng đắn của kết quả Join và chi phí nhân bản dữ liệu

Sau join, Spark tổng hợp theo cặp borough nguồn–đích: trip count, total miles, total fare và số `PULocationID` phân biệt. DuckDB tính ground truth độc lập từ Silver và OD trên cùng composite key. Audit outer-join hai bảng tổng hợp và kiểm tra coverage, sai khác trip count, fare, miles và distinct pickup zones. Các full run official ND3 tháng 01–05 đều lưu audit cùng application ID, manifest Silver/OD và worker evidence; mọi phương án/tháng đều PASS.

Tính đúng của salting còn dựa trên giả thiết OD dimension duy nhất theo `(PULocationID, DOLocationID)`. Mỗi ND3 application chạy preflight uniqueness/null check và ghi OD manifest. File audit riêng tháng 01 tại `data/benchmarks/od_matrix_key_audit.csv` ghi nhận 58.747 dòng, 58.747 composite key phân biệt, không có nhóm null/trùng. Chi phí replication được báo riêng: tháng 01 OD materialized count 58.747 → 83.646 (+24.899), kèm shuffle read 326,47 MB.

### 3.4.4. So sánh Baseline, AQE và Salting Join theo hiệu năng và tài nguyên phân tán

So sánh cần dùng cùng dữ liệu và các lượt đo có correctness PASS, collector status OK, cấu hình và môi trường được xác minh. Các chỉ số gồm wall-clock duration, TSR/PSR nếu có, shuffle read/write, memory spill và disk spill. Tỷ số skew tính trên successful tasks của stage đã chọn theo quy tắc collector; shuffle/spill cộng trên các stage hoàn tất trong job group. Giá trị thiếu phải giữ là unavailable, không thay bằng 0.

Run official tier hiện lưu metadata/provenance, hai worker, manifest, task metrics, final plan, correctness audit và summary spill. Chỉ 9 lượt có collector `OK` và validation `PASS` được tổng hợp. Wall time là thời gian của lượt đo; kết luận cần giữ trong phạm vi tập dữ liệu và cấu hình này, không suy rộng thành ưu thế phổ quát của một phương pháp.

> **Kết quả Standalone mới (3 lượt/phương án, application `app-20261030073818-0003`):** median wall time Baseline 7,218 s, AQE 7,216 s, Salting S=8 8,448 s. Median shuffle read lần lượt 295,72 MB, 295,22 MB và 326,47 MB; TSR lần lượt 3,19, 1,03 và 2,92; PSR 2,06, 1,00 và 1,60. Memory spill median lần lượt 0, 480, 0 MB; disk spill median 0, 128,36, 0 MB. Cả 9 lượt PASS DuckDB trên hai worker. AQE có spill đáng kể trong lần chạy này; physical plan cần được đọc cùng run summary và không coi config AQE là bằng chứng skew split. OD replication được materialize: 58.747 → 83.646 hàng (+24.899). Kết quả chỉ áp dụng cho input/cấu hình đo.

> **[CHÈN BẢNG KẾT QUẢ 3.4 TẠI ĐÂY]** Lấy số liệu từ `data/benchmarks/benchmark_nd3_join_summary_official_standalone.csv` và các lượt hợp lệ từ `data/benchmarks/benchmark_nd3_join_runs_official_standalone.csv`; audit correctness ở `data/benchmarks/benchmark_nd3_join_audit_official_standalone.csv`.

### 3.5.4. Đối soát kết quả bằng DuckDB và xác nhận tính toàn vẹn sau tối ưu hóa

DuckDB được dùng làm đường tính độc lập, không dựa trên kết quả trung gian của các nhánh Spark. ND2 đối chiếu theo `PULocationID`: trip count phải bằng nhau; average fare và total miles được so với dung sai được đặt trong hàm kiểm chứng. ND3 đối chiếu theo cặp borough nguồn–đích: trip count, total fare, total miles, distinct pickup zones và coverage của các cặp. Đầu ra audit lưu sai khác từng nhóm để phát hiện thiếu, thừa hoặc nhân bản kết quả.

Trong benchmark ND3, OD dimension cần duy nhất theo composite key. Preflight chạy trong mỗi application ghi OD uniqueness và manifest; một lượt chỉ được tổng hợp khi collector status là OK và correctness validation là PASS. Tất cả các tháng official hiện có đều đủ provenance và PASS.

> **[CHÈN LƯU ĐỒ 3.5 TẠI ĐÂY]** Chuẩn bị input/manifest → warm-up từng phương án → ba lượt đo xoay thứ tự → thu metrics/plan → kiểm tra DuckDB → lọc run hợp lệ → tổng hợp median/mean/std. Đặt cạnh bảng run metadata và correctness audit của tier Standalone.

## 3.6. Phương pháp phân tích mật độ trả khách quanh ranh giới thu phí Manhattan

### 3.6.1. Bối cảnh Congestion Relief Zone và nghiên cứu liên quan

CRZ là khu vực Manhattan phía nam và bao gồm 60th Street, đồng thời có một số roadway/connection được loại trừ. Vì builder hiện tại gán các Taxi Zone citywide bằng đường tham chiếu 60th Street xấp xỉ và quy tắc diện tích 95%, kết quả được gọi là nhóm phân tích bắc/nam; không coi đó là lớp xác định zone nằm chính thức trong CRZ. Nguồn geofence chính thức và chi tiết cách dùng ranh giới được ghi ở `docs/chapter3_spatial_research_notes.md`.

Zhang, Sang và Wu phân tích chính sách bằng dữ liệu ride-hailing và subway giai đoạn 2024–2025, mô hình two-way fixed effects và khai thác khoản credit Lyft tạm thời để tách biệt phản ứng giữa các nền tảng. Abstract của bài báo cáo Uber giảm tối đa 6%, Lyft tăng 2–5% trong giai đoạn credit, tổng ride-hailing giảm 0,5–1,5% và subway tăng khoảng 1% theo thiết kế của họ. Đây là nghiên cứu SSRN preprint mà bản PDF ghi chưa qua peer review. Đồ án này khác về kỳ quan sát (01/2026), đơn vị tổng hợp (Taxi Zone × Day/Night), dữ liệu đầu ra và thiết kế: chỉ mô tả mật độ, không có kỳ trước chính sách/nhóm đối chứng để ước lượng tác động. Vì thế các ước lượng của bài Zhang et al. chỉ dùng làm bối cảnh và đối chiếu phương pháp, không dùng làm ground truth hay bằng chứng nhân quả cho số liệu Gold.

### 3.6.2. Xây dựng nhóm Taxi Zone theo phía đường tham chiếu

Các zone Manhattan được chia theo tỷ lệ diện tích polygon hai phía đường tham chiếu: tối đa 5% diện tích ở phía bắc là south-reference, tối đa 5% ở phía nam là north-reference; các polygon còn lại là ambiguous. 41 zone được xếp south-reference, 23 north-reference và 5 ambiguous. Non-Manhattan được giữ trong Gold nhưng gán outside.

### 3.6.3. Tổng hợp lượt trả khách theo zone và khung giờ

Drop-off counts được nhóm theo `DOLocationID` và `time_period` của Silver. Daytime là giờ 05:00–20:59; Nighttime là 21:00–04:59. Gold giữ đủ mỗi zone × hai kỳ, gồm cả các zone-period có count bằng 0.

Trên Gold tháng 01/2026, nhóm phía nam gồm 41 zone, phía bắc 23 zone và 5 zone biên không phân loại chắc chắn. Tổng số lượt trả khách nhóm phía nam là 3.135.311 ban ngày và 1.138.423 ban đêm; nhóm phía bắc là 1.395.133 và 489.236. Median mật độ theo zone (chuyến/km²) lần lượt là 129.375/48.825 phía nam và 44.414/19.030 phía bắc cho Daytime/Nighttime. Đây là khác biệt mô tả, chịu ảnh hưởng số zone, chức năng sử dụng đất, cấu trúc giao thông và diện tích. Không thể dùng nó để khẳng định tác động nhân quả của congestion pricing, vì chỉ quan sát một tháng sau chính sách, không có giai đoạn trước/nhóm đối chứng và trip zone không tương đương bản ghi đi vào CRZ.

### 3.6.4. Chuẩn hóa theo diện tích polygon

Diện tích tính từ polygon đã dissolve theo LocationID, sau khi chiếu sang EPSG:32618, đơn vị mét vuông và quy đổi km². Mật độ bằng trip count / polygon area (km²). Zone geometry invalid được sửa bằng `make_valid`; zone có area không dương được gắn density null thay vì thay bằng 0.

### 3.6.5. Kepler.gl và giới hạn phân tích mô tả

Gold hiện có map JSON Kepler.gl dạng `.gl` tại `data/gold/dropoff_density_by_zone_period.gl`, chứa hai polygon layer Daytime/Nighttime và dữ liệu zone-period nhúng. Bản đồ dùng các tỷ lệ density riêng của mỗi layer để tô màu tương tác; khi so sánh sắc độ giữa hai lớp nên chuyển sang shared scale trong Kepler UI hoặc dùng Figure 3.1 có chung P95 scale. Nguồn phương pháp, các giới hạn và tài liệu tham khảo được trình bày trong spatial research notes.

### 3.7. Tổng kết chương

Chương triển khai hai bài toán lệch tải trên Spark: aggregation ND2 và OD key-based join ND3. Selective/Uniform Salting đều bảo toàn kết quả theo DuckDB nhưng có wall time cao hơn baseline trên dải dữ liệu đã đo. AQE giảm TSR trong các run, nhưng final plan không thể hiện skew partition splitting; các run AQE đồng thời có memory/disk spill. Salting ND3 tạo overhead materialized từ 58.747 lên 83.646 OD rows tháng 01 (+24.899) và shuffle read cao hơn baseline. Phân tích mật độ drop-off cho thấy tập trung chuyến ban ngày cao ở nhóm south-reference; đây là mô tả theo Taxi Zone và không nhận diện quan hệ nhân quả với chính sách. Mọi kết luận hiệu năng giới hạn ở Spark 3.5.9, hai Worker, 200 shuffle partitions, năm tháng độc lập có quy mô gần nhau, ba lượt đo và pipeline tiền xử lý hiện tại.


## Danh sách artifact cần gắn khi hoàn thiện bản luận văn

- ND2: `benchmark_nd2_aggregation_runs_official_standalone.csv`, `benchmark_nd2_aggregation_summary_official_standalone.csv`, `benchmark_nd2_aggregation_audit_official_standalone.csv` và thư mục plan đúng tier.
- ND3: `benchmark_nd3_join_runs_official_standalone.csv`, `benchmark_nd3_join_summary_official_standalone.csv`, `benchmark_nd3_join_audit_official_standalone.csv` và plan cuối cho mỗi phương án/lượt.
- Spark UI hoặc REST executor evidence thể hiện ít nhất hai worker; AQE final plan; OD composite-key uniqueness audit; manifest/config của đúng run.
- Với các mức scaling, gắn cả run CSV và summary theo tháng; không trộn kết quả của các tháng độc lập thành một tập tích lũy.
