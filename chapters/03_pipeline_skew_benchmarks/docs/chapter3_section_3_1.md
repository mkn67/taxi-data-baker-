# 3.1. Kiến trúc pipeline và mô hình hóa bài toán lệch tải

## 3.1.1. Kiến trúc tổng thể và luồng xử lý dữ liệu phân tán trên Apache Spark

Nghiên cứu tổ chức dữ liệu theo luồng từ dữ liệu chuyến đi thô đến hai nhánh phân tích. Ở lớp đầu vào, dữ liệu HVFHV tháng 01/2026 được làm sạch và lưu thành các tệp Parquet Silver. Tập Silver này là đầu vào thống nhất cho hai thực nghiệm Spark: ND2 tổng hợp theo vùng đón khách (`PULocationID`), còn ND3 nối chuyến đi với bảng chiều ma trận nguồn–đích (OD) theo cặp (`PULocationID`, `DOLocationID`). Bảng OD cung cấp thuộc tính vùng cho từng cặp, từ đó kết quả join có thể được tổng hợp theo cặp quận nguồn–đích. Danh sách vùng đón khách nóng được tính từ phân bố số chuyến theo `PULocationID` và được dùng nhất quán trong nhánh Selective Salting của ND2 và Salting Join của ND3.

![Hình 3.1 — Sơ đồ kiến trúc và luồng dữ liệu](../../../data/benchmarks/chapter3_pipeline_architecture.png)

![Hình 3.2 — Topology Spark Standalone được khai báo](../../../data/benchmarks/chapter3_spark_topology.png)

Trong ND2, Spark phân phối các bản ghi theo khóa tổng hợp. Baseline thực hiện `groupBy(PULocationID)`; hai phương án salting tạo khóa trung gian `(PULocationID, salt)`, tổng hợp từng nhóm con rồi gộp lại theo `PULocationID`. Trong ND3, dữ liệu chuyến được join với OD dimension trên hai khóa vùng. Ba nhánh được so sánh là Sort-Merge Join cơ sở, Sort-Merge Join có AQE skew handling và Salting Join với số salt bằng 8. Việc materialize kết quả buộc Spark thực thi phép biến đổi trước khi thu các bảng tổng hợp nhỏ về driver.

Sau mỗi nhánh, kết quả tổng hợp được so sánh với phép tính độc lập bằng DuckDB. Dữ liệu chi tiết vẫn được xử lý phân tán trong Spark; chỉ bảng tổng hợp theo vùng hoặc borough-pair được đưa về driver để đối chiếu. Phân tích mật độ điểm trả khách là một nhánh riêng: DuckDB đếm chuyến theo vùng/kỳ, còn GeoPandas xử lý hình học vùng. Vì vậy, kết quả không gian không được trình bày như kết quả thực thi phân tán của Spark.

## 3.1.2. Phân biệt mức độ tập trung dữ liệu tự nhiên, cân bằng khối lượng dữ liệu và lệch tải tính toán

Ba khái niệm cần được tách biệt khi diễn giải hiện tượng lệch tải. Thứ nhất, **mức độ tập trung dữ liệu tự nhiên** mô tả phân bố của đại lượng nghiên cứu theo khóa nghiệp vụ. Với ND2, số chuyến có thể tập trung không đồng đều giữa các `PULocationID`; đây là đặc điểm của nhu cầu đi lại và vị trí địa lý trong dữ liệu. Một vài vùng có số chuyến lớn chưa tự nó chứng minh rằng Spark đã tạo ra một tác vụ bị nghẽn.

Thứ hai, **cân bằng khối lượng dữ liệu giữa partition** mô tả số bản ghi hoặc lượng dữ liệu mà mỗi partition nhận sau khi phân vùng. Phân bố khóa nghiệp vụ và cách partition/shuffle tương tác với nhau để quyết định lượng dữ liệu ở từng partition. Nhiều khóa nhỏ có thể được đặt cùng một partition; một khóa lớn có thể chi phối partition chứa khóa đó. Do vậy, tần suất khóa và kích thước partition có liên quan nhưng không đồng nhất.

Thứ ba, **lệch tải tính toán** là chênh lệch công việc thực hiện được quan sát giữa các task: một số task mất nhiều thời gian hơn đáng kể, xử lý nhiều bản ghi hơn, đọc/ghi shuffle lớn hơn hoặc phát sinh spill nhiều hơn. Đây là biểu hiện ở cấp thực thi. Thời lượng task còn chịu ảnh hưởng của tài nguyên executor, I/O, garbage collection và cạnh tranh tài nguyên; vì vậy cần đọc PSR/TSR cùng shuffle, spill và trạng thái collector, thay vì suy ra lệch tải chỉ từ biểu đồ tần suất khóa.

Trong ND3, khóa join là cặp OD chứ không phải riêng vùng đón. Bảng chiều OD có quy mô nhỏ hơn dữ liệu chuyến, còn mỗi khóa OD có thể xuất hiện với tần suất khác nhau trong chuyến đi. Vì vậy, phân bố số chuyến theo pickup zone giúp xác định tập ứng viên cho salting, nhưng không thay thế việc kiểm tra phân bố task của chính phép join. Bài toán nghiên cứu là đánh giá liệu các biện pháp chia nhỏ công việc có cải thiện phân bố thực thi và thời gian trong các cấu hình thử nghiệm hay không; không mặc định rằng mọi tập trung tự nhiên đều gây ra skew xử lý.

## 3.1.3. Các chỉ số định lượng độ lệch tải (PSR, TSR) và tiêu chí nhận diện khóa nóng H

Thực nghiệm sử dụng hai tỷ số max/median để mô tả độ lệch tại task. **Task Skew Ratio (TSR)** được tính bằng thời lượng task thành công lớn nhất chia cho trung vị thời lượng task thành công. Thời lượng lấy từ `executorRunTime` theo mili giây; nếu metric này không có thì dùng thời lượng task do Spark ghi nhận. **Partition Skew Ratio (PSR)** được tính bằng số bản ghi lớn nhất chia cho trung vị số bản ghi trên các task thành công. Bộ thu thập chọn metric bản ghi chung đầu tiên có sẵn theo thứ tự: shuffle read records, shuffle write records, input records, rồi output records. PSR chỉ được báo cáo khi metric được chọn có ở mọi task thành công.

Để hai tỷ số có phạm vi nhất quán, bộ thu thập chọn stage đã hoàn thành có tổng shuffle read và write lớn nhất trong job group; nếu bằng nhau thì chọn stage có executor runtime lớn hơn. TSR và PSR được tính trên các task thành công của stage đó. Tổng shuffle và spill được cộng trên các stage đã hoàn thành của job group. Nếu metric không thu thập được, không tồn tại task thành công phù hợp hoặc mẫu số bằng 0, giá trị được để trống/không khả dụng, không thay bằng 0. Vì TSR/PSR là tỷ số max/median, chúng mô tả độ chênh tương đối trong stage đã chọn, không phải tỷ lệ phần trăm và cũng không xác định nguyên nhân gây chênh lệch.

Ký hiệu **H** dùng cho tập khóa nóng pickup được chọn bằng ngưỡng P95 chính xác. Trước hết, đếm số chuyến theo từng `PULocationID`, sắp tăng dần các số đếm có giá trị, rồi lấy phần tử ở hạng gần nhất `ceil(0,95 × n)` làm ngưỡng `T95`, trong đó `n` là số khóa pickup có số đếm hợp lệ. Tập khóa được định nghĩa:

> **H = {k : count(k) ≥ T95}.**

Cách lấy mọi khóa bằng hoặc lớn hơn ngưỡng giữ lại các trường hợp đồng hạng tại P95. Với Silver hiện có, audit độc lập ghi nhận 261 nhóm pickup khác null, `T95 = 194.522` chuyến và 14 khóa thuộc H: 7, 37, 61, 76, 79, 112, 132, 138, 161, 181, 230, 231, 234 và 255. Đây là tiêu chí vận hành được dùng trong thí nghiệm này, không phải một ngưỡng phổ quát cho mọi bộ dữ liệu.

> **[CHÈN HÌNH 3.3 TẠI ĐÂY — Phân bố số chuyến theo PULocationID và ngưỡng P95]**  
> Vẽ số chuyến theo vùng pickup, đánh dấu đường `T95` và tô màu các khóa H. Ghi rõ đây là phân bố tần suất khóa nghiệp vụ; không gọi trực tiếp là biểu đồ task skew.

## 3.1.4. Thiết kế các nhánh đối chứng, ma trận thực nghiệm và các biến kiểm soát

Thiết kế sử dụng baseline và hai nhánh xử lý cho mỗi bài toán. Ma trận phương pháp được tóm tắt ở Bảng 3.1.

| Bài toán | Nhánh đối chứng | Nhánh xử lý 1 | Nhánh xử lý 2 |
|---|---|---|---|
| ND2 — aggregation theo `PULocationID` | Baseline `groupBy` trực tiếp | Uniform Salting, S=8 | Selective Salting, S=8; chỉ khóa trong H được chia salt |
| ND3 — join theo (`PULocationID`, `DOLocationID`) | Sort-Merge Join, tắt AQE và broadcast | Spark AQE skew join, bật adaptive skew handling | Salting Join, S=8; chia salt cho chuyến có pickup thuộc H và nhân bản hàng OD tương ứng |

Ở ND2, cả ba nhánh tính cùng các đại lượng đầu ra: số chuyến, giá vé cơ sở trung bình và tổng quãng đường. Uniform Salting chia mọi khóa thành tám bucket; Selective Salting chỉ chia các khóa trong H, còn khóa khác giữ salt 0. Hai nhánh dùng tổng hợp hai giai đoạn rồi gộp về vùng pickup. Ở ND3, cả ba nhánh dùng cùng dữ liệu Silver, OD dimension, khóa join và inner-join semantics. Nhánh AQE dùng cấu hình skew factor 2 và ngưỡng 16 MiB theo runner. Nhánh salting gán một salt cho mỗi chuyến liên quan đến H và tạo các bản sao OD dimension tương ứng cho salt 0–7. Tính đúng đắn được kiểm tra bằng các tổng hợp borough-pair so với DuckDB; tính duy nhất của khóa OD cần được lưu cùng bằng chứng của lần chạy được báo cáo.

Các biến được giữ cố định trong mỗi bộ so sánh gồm tập Silver đầu vào, bảng OD ở ND3, danh sách H, số partition shuffle (mặc định 200), số salt (8), biểu thức đầu ra và quy tắc materialize. Baseline ND3 tắt AQE/broadcast để làm nhánh Sort-Merge Join đối chứng; nhánh AQE thay đổi cấu hình thích ứng được nghiên cứu; salting thay đổi khóa/replication theo thiết kế. Mỗi phương pháp có warm-up riêng trước các lượt đo. Runner full thực hiện ba lượt đo và xoay thứ tự phương pháp qua các lượt để giảm ảnh hưởng do thứ tự chạy. Kết quả từng lượt cần gắn với application ID, Spark version, master, executor/worker evidence, manifest dữ liệu, cấu hình thực tế và plan tương ứng trước khi dùng để kết luận hiệu năng.

**Bảng 3.1. Ma trận thực nghiệm và các biến kiểm soát**

| Bài toán/phương án | Đầu vào và khóa | Cấu hình Spark chính | Salt/AQE | Warm-up và lượt đo | Thứ tự đo | Kiểm tra đúng đắn | Chỉ số ghi nhận |
|---|---|---|---|---|---|---|---|
| ND2 — Baseline GroupBy | Silver 01/2026; `PULocationID` | `spark.sql.shuffle.partitions=200` mặc định; AQE tắt trong runner ND2 | Không salting | 1 warm-up; full mode 3 lượt | Xoay thứ tự giữa 3 phương án theo lượt | Count, fare trung bình, tổng miles đối chiếu DuckDB theo pickup zone | Wall time, TSR, PSR nếu có, shuffle read/write, memory/disk spill |
| ND2 — Uniform Salting | Cùng Silver và khóa với baseline | Cùng số shuffle partition; AQE tắt | S=8; mọi pickup key được chia bucket | 1 warm-up; full mode 3 lượt | Xoay thứ tự | Như baseline | Như baseline |
| ND2 — Selective Salting | Cùng Silver; khóa nóng H từ artifact P95 | Cùng số shuffle partition; AQE tắt | S=8; chỉ key trong H được chia, key khác salt 0 | 1 warm-up; full mode 3 lượt | Xoay thứ tự | Như baseline; kèm tập H/threshold | Như baseline |
| ND3 — Baseline Sort-Merge Join | Silver + OD dimension; join `(PULocationID, DOLocationID)` | AQE tắt; broadcast tắt (`autoBroadcastJoinThreshold=-1`); shuffle partitions mặc định 200 | Không salting | 1 warm-up; full mode 3 lượt | Xoay thứ tự giữa 3 phương án theo lượt | Count, fare, miles, distinct pickup zone và pair coverage đối chiếu DuckDB | Wall time, TSR, PSR nếu có, shuffle read/write, memory/disk spill |
| ND3 — Spark AQE Skew Join | Cùng Silver, OD và composite key | AQE bật; broadcast tắt; skew factor 2; threshold 16 MiB; shuffle partitions mặc định 200 | AQE skew join | 1 warm-up; full mode 3 lượt | Xoay thứ tự | Như baseline; lưu executed final plan để xác định có skew split | Như baseline; thêm AQE config và trạng thái plan |
| ND3 — Salting Join | Cùng Silver, OD và composite key | AQE skew join tắt; broadcast tắt; shuffle partitions mặc định 200 | S=8; salt chuyến thuộc H, nhân bản hàng OD tương ứng | 1 warm-up; full mode 3 lượt | Xoay thứ tự | Như baseline; kiểm tra OD key uniqueness cùng manifest run | Như baseline; theo dõi thêm replication overhead |

*Ghi chú:* cấu hình `200` là mặc định của runner và có thể đổi qua tham số `--partitions`; các run scaling dưới đây dùng 200. Full mode gồm một warm-up và ba lượt đo, xoay thứ tự phương án. ND3 dùng `MERGE` hint và lưu physical plan. Hash manifest nhận diện bộ tệp qua đường dẫn tương đối, kích thước và thời gian sửa đổi, không phải checksum nội dung.

## 3.1.5. Thiết kế thực nghiệm theo nhiều quy mô dữ liệu và giới hạn suy rộng kết quả từ quy mô nhỏ

Thực nghiệm scaling dùng năm tập dữ liệu tháng độc lập từ 01 đến 05/2026, không phải các tập tích lũy. Cần phân biệt **raw** và **Silver**: raw là tệp Parquet đầu vào trước bước làm sạch; Silver là dữ liệu đã qua tiền xử lý và mới là đầu vào của benchmark ND2/ND3. Số dòng và dung lượng raw chỉ mô tả quy mô dữ liệu nguồn, không phải kích thước đầu vào benchmark. Mỗi tháng được làm sạch bằng cùng quy tắc để tạo Silver, tính lại hotspot P95 nearest-rank từ Silver tháng đó, và tạo OD dimension tháng tương ứng cho ND3. Cùng cấu hình Spark 3.5.9 Standalone, hai Worker, 200 shuffle partitions, một warm-up và ba lượt đo mỗi phương án được giữ cố định. Mỗi tháng chạy đủ ba nhánh ND2 và ba nhánh ND3; tất cả 90 lượt đo có collector `OK`, DuckDB validation `PASS`, và worker evidence cho `spark-worker-1` cùng `spark-worker-2`.

![Hình 3.4 — Quy mô dữ liệu raw theo tháng](../../../data/benchmarks/figure_3_4_data_scale.png)

![Hình 3.5 — Thời gian benchmark ND2/ND3 theo tháng](../../../data/benchmarks/figure_3_5_benchmark_scaling.png)

Hình 3.4 mô tả kích thước tệp raw và số dòng raw; Hình 3.5 trình bày thời gian trung vị theo phương án, với độ lệch chuẩn của ba lượt đo. Kết quả cho thấy trên cả năm tháng, ND2 baseline có thời gian thấp hơn hai phương án salting; selective salting giảm shuffle write so với uniform salting nhưng chi phí tổng hợp hai giai đoạn vẫn làm thời gian cao hơn baseline. Với ND3, salting có median wall time cao nhất ở cả năm tháng. AQE gần baseline nhưng có memory spill median 480–600 MiB và disk spill median khoảng 128–161 MiB; các final plan ghi nhận AQE coalescing nhưng không có skew-specific partition split. Đây là mô tả kết quả trong benchmark hiện tại, không phải khẳng định ưu thế tổng quát.

Ngay cả khi có nhiều mức dữ liệu, khả năng suy rộng vẫn bị giới hạn bởi số tháng quan sát, phân bố vùng và OD của từng tháng, số worker/executor, partitioning, cache/I/O, phiên bản Spark và cấu hình tài nguyên. Một kết quả tốt trên quy mô nhỏ có thể thay đổi khi shuffle vượt bộ nhớ, khi tần suất khóa thay đổi hoặc khi chi phí replication của salting tăng. Vì vậy, báo cáo cần trình bày đầy đủ số lượt lặp, độ phân tán giữa các lượt, tài nguyên và metric còn thiếu; tách rõ kiểm tra chức năng chạy local khỏi benchmark phân tán trên Standalone. Không đưa số liệu giả định vào các ô còn thiếu; chỉ bổ sung kết luận sau khi có run artifacts cùng provenance.

**Bảng 3.2. Quy mô đầu vào và kết quả benchmark scaling theo tháng**

| Tháng | Raw rows | Raw size (MiB) | Silver rows | Silver size (MiB) | ND2 median (Baseline / Selective / Uniform), s | ND3 median (Baseline / AQE / Salting), s | Run evidence |
|---|---:|---:|---:|---:|---|---|---|
| 2026-01 | 20.940.373 | 482,4 | 19.993.097 | 324,8 | 1,214 / 3,064 / 3,638 | 7,218 / 7,216 / 8,448 | 18/18 method-runs PASS; 2 workers |
| 2026-02 | 19.875.686 | 454,7 | 19.045.927 | 308,8 | 1,411 / 2,943 / 3,377 | 7,040 / 7,502 / 8,779 | 18/18 method-runs PASS; 2 workers |
| 2026-03 | 22.058.358 | 507,9 | 21.094.362 | 345,7 | 1,410 / 2,959 / 3,417 | 7,343 / 8,085 / 9,359 | 18/18 method-runs PASS; 2 workers |
| 2026-04 | 20.995.953 | 486,2 | 20.038.336 | 327,1 | 1,410 / 2,775 / 3,400 | 7,078 / 7,496 / 8,862 | 18/18 method-runs PASS; 2 workers |
| 2026-05 | 22.125.744 | 512,7 | 21.092.006 | 345,2 | 1,114 / 2,545 / 3,092 | 7,585 / 7,686 / 10,054 | 18/18 method-runs PASS; 2 workers |

*Nguồn:* raw statistics từ `data/benchmarks/figure_3_4_data_scale.csv`; Silver rows/size và run metrics/provenance từ `data/benchmarks/scaling/monthly_benchmark_summary.csv`. Dung lượng được tính bằng byte chia cho 1.048.576 nên đơn vị chính xác là MiB. Raw chỉ mô tả tệp nguồn; benchmark đọc Silver. Các thời gian là median của ba lượt hợp lệ; SD, TSR, PSR, shuffle, spill, application ID và manifest có trong CSV summary/run artifacts theo tháng. Mỗi tháng chạy 9 lượt ND2 và 9 lượt ND3 (ba phương án × ba lượt đo), không tính warm-up.

Các tháng này có Silver khoảng 19,0–21,1 triệu dòng, nên phạm vi kích thước chỉ thay đổi hẹp (khoảng 11% từ nhỏ nhất đến lớn nhất). Chúng cho phép kiểm tra độ ổn định trên các tháng riêng, nhưng chưa đủ để suy rộng tới tập dữ liệu lớn hơn nhiều, nhiều tháng gộp, cụm khác hoặc dữ liệu có phân bố skew khác. Mỗi điểm cũng chỉ có ba lượt đo và có thể chịu ảnh hưởng trạng thái hệ thống/I/O; cần xem SD và metrics phân tán cùng median.
