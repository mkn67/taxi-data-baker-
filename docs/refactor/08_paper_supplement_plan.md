# DETOUR — Kế hoạch thực nghiệm bổ sung cho paper

> **Cập nhật 28/11:** Phiên ablation 96 lượt và replication 32 lượt đã hoàn tất. Repo Malviya đã được kiểm tra tại commit `b62cc0ecfc275681a1052e3aa30f6a6778698d36`; xem [đối chiếu chi tiết](15_malviya_comparison_20261128.md). Các bảng dưới đây mô tả kế hoạch lịch sử; không chạy lại mặc định. Câu hỏi AQE–Salting, ablation coalescing, final plan và replication có trùng lặp đáng kể với Malviya; chưa tuyên bố novelty. Ưu tiên rà claim, provenance, phạm vi noise và viết lại paper. Mọi nghiên cứu E/C matched, fanout có output được kiểm soát hoặc nhiều host cần protocol và ngân sách riêng trước khi chạy.

Phạm vi là ND3 trên Spark 3.5.9. Bản IS405 đã có kết quả quan sát theo phạm vi từng phiên. Kế hoạch này bổ sung bằng chứng cho bản thảo nghiên cứu về coalescing, skew splitting và chi phí Selective Salting. Mỗi lượt mới dùng thư mục output riêng cùng giao thức, manifest và mã phiên.

## 1. Câu hỏi và dấu hiệu cần kiểm chứng

1. **RQ-P1.** Trên cùng workload OD, AQE coalescing thay đổi wall-time bao nhiêu? Khi final plan xác nhận có skew splitting, bật cơ chế này đem lại lợi ích bổ sung bao nhiêu?
2. **RQ-P2.** Khi hai phương án đều được phép coalescing, Selective Salting S=8 so với AQE có skew splitting thế nào? Chi phí của Salting thay đổi ra sao khi số hàng chiều thuộc khóa được salt tăng?

Chứng cứ hiện có giúp chọn thí nghiệm tiếp theo. Trên 12 ô synthetic v3, nhánh AQE có median thấp nhất và cả 60 final plan có coalescing. Lưới chưa ghi nhận skew split. Trên NYC S3, phân tích hậu nghiệm của 10 block cho tỷ số thời gian ghép cặp Salting/AQE khoảng 1,160, KTC bootstrap 95% [1,121; 1,187]. Ở ô synthetic `strong_large`, bảng chiều có 5.000.256 hàng nhưng Salting chỉ nhân thêm 91 hàng. Các kết quả này là mốc tham chiếu theo workload, cấu hình và phạm vi đo đã có.

Nguồn: [Chương 4](../../chapters/04_results_tradeoffs/manuscript/chapter4_results.md), [kiểm toán bằng chứng](02_evidence_audit.md), [lưới v3](evidence/cell_results.csv), [bộ sinh synthetic v3](../../scripts/chapter_03/generate_phase_c_synthetic.py).

## 2. Công việc theo thứ tự

| Giai đoạn | Loại | Công việc | Điều kiện để đi tiếp |
| --- | --- | --- | --- |
| 0. Rà tài liệu và khóa protocol | DOC + AUDIT | Đọc bản Malviya khi có toàn văn, ghi rõ thân bài và phiên bản. Lập bảng đối chiếu với Flow-Join++ và nghiên cứu gần nhất theo câu hỏi, workload, nhánh đối chứng, kiểm chứng final plan, chi phí và bằng chứng thực nghiệm. Chốt cấu hình, block, seed, metric, quy tắc bỏ lượt trước phiên xác nhận. | Có câu hỏi có thể kiểm chứng và tuyên bố đóng góp dự kiến có phạm vi cụ thể. Nếu thiếu Malviya, pilot kỹ thuật vẫn có thể tiến hành. Tuyên bố độ mới chờ đối chiếu toàn văn. |
| 1. Pilot xác nhận cơ chế | CODE + AUDIT | Thêm chế độ ablation riêng. Dùng một input nhỏ từ lưới v3 làm đối chứng coalescing, kiểm tra thêm một input có hot key tạo partition đủ lớn và có dữ liệu từ nhiều shuffle map task. Dùng tối đa 3 cấu hình pilot, tối đa 2 block/cấu hình. | Có một workload mà AQE chạy với coalescing, final plan giữ SMJ và chưa split, cùng một workload có skew split xác nhận được. Nếu hết ngân sách mà chưa split, dừng nhánh xác nhận skew splitting và báo điều kiện/tín hiệu đã đo. |
| 2. Đo xác nhận | CODE + AUDIT | Dùng hai workload đã khóa, sáu nhánh, tám block ghép cặp/workload. Chạy đủ các nhánh trong mỗi block theo thứ tự ngẫu nhiên đã lập trước, kiểm tra phân bố vị trí chạy giữa các nhánh. Tổng **96 lượt đo** cộng warm-up riêng. | Kết quả đúng theo DuckDB, có provenance cho toàn bộ block. Báo tỷ lệ final plan kích hoạt cơ chế theo nhánh/workload. Hiệu ứng của skew splitting cần đi cùng bằng chứng cơ chế trong contrast tương ứng. |
| 3. Kiểm tra chi phí nhân bản | BOTH | Sau khi đóng xong đối chứng cơ chế, giữ một workload có split, thay đổi lượng hàng chiều thuộc hot pickup được salt. Hai mức × hai nhánh chính × tám block, tối đa **32 lượt đo** nếu cả hai mức được đo trong đợt này. | Giữ output join như nhau, chứng minh số hàng nhân thêm thay đổi và ghi nhận mức thay đổi đủ để diễn giải shuffle, CPU và wall-time. Nếu chọn ra điều kiện chiến thắng hoặc ngưỡng thực hành, xác nhận trên seed hoặc workload đã giữ lại trước khi đưa ra quy tắc chọn. |
| 4. Viết phần bổ sung cho paper | DOC | Trình bày bảng paired ratios, final plan, biểu đồ phân vị task và chi phí nhân bản. Bản đồ phương án nhanh nhất có thể để phụ lục. | Các câu kết luận truy ngược được về phép đối chiếu và kết quả ghép cặp. Kết luận độ mới có bảng đối chiếu với các bài liên quan. |

Pilot và phiên xác nhận dùng các đầu ra tách riêng. Chọn workload pilot theo dấu hiệu kích hoạt cơ chế và khả năng hoàn tất trong ngân sách cụm. Kết quả thời gian pilot chỉ dùng chẩn đoán. Các đối chiếu xác nhận dùng dữ liệu thu sau khi đã khóa giao thức. Ngân sách mặc định là **96 lượt xác nhận cơ chế** rồi quyết định có thực hiện thêm **tối đa 32 lượt nhân bản** hay không. Lượt kiểm tra trên workload giữ lại chỉ phát sinh nếu paper muốn công bố một quy tắc chọn phương án.

## 3. Ma trận đối chứng

Mỗi nhánh dùng cùng input, cấu hình tài nguyên, số shuffle partitions ban đầu, pipeline aggregate, cơ chế materialize và đơn vị đo. Giữ SMJ nhất quán, kiểm tra theo final plan. Đặt ngưỡng broadcast tĩnh và thích ứng bằng `-1`. Giữ các cơ chế AQE khác cố định trong nhóm nhánh thích ứng. Ghi cấu hình hiệu lực theo lượt để bắt cấu hình còn sót từ nhánh trước.

| Nhánh | AQE | Coalescing | Skew optimization | Salting | Phép đối chiếu chính |
| --- | --- | --- | --- | --- | --- |
| A. SMJ hiện hành | Tắt | Tắt | Tắt | Tắt | Mốc tương thích với benchmark DETOUR cũ |
| A0. AQE mở, coalescing và skew tắt | Bật | Tắt | Tắt | Tắt | Mốc giữ AQE cố định để đo riêng tác động của việc bật coalescing |
| B. AQE coalescing | Bật | Bật | Tắt | Tắt | B/A0 đo phần thay đổi khi chỉ bật coalescing |
| C. AQE coalescing + skew | Bật | Bật | Bật | Tắt | C/B đo lợi ích bổ sung khi bật skew optimization trên nền coalescing |
| D. Salting S=8 hiện hành | Tắt | Tắt | Tắt | Bật | D/A đo Salting khi hai nhánh cùng chạy với AQE tắt |
| E. Salting S=8 + coalescing | Bật | Bật | Tắt | Bật | E/B đo phần tăng thêm do Salting và E/C so hai phương án trên cùng nền coalescing |

B/A mô tả hiệu ứng của gói cấu hình vì cả AQE và coalescing thay đổi. B/A0 giữ trạng thái AQE cố định. C/B đo skew optimization khi coalescing đang bật. Thiết kế này chưa ước lượng riêng tương tác giữa hai cơ chế ở trạng thái coalescing tắt.

**Gate skew split.** Kiểm tra cả ngưỡng byte tuyệt đối và ngưỡng tương đối theo median partition của từng phía, căn cứ thống kê Spark dùng ở shuffle map stage. Ưu tiên số đo trực tiếp theo phía và partition. Nếu collector hiện tại chỉ có shuffle-read tổng từ task, ghi đó là số liệu chẩn đoán có giới hạn và dựa vào final adaptive plan hoặc SQL metrics xác nhận việc chia partition. Ghi số partition split và số split thực hiện nếu có. Cờ cấu hình, tổng shuffle bytes, Zipf exponent và số task giảm đều đo những đại lượng riêng. Khi hết pilot mà final plan vẫn chưa split, kết quả B/C chỉ cho phép kết luận về hiệu ứng khi bật cấu hình trong phạm vi đó.

**Khóa yếu tố đối chiếu.** Cố định cả số executor và core/heap thực cấp, input Parquet, cách chia input file/map task, số partition ban đầu, skew factor, byte threshold, advisory partition size, cấu hình coalescing và warm-up. Dùng cùng danh sách hotspot bất biến và cùng S=8 cho D/E. Kiểm tra join type, key, phía shuffle và kết quả tổng hợp theo DuckDB. Xác nhận số partition split ở hai workload bằng final plan. Nếu plan thực thi khác giao thức ở một số block, công bố tỷ lệ và nguyên nhân. Phân tích theo cấu hình đã chỉ định giữ các lượt hợp lệ theo quy tắc đã khóa. Phân tích riêng các lượt có split, nếu làm, ghi rõ là chẩn đoán.

Nguồn cấu hình và điều kiện Spark: [Spark 3.5.9 — SQL Performance Tuning](https://spark.apache.org/docs/3.5.9/sql-performance-tuning.html).

## 4. Thiết kế thí nghiệm nhân bản

- Giữ nguyên fact, tập dimension key khớp fact và kết quả join. Cố định tổng số hàng chiều. Dịch chuyển một phần hàng không khớp của dimension giữa hai loại pickup: thuộc và ngoài danh sách salted. Dùng cặp OD duy nhất và giữ schema, kiểu dữ liệu, độ rộng payload và kiểu phân vùng file tương đương.
- Đo `dim_hot_rows`, số hàng chiều sau nhân bản và shuffle/CPU. Với một hàng chiều cho mỗi cặp OD, mức tăng dự kiến là `(S − 1) × dim_hot_rows`. Đối chiếu theo dữ liệu thực tế sau khi tạo cấu hình.
- Báo độc lập kích thước Parquet và khối lượng shuffle. Dù tổng số hàng chiều giống nhau, thay key có thể đổi phân bố hash partition hoặc dung lượng nén. So sánh lại các đại lượng này để giới hạn lời giải thích. Can thiệp này đo **chi phí nhân bản các hàng chiều** khi giữ kết quả join cố định. Nếu nghiên cứu thêm fanout của các khóa khớp, dùng một thiết kế riêng có kiểm soát cardinality đầu ra.

## 5. Phân tích và báo cáo

- **Primary metric** là wall-time của action đã materialize. Chốt trước các tỷ số theo block: B/A0, C/B và E/C là các đối chiếu chính, D/A và E/B giải thích phần chênh lệch. Báo median tỷ số và khoảng tin cậy bootstrap 95% bằng lấy mẫu lại cả block trong từng workload. Dùng biên thực hành ±5% từ Phase C. KTC nằm hoàn toàn dưới 0,95 hoặc trên 1,05 cho thấy vượt biên theo hướng tương ứng. KTC nằm trong [0,95; 1,05] cho phép mô tả chênh lệch nhỏ ở độ phân giải đã đặt. Các trường hợp KTC giao ranh giới được báo là chưa phân định theo biên này. Nếu dùng kiểm định nhiều contrast, chốt nhóm kiểm định và hiệu chỉnh trước khi chạy.
- **Metric cơ chế** gồm số task tại stage join tương ứng, median/p95/max task duration, CPU executor, partition size, shuffle read/write, disk/memory spill, số split, số hàng chiều nhân thêm và độ phủ metric. Truy nguyên stage bằng kế hoạch thực thi và Event Log. Giữ thiếu dữ liệu ở trạng thái thiếu. TSR/PSR từ các stage khác vai trò dùng để mô tả.
- **Chi phí hotspot** có hai chế độ sử dụng. Danh sách đã lưu dùng để so thời gian join thường xuyên. Nếu muốn tuyên bố lợi ích với lần chạy đầu, đo thêm chính pipeline phát hiện hotspot rồi chạy join trong cùng giao thức. Nếu chỉ có hai số đo riêng, báo tổng ước lượng hoặc chi phí phân bổ theo số lần tái dùng.
- **Kết luận âm tính** có giá trị nếu phép thử xác nhận được cơ chế và phạm vi. Nếu AQE có split và vẫn nhanh hơn, báo chênh lệch cùng mức skew, số hàng nhân bản và tài nguyên đã đo. Nếu Salting thắng ở một điều kiện, chỉ mở rộng thành quy tắc sau kiểm tra độc lập. Nếu C và B gần nhau dù có split, báo khoảng bất định và chi phí của việc chia partition ở điều kiện đó.

## 6. File nào sửa khi triển khai

| File hoặc đường dẫn | Loại | Thay đổi dự kiến |
| --- | --- | --- |
| [`benchmark_engine.py`](../../scripts/chapter_03/benchmark_engine.py) và [`run_benchmark_nd3.py`](../../scripts/chapter_03/run_benchmark_nd3.py) | CODE | Thêm chế độ riêng cho ma trận ablation, set tường minh cờ theo nhánh, log cấu hình hiệu lực. Hành vi mặc định của các lệnh cũ tiếp tục phục vụ benchmark IS405. |
| [`generate_phase_c_synthetic.py`](../../scripts/chapter_03/generate_phase_c_synthetic.py) | CODE | Mở rộng bộ sinh cho workload kiểm chứng split và can thiệp số hàng chiều được salt. Ghi bộ dữ liệu mới theo manifest riêng có checksum và thông số sinh. |
| [`analyze_phase_c_paired.py`](../../scripts/chapter_03/analysis/analyze_phase_c_paired.py) | CODE + AUDIT | Tái sử dụng phân tích ghép cặp cho bộ contrast mới. Kiểm tra alignment theo block, phạm vi stage, plan và cấu hình trước khi xuất bảng. |
| `data/benchmarks/phase_c/` | AUDIT | Lưu thư mục mới theo phiên với protocol version, seed, raw CSV, final plans, Event Logs hoặc đường dẫn truy nguyên và DuckDB audit. |
| [Sổ thực nghiệm](01_experiment_registry.md), [kiểm toán](02_evidence_audit.md), [dàn ý paper](07_paper_outline.md) | DOC | Chỉ cập nhật số liệu sau nghiệm thu phiên xác nhận. Gắn từng claim với contrast và workload. Bản Word/PDF IS405 đã phát hành tiếp tục là mốc của đợt consolidation. |

## Đầu ra tối thiểu

- Biên bản pilot kèm bằng chứng một workload có skew split và một workload chỉ thấy coalescing, hoặc kết luận pilot chưa kích hoạt được split trong ngân sách.
- Bảng đối chứng ghép cặp có cấu hình, final plan và tỷ số wall-time. Nếu qua giai đoạn 3, thêm bảng số hàng chiều thực sự nhân bản và chi phí.
- Bảng đối chiếu nghiên cứu liên quan, với mục Malviya chỉ đánh dấu đã đối chiếu sau khi có bản thảo và phiên bản xác định. [Flow-Join++](https://doi.org/10.2197/ipsjjip.34.671) là mốc về nhận diện heavy hitter dựa trên cardinality.

Trạng thái triển khai và các gate còn mở được ghi trong [`09_paper_supplement_execution_log.md`](09_paper_supplement_execution_log.md).
