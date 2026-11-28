# Bản thảo paper — phương pháp và kết quả thực nghiệm

**Trạng thái:** Bản thảo làm việc; chưa xác định venue. Đã đọc PDF và đối chiếu repo Malviya commit `b62cc0ecfc275681a1052e3aa30f6a6778698d36`, thấy trùng lặp đáng kể về câu hỏi và ablation; chưa có raw results/Event Logs để kiểm chứng độc lập kết quả của họ. Cần viết related work chính xác và chưa khẳng định tính mới trên toàn bộ literature. Các số liệu dưới đây phải giữ cùng phạm vi phiên và không gộp mẫu giữa các phiên.

## Tên tạm

**When Skew Mitigation Does Not Pay Off: A Controlled Study of Spark Join Strategies under Resource Constraints**

Tên này là định vị dự kiến. Nó không tuyên bố rằng Salting luôn bất lợi hoặc rằng một thuật toán mới đã được đề xuất.

## Câu hỏi và phạm vi

Nghiên cứu xem xét (1) hiệu năng của các chiến lược Spark đối với phép nối theo khóa OD và (2) mức thay đổi wall-time khi điều khiển coalescing, skew optimization và số dòng dimension thuộc tập salting. Các phép nối là inner equi-join theo `(PULocationID, DOLocationID)`; chúng không phải geometric spatial joins. Thời gian chính tính cho action join và aggregate được materialize, không tự bao gồm chi phí phát hiện hotspot.

## Phương pháp

Các phép đo sử dụng Spark Standalone 3.5.9 với hai worker trên cùng một host. Số core và heap được ghi theo phiên; không coi hai worker là hai máy vật lý. Danh sách hotspot được xác định từ phân bố pickup bằng P95 và Selective Salting dùng S=8. Các final physical plan, Event Log và DuckDB audit được giữ cùng application ID và input manifest.

### Đối chứng trên NYC S3

Trên dữ liệu NYC S3 tích lũy, một phiên thực hiện 10 block ghép cặp với các phương án join. Kết quả được phân tích trong cùng phiên; cấu hình auto-strategy và các plan được kiểm tra riêng. Nhánh Spark tự chọn không phải triển khai Flow-Join++. Số đo chi phí phát hiện hotspot được lấy từ job riêng và, khi cộng vào wall-time join, chỉ tạo tổng ước lượng.

### Ablation cơ chế AQE

Phiên `app-20261125142144-0001` gồm 2 workload synthetic và 6 nhánh: SMJ AQE tắt (A); AQE bật nhưng coalescing và skew tắt (A0); AQE + coalescing (B); AQE + coalescing + skew optimization (C); Salting S=8 với AQE tắt (D); Salting S=8 với AQE + coalescing, skew tắt (E). Mỗi workload có 8 block, mỗi block gồm đủ sáu nhánh, tổng 96 action đo. So sánh B/A0, C/B và E/C được tính theo block trong từng workload. Nhánh C của workload 10M có marker skew split ở 8/8 final plan; nhánh C của workload 2M không có marker này trong 8/8 plan.

DuckDB được chạy lại 12 lần tương ứng 2 workload × 6 phương án; trạng thái PASS được gán cho các dòng đo thuộc cặp đó. Nó không phải 96 phép so khớp output action độc lập. Event Log cho phép đối chiếu 96 target stage với SQL execution đo và join codegen; TSR tính từ task thành công của stage được chọn. Shuffle và spill là tổng stage collector của action.

### Can thiệp chi phí nhân bản dimension

Phiên `app-20261128072824-0002` giữ fact synthetic 10M và 5.000.256 dòng dimension, đồng thời thay đổi khóa của các dòng filler không khớp fact. Hai mức có lần lượt 13 và 100.013 dòng dimension thuộc hotspot; các dòng khớp fact và giá trị của chúng giữ nguyên. Phép can thiệp không thay fanout của các khóa khớp và có thể đổi đặc tính nén/hash.

Mỗi trong 8 block gồm B và E tại cả hai mức, với thứ tự xác định từ seed. Tổng 32 action đo; output của chính từng action được so với DuckDB ngoài thời gian đo. Các tỷ số E/B thấp, E/B cao và `(E/B cao)/(E/B thấp)` được tính theo block. Khoảng percentile bootstrap 95% lấy mẫu lại block; CI không hiệu chỉnh đồng thời.

## Kết quả

### Coalescing và skew optimization

Trên workload synthetic 10M, C/B có median tỷ số 0,918 và CI [0,851; 0,989]. C có skew split trong 8/8 final plan, trong khi B không có marker split ở 8/8 plan. Kết quả nghiêng về C có wall-time thấp hơn; CI vẫn cắt ranh 0,95 nên chưa xác lập mức cải thiện vượt ngưỡng thực hành 5%. Không suy diễn kết quả này cho workload NYC S3, nơi các plan đã kiểm toán chưa thể hiện skew split.

### Dimension replication

Operator Generate ở E phát ra 5.000.347 dòng tại mức thấp và 5.700.347 dòng tại mức cao, được xác nhận ở từng lượt E. Median E/B là 1,0096 với CI [0,9852; 1,0424] tại mức thấp và 1,0063 với CI [0,9243; 1,0477] tại mức cao. Median ratio-of-ratios là 0,9972 với CI [0,9381; 1,0632]; khoảng này cắt hai biên ±5%, nên chưa phân định được mức thay đổi chi phí wall-time tương đối. E có median shuffle read tăng từ khoảng 243,24 triệu lên 253,02 triệu byte giữa hai mức; số liệu là tổng stage action, không đo NIC vật lý hay riêng operator join.

## Thảo luận và giới hạn

Trong một phiên đối chứng riêng gồm tám block, hai action cùng nhánh B có trung vị `abs(Y/X − 1)` 4,63% và bốn cặp vượt 5%. Đây là mô tả trên một workload, một application và một host; không áp dụng trực tiếp làm noise floor hay CI cho phiên 96/32 lượt. Chi tiết audit tại `19_same_plan_diagnostic_8blocks_20261128.md`.

Đối chứng B/A0 và C/B làm rõ khác biệt giữa cấu hình được bật và cơ chế quan sát được trên plan. Kết quả cho thấy không nên dùng TSR thấp hơn làm bằng chứng duy nhất rằng thời gian action giảm. Trong can thiệp dimension, việc tăng số dòng phát ra bởi Generate không đi kèm bằng chứng đủ rõ về thay đổi vượt 5% trong wall-time tương đối. Điều này không chứng minh replication miễn phí; hai mức, một bộ sinh, tám block và một host chỉ hỗ trợ suy luận trong điều kiện đo.

Không so ghép E của phiên replication với C của phiên ablation vì input dimension và phiên khác nhau. Không khẳng định Salting thắng C hoặc đưa ra ngưỡng lựa chọn. Kiểm định Wilcoxon–Holm của phiên 96 action được nhóm sau khi chạy; CI không hiệu chỉnh đồng thời. PDF Malviya đã nghiên cứu AQE so với Salting, tách coalescing và kiểm tra kích hoạt split; không tuyên bố những điểm này mới một cách tổng quát. Chưa benchmark trực tiếp Flow-Join++/CIBPartitioner và chưa xác minh đầy đủ literature, nên không tuyên bố novelty hoặc ưu thế so với chúng.

## Bằng chứng và tài liệu cần hoàn thiện

- NYC S3 và audit chính: `01_experiment_registry.md`, `02_evidence_audit.md`.
- Confirmation 96 action: `09_paper_supplement_execution_log.md`, `10_confirmation_evidence_audit.md`, application `app-20261125142144-0001`.
- Replication 32 action: `11_replication_experiment_execution_20261128.md`, application `app-20261128072824-0002`; `runs.csv`, `paired_analysis.csv`, `generate_metric_audit.csv`, `audit_io/`.
- Đối chiếu tác phẩm tham khảo: `12_related_work_comparison_20261128.md`; kiểm tra trang, phiên bản và DOI theo venue trước khi nộp.
- TODO: đối chiếu checksum PDF đã nhận với PDF trong repo, kiểm provenance 2M trong giới hạn raw sẵn có; viết abstract/introduction/related work có nguồn; quyết định venue, giới hạn trang và bố cục figures; reviewer rà số liệu và kết luận.
