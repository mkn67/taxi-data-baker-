# Dàn ý bài báo từ DETOUR

Tập trung vào câu hỏi khi nào xử lý lệch tải thay đổi thời gian của action join và aggregate trên Apache Spark. Phạm vi là nghiên cứu thực nghiệm trên workload OD NYC và các workload tổng hợp; thời gian không tự bao gồm tạo hotspot hay toàn bộ pipeline. Các phiên khác nhau được trình bày tách biệt.

| Phần | Nội dung và bằng chứng |
|---|---|
| Tóm tắt | Nêu action được đo, hai Worker cùng host, kết quả S3 và hai thực nghiệm bổ sung; nhấn mạnh khoảng bất định và không khẳng định Salting thắng AQE. Lựa chọn số liệu sau khi rà đầy đủ artifacts, không gộp phiên. |
| Mở đầu | Câu hỏi khi nào chia khóa làm đổi chi phí công việc, khi nào đổi chiến lược nối làm giảm wall-time. Định danh đóng góp là giao thức, bộ dữ liệu và phân tích theo kế hoạch vật lý. |
| Nghiên cứu liên quan | [Bảng đối chiếu](12_related_work_comparison_20261128.md): CIBPartitioner, Flow-Join++, MFTune, Zhang et al. và Malviya; Spark 3.5.9 SQL tuning. [Malviya](15_malviya_comparison_20261128.md) đã khảo sát AQE/salting và ablation coalescing; chưa khẳng định novelty tổng quát. |
| Hệ thống và phương pháp | NYC S3 10 block; synthetic sáu nhánh 96 action; replication B/E, hai mức dimension và 32 action. Đối soát DuckDB: 96 lượt xác nhận cấp workload/phương án; 32 lượt kiểm tra chính output action. Mô tả phân tích block, CI và protocol của từng phiên. |
| Kết quả | S3 so sánh các chiến lược; synthetic 96 action phân biệt coalescing/split; replication 32 action đo chi phí E/B ở hai mức. Báo marker final plan, ratio và CI; hình lưới ban đầu có thể là supplementary vì mốc luôn chọn AQE cho cùng kết quả ở ô giữ lại. |
| Thảo luận và giới hạn | Tài nguyên cấp phát khác nhau giữa ứng dụng; hai Worker cùng host, tám block trong một application, CI chưa hiệu chỉnh đồng thời, hiệu ứng hash/nén của filler và lựa chọn Holm sau khi chạy. Thiếu so sánh trực tiếp Flow-Join++, CIBPartitioner và Malviya. |
| Kết luận | Trình bày điều kiện cần thử nghiệm trước khi chọn SMJ, AQE, salting hoặc broadcast. Phản ánh trường hợp cân bằng task tốt hơn nhưng thời gian cao hơn. |

ND4 phù hợp với phần artifact tái lập. Manhattan Gold phù hợp với phần minh họa downstream hoặc tài liệu bổ sung. Dàn ý chưa chọn hội nghị, giới hạn trang hoặc tuyên bố một thuật toán mới. Danh tính phiên và nguồn tính lại trỏ theo [sổ thực nghiệm](01_experiment_registry.csv) và [kiểm toán](02_evidence_audit.md).

## Claim và provenance cho hai phiên bổ sung

| Claim có thể dùng | Bằng chứng | Giới hạn |
|---|---|---|
| Trên synthetic 10M, C/B có median tỷ số 0,918; plan C split 8/8 | [`09_paper_supplement_execution_log.md`](09_paper_supplement_execution_log.md), application `app-20261125142144-0001`, 96-run artifacts | CI [0,851; 0,989] cắt biên 0,95; 8 block cùng application; 12 phép DuckDB cấp workload/phương án. |
| Ở hai mức replication, ratio-of-ratios median 0,9972, CI [0,9381; 1,0632] | [`11_replication_experiment_execution_20261128.md`](11_replication_experiment_execution_20261128.md), application `app-20261128072824-0002`, 32-run artifacts | Chưa phân định theo ±5%; thay đổi filler có thể đổi nén/hash; E so với B, không phải C. |
| Salting E làm Generate phát ra thêm 700.000 dòng ở mức cao | `generate_metric_audit.csv`, 16 lượt E và Event Log 32-run application | Số dòng Generate là output của operator, không phải byte mạng vật lý. |

Chỉ dùng kết quả 32 action để kiểm tra ảnh hưởng trong hai mức đã đo; không khẳng định replication không có chi phí hay xác lập vùng Salting thắng AQE skew join.
