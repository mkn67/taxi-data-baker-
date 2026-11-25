# Dàn ý bài báo từ DETOUR

Tập trung vào câu hỏi khi nào xử lý lệch tải thay đổi thời gian thực thi toàn phần trong Apache Spark. Phạm vi hiện tại là nghiên cứu thực nghiệm trên một workload OD và một lưới tổng hợp, với điều kiện cụm được ghi theo phiên.

| Phần | Nội dung và bằng chứng |
|---|---|
| Tóm tắt | Nêu hạn chế của việc dùng TSR/PSR làm đại diện cho thời gian toàn phần. Tóm tắt E4.01 với 10 block, tỷ số ghép cặp Salting 1,161 và Auto Join 0,244 so với SMJ, kèm khoảng tin cậy. Nêu hai Worker cùng máy. |
| Mở đầu | Câu hỏi khi nào chia khóa làm đổi chi phí công việc, khi nào đổi chiến lược nối làm giảm wall-time. Định danh đóng góp là giao thức, bộ dữ liệu và phân tích theo kế hoạch vật lý. |
| Nghiên cứu liên quan | CIBPartitioner cho khái niệm cường độ tính toán trong spatial join. Flow-Join++ cho heavy hitter và chi phí chuẩn bị; MFTune cho giới hạn sử dụng quy mô nhỏ. Spark 3.5.9 SQL tuning cho cơ chế AQE và broadcast. |
| Hệ thống và phương pháp | E4.01 là đối chứng NYC S3, cùng manifest và cấu hình trong block. E1/E2 là kết quả hỗ trợ theo từng phiên. E5 tách Zipf và dung lượng Parquet, E6 phân tích độ nhạy. Đối soát kết quả cấp phương án bằng DuckDB. |
| Kết quả | Hình chính là bản đồ E5 có vùng chưa phân định theo biên 5%. Bảng chính ghép E4 với plan, CPU, task skew và shuffle. Chi phí tạo hotspot, replication và BroadcastExchange giải thích thêm chi phí. Báo rõ kết quả mốc luôn chọn AQE trong các ô giữ lại. |
| Thảo luận và giới hạn | Tài nguyên cấp phát khác nhau giữa ứng dụng; so sánh kết quả trong cùng phiên. Kết nối qua host vật lý, hạ tầng nhiều nút, giữ lại workload ngoài bộ sinh, độ ổn định giữa phiên và tính mới qua rà soát tài liệu là các phần cần xác nhận để nộp bài. |
| Kết luận | Trình bày điều kiện cần thử nghiệm trước khi chọn SMJ, AQE, salting hoặc broadcast. Phản ánh trường hợp cân bằng task tốt hơn nhưng thời gian cao hơn. |

ND4 phù hợp với phần artifact tái lập. Manhattan Gold phù hợp với phần minh họa downstream hoặc tài liệu bổ sung. Dàn ý chưa chọn hội nghị, giới hạn trang hoặc tuyên bố một thuật toán mới. Danh tính phiên và nguồn tính lại trỏ theo [sổ thực nghiệm](01_experiment_registry.csv) và [kiểm toán](02_evidence_audit.md).
