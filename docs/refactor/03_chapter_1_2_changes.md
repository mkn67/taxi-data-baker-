# Chỉnh Chương 1–2

Nguồn biên tập là `BigData.docx` do tác giả cung cấp. Bản phát hành nằm trong [release](release/). Những phần tài liệu đã khớp được giữ nguyên và kiểm tra theo [audit](02_evidence_audit.md).

| Vị trí | Đã sửa trong Word hoặc đã đối chiếu |
|---|---|
| Bìa | Hoàn chỉnh tên đề tài và thống nhất cách viết Apache Spark. Dùng năm 2026 vì ngày nộp chính thức chưa được xác nhận. |
| 1.1–1.2 | Giữ câu hỏi về đánh đổi wall-time, lệch task, shuffle và điều kiện lựa chọn join. Các RQ đã có trong nguồn Word dùng ND2, S3 và lưới tổng hợp theo phạm vi tương ứng. |
| 1.3 | Bổ sung vai trò của Flow-Join++ trong việc đưa chi phí nhận diện hotspot và broadcast vào đối chứng. Giữ phân biệt spatial join hình học của CIBPartitioner, equi-join OD trong DETOUR và tham khảo về fidelity từ MFTune. Tập Gold thuộc phân tích mô tả. |
| 1.4–1.5 | Giữ Spark 3.5.9, Podman Compose, hai Worker trên một máy, REST API và Event Log. Mô tả cấp phát executor chi tiết theo từng application ở B4.1. |
| 2.1–2.2 | Đối chiếu 25 trường Raw, 8 trường chọn và 2 trường phái sinh Silver. Bảng 2.2 dùng số dòng và dung lượng riêng cho Raw/Silver. Bảng 2.4 khớp phép kiểm DuckDB tuần tự dùng `trip_time`. |
| 2.3–2.4 | Giữ cột phí CBD ở phần khảo sát Raw và bảng 16 vùng ứng viên. Nhóm ứng viên là phạm vi rà soát cục bộ, còn Gold phân loại toàn bộ Taxi Zone. |
| 2.5–2.7 | Pilot là chế độ chạy với Silver tháng 01; phân biệt ứng dụng Spark theo tháng, tier S1–S3 và ND4. P95 là ngưỡng tần suất theo zone. |

Danh mục tài liệu tham khảo ở cuối Word liên kết [CIBPartitioner](https://doi.org/10.1080/10095020.2025.2510364), [MFTune](https://arxiv.org/abs/2603.16450), [Flow-Join++](https://doi.org/10.2197/ipsjjip.34.671) và [SSRN](https://ssrn.com/abstract=5549128) với trích dẫn trong nội dung.
