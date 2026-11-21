# Phase A — quyết định sau kiểm kê

**Quyết định: GO thu hẹp cho giai đoạn pilot có kiểm soát, chưa kết luận hướng bài báo.** Phase A đã cải thiện phần phương pháp và diễn giải Chương 3–4 từ artifact hiện có; chưa có benchmark tích lũy S2/S3 hay đối chứng Broadcast official.

1. Đầu vào và tính đúng đắn: official ND2/ND3 tháng 01 có 9/9 run rows mỗi bài toán ghi collector `OK`, validation `PASS`; application IDs và event logs tương ứng tồn tại. Đây là audit theo phương án được gán cho từng iteration, không phải 9 DuckDB audit độc lập.
2. Hotspot: P95 PU có 14 ID mỗi tháng, Jaccard trung vị 0,867 trên năm tháng; điều này hỗ trợ kiểm tra tiếp quy tắc H nhưng chưa chứng minh từng ID gây task skew.
3. AQE: final plan thể hiện coalescing, không thể hiện skew split. Đây là phát hiện âm tính ở cấu hình đã chạy, không phải lỗi cần ép ngưỡng cho PASS.
4. Runtime: ba lượt/phương án và CV ND3 cao ở Baseline/AQE. Chưa đủ để khẳng định phương án nhanh nhất ổn định hoặc suy ra điểm hòa vốn Salting.
5. Triển khai: official comparison cố ý ép SMJ; plan Broadcast development chưa đủ làm mốc vận hành. Cần đo riêng nếu mở pilot.

**Điều kiện trước khi mở pilot:** chốt cùng một Silver/OD manifest, projected OD bytes và cấu hình; định nghĩa trước các nhánh (SMJ, AQE, Salting S=8, engine-default/Broadcast nếu khả thi), phương pháp lấy thời gian và tiêu chí audit; lưu final plan và run metadata. Không dùng thời gian smoke hoặc các tháng riêng làm bằng chứng scaling tích lũy.

**Phần chưa hoàn thành:** đối chứng Broadcast official, CPU/memory usage gắn application, benchmark S2/S3 tích lũy, xác minh công trình “AQE or Salting?”, và quy tắc lựa chọn được kiểm chứng ngoài mẫu. Không gán tỷ lệ hoàn thành 75% cho các mục này khi chưa có artifact.

Phase A kết thúc ở tài liệu này. Giai đoạn pilot và full benchmark cần một phiên thực hiện riêng sau khi chốt đầu vào/cấu hình; không có job full-data mới được chạy trong lượt kiểm kê.
