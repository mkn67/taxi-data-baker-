# Phase A — smoke test

**Trạng thái: không chạy Spark job mới trong lượt này.** Các entry point và thư viện của `.venv` được kiểm tra bằng import; các run official và event log đã lưu đủ để thực hiện kiểm kê Phase A mà không materialize lại dữ liệu. Vì không sửa mã thực thi, không tạo một smoke test mới chỉ để xác nhận lại engine.

Khi mở Phase B, smoke nhỏ sẽ kiểm tra đầu ra ND3 và DuckDB trên cùng một mẫu có khóa OD khớp, rồi mới chạy pilot. Kết quả smoke sẽ được lưu riêng, không trộn với benchmark official.
