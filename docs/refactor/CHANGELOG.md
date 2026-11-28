# Nhật ký biên tập DETOUR

## 25/11/2026

- Bổ sung phương pháp sáu nhánh và kết quả confirmation synthetic 96 lượt vào Chương 3–4; tách phạm vi với NYC S3, sửa tổng kết về skew split, ghi giới hạn DuckDB theo phương án, CI, nhóm Holm và spill. Nguồn: `09_paper_supplement_execution_log.md`, `10_confirmation_evidence_audit.md` và artifacts confirmation; chưa sửa bản Word/PDF đã phát hành.
- Dựng bản báo cáo Word từ `BigData.docx` do tác giả cung cấp và xuất PDF xem trước. Giữ Chương 5 và ảnh Kepler.gl 3D, thêm danh mục và tài liệu tham khảo.
- Đồng bộ Chương 1–2 với phạm vi thực tế. Thay Chương 3 trong Word bằng bản phương pháp đã biên tập. Đưa kết quả định lượng vào các mục tương ứng của Chương 4.
- Cập nhật H3.1–3.2 và xuất ảnh mới vào `docs/assets/chapter3/`. Sơ đồ cũ trong benchmark vẫn là nguồn lịch sử.
- Xác nhận số hàng, tài nguyên, warm-up, plan, kết quả Phase C, các phiên ND4 và phần Gold. Lưu bảng tổng hợp và tệp tính lại trong thư mục này.
- Khi thử lấy trợ giúp từ script audit OD, script chạy theo đầu ra mặc định và ghi lại CSV audit OD tháng 01. Số đếm khớp bảng preflight đã lưu. Các tệp Raw, Silver, Gold, CSV đo thời gian và Spark Event Logs không bị ghi lại.
- Tạo dàn ý bài báo riêng. Chưa chạy lại benchmark, thay thuật toán hoặc đẩy Git.

## 27/11/2026

- Sắp xếp bản nháp Chương 3 vào `manuscript/drafts/`, cập nhật đường dẫn liên quan và thêm README phân biệt bản hoàn chỉnh với bản nháp. Bổ sung chỉ mục báo cáo/nghiên cứu và danh sách script Phase C; không di chuyển data, logs, scripts, Word hoặc PDF.

## 28/11/2026

- Sắp xếp script Chương 3 theo `analysis/` và `supplement_audit/`; 17 script được di chuyển nguyên byte. Chuyển mã tạo Hình 3.4 vào notebook artifact checks, dùng thư mục output riêng và bỏ script trùng ở thư mục gốc. Runner ND2/ND3, DAG, data, logs và manifest lịch sử không đổi; đường dẫn thực thi mới được ghi ở README.
- Chạy diagnostic cùng plan 16 action qua terminal Fedora, audit Event Log, correctness, config, plan và task; bổ sung mô tả và giới hạn vào Chương 3–4 Markdown và bản thảo paper. Chi tiết `19_same_plan_diagnostic_8blocks_20261128.md`; chưa xuất Word/PDF mới.
- Pilot cùng plan hoàn thành 4 action tại `app-20261128075314-0003`; audit plan, output, stage và I/O tại `18_same_plan_noise_pilot_20261128.md`. Chuẩn bị diagnostic 8 block, chưa chạy và không gộp pilot.
- Đọc lại contrast E/C và thống kê target-stage của phiên 96 lượt, ghi tại `17_ket_qua_kiem_tra_tiep_20261128.md`. Chuẩn bị pilot cùng plan B, 2 block × 2 action để kiểm tra kỹ thuật đo biến động; sau đó đã chạy theo mục ngay trên và không coi là xác nhận noise floor.
- Đối chiếu repo Malviya tại commit `b62cc0ecfc275681a1052e3aa30f6a6778698d36`; cập nhật kế hoạch, related work và trạng thái paper theo 96/32 lượt đã audit, phân biệt claim trong paper, mã công khai và raw artifacts chưa có. Chưa chạy benchmark mới và chưa xuất Word/PDF mới.
- Audit lại shuffle/spill 96 lượt theo task và SQL execution; chạy pilot và confirmation chi phí replication 32 lượt sau gate, giữ log hai lần khởi động lỗi. Cập nhật phương pháp và kết quả hai mức dimension ở Chương 3–4 Markdown. Đầu ra và giới hạn tại `11_replication_experiment_execution_20261128.md`; chưa xuất Word/PDF mới.
