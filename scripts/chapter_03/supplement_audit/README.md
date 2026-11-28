# Audit và phân tích thí nghiệm bổ sung

Các script ở đây đọc artifact đã có; chỉ `prepare_supplement_replication.py` tạo input mới. Runner Spark và engine chung vẫn ở thư mục cha để Airflow, notebook và các lệnh benchmark cũ hoạt động như trước.

| File | Vai trò |
| --- | --- |
| `audit_supplement_stage_events.py` | Ghép stage, SQL execution, plan và TSR của phiên 96 lượt. |
| `audit_supplement_io_events.py` | Đối chiếu shuffle/spill của collector với task Event Log. |
| `audit_replication_generate_events.py` | Kiểm tra metric Generate của phiên replication. |
| `audit_same_plan_diagnostic.py` | Kiểm plan, output và chênh lệch theo cặp trong đối chứng cùng plan. |
| `analyze_phase_c_supplement_confirmation.py` | Tính lại các contrast của phiên 96 lượt. |
| `analyze_supplement_replication.py` | Tính tỷ số theo block cho phiên 32 lượt. |
| `prepare_supplement_replication.py` | Tạo hai dimension cho thí nghiệm replication; đọc kỹ tham số trước khi chạy. |

Chạy từ project root, dùng `--help` để xem tham số. Các script trên được di chuyển nguyên byte ngày 28/11/2026: hash code không đổi. Artifacts gốc có thể vẫn ghi đường dẫn lịch sử `scripts/chapter_03/<tên file>`; đường dẫn hiện hành là `scripts/chapter_03/supplement_audit/<tên file>`. Không sửa các manifest hay Event Log cũ.
