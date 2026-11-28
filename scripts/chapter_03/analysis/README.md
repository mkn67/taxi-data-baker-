# Phân tích kết quả Chương 3

Chạy từ project root; các script này được chuyển nguyên byte từ `scripts/chapter_03/` nên checksum code không thay đổi. Tên cũ còn có thể xuất hiện trong manifest hoặc Event Log lịch sử; những artifact đó được giữ nguyên.

| Nhóm | Mã |
| --- | --- |
| Phân phối và ổn định hotspot | `analyze_hotspot_stability.py`, `analyze_hotspot_hourly_stability.py` |
| Scaling và telemetry | `analyze_nd3_scaling_evidence.py`, `analyze_s2_host_telemetry.py`, `compare_s1_s2_pilots.py` |
| Phase C và cơ chế | `analyze_phase_c_grid.py`, `analyze_phase_c_paired.py`, `extract_phase_c_broadcast.py`, `extract_phase_c_task_work.py` |
| Chi phí phát hiện hotspot | `measure_hotspot_detection.py` |

Các script có thể ghi CSV/hình hoặc chạy Spark tùy chức năng. Đọc phần tham số/đường dẫn trong file trước khi chạy và dùng đầu ra riêng nếu cần giữ artifact cũ.
