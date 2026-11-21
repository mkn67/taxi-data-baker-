# Phase A — thay đổi tài liệu và mã

- Đã xác nhận engine nằm tại `scripts/chapter_03/benchmark_engine.py`; các hàm collector, aggregate, join, audit và summary đã tồn tại. Không thêm bản engine khác ở repo root.
- Đã sửa `chapter3_sections_3_2_to_3_5.md`: bổ sung đủ 3.5.1–3.5.3 (trước đây nhảy thẳng tới 3.5.4), đưa phương pháp đối chiếu record/task time vào 3.5.3 và sửa cấp heading 3.7.
- Đã chèn Hình 3.3 có thật vào mục 3.1.3, bỏ các dấu `[CHÈN...]` trùng hoặc chưa có minh chứng ở phần 3.2–3.5 và nối sang các bảng/plan tương ứng của Chương 4.
- Đã bổ sung vào `chapter4_results.md` hệ số Spearman per-run từ event log tháng 01, cùng giới hạn so sánh AQE 5 task với Baseline/Salting 200 task.
- Đã thêm `scripts/chapter_04/analyze_task_load.py` để tính lại hệ số theo đúng `target_stage_id`/attempt trong run CSV và xuất `nd2_task_load.csv`/`nd3_task_load.csv`. Lần chạy đầu phát hiện `.venv` thiếu SciPy; công thức được sửa thành Pearson trên thứ hạng (định nghĩa Spearman), rồi cả hai lệnh chạy thành công và khớp các số trong Chương 4.
- Không sửa engine, Silver, Gold, CSV benchmark, event log hoặc Compose. Các thay đổi sẵn có trong working tree của người dùng được giữ nguyên.
