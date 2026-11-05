# Scripts Chương 4 — Tổng hợp kết quả

`build_chapter4_evidence.py` đọc các CSV official Standalone đã lưu để tính lại các bảng ND2/ND3, DuckDB correctness tháng 01 và số lượt hợp lệ tháng 01–05. Script không chạy Spark và không sửa input artifacts.

`build_chapter4_figures.py` tạo các hình benchmark tháng 01 với median và min–max; bản dùng để chèn vào tài liệu được lưu trong `docs/assets/chapter4/`.

Chạy từ project root:

```bash
python scripts/chapter_04/build_chapter4_evidence.py
python scripts/chapter_04/build_chapter4_figures.py
```

Kết quả được ghi tại `data/benchmarks/chapter4_evidence/`. Bản thảo phân tích nằm trong `chapters/04_results_tradeoffs/sections_4_1_to_4_4.md`.
