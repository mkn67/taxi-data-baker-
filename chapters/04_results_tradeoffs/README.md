# Chương 4 — Kết quả thực nghiệm và phân tích đánh đổi

[**Kế hoạch hoàn thiện Chương 4 theo mục 4.1–4.8**](plan_chuong_4.md)

[**Bản thảo các mục 4.1–4.4 từ official artifacts**](sections_4_1_to_4_4.md)

Tính lại các bảng ND2/ND3, DuckDB audit và trạng thái run bằng `python scripts/chapter_04/build_chapter4_evidence.py`; tạo Hình 4.2 và Hình 4.4 bằng `python scripts/chapter_04/build_chapter4_figures.py`.

Kế hoạch phân biệt artifacts đã có với phần cần viết/đối chiếu. Các điều chỉnh chính: mục 4.5 trình bày tính ổn định qua năm tháng độc lập, không gọi là scaling; ML chuyển thành hướng phát triển Chương 5; mục spatial/trade-offs/tổng kết lần lượt là 4.6–4.8. Cgroups hiện mới có giới hạn cấu hình, chưa có số liệu usage runtime; không có SparkListener tùy biến.

## Nguồn kết quả

- ND2/ND3 official Standalone: `data/benchmarks/*_official_standalone.csv`.
- Benchmark theo tháng: `data/benchmarks/scaling/monthly_benchmark_summary.csv` và các thư mục `data/benchmarks/scaling/2026_02`–`2026_05`.
- Plan, correctness audit, manifest và worker evidence: các thư mục/file cùng tier trong `data/benchmarks/`.
- Kết quả spatial: `data/gold/`, `data/benchmarks/spatial_analysis_audit.csv` và `data/benchmarks/figure_3_1_dropoff_density.png`.

## Nguyên tắc sử dụng

- Mỗi kết quả phải ghi rõ tier, tháng, cấu hình và số lượt; pilot là chế độ chạy thử local trên cùng Silver tháng 01, không phải tập dữ liệu 7 ngày.
- Các tháng 01–05 là input độc lập có dải Silver hẹp, không phải tập tích lũy hay benchmark scaling theo quy mô; raw row count/size không thay cho Silver benchmark input.
- So sánh phía Nam/phía Bắc trong cùng khung giờ; Daytime 16 giờ và Nighttime 8 giờ nên cần chuẩn hóa theo giờ nếu so Ngày với Đêm.
- Metric null/unavailable không được đổi thành 0.
- Không trình bày LightGBM, SparkListener hoặc mức dùng cgroup như kết quả đã đo nếu chưa có mã và artifact xác minh tương ứng.
