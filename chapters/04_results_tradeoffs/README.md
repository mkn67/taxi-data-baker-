# Chương 4 — Kết quả thực nghiệm và phân tích đánh đổi

[**Kế hoạch và trạng thái Chương 4 theo mục 4.1–4.8**](plan_chuong_4.md)

[**Bản thảo đầy đủ các mục 4.1–4.8 từ official artifacts**](chapter4_results.md)

Dựng lại các bảng/hình từ artifacts đã lưu bằng ba script trong `scripts/chapter_04/`: `build_chapter4_evidence.py`, `build_chapter4_figures.py` và `build_chapter4_remaining.py`. Các script này không khởi chạy Spark; chúng không tạo benchmark mới. Xem `scripts/chapter_04/README.md` để biết đầu ra và lệnh chạy.

ND4 đã chạy hoàn chỉnh trên Podman Compose ngày 20/11/2026. Run xác nhận `nd4-march-clean-20261120` có cả ba task Airflow ở trạng thái `success`; ND2 có action warm-up, còn ND3 cũ không tạo job warm-up dù có đoạn mã mang tên này. Mỗi phương án có ba lượt đo. Runner ND3 đã được sửa ngày 21/11/2026, nhưng kết quả ND4 ngày 20/11 chưa được chạy lại. Run CSV, audit, bảng tổng hợp, hình và provenance nằm ở `data/benchmarks/nd4_airflow/2026_03/nd4-march-clean-20261120/`. Một lượt thử trước cần chạy lại bước đóng gói do image cũ thiếu Matplotlib; lượt xác nhận sau khi dựng image mới chạy tự động toàn DAG.

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
