# Chương 4 — Kết quả thực nghiệm và phân tích đánh đổi

[**Kế hoạch và trạng thái Chương 4 theo mục 4.1–4.8**](planning/plan_chuong_4.md)

[**Bản thảo Chương 4 (Markdown)**](manuscript/chapter4_results.md) · [**Bản Word**](manuscript/chapter4_results.docx)

Chương 4 giữ bố cục 4.1–4.8 và tích hợp kết quả [Phase C WP0–WP7](../03_pipeline_skew_benchmarks/docs/phase_c/phase_c_execution_report_20261123.md). Mục 4.7 dùng phiên NYC S3 10 block làm so sánh chính của Phase C, đồng thời báo cáo riêng lưới tổng hợp, độ nhạy cấu hình và đánh giá trên các ô giữ lại.

Dựng lại các bảng/hình từ artifacts đã lưu bằng ba script trong `scripts/chapter_04/`: `build_chapter4_evidence.py`, `build_chapter4_figures.py` và `build_chapter4_remaining.py`. Các script này không khởi chạy Spark; chúng không tạo benchmark mới. Xem `scripts/chapter_04/README.md` để biết đầu ra và lệnh chạy.

ND4 đã chạy hoàn chỉnh trên Podman Compose ngày 20/11/2026. Run xác nhận `nd4-march-clean-20261120` có cả ba task Airflow ở trạng thái `success`; ND2 có action warm-up, còn ND3 cũ không tạo job warm-up dù có đoạn mã mang tên này. Mỗi phương án có ba lượt đo. Runner ND3 được sửa ngày 21/11/2026. Hai phiên ND4 ngày 22/11 dùng cùng manifest với runner đã sửa và được báo riêng trong [biên bản tái lập](evidence/nd4_reproducibility_20261122.md). Run CSV, audit, bảng tổng hợp, hình và provenance nằm ở `data/benchmarks/nd4_airflow/2026_03/nd4-march-clean-20261120/`. Một lượt thử trước cần chạy lại bước đóng gói do image cũ thiếu Matplotlib; lượt xác nhận sau khi dựng image mới chạy tự động toàn DAG.

Trong thư mục này, `manuscript/` chứa bản MD/DOCX, `planning/` chứa kế hoạch và checklist, `evidence/` chứa biên bản tái lập. Run data và hình vẫn ở `data/benchmarks/` và `docs/assets/`.

Kế hoạch phân biệt artifacts đã có với phần cần viết/đối chiếu. Các điều chỉnh chính: mục 4.5 trình bày tính ổn định qua năm tháng độc lập, không gọi là scaling; ML chuyển thành hướng phát triển Chương 5; mục spatial/trade-offs/tổng kết lần lượt là 4.6–4.8. Các phiên S3 ba lượt có telemetry host/cgroup. Với phiên S3 10 block, số đo công việc của executor lấy từ Event Log.

## Nguồn kết quả

- ND2/ND3 official Standalone: `data/benchmarks/*_official_standalone.csv`.
- Benchmark theo tháng: `data/benchmarks/scaling/monthly_benchmark_summary.csv` và các thư mục `data/benchmarks/scaling/2026_02`–`2026_05`.
- Plan, correctness audit, manifest và worker evidence: các thư mục/file cùng tier trong `data/benchmarks/`.
- Phase C: `data/benchmarks/phase_c/` và [báo cáo thực thi WP0–WP7](../03_pipeline_skew_benchmarks/docs/phase_c/phase_c_execution_report_20261123.md).
- Kết quả spatial: `data/gold/`, `data/benchmarks/spatial_analysis_audit.csv` và `data/benchmarks/figure_3_1_dropoff_density.png`.

## Nguyên tắc sử dụng

- Mỗi kết quả phải ghi rõ tier, tháng, cấu hình và số lượt; pilot là chế độ chạy thử local trên cùng Silver tháng 01, không phải tập dữ liệu 7 ngày.
- Các tháng 01–05 là input độc lập có dải Silver hẹp, không phải tập tích lũy hay benchmark scaling theo quy mô; raw row count/size không thay cho Silver benchmark input.
- So sánh phía Nam/phía Bắc trong cùng khung giờ; Daytime 16 giờ và Nighttime 8 giờ nên cần chuẩn hóa theo giờ nếu so Ngày với Đêm.
- Metric null/unavailable không được đổi thành 0.
- Không trình bày LightGBM, SparkListener hoặc mức dùng cgroup như kết quả đã đo nếu chưa có mã và artifact xác minh tương ứng.
