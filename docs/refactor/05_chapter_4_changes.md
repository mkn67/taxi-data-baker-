# Chỉnh Chương 4

Bản Word và [bản Markdown](../../chapters/04_results_tradeoffs/manuscript/chapter4_results.md) cùng mở đầu bằng tám phát hiện. B4.A đưa người đọc tới mục giải thích từng phát hiện. Chương 4 giữ hệ thống số B4.1–B4.21 và H4.1–H4.9.

| Mục | Biên tập và bằng chứng |
|---|---|
| 4.1 | B4.1 nêu giới hạn Compose và số core thực cấp. E4.01 là 2+2 core, hai phiên S3 bổ trợ là 1+1. Bổ sung caption H4.1. Thêm ghi chú warm-up theo Event Log. |
| 4.2–4.3 | Giữ kết quả ba lượt tháng 01; giải thích sự khác nhau giữa giảm TSR/PSR và giảm wall-time. AQE coalescing và skew split được đọc như hai trạng thái khác nhau. |
| 4.4.3 | Đối chiếu lại run CSV và audit DuckDB tháng 01–05. Xác nhận 90/90 dòng metadata `OK/PASS` và 30 audit cấp phương án đạt dung sai; làm rõ đây không phải 90 phép đối soát độc lập. |
| 4.4–4.5 | Phân biệt dòng metadata và audit cấp phương án. E7.03–04 là Airflow clean, E7.05–08 là hai lần lặp sau khi có ND3 warm-up. H4.5–4.6 dùng E1 cho tháng 01 và E2 cho tháng 02–05; các tháng là đầu vào độc lập. |
| 4.6 | B4.15 ghi mật độ trung vị theo zone. H4.7 là biểu đồ nhóm, dẫn H5.1 cho ảnh Kepler.gl 3D thật. Phạm vi chỉ là phân tích mô tả quanh đường tham chiếu. |
| 4.7.2 | E4.01 là so sánh chính có 10 block. B4.17 nêu rõ application và cách tính task work. Giữ lần thử 1.024 MiB như phiên thiếu, đọc ước lượng cold-start theo hai phép đo riêng. |
| 4.7.2–4.7.4 | Nêu khác biệt 1 so với 2 core/executor giữa hai phiên S3. Kiểm tra 60 final plan AQE của lưới v3 (coalescing, Sort-Merge Join; không broadcast/skew split) và giới hạn diễn giải AQE là kết quả hiệu năng, không phải bằng chứng xử lý skew. |
| 4.7.4–4.7.6 | Giữ synthetic v3 riêng khỏi NYC. Ghi cách trích BroadcastExchange từ Event Log, giữ rõ hai đại lượng trước/trong khi chạy. Regret 0% trùng mốc luôn chọn AQE; lưới giữ lại chưa phân biệt được quy tắc. |
| 4.7.7–4.8 | Ma trận khuyến nghị theo workload. Mục 4.7.8 gom các giới hạn host, mạng, sample, kế hoạch AQE và phân tích không gian. |
| Tài liệu tham khảo | Chuẩn hóa Flow-Join++ theo trích dẫn [7]; cập nhật [4] với ngày nộp và sửa bản LQRS; thay SSRN Zhang bằng bài đã xuất bản ở *Transportation Research Part A* 208 (2026), Article 104966; thêm ngày truy cập cho [8]–[9]. |

Bảng chi tiết bổ sung đã được đối chiếu theo [sổ thực nghiệm](01_experiment_registry.md), [bảng tổng hợp](02_canonical_method_summary.csv), [kiểm toán](02_evidence_audit.md) và [crosswalk](02_table_figure_crosswalk.md).
