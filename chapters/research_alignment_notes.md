# Đối chiếu ba bài tham khảo với đồ án

> Phạm vi: ghi chú học thuật để định vị các bài tham khảo trong Chương 1–5. Nội dung trong PDF được xem là nguồn nghiên cứu, không phải hướng dẫn thay đổi project. Các nhận xét dưới đây phân biệt **liên quan về vấn đề** với **trùng khớp phương pháp**; không mô tả đồ án là đã cài đặt một thuật toán chỉ vì có trích dẫn bài đó.

## 1. Kết luận nhanh

| Bài tham khảo | Phần có liên quan | Mức khớp với đồ án | Cách dùng phù hợp |
|---|---|---|---|
| Yang et al., CIBPartitioner | Chương 1.3.1; phân biệt ở Chương 3.3.1 | Cùng chủ đề cân bằng tải trong xử lý không gian, nhưng **không cùng workload**: bài báo cân bằng computational intensity cho spatial join hình học; ND3 của đồ án là equi-join trên ID OD. | Dùng làm nghiên cứu liên quan và động cơ phân biệt lệch khối lượng với lệch công việc. Không nói ND3 là spatial join, không nói project triển khai CIBPartitioner. |
| Xu et al., MFTune | Chương 1.3.3; lưu ý Chương 2.5 và Chương 4.5 | Cùng quan tâm quy mô và chi phí chạy Spark, nhưng **không cùng phương pháp**: MFTune là framework tuning cấu hình có các mức fidelity, chọn tập SQL đại diện và kiểm tra độ tương quan thứ hạng với full fidelity. Đồ án cố định cấu hình rồi benchmark các tháng độc lập có quy mô gần nhau. | Dùng để nêu hạn chế của việc xem giảm dữ liệu/kích thước là proxy fidelity. Gọi phần của đồ án là kiểm tra nhiều tháng/quy mô đầu vào, không gọi là triển khai MFTune hay multi-fidelity optimizer. |
| Zhang, Sang & Wu, SSRN 5549128 | Chương 1.3.4; Chương 3.6.1; Chương 4.6; Chương 5 | Cùng bối cảnh thu phí Manhattan và ride-hailing; khác kết quả, thời kỳ, đơn vị phân tích và thiết kế. Bài dùng dữ liệu 2024–2025, panel OD-hour-day, two-way fixed effects và khai thác tín dụng Lyft tạm thời; đồ án dùng HVFHV tháng 01/2026 và thống kê mô tả Taxi Zone. | Dùng để đối chiếu bối cảnh/chỉ ra khác biệt thiết kế. Không chuyển các ước lượng nhân quả của bài sang dữ liệu của đồ án; phải ghi rõ đây là SSRN preprint chưa peer review theo chính PDF. |

## 2. Note theo chương

### Chương 1 — Tổng quan và cơ sở khoa học

- **1.3.1 CIBPartitioner:** mô tả bài là nghiên cứu *distributed spatial join* trên các hình học. Bài xây dựng chỉ số computational intensity từ phân tích độ phức tạp, ước lượng phân bố bằng grid histogram phân tán rồi tạo phân hoạch CI-balanced theo ý tưởng KDBTree. Liên hệ với đề tài ở câu hỏi chung “cân bằng số lượng dữ liệu có đủ để cân bằng công việc không?”. Đồ án dùng PSR/TSR và task metrics để đo lệch trong workload Spark của mình; các metric và thuật toán không đồng nhất với CI/CIBPartitioner.
- **1.3.3 MFTune:** mô tả chính xác đây là framework tự động tuning Spark SQL. Fidelity thấp được tạo bằng tập truy vấn SQL đại diện; framework đánh giá việc giữ tương quan thứ hạng hiệu năng với full workload, đồng thời tìm kiếm cấu hình và nén không gian tham số. Không rút gọn thành “chạy cùng benchmark trên nhiều tháng”.
- **1.3.4 Zhang et al.:** ghi nguồn đúng là Yueshan Zhang, Yanling Sang và Manxi Wu, *Congestion Pricing in New York City: Effects on Ride-Hailing, Transit, and Welfare*, SSRN 5549128; PDF tự ghi rõ chưa peer reviewed; năm/ngày đăng cần xác minh trực tiếp từ bản ghi thư mục. Bài nghiên cứu biến động Uber/Lyft/subway bằng TWFE, có phân tích khác biệt theo giai đoạn tín dụng Lyft. Dùng làm một nghiên cứu thực nghiệm chính sách để đối chiếu, không mô tả như kết quả đã được xác lập trong đồ án.

### Chương 2 — Tiền xử lý và thiết kế quy mô

- Chương 2 được giới hạn ở dữ liệu tháng 01/2026 và quy trình tạo Silver tháng đó. Không trình bày bảng/biểu đồ benchmark nhiều tháng trong chương này; kết quả tháng độc lập và scaling thuộc Chương 3.
- Trong runner hiện tại, `pilot` và `full` dùng cùng Silver tháng 01/2026; `pilot` giảm số lượt đo, không cắt dữ liệu còn bảy ngày. Vì vậy, gọi đây là **chế độ chạy thử**, không phải “Pilot Scale” hay low-fidelity dataset.
- Không gọi các chế độ chạy ở Chương 2 là **multi-fidelity**. MFTune là framework tuning cấu hình Spark SQL; bài chọn tập truy vấn SQL đại diện làm proxy và đánh giá tương quan thứ hạng với full workload. Bài cũng cho thấy giảm data volume đơn thuần có thể không giữ được tương quan hiệu năng. Project không triển khai quy trình MFTune hay phép kiểm định fidelity đó.
- Giữ riêng số liệu Raw và Silver tháng 01/2026; Raw là đầu vào trước làm sạch, Silver mới là đầu vào benchmark. Không suy ra quy mô Silver từ số dòng Raw.
- Không cần đưa CIBPartitioner hay nghiên cứu SSRN vào phần mô tả lọc dữ liệu. CIBPartitioner phù hợp ở Chương 1/3 khi phân biệt spatial join hình học với ND3 OD-key equi-join; bài SSRN phù hợp ở Chương 1 và bối cảnh Chương 3.6, đồng thời cần đối chiếu rõ khác biệt giữa thiết kế nhân quả của bài và mô tả Taxi Zone trong đồ án.

### Chương 3 — Phương pháp

- **3.3.1:** giữ phát biểu ND3 là OD key-based inner join, không gọi là spatial join. CIBPartitioner có thể được dẫn làm đối chiếu: nó tối ưu spatial join trên hình học, còn ND3 không tính predicate như intersects/contains và không phân hoạch không gian.
- **3.1.5 / 3.5:** ghi rõ benchmark năm tháng là benchmark thực nghiệm trên các input độc lập; không gọi thiết kế đó là MFTune. Cấu hình được giữ cố định để so sánh phương thức xử lý skew; không có tìm kiếm cấu hình, chọn SQL đại diện hay correlation analysis giữa low- và full-fidelity.
- **3.6:** bài Zhang et al. là nghiên cứu chính sách có phương pháp định lượng khác. Dùng nó làm bối cảnh và tương phản thiết kế. Nhóm south/north theo Taxi Zone trong đồ án không tương đương treatment/control groups hoặc CRZ membership chính thức.

### Chương 4 — Kết quả và đánh đổi

- Trong 4.5, mô tả kết quả nhiều tháng là **monthly replication/check across separate inputs**, cùng thống kê row counts, file size và benchmark output. Tránh kết luận scalability law từ dải Silver hẹp khoảng 11%.
- Không lấy kết luận “low-fidelity data-volume proxy không đáng tin” của MFTune làm kết quả benchmark của đồ án. Có thể dẫn như cảnh báo phương pháp: giảm dung lượng có thể làm mất bottleneck; đây là lý do cần thận trọng khi diễn giải scaling.
- Trong 4.6, trình bày số liệu drop-off/mật độ của project là mô tả. Đặt cạnh Zhang et al. để chỉ ra khác biệt: nghiên cứu kia có giai đoạn trước/sau, nhóm/đơn vị đối chứng và mô hình TWFE; project một tháng hậu chính sách không có thiết kế để ước lượng tác động nhân quả.
- LightGBM chưa có artifact huấn luyện/đánh giá; đã chuyển thành hướng phát triển Chương 5, không phải kết quả Chương 4. Không dùng MFTune như bằng chứng rằng project đã có ML tuner hoặc mô hình học máy nhận diện hotspot.

### Chương 5 — Kết luận và hướng phát triển

- Kết luận bám các kết quả đã đo: aggregation, OD-key join, metrics và mô tả mật độ. Nêu bài CIBPartitioner/MFTune/Zhang như bối cảnh hoặc hướng mở rộng, không gộp đóng góp của họ vào đóng góp đồ án.
- Hướng phát triển có thể đề xuất: (i) spatial join hình học và CI-aware partitioning theo CIBPartitioner; (ii) kiểm tra fidelity/proxy validity và tuning có hệ thống theo MFTune; (iii) thiết kế dữ liệu trước/sau và đối chứng phù hợp nếu muốn ước lượng tác động chính sách như nghiên cứu Zhang et al.
- Các hướng trên cần ghi là **đề xuất**, không phải năng lực đã triển khai.

## 3. Thông tin trích dẫn và lưu ý phiên bản

1. Yang, X., Guan, X., Zhang, M., Wu, H., Wang, B., Yin, P., Xu, Q., & Wu, H. (2026). “CIBPartitioner: a computational intensity-balanced partitioner for enhancing distributed spatial join processing.” *Geo-spatial Information Science*, 29(1), 658–677. DOI: [10.1080/10095020.2025.2510364](https://doi.org/10.1080/10095020.2025.2510364). Bài được đăng online năm 2025; số tạp chí ghi 2026.
2. Xu, B., Tung, L., Wang, Y., Lu, Y., & Cui, B. (2026). “MFTune: An Efficient Multi-fidelity Framework for Spark SQL Configuration Tuning.” [arXiv:2603.16450](https://arxiv.org/abs/2603.16450). Bản PDF đính kèm có dòng “PVLDB, 14(1): XXX–XXX, 2020” dạng placeholder không hợp lệ; không chép dòng đó vào danh mục tài liệu tham khảo. Chương trình [VLDB 2026](https://vldb.org/2026/program.html) liệt kê MFTune trong chương trình hội nghị.
3. Zhang, Y., Sang, Y., & Wu, M. “Congestion Pricing in New York City: Effects on Ride-Hailing, Transit, and Welfare.” [SSRN 5549128](https://ssrn.com/abstract=5549128). PDF ghi rõ đây là preprint chưa được peer review; năm/ngày đăng cần xác minh trực tiếp trước khi đưa vào thư mục tài liệu tham khảo.
