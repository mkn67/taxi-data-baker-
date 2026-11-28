# Kế hoạch mở rộng Chương 5: MLlib dự đoán hotspot cho Selective Salting

**Trạng thái:** Đề xuất, chưa triển khai hay có kết quả. **Ngày:** 28/11/2026.

## 1. Mục tiêu và vị trí trong đồ án

Xây dựng một thử nghiệm dùng Spark ML để dự đoán số chuyến theo pickup zone ở kỳ tiếp theo, từ đó chọn tập khóa ứng viên cho Selective Salting ND3. Câu hỏi: **dùng thông tin lịch sử để chọn khóa có giúp pipeline join tốt hơn P95 lịch sử, sau khi tính chi phí dự đoán và chuẩn bị dữ liệu không?** Mô hình dự đoán tần suất, không trực tiếp dự đoán thời gian task hay chứng minh một zone gây skew.

Phần kết luận và bản đồ Kepler 3D hiện có vẫn giữ vai trò tổng kết và ứng dụng không gian. Nếu có thực nghiệm đạt cổng, đề xuất bổ sung mục 5.2.5 về thử nghiệm MLlib và cập nhật 5.3–5.5; phương pháp chi tiết, bảng kết quả và đối chứng cần đưa về Chương 3–4 hoặc phụ lục theo cấu trúc báo cáo. Nếu chưa đủ bằng chứng, đặt đề xuất ở 5.4.2 và giữ trạng thái hướng phát triển. Không gán tỷ lệ 30% là kết quả đã đạt.

## 2. Cơ sở nghiên cứu và giới hạn liên hệ

| Nguồn | Ý tưởng liên hệ | Giới hạn áp dụng |
|---|---|---|
| [Flow-Join++](https://doi.org/10.2197/ipsjjip.34.671) | Nhận diện heavy hitter dựa trên ước lượng cardinality và lựa chọn cách join. | Công trình dùng cơ chế và workload khác; dự án không triển khai Scardina, push-down hay planner của bài. |
| [ImRP](https://www.sciencedirect.com/science/article/abs/pii/S0167819120300892) | Tham khảo ý tưởng dùng phân bố lịch sử dự đoán đợt xử lý kế tiếp. | Bài nghiên cứu Spark Streaming và EWMA; thử nghiệm này là batch và không tái lập ImRP. |
| CIBPartitioner, PDF của dự án | Động cơ cân nhắc tải tính toán, không chỉ số bản ghi. | Spatial join và thiết kế partitioner khác; frequency prediction là proxy, chưa đo cường độ tính toán theo khóa. |
| Malviya, đối chiếu [repo và paper đã pin](../../docs/refactor/15_malviya_comparison_20261128.md) | Mốc so sánh AQE, Salting, coalescing và kiểm soát thực nghiệm. | Raw/Event Log của tác giả chưa có để đối chiếu độc lập; không coi thiết kế AQE–Salting là novelty chung. |
| [Spark ML 3.5.9: classification và regression](https://spark.apache.org/docs/3.5.9/ml-classification-regression.html) | Công cụ VectorAssembler, GBTRegressor và pipeline DataFrame. | Tài liệu công cụ, không chứng minh mô hình có lợi cho workload này. |

Cần đọc đầy đủ đoạn dùng trích dẫn và ghi trang/phiên bản trước khi đưa vào báo cáo; bảng trên là định hướng, không khẳng định tính mới. Không sửa các kết luận đã có của 96/32 action hoặc so ghép lượt khác phiên.

## 3. Thiết kế dữ liệu và nhãn

- Đơn vị: `(PULocationID, ngày)`; dự đoán số chuyến đón của zone trong ngày kế tiếp. Giữ số 0 cho zone hợp lệ không có chuyến, ghi quy tắc timezone-naive và thời gian cắt ngày của Silver.
- Đặc trưng chỉ từ dữ liệu có sẵn trước giờ chạy: số chuyến ngày trước, trung bình 7 ngày trước, ngày trong tuần, tỷ trọng lịch sử; tùy chọn số OD khác nhau và thông tin dimension từ snapshot đã tồn tại tại thời điểm dự đoán. Ghi thời điểm chốt snapshot. Không dùng count, P95, OD hay chỉ số task của chính ngày mục tiêu làm đầu vào.
- Nhãn: count ngày mục tiêu tính sau thực tế; dùng để đánh giá dự đoán, không dùng lúc chọn khóa. Kiểm tra thiếu ngày, duplicate, zone IDs, ranh giới thời gian và có đủ lịch sử 7 ngày; ghi manifest nguồn và fingerprint.
- P95 hiện tại dùng count của chính phạm vi Silver. Trong kịch bản dự đoán phải có **P95 lịch sử** chỉ dùng cửa sổ đã biết; thêm **P95 cùng ngày** như oracle có chi phí quét, không xem là phương án dự báo trước. Đo chi phí của cả hai chính sách.
- Dữ liệu 5 tháng không đồng nghĩa 5 mẫu khi dùng zone-ngày, nhưng các mẫu cùng ngày và cùng zone phụ thuộc nhau. Kiểm kê thực tế số ngày/zone, tỷ lệ zero, coverage và dịch chuyển phân bố trước khi chọn mô hình; không dùng randomSplit theo dòng.

## 4. Đối chứng dự đoán và quy tắc chọn

Huấn luyện GBTRegressor trong `spark.ml` với VectorAssembler; dùng dự đoán không âm sau clipping và ghi seed, thông số. Các mốc tối thiểu: (1) count ngày trước; (2) trung bình 7 ngày; (3) hotspot P95 tính trên lịch sử; (4) P95 cùng ngày có tính chi phí quét. GBT có thể dừng ở bước so sánh dự báo nếu không vượt mốc đơn giản. Dùng cùng số lượng khóa K như P95 lịch sử, xử lý hòa điểm theo LocationID để so sánh ngân sách; ghi K và độ phủ trước khi đo. Quy tắc K cố định theo tập phát triển, không chọn sau khi nhìn test.

Chia theo thời gian: tháng 01–03 phát triển với validation theo ngày cuối tháng 03; tháng 04 validation giữ lại để chọn mô hình, tham số và K; tháng 05 chỉ dùng kiểm tra cuối sau khi khóa protocol. Nếu dữ liệu theo ngày không đủ hoặc validation không có tính đại diện, dừng và báo thiếu bằng chứng. Test tháng 05 gồm nhiều ngày tương quan: báo kết quả theo ngày và khoảng dao động; không xem zone trong một ngày là các mẫu độc lập.

Đánh giá forecast: MAE/WAPE khi mẫu số khác 0; precision@K, recall@K và tỷ trọng chuyến của tập chọn so với top-K thực tế, kèm baseline. Top-K thực tế chỉ là thước đo frequency, không phải nhãn skew. Lưu dự đoán theo zone-ngày, số khóa, thời gian tạo đặc trưng/dự đoán và provenance. Không tuyên bố cải thiện nếu thiếu số liệu.

## 5. Pilot benchmark ND3 và correctness

Sau khi chốt mô hình trên validation, chọn trước tối đa 3 ngày test đại diện theo tiêu chí lịch/calendar, không theo tốc độ đo. Trên mỗi ngày, tái dựng fact, OD unique và dimension phù hợp, bảo đảm có sẵn trước thời điểm chọn khóa; cùng input và action giữa các nhánh:

1. Baseline SMJ và AQE theo cấu hình đã khóa;
2. Selective Salting S=8 với P95 lịch sử;
3. Selective Salting S=8 với tập ML;
4. P95 cùng ngày có quét, làm oracle tham khảo; Broadcast hoặc Spark Auto nếu đủ tài nguyên, giữ chính sách nhất quán.

Mỗi nhánh warm-up bằng action; pilot 3 block/ngày, xoay thứ tự theo seed đã lưu. Thu manifest, Spark/application/worker, config hiệu lực, timestamp, job group, final plan, stage/task, shuffle, spill và output; DuckDB đối soát count, sums, coverage, OD uniqueness theo đúng phạm vi. Audit nêu rõ cấp phương án hay từng action, NA giữ NA. Không mô tả plan AQE có skew split nếu không có marker tương ứng.

Báo hai chi phí: **join-only** với tập khóa sẵn có và **end-to-end** từ tạo đặc trưng, dự đoán/chọn khóa đến hoàn thành join; dùng cùng điều kiện cache/I/O, thời gian đo tường minh. Tách thời gian huấn luyện offline và nêu khi nào cần huấn luyện lại. So sánh wall-time, task count, max/median task runtime, TSR, shuffle và spill ở cùng stage phù hợp; đo số dòng OD phát sinh. Đừng quy mọi task metric cho một zone.

## 6. Gate, phân tích và ngân sách

| Giai đoạn | Đầu ra | Tiêu chí chuyển bước |
|---|---|---|
| A. Khảo sát, tối đa 0,5–1 ngày | Bảng zone-ngày, chất lượng, split theo thời gian, manifest | Đủ lịch sử, nhãn và snapshot; không rò rỉ dữ liệu; nếu không đạt thì dừng. |
| B. Forecast pilot, 1–2 ngày | Baseline, GBT, predictions, sai số và top-K trên validation | Chạy đúng, có đánh giá theo ngày; quyết định trước khi mở test. Nếu không hơn mốc đơn giản, báo kết quả và dừng hoặc biện minh thí nghiệm benchmark nhỏ. |
| C. Join pilot, 1–2 ngày cộng runtime | Tối đa 3 ngày × 5 nhánh × 3 block = 45 action đo, ngoài warm-up/audit; nếu bỏ Auto là 36 | Correctness, plan, worker và metric đủ; kiểm tài nguyên và thời gian dựa vào pilot trước khi chạy hết. Lỗi/OOM giữ lại, không tự chạy bù. |
| D. Xác nhận tùy chọn | Protocol riêng, các ngày/phiên giữ lại, số block và ngân sách khóa trước | Chỉ thực hiện khi pilot cho tín hiệu hữu ích, độ nhiễu cho phép phân biệt chênh lệch và đủ thời gian. |
| E. Báo cáo | Bảng phương pháp/kết quả, hạn chế, hình và bản Word cập nhật | Có claim → artifact; không tự tuyên bố cải thiện hoặc tính mới. |

Trước benchmark khóa hypothesis, seed, số lượt, thứ tự, cách xử lý thất bại, điều kiện dừng và chênh lệch thực hành. Có thể dùng 5% làm **mức quan tâm được chọn trước**, không coi đó là noise floor đo được. Pilot 3 block chỉ mô tả median/min–max, không khẳng định ý nghĩa thống kê; nếu xác nhận thì ghép cặp theo block và ngày, báo khoảng bất định phù hợp số phiên/ngày, không coi task hoặc zone là lượt đo độc lập.

## 7. Đầu ra dự kiến

- Script tạo đặc trưng và split, train/predict MLlib, so sánh forecast; script tích hợp hotspot theo input và output riêng, giữ runner cũ làm đối chứng.
- Protocol, danh sách ngày/khóa, manifest, predictions, model metadata, log, plan, CSV metrics và DuckDB audit; không ghi đè artifact hiện có.
- Hình đối chiếu dự đoán với thực tế và bảng so sánh pipeline có/không tính overhead; diễn giải kết quả âm tính nếu ML không cải thiện.
- Đồng bộ Markdown Chương 3–5, changelog rồi mới xuất lại DOCX/PDF và kiểm tra bố cục. Thử nghiệm chưa hoàn tất thì Chương 5 mô tả là kế hoạch.

## 8. Rủi ro cần nói rõ

P95 và dự đoán tần suất chỉ nhận diện mức tập trung, không chứng minh skew tính toán. OD có khóa duy nhất nên không giả định số OD khác nhau tự động nhân fanout. Ngày liền nhau, cùng zone và cùng host không độc lập; dữ liệu ít có thể khiến mô hình phức tạp kém hơn baseline. Đo sample và thời gian tạo feature có thể triệt tiêu lợi ích. Một máy vật lý không chứng minh hiệu quả trên cụm nhiều máy. Không khẳng định triển khai CIBPartitioner, Flow-Join++, ImRP hay vượt nghiên cứu trước.
