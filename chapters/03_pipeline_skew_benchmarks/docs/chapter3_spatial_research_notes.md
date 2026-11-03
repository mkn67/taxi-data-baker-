# Ghi chú nghiên cứu và diễn giải không gian (mục 3.6)

## Phạm vi địa lý cần gọi đúng tên

CRZ theo mô tả của [NYC311/MTA](https://portal.311.nyc.gov/article/?kanumber=KA-03612) bao gồm các phố và đại lộ Manhattan phía nam và bao gồm 60th Street; một số đoạn đường cao tốc/đường kết nối được loại trừ. Vì vậy cách chia các đa giác Taxi Zone theo đường xấp xỉ và ngưỡng 95% **không phải** phép gán CRZ chính thức. Taxi Zone là vùng thống kê lớn, có thể cắt qua ranh giới và gộp cả đường loại trừ; bộ dữ liệu taxi cũng ghi điểm zone thay vì quỹ đạo/đoạn đường chính xác. Trong phiên bản luận văn hiện tại, nên gọi đây là “nhóm phân tích phía nam/phía bắc đường tham chiếu xấp xỉ 60th Street”, chỉ dùng cho so sánh mô tả. Không đặt nhãn “inside/outside CRZ” cho từng zone nếu chưa spatial-join với geofence chính thức và xác định rõ cách xử lý các vùng cắt biên/đường bị loại trừ.

[NY Open Data](https://catalog.data.gov/dataset/mta-central-business-district-geofence-beginning-june-2024) cung cấp bộ geofence MTA Central Business District gồm polygon, được mô tả theo phạm vi pháp lý của Manhattan phía nam và bao gồm 60th Street, đồng thời loại trừ FDR Drive, West Side Highway/Route 9A, Battery Park Underpass và một đoạn kết nối Hugh L. Carey Tunnel. Đây là nguồn phù hợp để thay thế đường xấp xỉ nếu nghiên cứu cần phân loại CRZ chính thức. Tuy nhiên ngay cả geofence chính thức cũng không giải quyết hoàn toàn việc ánh xạ điểm trả khách Taxi Zone sang “nằm trong CRZ”: cần điểm đại diện/địa chỉ trip chính xác hoặc quy tắc diện tích giao nhau được định nghĩa trước.

## Diễn giải Gold tháng 01/2026

Gold builder tạo 526 zone-period rows. Trong Manhattan, nhóm xác định phía nam có 41 zone; phía bắc có 23 zone; 5 zone là boundary ambiguous. Tổng trip count quan sát được:

| Nhóm | Zones | Daytime | Nighttime |
|---|---:|---:|---:|
| Phía nam đường tham chiếu | 41 | 3.135.311 | 1.138.423 |
| Phía bắc đường tham chiếu | 23 | 1.395.133 | 489.236 |
| Biên không phân loại chắc chắn | 5 | 384.837 | 105.544 |

Các con số này là tổng lượt drop-off của tháng 01/2026 trong các Taxi Zone được gán theo quy tắc hình học của dự án. Mật độ được tính bằng count chia cho diện tích polygon; median mật độ theo zone ở nhóm phía nam là 129.375 chuyến/km² ban ngày và 48.825 ban đêm, còn nhóm phía bắc tương ứng 44.414 và 19.030 chuyến/km². Các zone có mật độ daytime lớn trong bảng hiện tại gồm Penn Station/Madison Sq West (291.115 chuyến/km²), Times Sq/Theatre District (280.055), Midtown North (272.349), Midtown East (266.298) và Midtown Center (255.049). Đây là mô tả mức tập trung chuyến, không chứng minh thay đổi do toll.

Không so sánh trực tiếp tổng trips giữa hai nhóm như thể chúng có cùng diện tích, số zone hoặc cấu trúc giao thông. Mật độ đã chuẩn hóa diện tích nhưng không kiểm soát lượng dân cư/lao động, loại hình điểm đến, thời gian hoạt động, số xe FHV, thay đổi mùa vụ hay nguồn cầu. Một trip drop-off không đồng nghĩa một lượt xe đi vào CRZ, cũng không cho biết xe có đi trên đường chịu toll hay chuyến có phát sinh phụ phí cụ thể hay không.

## Nghiên cứu và nguồn có thể trích dẫn

1. Metropolitan Transportation Authority (MTA). (2026). *Congestion Relief Zone Tolling: First Evaluation Report*. Phạm vi đánh giá hành chính đầu tiên của chương trình; cần đọc rõ kỳ báo cáo và phương pháp của từng chỉ số trước khi đối chiếu với phân tích trip-level.
2. New York State Open Data. *MTA Central Business District Geofence: Beginning June 2024*. Polygon geofence và mô tả phạm vi pháp lý, gồm các đường bị loại trừ.
3. Cook, C., Kreidieh, A., Vasserman, S., Allcott, H., Arora, N., Tomkins, A., Turkel, E., & van Sambeek, F. (2025). *The Network-Wide Effects of Congestion Pricing: Evidence from New York City*. [NBER Working Paper 33584](https://www.nber.org/papers/w33584). Phân tích tốc độ mạng lưới bằng dữ liệu Google Maps và generalized synthetic control; là nghiên cứu đánh giá có thiết kế đối chứng, khác với thống kê mô tả Taxi Zone trong luận văn.
4. NYC311. *Congestion Pricing Program*. Tóm lược phạm vi CRZ và các đường bị loại trừ theo thông tin chương trình.
5. Zhang, Y., Sang, Y., & Wu, M. *Congestion Pricing in New York City: Effects on Ride-Hailing, Transit, and Welfare*. [SSRN 5549128](https://ssrn.com/abstract=5549128). Bài dùng dữ liệu Uber/Lyft và subway 2024–2025, panel OD-hour-day, two-way fixed effects và phân tích credit Lyft. File PDF được cung cấp ghi đây là preprint chưa peer reviewed; năm/ngày đăng cần xác minh trực tiếp từ bản ghi thư mục. Dùng để định vị nghiên cứu liên quan; không nhập các ước lượng của bài vào kết quả Gold mô tả tháng 01/2026.

**Ranh giới suy luận:** nghiên cứu Zhang et al. ước lượng phản ứng chính sách theo thiết kế before/after và nhóm/đơn vị so sánh; project hiện tại không tái tạo thiết kế đó. Nhóm Taxi Zone south/north là proxy địa lý dựa trên diện tích polygon, không phải biến treatment/control theo bài báo.

## Tài liệu tham khảo dạng liên kết

- MTA report: <https://www.mta.info/document/195631>
- Geofence metadata: <https://catalog.data.gov/dataset/mta-central-business-district-geofence-beginning-june-2024>
- NBER paper: <https://www.nber.org/papers/w33584>
- NYC311 program description: <https://portal.311.nyc.gov/article/?kanumber=KA-03612>
- Zhang, Sang & Wu, SSRN preprint: <https://ssrn.com/abstract=5549128>
- Kepler.gl Save and Export documentation: <https://docs.kepler.gl/docs/user-guides/k-save-and-export>

## Artifact Kepler.gl

`data/gold/dropoff_density_by_zone_period.gl` là map JSON dạng Kepler.gl v1 chứa hai dataset/layer polygon (Daytime và Nighttime), có thể mở bằng Kepler.gl và chỉnh tiếp. Các dataset và cấu hình layer được nhúng trong file; geometry được lưu trong EPSG:4326. Kepler.gl cho phép xuất JSON gồm dataset và config, hoặc HTML map; HTML export có thể cần Mapbox token. Bản `.gl` trong repository là artifact có thể nạp/chỉnh, chưa phải ảnh chụp giao diện đã được render/QA trong ứng dụng Kepler.gl.
