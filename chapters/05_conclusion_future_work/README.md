# Chương 5 và Nội dung 5

[Bản thảo Chương 5](manuscript/chapter5_conclusion.md) tổng hợp kết quả Chương 3–4 và mô tả công cụ tra cứu mật độ trả khách. [Bản Word Chương 5](manuscript/Chuong_5_Ket_luan_va_Huong_phat_trien.docx) dùng cùng định dạng với Chương 3. [Kết quả Chương 4](../04_results_tradeoffs/manuscript/chapter4_results.md) lưu bảng benchmark và phạm vi diễn giải.

## Tạo lại bản đồ và bảng xếp hạng

Chạy từ thư mục gốc trong môi trường Python có Shapely 2:

```bash
.venv/bin/python scripts/chapter_05/build_nd5_explorer.py
```

Đầu vào là Gold CSV và audit không gian tháng 01/2026. Có thể đổi đường dẫn bằng `--gold`, `--audit` và `--output`.

| Tệp | Nội dung |
|---|---|
| [Bản đồ Kepler.gl 3D](../../data/gold/nd5/nd5_kepler_map.json) | Dữ liệu 526 cặp zone–khung giờ, chiều cao đa giác theo mật độ và cấu hình lọc/tooltip |
| [Top 10 Manhattan](../../data/gold/nd5/nd5_manhattan_top10.csv) | 10 zone mỗi khung giờ, xếp theo mật độ chưa làm tròn |
| [Kiểm toán ND5](../../data/gold/nd5/nd5_data_audit.json) | Đối chiếu Gold và hash SHA-256 của nguồn và sản phẩm |

## Mở bản đồ và nghiệm thu

1. Mở [Kepler.gl Demo](https://kepler.gl/demo). Chọn **Load Files** và mở `nd5_kepler_map.json`. Bản lưu đặt góc nhìn nghiêng 52° và hiển thị Ban ngày ở Manhattan. Kiểm tra lớp **Mật độ trả khách 3D** và trường **Height Based On**. Có thể tắt **Extruded** trong cấu hình lớp để xem phẳng.
2. Trong **Filters**, ở **Khung giờ** bỏ `Daytime` và chọn `Nighttime`. Mỗi lần chọn một khung giờ để tránh chồng hai bản ghi của cùng đa giác.
3. Dùng **Nhóm vùng** để giới hạn Bắc, Nam hoặc Vùng biên. Thêm **Ngoài Manhattan** nếu cần xem toàn thành phố. Trong **Taxi Zone**, tìm `Penn Station` hoặc mã `186`. Nếu phần mềm không giữ bộ lọc zone chưa có giá trị, chọn **Add Filter → Taxi Zone**.
4. Chọn lại Ban ngày. Rà mã 186: 101.155 lượt, khoảng 0,3475 km² và 291.115 chuyến/km². Mã 230: 147.209 lượt, khoảng 0,5256 km² và 280.055 chuyến/km². Đối chiếu với CSV. Ghi nhận kết quả thao tác và chụp màn hình sau khi mở bản đồ.
5. Khi đọc hai kỳ hoặc hai nhóm, dùng số trên tooltip và bảng. Chiều cao đa giác mã hóa mật độ, còn màu quantile thể hiện vị trí tương đối trong phân bố. Đọc tooltip để lấy giá trị và đơn vị. Nhóm Bắc/Nam theo đường 60th xấp xỉ. Ngày 16 giờ và Đêm 8 giờ, nên so số chuyến/km² trong cùng kỳ.

Đã đọc lại tệp xuất và đối chiếu 526/526 dòng dữ liệu bản đồ với Gold theo mã zone và khung giờ. Cả 20 thứ hạng cũng khớp khi tính lại trực tiếp từ Gold CSV. Đã kiểm tra cấu hình 3D trong JSON. Phần hiển thị khối, bộ lọc và tooltip trên giao diện Kepler.gl cần được xác nhận riêng. Tham khảo [cách mở dữ liệu](https://docs.kepler.gl/docs/user-guides/b-kepler-gl-workflow/a-add-data-to-the-map) và [cách lọc](https://docs.kepler.gl/docs/user-guides/e-filters).
