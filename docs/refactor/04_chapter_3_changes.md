# Chỉnh Chương 3

Bản Word dùng chương đã biên tập trong [chapter3_complete.md](../../chapters/03_pipeline_skew_benchmarks/manuscript/chapter3_complete.md). Giữ hệ thống mục hiện có để bảo toàn các tham chiếu của Chương 4–5.

| Mục | Nội dung trong bản phát hành |
|---|---|
| 3.1 | Luồng Raw → Silver, hai workload Spark và nhánh Gold. Định nghĩa tập P95, stage selection và phạm vi của TSR/PSR. H3.1–3.2 dùng ảnh xuất mới, sửa đường đi shapefile và ghi Podman Compose. H3.3 giữ phân bố tần suất pickup. |
| 3.2 | Ba phương án ND2, salting S=8, hai giai đoạn tổng hợp, cách tính average bằng tổng và số lượng. |
| 3.3–3.4 | Inner equi-join theo composite key, nhóm SMJ và nhánh Spark tự chọn, điều kiện AQE xét lệch. Xác nhận cơ chế trực tiếp từ plan và đối chiếu uniqueness/replication theo tier. |
| 3.5 | Warm-up thật so với ND3 lịch sử, manifest, block ghép cặp Phase C, dữ liệu tổng hợp, cổng đúng đắn và ND4 Airflow. |
| 3.6 | Taxi Zone, đường xấp xỉ 60th Street, phân nhóm hình học, diện tích EPSG:32618 và mật độ. H3.4 là bản đồ tĩnh kiểm tra phương pháp hiển thị. |
| 3.7 | Kết nối phương pháp với các kết quả ở Chương 4. |

Các bảng runtime, kết quả scaling và số chuyến theo nhóm đã có vị trí tại Chương 4. Chương 3 giữ B3.1–B3.2 về thiết kế. Việc kiểm toán nguồn bảng và ảnh nằm trong [crosswalk](02_table_figure_crosswalk.md). Phần mã nguồn sửa trong đợt này chỉ là bộ dựng H3.1–3.2, có tùy chọn xuất vào thư mục ảnh báo cáo.
