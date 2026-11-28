# Audit confirmation bổ sung ND3

Phiên: `app-20261125142144-0001`, Spark 3.5.9. Bằng chứng: `logs/spark-events/app-20261125142144-0001` và `data/benchmarks/phase_c/supplement_pilot_20261125_OQ8HCM/results/phase_c_supplement_confirmation_20261125T142141Z/`.

| Kiểm tra | Trạng thái | Bằng chứng và giới hạn |
| --- | --- | --- |
| Số lượt, pairing | Verified | `runs.csv`: 96 lượt, 2 workload × 6 phương án × 8 block; collector ghi OK 96 lượt. |
| Event Log | Verified | 96 job group đo khác nhau; 373/373 Spark jobs kết thúc thành công; có ApplicationEnd. |
| Executor | Verified một phần | Event Log ghi 2 executor trên worker khác nhau, 2 core mỗi executor; resource profile yêu cầu 1024 MiB heap/executor. Heap được yêu cầu không phải đo dung lượng sử dụng thực tế. |
| Đúng kết quả | Verified ở cấp workload/phương án | DuckDB audit có 12 nhóm, 3.072 dòng, không thiếu các trường kết quả. Runner chạy lại một phép kiểm chứng cho mỗi workload/phương án; trạng thái PASS gán lại cho cả 8 lượt đo, không phải 96 phép xác minh độc lập. |
| Kích hoạt cơ chế | Verified qua plan | Nhánh C workload 10M có skew marker 8/8; nhánh B không có split 8/8. Workload 2M nhánh C không có split 8/8. Xem `plan_activation_audit.csv`. |
| Cấu hình hiệu lực | Verified theo stage submission | Thuộc tính Spark ở stage được chọn trong Event Log khớp toàn bộ sáu trường cấu hình ghi trong CSV ở 96/96 lượt; thêm adaptive broadcast threshold = -1 và shuffle partitions = 200 ở 96/96 lượt. Không suy rộng sang các cấu hình chưa kiểm tra. |
| Warm-up và nhóm job | Verified có giới hạn | Runner không gắn group riêng và không clear group. Hai group đo cuối chứa 8 và 13 SQL execution ID tính cả execution đo; những execution sau là action tiếp theo. Collector được gọi ngay sau action đo. Đối chiếu execution đầu trong group cho thấy target stage thuộc execution đo ở 96/96 lượt. Không dùng toàn bộ Event Log theo group để diễn giải như chỉ một lượt đo. Runner đã sửa cho phiên sau. |
| Stage và TSR | Verified | Đối chiếu theo SQL execution ID và stage attempt: 96/96 target stage thuộc execution đo; codegen ID của `SortMergeJoin` trong final plan xuất hiện trong RDD scope của target stage ở 96/96 lượt. Tính lại max/median executor runtime của các task thành công từ Event Log khớp TSR CSV 96/96. Stage chứa join và có thể gồm cả các operator khác như partial aggregation; TSR không trực tiếp đếm split. |
| Spill và shuffle | Verified theo phạm vi collector | Audit 28/11 đối chiếu các task thành công của 384 stage thuộc 96 SQL execution đo; shuffle read/write và memory/disk spill khớp collector ở 96/96 lượt, 0 stage chênh lệch. Số liệu là tổng stage collector của action, không gán cho riêng join. |
| Thống kê | Needs correction trong diễn giải | Các contrast và biên ±5% có trong plan. Nhóm 6 kiểm định Holm và phép Wilcoxon được chọn sau khi chạy; CI bootstrap chưa hiệu chỉnh đồng thời, 8 block, cùng một application. Không gọi nhóm kiểm định là đăng ký trước. |
| Input | Verified một phần | CSV có cùng manifest fingerprint dimension ở hai workload và fingerprint fact riêng. Đây là hash tên/kích thước/mtime, không phải checksum nội dung; chưa xác nhận độc lập hash byte của toàn bộ input tại thời điểm chạy. |

Không dùng trạng thái PASS ở cấp phương án để tuyên bố rằng cả 96 output được so khớp độc lập. Có thể trình bày contrast wall-time, marker plan và TSR tại stage chứa join kèm giới hạn nêu trên. Số spill và shuffle đã đối chiếu là tổng các stage collector của action, không gán riêng operator. Script tái lập stage: `scripts/chapter_03/supplement_audit/audit_supplement_stage_events.py`; audit IO: `scripts/chapter_03/supplement_audit/audit_supplement_io_events.py`, output `data/benchmarks/phase_c/supplement_pilot_20261125_OQ8HCM/audit_io_20261128/`.
