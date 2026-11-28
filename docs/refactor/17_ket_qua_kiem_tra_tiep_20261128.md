# DETOUR — Kiểm tra tiếp evidence sau đối chiếu Malviya

Ngày 28/11/2026. Đây là phân tích bổ sung trên dữ liệu đã có, không phải phiên Spark mới. Các giá trị dưới đây là median trên tám block trong một application, một host; không suy rộng thành quy luật tổng quát.

## E so với C trong cùng phiên 96 lượt

Nguồn: `data/benchmarks/phase_c/supplement_pilot_20261125_OQ8HCM/results/phase_c_supplement_confirmation_20261125T142141Z/paired_contrasts.csv`, application `app-20261125142144-0001`. E dùng salting S=8, coalescing bật và skew optimization tắt; C dùng coalescing và skew optimization, không salting. Khác biệt giữa E và C không chỉ là một switch AQE.

| Workload | E/C, median tỷ số theo block | Bootstrap percentile 95% CI | Phạm vi |
| --- | ---: | ---: | --- |
| Synthetic 2M | 1,134 | [1,088; 1,152] | C không có split marker 8/8 plan. |
| Synthetic 10M | 1,148 | [1,065; 1,200] | C có split marker 8/8 plan. |

Trong hai cấu hình này E chậm hơn C và CI nằm trên 1,05. CI chưa hiệu chỉnh đồng thời, tám block cùng application; p-value Holm là phân tích được nhóm sau khi chạy. Không dùng kết quả để khẳng định E luôn chậm hơn C, hoặc giải thích khác biệt chỉ bởi skew split. DuckDB được kiểm ở mức 12 workload × phương án chứ không kiểm output riêng của 96 action.

## Đọc TSR cùng với số task

Nguồn: `runs.csv` của cùng phiên; các con số là median theo từng cột, không nhất thiết cùng một block. Số task và task duration thuộc target stage được chọn bởi collector; shuffle là tổng stage collector của action. `executorRunTime` là ms, có quy tắc fallback như scope trong CSV. Stage được audit là stage chứa join, nhưng có thể chứa operator khác.

| Workload | Arm | Task target stage | Max task (ms) | Median task (ms) | TSR max/median | Wall-time (s) |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 2M | A0: AQE, không coalescing | 200 | 419,5 | 8,0 | 48,06 | 2,891 |
| 2M | B: AQE + coalescing | 5 | 554,5 | 414,5 | 1,365 | 1,923 |
| 10M | A0: AQE, không coalescing | 200 | 2.329,5 | 9,0 | 249,835 | 4,297 |
| 10M | B: AQE + coalescing | 4 | 2.223,5 | 1.251,25 | 1,810 | 3,574 |
| 10M | C: AQE + coalescing + skew | 5 | 1.881,0 | 1.597,0 | 1,205 | 3,295 |
| 10M | E: salting + coalescing | 5 | 1.916,5 | 1.801,5 | 1,060 | 3,796 |

Ví dụ 2M, từ A0 sang B median TSR giảm mạnh trong khi max task tăng; số task giảm từ 200 xuống 5 và median task tăng. Đây là lý do không được coi TSR giảm là bằng chứng trực tiếp task chậm nhất đã nhanh hơn. Các median theo cột chỉ mô tả dữ liệu, không phân rã nguyên nhân hay chứng minh cơ chế.

## Replication và giới hạn

Phiên 32 lượt riêng `app-20261128072824-0002` có ratio-of-ratios 0,9972 [0,9381; 1,0632]. Đây là E/B ở hai dimension với unmatched filler, không phải thí nghiệm thay fanout của hot key khớp và không được ghép với C trong phiên 96 lượt. Không tuyên bố chi phí nhân bản bằng không.

Chưa có identical-plan control đo trực tiếp noise floor; ±5% là biên thực hành định trước. Một đối chứng lặp cùng plan chỉ đo mức biến động trong workload, application và host đó; không tự động biến nó thành độ bất định tổng quát hay chứng minh crossover.

## Việc còn thiếu

1. Rà lại từng claim trong Chương 3–4 và bản thảo theo nguồn trên; kiểm định tính đúng của các dẫn chiếu và xác định Word/PDF đã đồng bộ hay chưa.
2. Nếu chạy pilot noise control, khóa input, plan, warm-up, thứ tự, số lượt và ngân sách; lưu Event Log, plan, output và DuckDB. Đối chiếu plan sau action trước khi diễn giải; không dùng kết quả pilot để chọn mức benchmark có lợi.
3. Muốn nghiên cứu E/C theo fanout matched cần thiết kế dataset và ground truth riêng, vì dimension hiện tại là unique OD. Cần preflight cardinality và tài nguyên; đây là thí nghiệm khác, không suy từ phiên 32 lượt.
