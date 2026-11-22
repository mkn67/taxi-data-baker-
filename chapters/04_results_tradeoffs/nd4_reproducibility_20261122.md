# Kiểm tra tái lập ND4 trên Silver tháng 03/2026

Ngày 22/11/2026, DAG `chapter4_nd4_march_2026_benchmark` chạy hai phiên liên tiếp: `nd4-march-repeat-20261122` và `nd4-march-repeat2-20261122`. Cả ba task của từng phiên (`nd2_aggregation_full`, `nd3_join_full`, `validate_and_package_results`) đều `success`. Mỗi phiên có 9 lượt ND2 và 9 lượt ND3, tức ba phương án × ba lượt đo cho mỗi bài toán sau action warm-up. Tổng cộng 36/36 dòng đo có `collector_status=OK`, `validation_status=PASS`, Spark 3.5.9 và bằng chứng hai Worker. Cả bốn ứng dụng đọc cùng manifest Silver `ec48f05223ddc66ab2f3dc1e43d302883e4badb36d5bbe1c15ceb99f4537217e`.

| Bài toán | Phương án | Phiên 1: trung vị [min–max] (giây) | Phiên 2: trung vị [min–max] (giây) |
|---|---|---:|---:|
| ND2 | Baseline GroupBy | 1,528 [1,283–2,752] | 1,894 [1,592–2,159] |
| ND2 | Selective Salting S=8 | 3,566 [3,411–5,763] | 4,204 [3,862–4,241] |
| ND2 | Uniform Salting S=8 | 4,981 [4,130–6,539] | 4,668 [3,864–5,264] |
| ND3 | Baseline SMJ | 7,669 [7,390–8,407] | 8,467 [8,452–9,739] |
| ND3 | AQE Skew Join | 8,984 [8,599–9,473] | 9,409 [9,132–9,628] |
| ND3 | Salting Join S=8 | 8,966 [8,507–11,260] | 9,579 [8,280–10,850] |

Hai phiên giữ nguyên thứ tự ND2: Baseline, Selective, Uniform. Baseline ND3 có trung vị thấp nhất ở cả hai phiên; AQE và Salting đổi thứ tự với chênh lệch rất nhỏ ở phiên đầu, nên không kết luận nhánh nào nhanh hơn một cách ổn định. Không gộp hai phiên thành một median hoặc loại lượt chậm. Shuffle, spill, TSR và PSR ở CSV cấp lượt; PSR không khả dụng cho 1 lượt ND2 phiên 1 và 2 lượt ND2 phiên 2, được giữ `NA` thay vì 0. Hai phiên chưa có bộ đo host/cgroup xuyên suốt, vì vậy không quy nguyên nhân dao động thời gian cho CPU, I/O hay throttling.

Lượt sạch ngày 20/11 (`nd4-march-clean-20261120`) cũng dùng manifest trên và 18/18 dòng `OK/PASS`, nhưng runner ND3 lúc đó chưa tạo action warm-up. Bảng 4.12 và Hình 4.4 của bản thảo dùng riêng lượt này; phép kiểm tra lặp lại quy trình warm-up dùng **hai phiên ngày 22/11**, không coi ngày 20/11 là một lần đo cùng giao thức.

Nguồn: `data/benchmarks/nd4_airflow/2026_03/` dưới từng run ID, gồm CSV cấp lượt, CSV tổng hợp, hình và `nd4_march_2026_airflow_provenance.json`. Ứng dụng Spark phiên 1: ND2 `app-20261122085109-0001`, ND3 `app-20261122085234-0002`; phiên 2: ND2 `app-20261122085600-0003`, ND3 `app-20261122085712-0004`.
