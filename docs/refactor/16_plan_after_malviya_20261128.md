# DETOUR — Kế hoạch kiểm toán và thực nghiệm bổ sung

**Phiên bản:** 2.0 · 28/11/2026  
**Mục đích:** Tài liệu làm việc cho tác giả và Codex; dùng để kiểm toán bằng chứng, đánh giá chi phí nhân bản của Selective Salting và chuẩn bị viết báo cáo/paper.

## Cập nhật sau phiên 28/11 và paper Malviya

**Cập nhật tiếp sau đối chứng cùng plan:** pilot 4 action và diagnostic độc lập 16 action đã hoàn tất, audit PASS trong phạm vi nêu tại [báo cáo 8 block](19_same_plan_diagnostic_8blocks_20261128.md). Ở diagnostic 16 action, trung vị `abs(Y/X − 1)` là 4,63%, 4/8 block vượt 5%; đây chỉ là mô tả cho workload/application/host đó, không phải noise floor chung hoặc CI cho các phiên khác. Chương 3–4 Markdown và bản thảo paper đã bổ sung giới hạn; Word/PDF chưa cập nhật. Các bước còn lại vẫn là rà claim, related work và quyết định hướng bài; không tự mở sweep mới.

**Trạng thái:** 96 lượt cũ đã được audit I/O; pilot 4 action và confirmation replication 32 action đã hoàn tất. Không chạy lại các bước này như công việc còn tồn. Repo Malviya đã được đối chiếu ở commit `b62cc0ecfc275681a1052e3aa30f6a6778698d36`; đã đọc README, cấu hình core, generator, arms và audit ledger. Đây là kiểm tra mã và tài liệu, chưa chạy tái lập hoặc xác minh kết quả gốc của họ.

- Cả hai workload 2M và 10M của confirmation 96 lượt thuộc cùng application `app-20261125142144-0001`; input và đặc điểm workload khác nhau. Không nêu rằng hai workload này thuộc hai phiên confirmation khác nhau hoặc kết luận chỉ đổi số dòng đã gây split.
- Paper Malviya có AQE vs Salting, đối chứng coalescing, final plan, noise-floor control và probe bật skew split. Không nhận các yếu tố này là novelty tổng quát của DETOUR. Cần đối chiếu cụ thể workload NYC thực, join hai khóa, thiết kế 96/32 lượt, correctness, điều kiện hardware và các giới hạn trước khi xác định đóng góp.
- Audit 96 lượt: 384 stage, 96/96 lượt có shuffle read/write và memory/disk spill khớp task Event Log trong phạm vi collector; DuckDB kiểm 12 kết quả workload × phương án, không phải từng action.
- Replication confirmation `app-20261128072824-0002`: 32/32 output action PASS DuckDB, Generate low/high tương ứng 5.000.347/5.700.347 dòng ở từng lượt E, I/O 32/32 khớp Event Log. Median ratio-of-ratios 0,9972, CI 95% [0,9381; 1,0632] cắt ±5%, chưa phân định. Hai mức thay unmatched filler, khác tăng fanout ở các khóa dimension khớp fact như Malviya khảo sát; không so trực tiếp với AQE skew join C hay kết luận replication miễn phí.
- Cần đánh giá noise floor một cách có kiểm soát: dùng các block/đối chứng có sẵn trước; chỉ đề xuất đo thêm sau khi đối chiếu repo và chốt hypothesis, phạm vi, ngân sách. Nếu không có identical-plan control thì ghi **chưa đo**, không tự đặt ±5% là noise floor đo được. Khi so TSR, báo thêm số task và max task duration; mức parallelism/stage khác nhau khiến max/median có thể thay đổi do mẫu số.
- Ưu tiên hiện tại: kiểm tra provenance và những raw artifacts còn thiếu, đối chiếu paper với code đã pin; audit lại các claim; chỉnh paper và tài liệu. Không tự chạy 32/48 lượt mới hoặc triển khai thuật toán mới chỉ vì paper liên quan có sweep.

### Điều chỉnh dựa trên repo Malviya tại commit đã kiểm

1. **Chọn nguồn hiện hành.** README phân biệt paper 13 trang hiện tại với phần overview cũ. Phần cũ nói có crossover, nhưng ledger của repo giải thích kết luận này bị ảnh hưởng bởi thứ tự baseline/JVM lạnh và nói kết quả mới không ủng hộ crossover đó. Không trích bảng cũ làm finding mới; trích phiên bản, trang và commit chính xác.
2. **Phân biệt code với dữ liệu raw.** README nêu generated data, raw results và Event Logs bị loại khỏi Git; không có kho raw hay DOI được dẫn. Vì vậy có thể so code/config và claim được công bố, nhưng chưa kiểm chứng độc lập các số của họ. `docs/REPRODUCIBILITY.md` và README có thông tin lịch sử khác nhau; báo rõ nguồn, không mặc định mọi chỉ dẫn đều áp dụng cho phiên mới nhất.
3. **Provenance 2M:** repo `config/v3_core.yaml` đặt seed 20260818, 2.000 engines, theta 1,2, dimension 20.000 và schema telemetry; `src/skewbench/datagen.py` sinh fact/dimension bằng NumPy/pandas. DETOUR `generate_phase_c_synthetic.py` dùng schema OD, 256 pickup keys, seed 20261123 và dimension khác. Các mô tả generator/config tại hai commit khác nhau rõ rệt; sự trùng số 2M không đủ kết luận chia sẻ dữ liệu hay sao chép. Chưa có raw phía kia để so checksum trực tiếp; không tuyên bố quyền tác giả hoặc độc lập tuyệt đối.
4. **Chỉnh phát biểu advisory:** PDF có probe bật split khi thay advisory và threshold; audit ledger H4 tự ghi phép thử một biến còn thiếu. Vì vậy không viết rằng họ đã chứng minh riêng advisory gây split bằng đối chứng một biến. Không tự thiết kế một nhánh mới cho DETOUR nếu không có hypothesis và ngân sách được chốt.
5. **Không xem TSR là metric quyết định đơn lẻ.** Đối chiếu số task, max/median executor runtime, plan, shuffle records/bytes và wall-time theo cùng execution/stage. Max/mean cũng thay đổi theo phân hoạch và workload; không gọi bất kỳ ratio nào là hoàn toàn bất biến theo phần cứng hay coalescing. Nếu thiếu cùng-stage metric thì báo unavailable.
6. **Ưu tiên evidence thật và giới hạn theo phiên:** NYC S3 cho workload thực nhưng chưa thấy split; synthetic 10M cho C/B có split 8/8 với CI chạm biên thực hành; replication 32 lượt B/E cho unmatched filler, chưa kiểm P của khóa khớp như bên họ. Chưa có vùng E thắng C. Ba kết quả phục vụ các câu hỏi khác nhau và không gộp thành một mẫu.
7. **Tiêu chí chọn hướng paper:** trước hết viết đóng góp ứng viên và ma trận so với Malviya, Flow-Join++ cùng literature gần nhất. Nếu không chỉ ra được câu hỏi/điều kiện và evidence khác biệt đủ rõ, báo paper cần thu hẹp thành báo cáo/case study hoặc nghiên cứu thêm có kiểm soát; không khẳng định novelty do khác tên dữ liệu hay khác số lượng nhánh.
8. **Tùy chọn tương lai (chưa phê duyệt/chưa chạy):** đối chứng matched hot-key fanout có kiểm soát cardinality, hoặc E/C cùng block, hoặc xác nhận trên nhiều application/host. Mỗi hướng cần câu hỏi, tài nguyên, validity gate và budget riêng trước khi triển khai. Không so E 32 lượt với C 96 lượt như kết quả ghép cặp.

### Việc cần làm tiếp theo — theo thứ tự

| Ưu tiên | Việc cần làm | Đầu ra và cổng hoàn thành |
|---|---|---|
| 1 | Rà Chương 3–4 với registry, plan, Event Log và audit 96/32; đối chiếu nguồn, phạm vi metric, số task và correctness. | Bảng claim → artifact → trạng thái; correction được ghi changelog. Không điều chỉnh số khi thiếu evidence. |
| 2 | Đọc đối chiếu bản PDF với repo đã pin và lập bảng Malviya/DETOUR: dữ liệu, join, AQE, kiểm soát, thiết kế, metric và giới hạn. | Trích trang/đường dẫn và commit; phân biệt code, claim trong paper và kết quả raw chưa có. |
| 3 | Sửa lập luận và dàn ý paper theo evidence đã có: NYC S3, 96 lượt ablation, 32 lượt replication, mỗi phiên với giới hạn riêng. | Không khẳng định thắng C, causal effect riêng advisory, hay novelty tổng quát; giữ khác biệt thực nghiệm như đóng góp ứng viên cần review. |
| 4 | Review độ sẵn sàng. Nếu câu hỏi paper vẫn chưa đủ khác biệt, lựa chọn báo cáo case study hoặc thiết kế bổ sung theo gate riêng. | Chỉ đề xuất thử nghiệm có hypothesis, control, thông số, tài nguyên và tiêu chí dừng đã khóa; chưa tự chạy. |

Hoàn tất khi claim định lượng trong bản thảo có evidence tương ứng, phần so sánh nghiên cứu có nguồn, và mọi giới hạn chính được nêu. Trạng thái xuất Word/PDF phải được kiểm tra riêng; Markdown cập nhật không đồng nghĩa bản phát hành đã cập nhật.

## 1. Mục tiêu và giới hạn

1. Hoàn tất kiểm toán provenance và metric của 96 lượt confirmation hiện có, nhất là shuffle/spill.
2. Đo tác động của việc tăng số dòng dimension thuộc hotspot tới chi phí Salting S=8 trên cùng workload synthetic 10M.
3. Đối chiếu tài liệu tham khảo, ghi chính xác đóng góp và giới hạn của DETOUR.
4. Cập nhật Chương 3–4 và tư liệu paper sau khi nghiệm thu evidence; giữ bản phát hành hiện có làm mốc.

Không thay thuật toán, không tạo research question mới và không chạy toàn bộ benchmark cũ. Không chọn kết quả bằng cách thử lặp đến khi Salting thắng. Những nhánh ngoài ngân sách và thiết kế dưới đây chỉ là lựa chọn tương lai, không tự động triển khai. Thời gian trong plan là ước lượng, phụ thuộc môi trường và thời gian audit.

## 2. Trạng thái đã có và phản hồi nhận xét trong ảnh

Nhận xét cho rằng không thấy ma trận sáu nhánh và run 10M trong `DETOUR_BigData_2026-11-25.docx` là hợp lý đối với bản Word được xem; việc không thấy trong bản Word đó không tự nó chứng minh số liệu của người khác. Thực tế workspace hiện có các artifact sau; cần kiểm tra chéo nội dung và nguồn gốc trước khi dùng làm claim:

| Bằng chứng | Trạng thái hiện có | Phạm vi được phép diễn giải |
|---|---|---|
| Pilot `supplement_pilot_20261125_OQ8HCM` | Có manifest, fact 10M và final plan pilot. | Một block chẩn đoán; không dùng kết luận ổn định. |
| Confirmation `app-20261125142144-0001` | Có Event Log, protocol, 96 dòng đo, 96 plan và audit. | 2 workload × 6 nhánh × 8 block, một Spark application trên một host. |
| AQE C, synthetic 10M | Audit final plan báo split 8/8. | Chỉ phiên và cấu hình này; đối chiếu plan và Event Log trước khi trích dẫn. |
| AQE C, synthetic 2M | Audit plan báo không thấy split 8/8. | Không quan sát marker trong phạm vi phiên; không suy ra AQE không bao giờ split. |
| DuckDB | 12 phép kiểm tra chạy lại, 2 workload × 6 phương án; PASS được gán cho các dòng tương ứng. | Không mô tả là 96 output đo được kiểm chứng độc lập. |
| Stage và TSR | Event Log ghép SQL execution ID và join codegen, tính lại TSR khớp 96/96. | Stage chứa join có thể chứa thêm operator; TSR không trực tiếp đếm split. |
| Shuffle/spill | Audit theo task Event Log khớp 96/96 lượt trong phạm vi collector; xem `10_confirmation_evidence_audit.md`. | Không quy spill toàn action cho riêng join. |
| Nguồn 2M và Malviya | Fact 2M có manifest và generator trong project; đã đối chiếu generator/config của repo Malviya tại commit đã pin. | Schema và cấu hình được kiểm khác nhau; chưa có raw phía Malviya để so checksum. Trùng số dòng không đủ chứng minh quan hệ nguồn gốc. |

Chương 3 và 4 Markdown đã có mục bổ sung; Word/PDF phát hành có thể chưa chứa các mục đó. Không gộp kết quả 2M/10M với NYC S3, không kết luận thay đổi quy mô fact là nguyên nhân duy nhất làm split xuất hiện. Ghi mọi claim bằng artifact và phiên cụ thể.

### Đường dẫn truy nguyên hiện có

- Event Log confirmation: `logs/spark-events/app-20261125142144-0001`
- Thư mục confirmation: `data/benchmarks/phase_c/supplement_pilot_20261125_OQ8HCM/results/phase_c_supplement_confirmation_20261125T142141Z/`
- Trong thư mục confirmation: `protocol.json`, `runs.csv`, `duckdb_audit.csv`, `paired_contrasts.csv`, `stage_execution_audit.csv`, `plan_activation_audit.csv`, `plans/`.
- Pilot manifest: `data/benchmarks/phase_c/supplement_pilot_20261125_OQ8HCM/pilot_manifest.json`.
- Fact 2M: `data/benchmarks/phase_c/synthetic_v3/synthetic_manifest.json` và script `scripts/chapter_03/generate_phase_c_synthetic.py`.
- Nhật ký và audit hiện có: `docs/refactor/09_paper_supplement_execution_log.md`, `docs/refactor/10_confirmation_evidence_audit.md`.
- Bản thảo đang biên tập: `chapters/03_pipeline_skew_benchmarks/manuscript/chapter3_complete.md`, `chapters/04_results_tradeoffs/manuscript/chapter4_results.md`.

## 3. Thứ tự triển khai gốc (lưu vết lịch sử)

Các giai đoạn 0–D dưới đây là kế hoạch trước khi chạy; trạng thái và evidence hiện tại ở phần cập nhật đầu tài liệu. Giai đoạn E và việc đối chiếu repo vẫn còn tiếp tục.

| Giai đoạn | Công việc | Ước lượng | Gate |
|---|---|---:|---|
| 0 | Kiểm tra workspace, Compose, hai Worker, dung lượng và file | 15–30 phút | Đủ hạ tầng và input mới mở pilot |
| A | Xác minh provenance + audit 96 lượt | 1–3 giờ | Claim gắn được với evidence, metric chưa rõ được gắn nhãn |
| B | Thiết kế và sinh dimension hai mức | 1–3 giờ | Invariants, baseline ground truth và checksum đạt |
| C | Pilot 4 điều kiện | 30–90 phút + runtime | Plan, output, metric và tài nguyên đạt |
| D | Confirmation: 8 block × 4 điều kiện | runtime tùy cụm | Đủ dữ liệu hoặc ghi phiên không hoàn tất |
| E | Phân tích, đối chiếu nghiên cứu, cập nhật Chương 3–4 | 1–3 giờ hoặc hơn | Có bảng evidence và mô tả giới hạn |

Không cam kết hoàn thành trong một ngày. Nếu ở A còn sai sót có thể ảnh hưởng đến tính hợp lệ thì sửa/ghi rõ trước khi dùng kết luận. B–D chỉ thực hiện sau khi thiết kế được khóa và pilot qua gate.

## 4. Giai đoạn 0 — Khởi động và kiểm tra

Tại root repo trên Fedora:

```bash
cd ~/SEM5/Bigdata/taxi-data-baker-
podman compose up -d
podman ps
curl -fsS http://127.0.0.1:8080/json/
df -h .
```

Xác nhận `spark-master`, `spark-driver`, hai worker ALIVE; đọc số core và memory thực cấp, kiểm tra container nào đang chạy công việc khác, ổ đĩa và đường mount. Nếu lệnh `podman compose` không khả dụng, dùng lệnh compose hiện hành của máy. Không đo khi cụm đang chạy workload khác. Công cụ Codex có thể không truy cập được rootless Podman của Fedora; khi đó người dùng chạy lệnh và gửi output, không giả vờ job đã chạy.

## 5. Giai đoạn A — Xác minh evidence và audit shuffle/spill

### A1. Provenance 10M và 2M

1. Đọc manifest, input, generator, seed, tạo ngày nào và app ID. Kiểm SHA-256 nội dung hiện tại với SHA-256 được ghi trong manifest, nếu có; fingerprint tên/kích thước/mtime không phải SHA-256 nội dung.
2. Nối từng dòng đo với application ID, job group, SQL execution ID, final plan, Event Log, config và DuckDB audit. Giữ nguyên file gốc; đầu ra audit ghi thư mục riêng.
3. Với 10M: kiểm 8 plan C có `isFinalPlan=true`, `SortMergeJoin(skew=true)` hoặc marker shuffle skew; 8 plan B không split. Với 2M: kiểm các plan C không có marker. Ghi số plan, đường dẫn và kết quả.
4. Kiểm tra ngày tạo và checksum, lịch sử nguồn generator và tài liệu tham khảo. Đối chiếu Malviya khi xác định được đúng bản thảo/README. Trạng thái nguồn gốc chưa xác định khi thiếu tài liệu; sự trùng 2M không đủ kết luận.
5. Mỗi claim đánh dấu **Verified / Needs correction / Unavailable**. Nếu thiếu plan split, không tuyên bố split cho run đó. Các artifact có sẵn được kiểm toán, không tự động giả định sai chỉ vì Word cũ chưa nêu.

### A2. Audit metric theo task

- Truy execution đầu của từng group tương ứng action đo; lọc các action warm-up/audit bị thừa job group theo SQL execution ID và thời gian.
- Lập bảng theo `(workload, block, arm, application_id, sql_execution_id, job_ids, stage_id, attempt_id)`; so với `stages_detail`, CSV collector và plan.
- Lấy các task thành công theo stage attempt, ghi nhận số task, retry/speculation nếu có. Tính memory/disk spill, shuffle read/write và kiểm tra trường thiếu. So tổng theo phạm vi collector; không cộng hai lần stage chia sẻ hoặc attempt.
- Khác biệt do phạm vi REST/attempt/task phải điều tra; số không đủ evidence để là `Unavailable`, không đổi NA thành 0. Ghi sai khác byte, tỷ lệ và nguyên nhân, giữ nguyên CSV gốc.
- Lưu `audit_96_runs.csv`, `audit_stage_task.csv`, `audit_summary.md`, script tái lập và version/code hash. Không gán spill toàn action cho riêng join.

**Gate A:** đối chiếu được 96 lượt hoặc nêu rõ các lượt không thể đối chiếu. Chỉ dùng số liệu Verified khi viết kết luận định lượng tương ứng; các phát hiện khác ghi là chưa xác minh.

## 6. Giai đoạn B — Thiết kế nhân bản dimension

### B1. Câu hỏi và can thiệp

Trên fact 10M, khi tổng số dòng dimension và kết quả join được giữ cố định, thay đổi số dòng dimension có pickup thuộc tập hotspot nhưng không khớp fact có làm thay đổi chi phí tương đối của E so với B hay không?

- B = AQE bật, coalescing bật, skew optimization tắt, không salting.
- E = cùng nền AQE/coalescing, skew optimization tắt, Selective Salting S=8.
- Hai mức dimension: **thấp** và **cao**. Chọn số dòng hotspot cụ thể dựa trên preflight RAM/ổ đĩa rồi ghi vào protocol trước pilot/confirmation. Số ví dụ 13, 10.000 hoặc 100.000 không phải thông số đã khóa.
- Giữ fact, danh sách hotspot, schema/kiểu dữ liệu, payload, tổng dòng dimension, keys và giá trị khớp fact, số file/row groups và cấu hình Spark tương đương. Dịch chuyển một phần khóa filler không khớp fact giữa pickup thuộc và không thuộc hotspot, bảo đảm composite OD key duy nhất.
- Ghi Parquet bytes, kích thước dòng, phân bố hash và số input splits/map tasks; thay khóa vẫn có thể làm đổi nén, partition, optimizer hoặc I/O. Đây là yếu tố gây nhiễu cần báo cáo, không tự xem như được kiểm soát hoàn hảo.
- Dự kiến dòng tăng thêm `(S - 1) × dim_hot_rows`; kiểm tra cả số đếm dự kiến và sau materialize transformation. Xác nhận input đi qua `Generate/explode` và shuffle bằng plan/metric phù hợp; nếu optimizer loại bỏ filler, dừng/diễn giải lại.
- Đây là phép đo chi phí replication khi giữ output join, không đo fanout thay đổi ở các khóa khớp và không tự xác định vùng E thắng AQE skew join.

### B2. Khóa protocol trước confirmation

Lưu một file `protocol.json` mới có: ID phiên, thời gian, phiên bản code/git và working-tree status, input paths + SHA-256 nội dung, generator version/seed, số dòng và số byte mỗi input, danh sách hotspot, số dòng nhân bản, nhánh, Spark version/config hiệu lực, tài nguyên thực cấp, warm-up, action đo, 8 block, seed và danh sách thứ tự bốn điều kiện, metric, contrast, điều kiện sai/lỗi và phương án phân tích. Không ghi đè pilot/confirmation cũ. Thông số được chọn sau pilot phải ghi rõ và khóa trước confirmation.

## 7. Giai đoạn C — Pilot và cổng nghiệm thu

Pilot: 2 mức × B/E × 1 block = **4 action đo**, cộng warm-up và audit. Kết quả chỉ để kiểm tra kỹ thuật, không đưa vào confirmation.

Checklist đạt:

- Hai dimension có cùng tổng số dòng/schema; OD unique, non-null; tập khóa và giá trị khớp fact giống nhau; số dòng hotspot và số replication thực tế đúng theo manifest.
- Bốn output của action đo được lưu/đối chiếu với DuckDB reference sau khi dừng đồng hồ; coverage, counts và sums thỏa dung sai đã khóa. Không chạy action khác rồi nói đã kiểm chứng chính action đo.
- Plan đúng nhánh, không broadcast, coalescing theo cấu hình; E có salt/explode và quan sát được dữ liệu filler được xử lý. Ghi input split, task, shuffle, spill; missing giữ nguyên.
- Spark thu đủ executor/worker evidence; không OOM, thiếu disk, mất executor hay trùng workload; output/plan/event log được lưu.
- Chênh lệch số dòng replication đủ lớn và khả năng hoàn tất được xác nhận. Nếu chưa đạt, sửa nguyên nhân hoặc dừng; mọi điều chỉnh được ghi trước khi khóa confirmation.

## 8. Giai đoạn D — Confirmation tối đa 32 lượt

Mỗi block gồm **B thấp, E thấp, B cao, E cao**; cố định input và ngẫu nhiên hóa thứ tự bốn điều kiện bằng seed, kiểm tra cân bằng thứ tự. Chạy 8 block = 32 action đo, ngoài warm-up, preflight, DuckDB và audit. Chỉ chạy sau pilot PASS. Ghi checkpoint từng block; khi lỗi không tự chạy bù hoặc xóa lượt. Phân loại nguyên nhân và áp dụng quy tắc thất bại đã khóa, báo số block hợp lệ theo từng contrast.

Ghi per run: block, thứ tự, condition, app/job group/SQL execution IDs, dataset content hashes, timestamp, config `spark.conf.get`, executor thực cấp, plan sau action, thời gian action, metrics, output reference/hash và kết quả đối soát trực tiếp của action. Correctness, parse plan, ghi file và DuckDB diễn ra ngoài đồng hồ wall-time. Không thay NA thành 0.

### Phân tích khóa trước

Với block i, tính `R_low = E_low / B_low`, `R_high = E_high / B_high` và `R_effect = R_high / R_low`. Mẫu số phải dương, chỉ ghép trong cùng block hợp lệ. Báo median tỷ số theo block; bootstrap percentile 95% lấy mẫu lại toàn bộ block (bao gồm cả bốn điều kiện), seed và số lần lấy mẫu được ghi trước. Đặt ngưỡng thực hành ±5% cho từng tỷ số; CI giao ngưỡng được coi là chưa phân định, CI chưa hiệu chỉnh đồng thời phải nêu rõ. Nếu dùng kiểm định nhiều contrast, định nghĩa trước family, phép kiểm định và điều chỉnh; không chọn sau khi xem kết quả.

Báo thêm số dòng dimension hot, dòng phát sinh, Parquet bytes, shuffle, memory/disk spill, task metrics và coverage theo nhánh. Ghi rõ spill là stage/action nào. Không suy ra nguyên nhân chỉ từ tương quan hoặc median; đánh giá alternative explanations như nén, hash, map split, optimizer và overhead E. Không so ghép E mới với C của phiên 96 lượt vì khác input/thời điểm.

## 9. Cổng quyết định và hướng bài

- **Nếu E/B cao tăng rõ so với E/B thấp:** báo bằng chứng chi phí tương đối tăng trong hai mức và cấu hình đo, kèm bất định và yếu tố gây nhiễu.
- **Nếu không khác rõ:** báo kết quả chưa phân định hoặc khác biệt nhỏ theo CI; không kết luận replication không có chi phí.
- **Nếu E/B < 1:** báo Salting nhanh hơn B trong điều kiện đó sau kiểm chứng, không gọi là thắng AQE skew join C.
- **Nếu muốn so trực tiếp E/C:** thiết kế thêm nhánh C cùng input và block; 3 nhánh × 2 mức × 8 block = 48 lượt, vượt ngân sách mặc định 32. Đây là lựa chọn sau phân tích, không tự mở rộng.
- **Nếu muốn tuyên bố vùng chiến thắng hoặc ngưỡng:** cần workload/seed giữ lại và xác nhận độc lập trước khi đưa ra quy tắc thực hành. Không chọn điều kiện dựa vào một pilot.

## 10. Nghiên cứu liên quan, viết bài và đầu ra nghề nghiệp

Lập bảng cho từng bài: câu hỏi; operator/khóa; dataset; baseline; AQE on/off; hạ tầng; phát hiện; hạn chế; mối liên hệ DETOUR; số trang và phiên bản nguồn. Bốn tài liệu đã đọc: Flow-Join++ (AQE tắt trong cấu hình báo cáo), CIBPartitioner (spatial join và computational intensity), MFTune (tuning và giới hạn proxy quy mô nhỏ; file arXiv v1 có metadata placeholder), Zhang–Sang–Wu (thiết kế chính sách trước/sau và nhóm đối chứng). Không nhận triển khai hoặc vượt phương pháp của họ khi chưa so trực tiếp. Malviya: repo và bản paper trong repo đã định danh; còn cần đối chiếu phiên bản PDF đã nhận và chưa có raw artifacts để xác minh số của họ.

Sau gate audit, sửa Chương 3: workload, B/E, protocol, correctness, thời gian đo. Sửa Chương 4: paired ratios, plan, audit, kết quả âm tính và giới hạn; tách NYC và synthetic; ghi changelog, application ID, đường dẫn evidence. Giữ Word/PDF cũ làm mốc, chỉ xuất bản cập nhật sau khi rà liên kết, hình bảng và phân trang.

Hồ sơ Grab/Xanh SM (tùy chọn): viết 1 trang mô tả bài toán, vai trò, dữ liệu công khai, công nghệ, quyết định dựa trên evidence, kết quả đúng phạm vi, dashboard/hình và link repo đã loại bí mật. Nêu đây là project nghiên cứu sử dụng dữ liệu NYC; không tuyên bố quan hệ doanh nghiệp hay kết quả vận hành của Grab/Xanh SM. Không đưa địa chỉ, credential hay dữ liệu riêng tư vào hồ sơ.

## 11. Checklist hoàn thành

- [ ] Môi trường và tài nguyên đã kiểm tra; không chạy cùng benchmark khác.
- [x] Kiểm manifest/checksum khả dụng của input 2M/10M và định danh repo Malviya; chưa so được raw của họ.
- [x] Audit shuffle/spill 96 lượt theo phạm vi collector.
- [x] Protocol nhân bản, hai mức và seed được khóa trước confirmation.
- [x] Pilot 4 điều kiện qua gate; giữ artifact riêng.
- [x] Confirmation 32 lượt hoàn tất.
- [x] Output từng action của confirmation replication được đối chiếu DuckDB ngoài thời gian đo.
- [x] Contrast cùng block, CI và phạm vi kiểm định được ghi trong nhật ký replication.
- [ ] Báo cáo tách synthetic với NYC, nêu một host và giới hạn suy rộng.
- [ ] Tài liệu tham khảo, novelty và claims có nguồn; cập nhật changelog.

**Bước tiếp theo:** thực hiện bốn ưu tiên ở đầu tài liệu; các runner và kết quả 32 lượt đã có. Không khởi động lại benchmark chỉ vì hướng dẫn lịch sử ở mục 4–8 còn được giữ để truy vết.
