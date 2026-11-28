# DETOUR — Rà soát trước khi viết paper

Ngày 28/11/2026. Đây là trạng thái bản thảo nghiên cứu; không thay thế checklist của bản Word/PDF phát hành ngày 25/11.

| Hạng mục | Trạng thái | Bằng chứng/việc còn lại |
| --- | --- | --- |
| Audit stage và I/O phiên 96 lượt | Hoàn tất theo phạm vi collector | 96/96 lượt, 384 stage đối chiếu với task Event Log. Xem `10_confirmation_evidence_audit.md` và `audit_io_20261128/`. |
| Khóa input 2M/10M theo manifest | Hoàn tất kiểm checksum | SHA-256 hiện tại khớp manifest project. Chưa chứng minh độc lập nguồn gốc hay quan hệ với Malviya. |
| Pilot replication | Hoàn tất | 4 action đo riêng, plan/DuckDB/collector PASS; Event Log xác nhận Generate. |
| Confirmation replication | Hoàn tất; cần rà paper | 32/32 output đo được kiểm DuckDB, đủ plan; audit 32/32 I/O và 16 metric Generate. Phân tích block và CI có tại `11_replication_experiment_execution_20261128.md`. |
| Kiểm tra lại output đã lưu | Hoàn tất | Ngày 28/11 đối chiếu 32 SHA-256 output, 256 nhóm mỗi output với DuckDB reference theo count và tolerance 0,05; 0 sai lệch. Thứ tự 32 action khớp protocol. Đây là kiểm lại output đã lưu, không phải tính lại DuckDB query. |
| Đối chiếu bốn bài liên quan | Bản nháp hoàn tất | Xem `12_related_work_comparison_20261128.md`; cần rà nguồn/trang chi tiết trước khi nộp và mở rộng tìm kiếm để xác lập đóng góp mới. |
| Malviya | Đã đối chiếu PDF và repo | Repo ở commit `b62cc0ecfc275681a1052e3aa30f6a6778698d36`; generator/config khác mô tả DETOUR nhưng không có raw phía họ để đối chiếu checksum. Version paper/peer review và nguồn dữ liệu đầy đủ chưa xác minh. Xem `15_malviya_comparison_20261128.md`. |
| Chương 3–4 Markdown | Đã cập nhật | Có phương pháp và kết quả replication, cần reviewer rà wording/đơn vị, đánh số và trích dẫn. |
| Paper riêng | Dàn ý, chưa phải manuscript hoàn chỉnh | Xem `07_paper_outline.md`; cần viết introduction, related work, phương pháp, kết quả, thảo luận và nguồn theo venue. |
| Word/PDF | Chưa xuất mới | Bản release cũ chưa được cập nhật các kết quả bổ sung; không xem là bản hiện hành của Markdown. |

## Những giới hạn không được làm mất khi viết

1. Hai worker chạy trên cùng host; mỗi phiên là một application. Các block không phải các lần triển khai độc lập.
2. Phiên 96 lượt có 12 kiểm chứng DuckDB chạy lại theo phương án. Phiên 32 lượt kiểm đúng output của từng action; thời gian kiểm nằm ngoài đồng hồ đo.
3. Hai mức dimension thay đổi khóa filler không khớp fact, nên hash, nén và I/O có thể thay đổi; chỉ hai mức và một workload synthetic. CI ratio-of-ratios [0,9381; 1,0632] cắt biên ±5%; chưa phân định được hiệu ứng wall-time tương đối.
4. B/E đều tắt skew optimization; thí nghiệm 32 lượt không so Salting trực tiếp với AQE skew join C. Không tạo claim Salting thắng C.
5. CI percentile chưa hiệu chỉnh đồng thời; nhóm Wilcoxon–Holm của phiên 96 lượt được chọn sau chạy. Không gọi đăng ký trước.
6. Final plan xác nhận marker split trong synthetic 10M; không suy ra NYC S3 có split hoặc quy sự khác biệt 2M/10M cho duy nhất kích thước fact.
7. Action đo join + aggregate; chi phí tính hotspot được xử lý riêng. Shuffle/spill là tổng stage action theo collector, không phải số byte qua mạng vật lý hoặc chi phí riêng join.

## Thứ tự đóng phần còn lại

1. Xác minh phiên bản và nguồn artifact Malviya, hoàn thành đối chiếu provenance 2M và mở rộng related work.
2. Rà consistency giữa Chương 3, 4, evidence audit và dàn ý; giữ claim không vượt nguồn.
3. Viết paper theo venue và giới hạn trang khi xác định; xem kết quả âm tính là kết quả có giới hạn.
4. Nếu cần Word/PDF cập nhật, xuất thành phiên bản mới và rà hình, bảng, trích dẫn, phân trang; giữ release cũ.
