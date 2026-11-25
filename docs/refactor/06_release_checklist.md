# Nghiệm thu bản báo cáo DETOUR

Bản phát hành ngày 25/11/2026 gồm Word và PDF trong [release](release/). Mỗi trạng thái áp dụng cho phạm vi ghi trong dòng.

| Gate | Trạng thái | Bằng chứng hoặc giới hạn |
|---|---|---|
| G0 · Kiểm kê | PASS | [Nguồn Word, repo, runner, data và logs](00_repo_inventory.md). |
| G1 · Truy vết kết quả chính | PASS | [Sổ 52 mục](01_experiment_registry.csv), 49 Event Log có thể đối chiếu, [173 nhóm kết quả theo phiên/phương án](02_canonical_method_summary.csv). Giữ raw CSV và đường dẫn final plan. |
| G2 · Kiểm toán | PASS | [Ma trận claim](02_evidence_audit.md). Kiểm tra Raw/Silver, OD uniqueness, replication, warm-up, Spark plan, paired bootstrap/Holm, BroadcastExchange, spill, Gold và scope audit. |
| G3 · Word Chương 1–2 | PASS | Bìa, phạm vi, pipeline, nghiên cứu liên quan và niên đại trích dẫn khớp nguồn diễn giải. [Nhật ký](03_chapter_1_2_changes.md). |
| G4 · Word Chương 3 | PASS | Phương pháp, metrics, provenance, ND4 và Gold; ảnh H3.1/H3.2 cập nhật. [Nhật ký](04_chapter_3_changes.md). |
| G5 · Word Chương 4 | PASS | B4.A và E4.01 làm trọng tâm, tài nguyên và giao thức theo từng phiên, giới hạn riêng. [Nhật ký](05_chapter_4_changes.md). |
| G6 · Trình bày | PASS | Word khổ A4; số trang La Mã phần đầu, số Ả Rập từ Chương 1; 97 mục liên kết và có số trang kiểm bằng PDF. Hình được nhúng, H5.1 dùng ảnh tác giả, danh mục có tài liệu tham khảo. [Nguồn hình và bảng](02_table_figure_crosswalk.md). |
| G7 · Tài liệu bàn giao | PASS | [Changelog](CHANGELOG.md), checklist và [dàn ý bài báo](07_paper_outline.md). |
| Ngày nộp và rà mẫu trường | UNVERIFIED | Chưa có ngày nộp đã xác nhận. Bìa dùng năm 2026. Bản PDF là mốc phân trang đã kiểm tra; mục lục hiện tại là các mục liên kết có số trang cố định theo lần xuất này. |
| Kepler.gl · kiểm thử thao tác | UNVERIFIED | Xác nhận ảnh 3D, dữ liệu và cấu hình lọc/tooltip trong `data/gold/nd5/nd5_kepler_map.json`. Chưa thao tác bộ lọc hay tooltip trong trình duyệt trong đợt này. |
| Nguồn ảnh cũ | UNVERIFIED | Một số ảnh sơ đồ trong Word cũ không có bộ tạo tương ứng. Giữ ảnh gốc và trỏ nguồn phương pháp ở crosswalk. |
| Nghiên cứu mở rộng | UNVERIFIED | Chưa có bằng chứng khóa quy tắc trước khi xem hold-out, NIC/storage-wait hoặc nhiều host. Đây là giới hạn kết luận nghiên cứu. |

Kiểm tra đã dùng: `git diff --check`; kiểm toán lọc DuckDB; đọc Parquet để đếm hàng, dung lượng và duy nhất khóa OD; đọc Event Log và final plans từ sổ; ba bộ phân tích `analyze_phase_c_paired.py`, `extract_phase_c_broadcast.py`, `analyze_phase_c_grid.py`; kiểm tra tương quan task với `analyze_task_load.py`; xuất và rà Word/PDF. Một audit OD dẫn xuất tháng 01 được tạo lại trong lần gọi script trợ giúp. Chi tiết ở changelog.
