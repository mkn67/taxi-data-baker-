import matplotlib.pyplot as plt
import matplotlib.patches as patches

# Khởi tạo khung vẽ học thuật
fig, ax = plt.subplots(figsize=(11, 6.2), dpi=300)
ax.set_xlim(0, 10)
ax.set_ylim(0, 10)
ax.axis('off')

# Màu sắc phân cấp (Xanh lam nhạt -> Xanh ngọc -> Cam đất)
c_pilot = '#e3f2fd'
c_primary = '#e8f8f5'
c_scaling = '#fef5e7'
border_colors = ['#1976d2', '#16a085', '#d35400']

# 1. CẤP ĐỘ 1: PILOT SCALE
rect1 = patches.FancyBboxPatch((0.5, 6.9), 9.0, 2.3, boxstyle="round,pad=0.2",
                              facecolor=c_pilot, edgecolor=border_colors[0], linewidth=1.5)
ax.add_patch(rect1)
ax.text(0.8, 8.8, "CẤP ĐỘ 1: PILOT SCALE (LOW-FIDELITY PROXY)", fontsize=11, fontweight='bold', color=border_colors[0])
ax.text(0.8, 8.1, "• Quy mô dữ liệu: ~4,72 Triệu dòng (~105 MB Parquet thô) — Mẫu Tuần 1 Tháng 01/2026\n"
                  "• Mục tiêu thực nghiệm: Xác thực cú pháp pipeline, kiểm tra phân bổ tác vụ không lỗi cú pháp\n"
                  "• Vai trò: Đóng vai trò Proxy chi phí thấp theo nguyên lý MFTune để đối soát kết quả bằng DuckDB",
        fontsize=9.5, color='#2c3e50', linespacing=1.4)

# Mũi tên chuyển tiếp 1 -> 2
ax.annotate('', xy=(5.0, 6.4), xytext=(5.0, 6.8),
            arrowprops=dict(facecolor='#7f8c8d', edgecolor='none', width=2, headwidth=7))

# 2. CẤP ĐỘ 2: PRIMARY BENCHMARK
rect2 = patches.FancyBboxPatch((0.5, 3.8), 9.0, 2.4, boxstyle="round,pad=0.2",
                              facecolor=c_primary, edgecolor=border_colors[1], linewidth=1.5)
ax.add_patch(rect2)
ax.text(0.8, 5.8, "CẤP ĐỘ 2: PRIMARY BENCHMARK (STANDARD-FIDELITY EVALUATION)", fontsize=11, fontweight='bold', color=border_colors[1])
ax.text(0.8, 4.9, "• Quy mô dữ liệu: 19.993.102 dòng sạch (~440 MB Parquet Snappy) — Trọn vẹn Tháng 01/2026\n"
                  "• Mục tiêu thực nghiệm: Benchmark chính thức so sánh 3 nhánh (Baseline vs Uniform vs Selective Salting)\n"
                  "• Chỉ số đo đạc: Thời gian chạy (Duration), Task Skew Ratio (TSR), Shuffle I/O và độ lệch tải Worker",
        fontsize=9.5, color='#2c3e50', linespacing=1.4)

# Mũi tên chuyển tiếp 2 -> 3
ax.annotate('', xy=(5.0, 3.3), xytext=(5.0, 3.7),
            arrowprops=dict(facecolor='#7f8c8d', edgecolor='none', width=2, headwidth=7))

# 3. CẤP ĐỘ 3: SCALING TEST
rect3 = patches.FancyBboxPatch((0.5, 0.7), 9.0, 2.4, boxstyle="round,pad=0.2",
                              facecolor=c_scaling, edgecolor=border_colors[2], linewidth=1.5)
ax.add_patch(rect3)
ax.text(0.8, 2.7, "CẤP ĐỘ 3: SCALING TEST (HIGH-FIDELITY STRESS TEST)", fontsize=11, fontweight='bold', color=border_colors[2])
ax.text(0.8, 1.8, "• Quy mô dữ liệu: ~104 Triệu dòng (~2,4 GB Parquet / >18 GB In-Memory) — Gộp 5 tháng (01 - 05/2026)\n"
                  "• Mục tiêu thực nghiệm: Khảo sát áp lực bộ nhớ khi kích thước vượt tổng RAM vật lý cụm Worker (5,0 GB)\n"
                  "• Hiện tượng quan sát: Ngưỡng phát sinh Disk Spill, suy thoái Shuffle Stage và kiểm chứng độ ổn định thuật toán",
        fontsize=9.5, color='#2c3e50', linespacing=1.4)

# Khung ghi chú môi trường phần cứng cố định ở chân hình
ax.text(5.0, 0.2, "* Môi trường phần cứng cố định (cgroups): 2 Workers (2 Cores, 2.5 GB RAM/Worker = 5.0 GB tổng RAM thực thi) | Spark Standalone 3.5.6",
        ha='center', fontsize=8.5, fontstyle='italic', color='#555555')

plt.tight_layout()
plt.savefig("data/zones/figure_2_7_data_hierarchy.png", dpi=300)
print(">>> Đã lưu Hình 2.7 tại: data/zones/figure_2_7_data_hierarchy.png")