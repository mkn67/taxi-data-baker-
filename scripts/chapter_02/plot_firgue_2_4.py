import duckdb
import matplotlib.pyplot as plt
import os

# Cấu hình phong cách biểu đồ học thuật
plt.rcParams['font.sans-serif'] = 'DejaVu Sans'
plt.rcParams['axes.edgecolor'] = '#333333'
plt.rcParams['axes.linewidth'] = 0.8

con = duckdb.connect()
sql = """
SELECT 
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE cbd_congestion_fee IS NULL) AS cnt_null,
    COUNT(*) FILTER (WHERE cbd_congestion_fee = 0.0) AS cnt_zero,
    COUNT(*) FILTER (WHERE cbd_congestion_fee > 0.0) AS cnt_pos,
    COUNT(*) FILTER (WHERE cbd_congestion_fee < 0.0) AS cnt_neg
FROM 'data/raw/fhvhv_tripdata_2026-01.parquet';
"""
tot, c_null, c_zero, c_pos, c_neg = con.execute(sql).fetchone()

categories = [
    "null\n(Không xác định)",
    "Bằng 0.0\n(Không chịu phí)",
    "Lớn hơn 0.0\n(Phát sinh phí CRZ)",
    "Nhỏ hơn 0.0\n(Bất thường/Hoàn phí)"
]
counts = [c_null, c_zero, c_pos, c_neg]
pcts = [(c / tot) * 100 for c in counts]
colors = ['#7f8c8d', '#3498db', '#27ae60', '#e74c3c']

fig, ax = plt.subplots(figsize=(8.5, 5.0), dpi=300)
bars = ax.bar(categories, [c / 1e6 for c in counts], color=colors, width=0.5, edgecolor='#1b2631', linewidth=1)

ax.set_ylabel("Số lượng bản ghi (Triệu dòng)", fontsize=11, fontweight='bold', labelpad=10)
ax.set_title("Hình 2.4. Kết quả khảo sát thuộc tính cbd_congestion_fee trên tập dữ liệu đầu vào", 
             fontsize=11.5, fontweight='bold', pad=15)
ax.grid(axis='y', linestyle='--', alpha=0.4)

# Ghi nhãn số lượng và % trên đầu từng cột
max_val = max(counts) / 1e6
ax.set_ylim(0, max_val * 1.18 if max_val > 0 else 1)

for bar, count, pct in zip(bars, counts, pcts):
    yval = bar.get_height()
    label = f"{count:,}\n({pct:.2f}%)"
    ax.text(bar.get_x() + bar.get_width() / 2.0, yval + (max_val * 0.02), label,
            ha='center', va='bottom', fontsize=9, fontweight='bold', color='#2c3e50')

plt.tight_layout()
os.makedirs("data/zones", exist_ok=True)
out_path = "data/zones/figure_2_4_cbd_congestion_fee.png"
plt.savefig(out_path, dpi=300)
print(f">>> Đã xuất Hình 2.4 thành công vào: {out_path}")