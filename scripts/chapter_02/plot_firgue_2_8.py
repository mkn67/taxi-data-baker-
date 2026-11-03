import os
import pandas as pd
import matplotlib.pyplot as plt

plt.rcParams['font.sans-serif'] = 'DejaVu Sans'
plt.rcParams['axes.edgecolor'] = '#333333'

# 1. DỮ LIỆU ĐÓN (PU) TỪ FILE HOTSPOTS_P95.CSV
df_pu = pd.read_csv("data/zones/hotspots_p95.csv")
top10_pu = df_pu.sort_values(by="trip_count", ascending=False).head(10)

# 2. DỮ LIỆU TRẢ (DO) TỪ LOG THỰC TẾ CỦA SPARK
do_data = [
    {"DOLocationID": 138, "trip_count": 363129},
    {"DOLocationID": 132, "trip_count": 356870},
    {"DOLocationID": 61,  "trip_count": 292402},
    {"DOLocationID": 37,  "trip_count": 248455},
    {"DOLocationID": 76,  "trip_count": 243721},
    {"DOLocationID": 181, "trip_count": 214888},
    {"DOLocationID": 112, "trip_count": 206482},
    {"DOLocationID": 79,  "trip_count": 206076},
    {"DOLocationID": 7,   "trip_count": 203695},
    {"DOLocationID": 246, "trip_count": 195942}
]
df_do = pd.DataFrame(do_data)

# Ánh xạ tên zone tiêu biểu
zone_names = {
    138: "LaGuardia Airport (138)",
    132: "JFK Airport (132)",
    61:  "Crown Heights North (61)",
    37:  "Bushwick South (37)",
    76:  "East New York (76)",
    79:  "East Village (79)",
    230: "Times Sq/Theatre (230)",
    161: "Midtown Center (161)",
    181: "Park Slope (181)",
    231: "TriBeCa/Civic Ctr (231)",
    112: "Greenpoint (112)",
    7:   "Astoria (7)",
    246: "West Chelsea (246)"
}

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5.5), dpi=300)

# Biểu đồ Điểm đón (PU)
y_pu = [zone_names.get(x, f"Zone {x}") for x in top10_pu['PULocationID']][::-1]
v_pu = (top10_pu['trip_count'] / 1e3).values[::-1]
bars1 = ax1.barh(y_pu, v_pu, color='#2980b9', edgecolor='#1b2631', height=0.6)
ax1.set_xlabel("Số chuyến đón (Nghìn chuyến)", fontsize=10, fontweight='bold')
ax1.set_title("Top 10 Điểm đón khách (PULocationID)\nNgưỡng P95 = 207.431 chuyến", fontsize=10.5, fontweight='bold', pad=10)
ax1.grid(axis='x', linestyle='--', alpha=0.5)
ax1.set_xlim(0, 420)
for bar in bars1:
    w = bar.get_width()
    ax1.text(w + 5, bar.get_y() + bar.get_height()/2, f"{w:.1f}k", va='center', fontsize=8.5, color='#2c3e50')

# Biểu đồ Điểm trả (DO)
y_do = [zone_names.get(x, f"Zone {x}") for x in df_do['DOLocationID']][::-1]
v_do = (df_do['trip_count'] / 1e3).values[::-1]
bars2 = ax2.barh(y_do, v_do, color='#27ae60', edgecolor='#1b2631', height=0.6)
ax2.set_xlabel("Số chuyến trả (Nghìn chuyến)", fontsize=10, fontweight='bold')
ax2.set_title("Top 10 Điểm trả khách (DOLocationID)\nNgưỡng P95 = 187.867 chuyến", fontsize=10.5, fontweight='bold', pad=10)
ax2.grid(axis='x', linestyle='--', alpha=0.5)
ax2.set_xlim(0, 420)
for bar in bars2:
    w = bar.get_width()
    ax2.text(w + 5, bar.get_y() + bar.get_height()/2, f"{w:.1f}k", va='center', fontsize=8.5, color='#2c3e50')

plt.suptitle("Hình 2.8. Phân bố tần suất chuyến xe theo các zone có tần suất cao (Hotspots P95)", 
             fontsize=12, fontweight='bold', y=1.02)
plt.tight_layout()
os.makedirs("data/zones", exist_ok=True)
out_path = "data/zones/figure_2_8_hotspots_top10.png"
plt.savefig(out_path, dpi=300, bbox_inches='tight')
print(f">>> Đã xuất Hình 2.8 tại: {out_path}")