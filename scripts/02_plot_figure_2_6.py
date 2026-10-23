import os
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
from shapely.geometry import LineString, Point

# 1. ĐỌC DỮ LIỆU
SHAPEFILE = "data/zones/shp/taxi_zones.shp"
AUDIT_CSV = "data/zones/manhattan_60th_boundary_audit.csv"

if not os.path.exists(SHAPEFILE) or not os.path.exists(AUDIT_CSV):
    raise FileNotFoundError("Thiếu tệp shapefile hoặc audit CSV!")

gdf = gpd.read_file(SHAPEFILE).to_crs(epsg=32618)
audit_df = pd.read_csv(AUDIT_CSV)

# Merge dữ liệu kiểm toán vào GeoDataFrame
merged_gdf = gdf.merge(audit_df, on='LocationID', how='inner')

# Tọa độ đường tham chiếu xấp xỉ 60th Street (EPSG:4326 -> EPSG:32618)
east_pt = gpd.GeoSeries([Point(-73.9585, 40.7590)], crs="EPSG:4326").to_crs(epsg=32618).iloc[0]
west_pt = gpd.GeoSeries([Point(-73.9918, 40.7738)], crs="EPSG:4326").to_crs(epsg=32618).iloc[0]
ref_line = LineString([west_pt, east_pt])

# 2. BẢNG MÀU KHỚP VỚI Assigned_Group THỰC TẾ
color_map = {
    'CRZ_SOUTH_ADJACENT': '#2ecc71',        # Xanh lục (Phía Nam - CRZ)
    'NON_CRZ_NORTH_ADJACENT': '#e67e22',    # Cam (Phía Bắc - Non-CRZ)
}
labels_vn = {
    'CRZ_SOUTH_ADJACENT': 'Nhóm Phía Nam (Thuộc vùng CRZ)',
    'NON_CRZ_NORTH_ADJACENT': 'Nhóm Phía Bắc (Ngoài vùng CRZ)',
    'LOAI_KHOI_DOI_CHUNG (Mo ho)': 'Loại khỏi đối chứng (Cắt ngang / Mơ hồ)'
}

fig, ax = plt.subplots(figsize=(10, 11), dpi=300)

# Vẽ nền Manhattan
manhattan_all = gdf[gdf['borough'] == 'Manhattan']
manhattan_all.plot(ax=ax, color='#f8f9f9', edgecolor='#bdc3c7', linewidth=0.5)

# Vẽ nhóm bị loại (màu xám)
excluded = merged_gdf[merged_gdf['Assigned_Group'].str.startswith('LOAI_KHOI_DOI_CHUNG')]
if len(excluded) > 0:
    excluded.plot(ax=ax, color='#95a5a6', edgecolor='#2c3e50', linewidth=0.8, alpha=0.6, label=labels_vn['LOAI_KHOI_DOI_CHUNG (Mo ho)'])

# Vẽ các nhóm Nam và Bắc
for group_name, color in color_map.items():
    sub = merged_gdf[merged_gdf['Assigned_Group'] == group_name]
    if len(sub) > 0:
        sub.plot(ax=ax, color=color, edgecolor='#2c3e50', linewidth=0.8, alpha=0.75, label=labels_vn[group_name])

# Vẽ tâm đa giác (Centroid) và nhãn mã vùng
for _, row in merged_gdf.iterrows():
    c = row.geometry.centroid
    ax.plot(c.x, c.y, marker='o', color='#1a5276', markersize=3.5)
    ax.annotate(str(row['LocationID']), (c.x, c.y), textcoords="offset points", 
                xytext=(0, 3.5), ha='center', fontsize=7.5, fontweight='bold', color='#1b2631')

# Vẽ đường tham chiếu 60th Street
x_line, y_line = ref_line.xy
ax.plot(x_line, y_line, color='#c0392b', linestyle='--', linewidth=2.2, label='Đường tham chiếu xấp xỉ 60th St', zorder=5)

# Zoom khu vực 60th Street
minx, miny, maxx, maxy = merged_gdf.total_bounds
buffer = 600
ax.set_xlim(minx - buffer, maxx + buffer)
ax.set_ylim(miny - buffer, maxy + buffer)

ax.set_title("Hình 2.6. Kết quả phân loại hình học Taxi Zones quanh đường tham chiếu xấp xỉ 60th Street\n(Cơ chế đồng thuận kép trong bán kính 1.000m — EPSG:32618)", 
             fontsize=11, fontweight='bold', pad=15)
ax.set_xlabel("Tọa độ X (Mét — Easting)", fontsize=9.5)
ax.set_ylabel("Tọa độ Y (Mét — Northing)", fontsize=9.5)
ax.ticklabel_format(style='plain', useOffset=False)
ax.grid(color='#d5dbdb', linestyle=':', alpha=0.6)
ax.legend(loc='lower left', framealpha=0.9, fontsize=8.5)

plt.tight_layout()
os.makedirs("data/zones", exist_ok=True)
out_img = "data/zones/figure_2_6_manhattan_60th_boundary.png"
plt.savefig(out_img, dpi=300)
print(f">>> Đã xuất lại Hình 2.6 thành công tại: {out_img}")