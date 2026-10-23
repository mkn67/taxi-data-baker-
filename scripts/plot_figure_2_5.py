import os
import geopandas as gpd
import matplotlib.pyplot as plt
from mpl_toolkits.axes_grid1 import make_axes_locatable

# 1. ĐỌC DỮ LIỆU VÀ CHUẨN HÓA KHÔNG GIAN
SHAPEFILE = "data/zones/shp/taxi_zones.shp"
if not os.path.exists(SHAPEFILE):
    raise FileNotFoundError(f"Không tìm thấy shapefile tại {SHAPEFILE}")

gdf = gpd.read_file(SHAPEFILE)

# Lọc các phân vùng thuộc quận Manhattan
manhattan_zones = gdf[gdf['borough'] == 'Manhattan'].copy()

# Chuyển đổi hệ tọa độ nguồn sang EPSG:32618 (Hệ tọa độ phẳng mét UTM Zone 18N)
manhattan_utm = manhattan_zones.to_crs(epsg=32618)

# Hợp nhất các polygon con cùng LocationID
manhattan_dissolved = manhattan_utm.dissolve(by='LocationID', as_index=False)

# Tính diện tích (km2)
manhattan_dissolved['area_km2'] = manhattan_dissolved.geometry.area / 1e6

# 2. KHỞI TẠO BẢN ĐỒ HỌC THUẬT
fig, ax = plt.subplots(figsize=(9, 12), dpi=300)

# Vẽ các đa giác Taxi Zones tô màu theo diện tích hoặc mã vùng
divider = make_axes_locatable(ax)
cax = divider.append_axes("right", size="3%", pad=0.1)

plot_map = manhattan_dissolved.plot(
    column='LocationID',
    cmap='viridis',
    linewidth=0.6,
    edgecolor='#2c3e50',
    legend=True,
    cax=cax,
    ax=ax
)

cax.set_ylabel("Mã định danh phân vùng (LocationID)", fontsize=10, fontweight='bold', labelpad=8)

# Định dạng nhãn trục tọa độ phẳng (mét)
ax.set_title("Hình 2.5. Dữ liệu đa giác Taxi Zones được chuẩn hóa để phục vụ phân tích không gian\n(Quận Manhattan — Hệ quy chiếu phẳng EPSG:32618)", 
             fontsize=11, fontweight='bold', pad=15)
ax.set_xlabel("Tọa độ X (Mét — Easting)", fontsize=10, labelpad=8)
ax.set_ylabel("Tọa độ Y (Mét — Northing)", fontsize=10, labelpad=8)
ax.ticklabel_format(style='plain', useOffset=False)
ax.grid(color='#bdc3c7', linestyle='--', linewidth=0.5, alpha=0.7)

# Ghi chú nguồn dữ liệu ở góc bản đồ
ax.text(0.02, 0.02, f"Tổng số vùng: {len(manhattan_dissolved)} Zones\nHệ quy chiếu: WGS 84 / UTM Zone 18N", 
        transform=ax.transAxes, fontsize=9, bbox=dict(boxstyle="round,pad=0.4", facecolor='white', alpha=0.85, edgecolor='#bdc3c7'))

plt.tight_layout()
os.makedirs("data/zones", exist_ok=True)
out_png = "data/zones/figure_2_5_manhattan_zones.png"
plt.savefig(out_png, dpi=300)
print(f">>> Đã xuất bản đồ Hình 2.5 thành công tại: {out_png}")