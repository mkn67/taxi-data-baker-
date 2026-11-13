
import pandas as pd

df = pd.read_csv('data/zones/manhattan_60th_boundary_audit.csv')

# Chọn đúng tên cột thực tế trong file CSV
cols = ['LocationID', 'Zone', 'Dist_to_60th_m', 'Centroid_Side', 'Crosses_Line', 'North_Area_Pct', 'Assigned_Group']
df_table = df[cols].copy()

# Đổi sang tiêu đề tiếng Việt chuẩn học thuật cho Báo cáo
df_table.columns = ['LocationID', 'Tên zone', 'Khoảng cách biên (m)', 'Phía centroid', 'Giao cắt đường', 'Diện tích Bắc (%)', 'Nhóm sau kiểm tra']

# Format hiển thị
df_table['Khoảng cách biên (m)'] = df_table['Khoảng cách biên (m)'].apply(lambda x: f'{x:.1f}')
df_table['Diện tích Bắc (%)'] = df_table['Diện tích Bắc (%)'].apply(lambda x: f'{x:.1f}%')
df_table['Giao cắt đường'] = df_table['Giao cắt đường'].apply(lambda x: 'Có' if x == True or str(x).lower() == 'true' else 'Không')
df_table['Phía centroid'] = df_table['Phía centroid'].apply(lambda x: 'Phía Bắc' if x == 'Bac' else ('Phía Nam' if x == 'Nam' else str(x)))

print(df_table.to_markdown(index=False))
