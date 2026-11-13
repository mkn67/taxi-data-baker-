import duckdb

con = duckdb.connect()
sql = '''
SELECT 
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE cbd_congestion_fee IS NULL) AS cnt_null,
    COUNT(*) FILTER (WHERE cbd_congestion_fee = 0.0) AS cnt_zero,
    COUNT(*) FILTER (WHERE cbd_congestion_fee > 0.0) AS cnt_pos,
    COUNT(*) FILTER (WHERE cbd_congestion_fee < 0.0) AS cnt_neg
FROM 'data/raw/fhvhv_tripdata_2026-01.parquet';
'''
r = con.execute(sql).fetchone()
tot, c_null, c_zero, c_pos, c_neg = r

print('\n### BẢNG 2.5 THỰC TẾ:')
print('| Trạng thái | Số bản ghi | Tỷ lệ trên tập gốc |')
print('| :--- | :---: | :---: |')
print(f'| null | {c_null:,} | {(c_null/tot)*100:.3f}% |')
print(f'| Bằng 0.0 | {c_zero:,} | {(c_zero/tot)*100:.3f}% |')
print(f'| Lớn hơn 0.0 | {c_pos:,} | {(c_pos/tot)*100:.3f}% |')
print(f'| Nhỏ hơn 0.0 | {c_neg:,} | {(c_neg/tot)*100:.3f}% |')
print(f'| **Tổng cộng** | **{tot:,}** | **100,00%** |\n')