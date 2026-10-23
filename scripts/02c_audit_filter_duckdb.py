import duckdb

# Kết nối in-memory database của DuckDB
con = duckdb.connect()

# Câu lệnh SQL thực hiện Single-pass Scan đếm đồng thời 5 bước lọc
sql = """
SELECT 
    COUNT(*) AS n0,
    -- c1: NOT NULL ở các cột thuộc tính bắt buộc
    COUNT(*) FILTER (WHERE pickup_datetime IS NOT NULL 
                       AND dropoff_datetime IS NOT NULL 
                       AND PULocationID IS NOT NULL 
                       AND DOLocationID IS NOT NULL 
                       AND trip_miles IS NOT NULL 
                       AND base_passenger_fare IS NOT NULL) AS n1,
    -- c2: c1 VÀ Mã vùng hợp lệ [1, 263]
    COUNT(*) FILTER (WHERE pickup_datetime IS NOT NULL 
                       AND dropoff_datetime IS NOT NULL 
                       AND PULocationID BETWEEN 1 AND 263 
                       AND DOLocationID BETWEEN 1 AND 263 
                       AND trip_miles IS NOT NULL 
                       AND base_passenger_fare IS NOT NULL) AS n2,
    -- c3: c2 VÀ Thời gian / Quãng đường mang giá trị dương
    COUNT(*) FILTER (WHERE pickup_datetime IS NOT NULL 
                       AND dropoff_datetime IS NOT NULL 
                       AND PULocationID BETWEEN 1 AND 263 
                       AND DOLocationID BETWEEN 1 AND 263 
                       AND epoch(dropoff_datetime) - epoch(pickup_datetime) > 0 
                       AND trip_miles > 0 
                       AND base_passenger_fare IS NOT NULL) AS n3,
    -- c4: c3 VÀ Cước cơ sở hợp lệ (0, 500] USD
    COUNT(*) FILTER (WHERE pickup_datetime IS NOT NULL 
                       AND dropoff_datetime IS NOT NULL 
                       AND PULocationID BETWEEN 1 AND 263 
                       AND DOLocationID BETWEEN 1 AND 263 
                       AND epoch(dropoff_datetime) - epoch(pickup_datetime) > 0 
                       AND trip_miles > 0 
                       AND base_passenger_fare > 0 AND base_passenger_fare <= 500) AS n4,
    -- c5: c4 VÀ Giới hạn ngoại lai cực đại (time <= 4h, miles <= 100, v <= 80 mph)
    COUNT(*) FILTER (WHERE pickup_datetime IS NOT NULL 
                       AND dropoff_datetime IS NOT NULL 
                       AND PULocationID BETWEEN 1 AND 263 
                       AND DOLocationID BETWEEN 1 AND 263 
                       AND (epoch(dropoff_datetime) - epoch(pickup_datetime)) BETWEEN 1 AND 14400 
                       AND trip_miles > 0 AND trip_miles <= 100 
                       AND (trip_miles / ((epoch(dropoff_datetime) - epoch(pickup_datetime)) / 3600.0)) <= 80.0
                       AND base_passenger_fare > 0 AND base_passenger_fare <= 500) AS n5
FROM 'data/raw/fhvhv_tripdata_2026-01.parquet';
"""

print("Đang quét dữ liệu thô bằng DuckDB...")
res = con.execute(sql).fetchone()
n = list(res)
n0 = n[0]

labels = [
    "Loại null ở các trường bắt buộc",
    "Loại mã vùng ngoài [1, 263]",
    "Loại thời gian/quãng đường không dương hoặc timestamp không hợp lệ",
    "Giới hạn cước cơ sở trong (0, 500] USD",
    "Loại giá trị cực đại vượt ngưỡng thời gian, quãng đường hoặc vận tốc"
]

print("\n| Bước | Quy tắc lọc | Số dòng còn lại | Số dòng bị loại thêm | Tỷ lệ loại trên tập gốc |")
print("| :---: | :--- | :---: | :---: | :---: |")
print(f"| 0 | Dữ liệu gốc đầu vào | {n0:,} | — | 0,00% |")

for i in range(1, 6):
    dropped = n[i-1] - n[i]
    pct = (dropped / n0) * 100
    print(f"| {i} | {labels[i-1]} | {n[i]:,} | {dropped:,} | {pct:.3f}% |")

total_dropped = n0 - n[5]
total_pct = (total_dropped / n0) * 100
print(f"| **Tổng** | **Tập Silver sau tiền xử lý** | **{n[5]:,}** | **{total_dropped:,}** | **{total_pct:.3f}%** |\n")