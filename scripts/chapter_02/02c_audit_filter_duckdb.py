"""Independently reproduce the Silver sequential filter counts on January raw data."""

import duckdb


sql = """
WITH projected AS (
    SELECT *,
           trip_miles / (NULLIF(trip_time, 0) / 3600.0) AS speed_mph
    FROM read_parquet('data/raw/fhvhv_tripdata_2026-01.parquet')
), rules AS (
    SELECT
        pickup_datetime IS NOT NULL AND dropoff_datetime IS NOT NULL
            AND PULocationID IS NOT NULL AND DOLocationID IS NOT NULL
            AND trip_miles IS NOT NULL AND trip_time IS NOT NULL
            AND base_passenger_fare IS NOT NULL AS required_fields,
        PULocationID BETWEEN 1 AND 263
            AND DOLocationID BETWEEN 1 AND 263 AS valid_zones,
        dropoff_datetime > pickup_datetime AND trip_time > 0
            AND trip_miles > 0 AS valid_trip,
        base_passenger_fare > 0 AND base_passenger_fare <= 500 AS valid_fare,
        trip_time <= 14400 AND trip_miles <= 100
            AND speed_mph <= 80 AS within_limits
    FROM projected
)
SELECT
    COUNT(*) AS n0,
    COUNT(*) FILTER (WHERE required_fields) AS n1,
    COUNT(*) FILTER (WHERE required_fields AND valid_zones) AS n2,
    COUNT(*) FILTER (WHERE required_fields AND valid_zones AND valid_trip) AS n3,
    COUNT(*) FILTER (WHERE required_fields AND valid_zones AND valid_trip
                     AND valid_fare) AS n4,
    COUNT(*) FILTER (WHERE required_fields AND valid_zones AND valid_trip
                     AND valid_fare AND within_limits) AS n5
FROM rules
"""

labels = (
    "Loại null ở các trường bắt buộc",
    "Loại mã vùng ngoài [1, 263]",
    "Giữ timestamp tăng, thời lượng và quãng đường dương",
    "Giới hạn cước cơ sở trong (0, 500] USD",
    "Giới hạn trên của thời lượng, quãng đường và vận tốc",
)


def main():
    with duckdb.connect() as connection:
        counts = connection.execute(sql).fetchone()

    raw_count = counts[0]
    if not raw_count:
        raise ValueError("Tập dữ liệu gốc rỗng")

    print("| Bước | Quy tắc lọc | Số dòng còn lại | Số dòng bị loại thêm | Tỷ lệ loại trên tập gốc |")
    print("| --- | --- | ---: | ---: | ---: |")
    print(f"| 0 | Dữ liệu gốc đầu vào | {raw_count:,} | — | 0.000% |")

    for step, label in enumerate(labels, start=1):
        dropped = counts[step - 1] - counts[step]
        print(f"| {step} | {label} | {counts[step]:,} | {dropped:,} | {dropped / raw_count:.3%} |")

    total_dropped = raw_count - counts[-1]
    print(f"| **Tổng** | **Silver theo điều kiện pipeline** | **{counts[-1]:,}** | "
          f"**{total_dropped:,}** | **{total_dropped / raw_count:.3%}** |")


if __name__ == "__main__":
    main()
