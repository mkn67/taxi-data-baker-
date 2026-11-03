"""Tien xu ly, audit loc tuan tu, ghi tang Silver va luu hotspot P95 (Chuong 2).

Cau hinh RAM/core phai truyen qua spark-submit (client mode: JVM driver da chay truoc khi
SparkSession duoc tao nen khong dat spark.driver.memory trong code duoc):

    podman exec -it spark-master spark-submit \\
      --master spark://spark-master:7077 \\
      --driver-memory 1500m \\
      --conf spark.executor.memory=1800m \\
      /opt/spark/scripts/chapter_02/02_preprocess_and_audit.py

Sau khi chay, kiem tra bo nho thuc te cua driver/executor trong Spark UI.
"""
import math
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT_PATH = os.environ.get("INPUT_PATH", "/opt/bitnami/spark/data/raw/fhvhv_tripdata_2026-01.parquet")
SILVER_PATH = os.environ.get("SILVER_PATH", "/opt/bitnami/spark/data/silver/fhvhv_2026_01_clean.parquet")
HOTSPOT_PU_PATH = os.environ.get("HOTSPOT_PU_PATH", "/opt/bitnami/spark/data/silver/hotspots_p95_pu_2026_01")
HOTSPOT_DO_PATH = os.environ.get("HOTSPOT_DO_PATH", "/opt/bitnami/spark/data/silver/hotspots_p95_do_2026_01")


def dir_size_mb(path):
    """Tinh tong dung luong cac file trong thu muc Parquet (MB)."""
    if not os.path.isdir(path):
        return None
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
    return total / (1024 * 1024)


def exact_p95(counts):
    """P95 chinh xac theo phuong phap nearest-rank tren danh sach cac zone co chuyen."""
    if not counts:
        raise ValueError("Khong co zone hop le de tinh P95.")
    ordered = sorted(counts)
    rank = math.ceil(0.95 * len(ordered))
    return ordered[rank - 1]


def build_hotspots(df, key_col, out_path, label):
    # Cache bang tan suat (chi toi da 263 dong) de tranh tinh lai khi collect, show va write
    zone_freq = (
        df.groupBy(key_col)
          .count()
          .withColumnRenamed("count", "trip_count")
          .cache()
    )
    try:
        counts = [r["trip_count"] for r in zone_freq.select("trip_count").collect()]
        p95_val = exact_p95(counts)
        hotspots = zone_freq.filter(F.col("trip_count") >= p95_val).orderBy(F.desc("trip_count"))
        num_hotspots = hotspots.count()

        print(f"\n>>> [{label}] So zone co chuyen hop le: {len(counts)}/263 | "
              f"Nguong P95 (exact nearest-rank) = {p95_val:,.0f} chuyen | "
              f"So zone diem nong = {num_hotspots}")
        hotspots.show(20, truncate=False)

        hotspots.select(key_col, "trip_count") \
            .write.mode("overwrite").parquet(out_path)
        print(f">>> [{label}] Da ghi hotspot: {out_path}")
    finally:
        zone_freq.unpersist()


def main():
    # Cau hinh RAM va Core duoc truyen tuong minh qua spark-submit (xem docstring)
    spark = SparkSession.builder \
        .appName("02_Preprocess_and_Sequential_Audit") \
        .getOrCreate()

    # 1. Projection Pruning: chi doc 8 cot cot loi
    core_cols = [
        "pickup_datetime", "dropoff_datetime",
        "PULocationID", "DOLocationID",
        "trip_miles", "trip_time",
        "base_passenger_fare", "cbd_congestion_fee",
    ]
    df_raw = spark.read.parquet(INPUT_PATH).select(*core_cols)

    # 2. Dinh nghia bieu thuc dieu kien tuan tu (Dam bao an toan chia cho 0)
    mandatory_cols = [
        "pickup_datetime", "dropoff_datetime",
        "PULocationID", "DOLocationID",
        "trip_miles", "trip_time", "base_passenger_fare",
    ]
    c1 = F.lit(True)
    for col_name in mandatory_cols:
        c1 = c1 & F.col(col_name).isNotNull()

    c2 = c1 & F.col("PULocationID").between(1, 263) & F.col("DOLocationID").between(1, 263)

    c3 = c2 & (F.col("dropoff_datetime") > F.col("pickup_datetime")) & \
         (F.col("trip_time") > 0) & (F.col("trip_miles") > 0.0)

    c4 = c3 & (F.col("base_passenger_fare") > 0.0) & (F.col("base_passenger_fare") <= 500.0)

    # Chi tinh van toc khi trip_time > 0 de tuyet doi khong chia cho 0
    safe_speed_mph = F.when(
        F.col("trip_time") > 0,
        F.col("trip_miles") / (F.col("trip_time") / 3600.0)
    ).otherwise(F.lit(None))

    c5 = c4 & (F.col("trip_time") <= 14400) & \
         (F.col("trip_miles") <= 100.0) & \
         (safe_speed_mph <= 80.0)

    # 3. Audit loc tuan tu + thong ke cbd_congestion_fee: gom thanh MOT lan quet du lieu nguon
    #    (chi phan audit la single-pass; ghi Silver va tinh hotspot van la cac lan doc/ghi rieng)
    audit_row = df_raw.select(
        F.count(F.lit(1)).alias("n0"),
        F.sum(F.when(c1, 1).otherwise(0)).alias("n1"),
        F.sum(F.when(c2, 1).otherwise(0)).alias("n2"),
        F.sum(F.when(c3, 1).otherwise(0)).alias("n3"),
        F.sum(F.when(c4, 1).otherwise(0)).alias("n4"),
        F.sum(F.when(c5, 1).otherwise(0)).alias("n5"),
        F.sum(F.when(F.col("cbd_congestion_fee").isNull(), 1).otherwise(0)).alias("cbd_null"),
        F.sum(F.when(F.col("cbd_congestion_fee") == 0.0, 1).otherwise(0)).alias("cbd_zero"),
        F.sum(F.when(F.col("cbd_congestion_fee") > 0.0, 1).otherwise(0)).alias("cbd_pos"),
        F.sum(F.when(F.col("cbd_congestion_fee") < 0.0, 1).otherwise(0)).alias("cbd_neg"),
    ).first()

    n0, n1, n2, n3, n4, n5 = (
        audit_row["n0"], audit_row["n1"], audit_row["n2"],
        audit_row["n3"], audit_row["n4"], audit_row["n5"]
    )
    if not n0:
        spark.stop()
        sys.exit(f"Tap dau vao rong hoac khong doc duoc: {INPUT_PATH}")

    cbd_null = audit_row["cbd_null"] or 0

    print("\n" + "=" * 65)
    print(">>> KIEM TRA PHAN PHOI CBD_CONGESTION_FEE TREN TAP GOC:")
    print("=" * 65)
    print(f" - null_count     : {cbd_null:,}")
    print(f" - cbd_null_pct   : {100.0 * cbd_null / n0:.4f}%")
    print(f" - zero_count     : {(audit_row['cbd_zero'] or 0):,}")
    print(f" - positive_count : {(audit_row['cbd_pos'] or 0):,}")
    print(f" - negative_count : {(audit_row['cbd_neg'] or 0):,}")
    if cbd_null > 0:
        print(f"!!! CANH BAO: cbd_congestion_fee co {cbd_null:,} ban ghi null. "
              "Cot duoc GIU NGUYEN (khong gop vao 0.0).")
    else:
        print(">>> cbd_congestion_fee khong co null: giu nguyen gia tri goc.")

    print("\n" + "=" * 65)
    print(">>> BANG THONG KE LOC TUAN TU (SEQUENTIAL FILTERING AUDIT):")
    print("=" * 65)
    steps = [
        ("Buoc 0: Du lieu goc dau vao", n0, 0),
        ("Buoc 1: Loai null truong bat buoc", n1, n0 - n1),
        ("Buoc 2: Loai LocationID ngoai [1, 263]", n2, n1 - n2),
        ("Buoc 3: Loai trip_time <= 0 hoac trip_miles <= 0", n3, n2 - n3),
        ("Buoc 4: Loai base_passenger_fare ngoai (0, 500]", n4, n3 - n4),
        ("Buoc 5: Loai ngoai lai cuc dai (time>4h, miles>100, v>80mph)", n5, n4 - n5),
    ]
    for label, rem, dropped in steps:
        pct = (dropped / n0) * 100.0
        print(f"{label:62s} | Con lai: {rem:11,d} | Loai them: {dropped:10,d} ({pct:5.2f}%)")

    total_dropped = n0 - n5
    print("-" * 65)
    print(f"TONG CONG CON LAI: {n5:,} dong | TONG BI LOAI: {total_dropped:,} dong "
          f"({(total_dropped / n0) * 100:.2f}%)")

    # 4. Tao tap du lieu sach va ghi xuong tang Silver
    df_clean = df_raw.filter(c5).withColumn(
        "dropoff_hour", F.hour(F.col("dropoff_datetime"))
    ).withColumn(
        "time_period",
        F.when(F.col("dropoff_hour").between(5, 20), F.lit("Daytime")).otherwise(F.lit("Nighttime")),
    )
    print(f"\n>>> So cot tap sach: {len(df_clean.columns)} -> {df_clean.columns}")

    df_clean.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(SILVER_PATH)
    print(f">>> Da ghi Silver: {SILVER_PATH}")

    size_mb = dir_size_mb(SILVER_PATH)
    if size_mb is not None:
        print(f">>> Dung luong Silver (Snappy): {size_mb:,.1f} MB  -> dien vao bang Muc 2.1")
    else:
        print(">>> Driver khong truy cap truc tiep duoc duong dan Silver; "
              "do dung luong bang: du -sh <thu muc silver> tren volume.")

    # 5. Doc lai tu tap Silver da luu de tinh Hotspot (tranh tinh lai phan loc o tren)
    df_silver = spark.read.parquet(SILVER_PATH)
    build_hotspots(df_silver, "PULocationID", HOTSPOT_PU_PATH, "PULocationID")
    build_hotspots(df_silver, "DOLocationID", HOTSPOT_DO_PATH, "DOLocationID")

    spark.stop()


if __name__ == "__main__":
    main()
