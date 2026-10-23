
import duckdb, glob, os

files = sorted(glob.glob('data/raw/fhvhv_tripdata_2026-0*.parquet'))
total_rows = 0
total_bytes = 0

print(f'| Tháng | Tệp Parquet | Dung lượng | Số bản ghi thô |')
print('| :---: | :--- | :---: | :---: |')

for f in files:
    size_mb = os.path.getsize(f) / (1024 * 1024)
    total_bytes += os.path.getsize(f)
    rows = duckdb.execute("SELECT COUNT(*) FROM read_parquet(?)", [f]).fetchone()[0]
    total_rows += rows
    print(f'| {f[-10:-8]} | {os.path.basename(f)} | {size_mb:.1f} MB | {rows:,} |')

print(f'| **Tổng (Scaling)** | **5 tháng (01-05/2026)** | **{total_bytes/(1024*1024):.1f} MB** | **{total_rows:,}** |')
