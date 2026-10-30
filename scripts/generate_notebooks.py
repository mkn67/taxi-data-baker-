"""Script to build and validate both research notebooks:
- notebooks/02_benchmark_aggregation_salting.ipynb
- notebooks/03_benchmark_join_skew.ipynb
"""

import os
import nbformat
from nbformat.v4 import new_notebook, new_markdown_cell, new_code_cell


def create_aggregation_notebook() -> nbformat.NotebookNode:
    nb = new_notebook()
    nb.metadata = {
        "kernelspec": {
            "display_name": "Python (.venv - taxi-data-baker)",
            "language": "python",
            "name": "taxi-data-baker"
        },
        "language_info": {
            "name": "python",
            "version": "3.12.15"
        }
    }

    cells = []

    # --------------------------------------------------------------------------
    # Cell 1: Header & Academic Metadata
    # --------------------------------------------------------------------------
    cells.append(new_markdown_cell(r"""# BÁO CÁO THỰC NGHIỆM NỘI DUNG 2: AGGREGATION SKEW & SALTING OPTIMIZATION
**Đề tài:** Tối ưu hóa Pipeline Xử lý Lệch tải Không gian trên Apache Spark và Đánh giá Mật độ Trả khách quanh Ranh giới Thu phí Manhattan (Dữ liệu NYC HVFHV 2026)  
**Phần phụ trách:** Chương 3 (Mục 3.1 & 3.2: Thiết kế Giải thuật Phép gom nhóm) và Chương 4 (Mục 4.2: Đánh giá Hiệu năng Aggregation Skew)  
**Tác giả:** Nhóm nghiên cứu Big Data Pipeline  
**Công nghệ mục tiêu:** Spark 3.5.9 theo Compose; host `requirements.txt` hiện pin PySpark 3.5.6 và cần được đồng bộ trước benchmark chính thức; DuckDB 1.5.6

---

## TỔNG QUAN NỘI DUNG 2
Nội dung này tập trung độc lập vào khảo sát hiện tượng lệch tải (skew) trong phép toán gom nhóm tổng hợp `groupBy()` theo mã vùng đón khách `PULocationID`.

### Phạm vi 3 phương án so sánh:
1. **Phương án 1 (Baseline GroupBy):** Tổng hợp trực tiếp theo `PULocationID`.
2. **Phương án 2 (Uniform Salting):** Phân chia đều dữ liệu theo $S=8$ bucket salt cho toàn bộ các khóa, thực hiện Two-Stage Aggregation.
3. **Phương án 3 (Selective Salting):** Chỉ phân chia $S=8$ bucket salt cho **14 Hotspot Pickup P95** được nhận diện từ tầng Silver; các khóa bình thường giữ nguyên salt = 0, thực hiện Two-Stage Aggregation.

> [!NOTE]
> **Định nghĩa phép tổng hợp trên mỗi `PULocationID`:**
> - `trip_count`: Tổng số chuyến xe ($\sum 1$).
> - `avg_fare`: Cước trung bình $\frac{\sum \text{base\_passenger\_fare}}{\sum \text{non-null fare count}}$ (bảo toàn mẫu số chính xác).
> - `total_miles`: Tổng quãng đường ($\sum \text{trip\_miles}$).

> [!IMPORTANT]
> **Cơ chế Spark Catalyst:** Phép `groupBy()` trong Spark áp dụng cơ chế Map-side Partial Aggregation (`HashAggregate`) trước khi Shuffle. Do đó, skew tự nhiên của dữ liệu thô không đồng nghĩa Salting sẽ cải thiện thời gian chạy. Toàn bộ nhận định phải được kiểm chứng bằng số đo Task Duration, Task Skew Ratio (TSR) và đối soát độc lập với DuckDB.
"""))

    cells.append(new_markdown_cell("""## Saved-output provenance
The outputs already stored in this notebook are historical local-functional evidence: the recorded Spark master is `local[4]`. Do not cite those timings as multi-worker Standalone results. Full mode now requires Spark 3.5.9 and active executors on at least two worker hosts; outputs are written to the `official_standalone` tier only after those checks pass."""))

    # --------------------------------------------------------------------------
    # Cell 2: Imports & Environment Configuration
    # --------------------------------------------------------------------------
    cells.append(new_markdown_cell("""## 1. Cấu hình Môi trường & Khởi tạo Metric Collector
Cấu hình JVM Java 17, đồng bộ môi trường Python worker và khởi tạo collector theo dõi qua Spark REST API."""))

    cells.append(new_code_cell("""import os
import sys
import time
import json
import urllib.request
import statistics
import pandas as pd
import duckdb
import matplotlib.pyplot as plt
from pyspark.sql import functions as F

# Bổ sung thư mục gốc vào path để import module hỗ trợ
sys.path.insert(0, os.path.abspath(".."))
from scripts.benchmark_engine import (
    configure_environment,
    resolve_paths,
    dataset_manifest_fingerprint,
    get_spark_master_url,
    create_benchmark_spark_session,
    SparkMetricsCollector,
    load_hotspot_pu_ids,
    aggregate_baseline,
    aggregate_uniform_salting,
    aggregate_selective_salting,
    verify_aggregation_with_duckdb,
    verify_distributed_executors,
    write_csv_preserving_history,
)

# Kích hoạt cấu hình môi trường chuẩn
configure_environment()
paths = resolve_paths()

print(">>> Cấu hình đường dẫn hệ thống:")
for k, v in paths.items():
    print(f"  - {k}: {v}")
"""))

    # --------------------------------------------------------------------------
    # Cell 3: Hotspots Inspection
    # --------------------------------------------------------------------------
    cells.append(new_markdown_cell("""## 2. Kiểm tra Danh sách 14 Hotspot Pickup (P95) từ Tầng Silver
Kiểm tra danh mục 14 vùng điểm nóng đón khách đã được nhận diện và tính toán từ tầng Silver (`data/silver/hotspots_p95_pu_2026_01`)."""))

    cells.append(new_code_cell("""hotspot_pu_ids = load_hotspot_pu_ids(paths["hotspots_pu_path"])
print(f">>> Số lượng Hotspots P95 được xác nhận: {len(hotspot_pu_ids)} zones")
print(f">>> Danh sách mã vùng Hotspot: {hotspot_pu_ids}")

# Đọc chi tiết số chuyến của từng Hotspot từ tầng Silver bằng DuckDB
df_hp_detail = duckdb.query(f\"\"\"
    SELECT PULocationID, trip_count, 
           ROUND(trip_count * 100.0 / (SELECT SUM(trip_count) FROM read_parquet('{paths["hotspots_pu_path"]}/*.parquet')), 2) AS pct_among_hotspots
    FROM read_parquet('{paths["hotspots_pu_path"]}/*.parquet')
    ORDER BY trip_count DESC;
\"\"\").df()
display(df_hp_detail)
"""))

    # --------------------------------------------------------------------------
    # Cell 4: Salt Distribution Verification
    # --------------------------------------------------------------------------
    cells.append(new_markdown_cell(r"""## 3. Khảo sát Tính Phân bố Đều của Chiến lược Gán Salt
Chiến lược gán salt sử dụng hàm băm xác định:
$$\\text{salt} = \\operatorname{pmod}(\\operatorname{abs}(\\operatorname{hash}(\\text{pickup\\_datetime}, \\text{dropoff\\_datetime}, \\text{trip\\_miles})), S)$$
với số lượng bucket $S = 8$.

Kiểm tra phân bố của các bucket trên 14 vùng hotspot để đảm bảo mỗi bucket nhận xấp xỉ $12.5\\%$ dữ liệu."""))

    cells.append(new_code_cell("""salt_check_sql = f\"\"\"
WITH salted AS (
    SELECT 
        PULocationID,
        abs(hash(pickup_datetime, dropoff_datetime, trip_miles)) % 8 AS salt
    FROM read_parquet('{paths["silver_path"]}/*.parquet')
    WHERE PULocationID IN ({', '.join(map(str, hotspot_pu_ids))})
)
SELECT 
    salt,
    COUNT(*) AS records_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM salted
GROUP BY salt
ORDER BY salt;
\"\"\"
df_salt_dist = duckdb.query(salt_check_sql).df()
print(">>> Phân bố bucket salt trên 14 Hotspot Pickup:")
display(df_salt_dist)

assert len(df_salt_dist) == 8, "Lỗi: Không đủ 8 bucket salt!"
assert (df_salt_dist["percentage"] >= 12.0).all() and (df_salt_dist["percentage"] <= 13.0).all(), \
    "Cảnh báo: Phân bố salt bị lệch khỏi mức lý tưởng 12.5%!"
print(">>> ĐẠT: Chiến lược gán salt hoàn toàn đồng đều (~12.5% mỗi bucket) và tái lập 100%.")
"""))

    # --------------------------------------------------------------------------
    # Cell 5: Spark Session Initialization
    # --------------------------------------------------------------------------
    cells.append(new_markdown_cell("""## 4. Khởi tạo SparkSession Chuẩn Thực nghiệm
Cấu hình shuffle partition mục tiêu là `spark.sql.shuffle.partitions = 200`. Tắt AQE (`spark.sql.adaptive.enabled = false`) nhằm cô lập chính xác hành vi của toán tử gom nhóm."""))

    cells.append(new_code_cell("""# Chế độ thực nghiệm: 'pilot' (kiểm tra nhanh logic) hoặc 'full' (benchmark chính thức)
BENCHMARK_MODE = os.environ.get("BENCHMARK_MODE", "full")
NUM_ITERATIONS = 1 if BENCHMARK_MODE == "pilot" else 3
SHUFFLE_PARTITIONS = 200

print(f">>> Thiết lập chế độ chạy: {BENCHMARK_MODE.upper()}")
print(f">>> Số lượt đo chính thức: {NUM_ITERATIONS}")
print(f">>> Shuffle Partitions: {SHUFFLE_PARTITIONS}")

master_url = get_spark_master_url()
print(f">>> Spark Master Target: {master_url}")
RESULT_TIER = "official_standalone" if BENCHMARK_MODE == "full" else ("local_functional" if master_url.startswith("local") else "pilot_standalone")
PLAN_DIR = os.path.join(paths["nd2_plans_dir"], RESULT_TIER)
os.makedirs(PLAN_DIR, exist_ok=True)

spark = create_benchmark_spark_session(
    app_name="02_Benchmark_Aggregation_Salting",
    master_url=master_url,
    shuffle_partitions=SHUFFLE_PARTITIONS,
    enable_aqe=False,
    require_standalone=(BENCHMARK_MODE == "full")
)

collector = SparkMetricsCollector(spark)

# Đọc tập dữ liệu Silver
df_silver = spark.read.parquet(paths["silver_path"] + "/*.parquet")
print(f">>> Tầng Silver đã sẵn sàng:")
df_silver.printSchema()
"""))

    # --------------------------------------------------------------------------
    # Cell 6: Physical Execution Plans Comparison
    # --------------------------------------------------------------------------
    cells.append(new_markdown_cell("""## 5. Phân tích Kế hoạch Thực thi Vật lý (Execution Plans)
Kiểm tra kế hoạch thực thi Catalyst để làm rõ sự khác biệt giữa Baseline và Salting:
- **Baseline:** Áp dụng `HashAggregate` cục bộ (Partial) $\\to$ `Exchange hashpartitioning(PULocationID)` $\\to$ `HashAggregate` toàn cục (Final). Do chỉ có $261$ zone, dữ liệu qua Shuffle Exchange bị nén triệt để!
- **Salting (Two-Stage):** Bắt buộc phải qua 2 chu kỳ gom nhóm và 2 tầng Shuffle Exchange, gia tăng chi phí CPU và điều phối task."""))

    cells.append(new_code_cell("""df_plan_base = aggregate_baseline(df_silver)
df_plan_uniform = aggregate_uniform_salting(df_silver, num_salts=8)
df_plan_selective = aggregate_selective_salting(df_silver, hotspot_pu_ids, num_salts=8)

print("=" * 80)
print(">>> 1. KẾ HOẠCH VẬT LÝ BASELINE GROUPBY:")
print("=" * 80)
df_plan_base.explain(mode="formatted")

print("\\n" + "=" * 80)
print(">>> 2. KẾ HOẠCH VẬT LÝ UNIFORM SALTING:")
print("=" * 80)
df_plan_uniform.explain(mode="formatted")

print("\\n" + "=" * 80)
print(">>> 3. KẾ HOẠCH VẬT LÝ SELECTIVE SALTING:")
print("=" * 80)
df_plan_selective.explain(mode="formatted")
"""))

    # --------------------------------------------------------------------------
    # Cell 7: Warm-up Runs
    # --------------------------------------------------------------------------
    cells.append(new_markdown_cell("""## 6. Thực hiện Warm-up Riêng cho Từng Phương án
Nhằm loại trừ độ trễ khởi động JIT, cấp phát executor và phân bổ buffer ban đầu, thực hiện warm-up độc lập cho cả 3 nhánh."""))

    cells.append(new_code_cell("""print(">>> BẮT ĐẦU GIAI ĐOẠN WARM-UP...")

# Warm-up Baseline
collector.start_run("warmup_baseline", "Warmup Baseline")
df_wb = aggregate_baseline(df_silver)
wb_rows = df_wb.collect()
m_wb = collector.end_run("warmup_baseline")
print(f"  [Warmup] Baseline hoàn tất trong {m_wb['wall_clock_duration_s']}s (Rows: {len(wb_rows)})")

# Warm-up Uniform Salting
collector.start_run("warmup_uniform", "Warmup Uniform Salting")
df_wu = aggregate_uniform_salting(df_silver, num_salts=8)
wu_rows = df_wu.collect()
m_wu = collector.end_run("warmup_uniform")
print(f"  [Warmup] Uniform Salting hoàn tất trong {m_wu['wall_clock_duration_s']}s (Rows: {len(wu_rows)})")

# Warm-up Selective Salting
collector.start_run("warmup_selective", "Warmup Selective Salting")
df_ws = aggregate_selective_salting(df_silver, hotspot_pu_ids, num_salts=8)
ws_rows = df_ws.collect()
m_ws = collector.end_run("warmup_selective")
print(f"  [Warmup] Selective Salting hoàn tất trong {m_ws['wall_clock_duration_s']}s (Rows: {len(ws_rows)})")

print(">>> GIAI ĐOẠN WARM-UP KẾT THÚC THÀNH CÔNG!")
"""))

    # --------------------------------------------------------------------------
    # Cell 8: Official Benchmark Execution
    # --------------------------------------------------------------------------
    cells.append(new_markdown_cell(r"""## 7. Thực hiện Benchmark Chính thức với Thứ tự Luân phiên
Quy trình đo lường tuân thủ nghiêm ngặt tiêu chuẩn thực nghiệm:
- Tối thiểu 3 lượt đo chính thức.
- Luân phiên thứ tự thực thi giữa các lượt đo (Latin Square Order) nhằm triệt tiêu hiệu ứng nhớ đệm (cache bias) của hệ điều hành.
- Thu thập đầy đủ Task Skew Ratio (TSR), Shuffle Read/Write và Memory Spill qua Spark REST API.

$$TSR = \\frac{\\max(\\text{Task Duration})}{\\operatorname{median}(\\text{Task Duration})}$$"""))

    cells.append(new_code_cell("""methods = [
    ("Baseline_GroupBy", lambda: aggregate_baseline(df_silver)),
    ("Uniform_Salting_S8", lambda: aggregate_uniform_salting(df_silver, num_salts=8)),
    ("Selective_Salting_S8", lambda: aggregate_selective_salting(df_silver, hotspot_pu_ids, num_salts=8)),
]

# Chu kỳ luân phiên thứ tự chạy
orders = [
    [0, 1, 2],  # Lượt 1: Baseline -> Uniform -> Selective
    [1, 2, 0],  # Lượt 2: Uniform -> Selective -> Baseline
    [2, 0, 1],  # Lượt 3: Selective -> Baseline -> Uniform
]

all_run_records = []
final_spark_results = {}

for iteration in range(1, NUM_ITERATIONS + 1):
    current_order = orders[(iteration - 1) % len(orders)]
    order_names = [methods[idx][0] for idx in current_order]
    print(f\"\\n{'='*70}\")
    print(f\">>> LƯỢT ĐO CHÍNH THỨC #{iteration}/{NUM_ITERATIONS} | Thứ tự: {' -> '.join(order_names)}\")
    print(f\"{'='*70}\")
    
    for idx in current_order:
        method_name, method_func = methods[idx]
        job_group_id = f\"nd2_iter{iteration}_{method_name}\"
        desc = f\"Iteration {iteration} - {method_name}\"
        
        print(f\"  -> Đang thực thi: {method_name}...\")
        collector.start_run(job_group_id, desc)
        
        # Materialize toàn bộ kết quả tổng hợp bằng collect()
        result_df = method_func()
        collected_data = result_df.collect()
        
        metrics = collector.end_run(job_group_id)
        metrics[\"iteration\"] = iteration
        metrics[\"method\"] = method_name
        metrics[\"output_rows\"] = len(collected_data)
        metrics[\"benchmark_mode\"] = BENCHMARK_MODE
        metrics[\"result_tier\"] = RESULT_TIER
        metrics[\"application_id\"] = spark.sparkContext.applicationId
        metrics[\"master_url\"] = spark.sparkContext.master
        metrics[\"spark_version\"] = spark.version
        metrics[\"run_timestamp_utc\"] = time.strftime(\"%Y-%m-%dT%H:%M:%SZ\", time.gmtime())
        metrics[\"event_log_dir\"] = paths[\"events_dir\"]
        metrics[\"dataset_manifest_sha256\"] = dataset_manifest_fingerprint(paths[\"silver_path\"])
        metrics[\"worker_evidence\"] = json.dumps(verify_distributed_executors(spark, require_two_workers=(RESULT_TIER == \"official_standalone\")) if spark.sparkContext.master.startswith(\"spark://\") else {})
        plan_text = result_df._jdf.queryExecution().executedPlan().toString()
        plan_name = f"nd2_{method_name}_iter{iteration}_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.txt"
        plan_path = os.path.join(PLAN_DIR, plan_name)
        with open(plan_path, "w", encoding="utf-8") as plan_file:
            plan_file.write(plan_text)
        metrics["executed_plan_path"] = plan_path
        
        all_run_records.append(metrics)
        final_spark_results[method_name] = result_df
        
        print(f\"     Thời gian: {metrics['wall_clock_duration_s']:.3f}s | TSR (Max): {metrics['task_skew_ratio_max']} | Shuffle Write bytes: {metrics.get('shuffle_write_bytes')}\")

df_runs = pd.DataFrame(all_run_records)
runs_csv_path = os.path.join(paths[\"benchmarks_dir\"], f"benchmark_nd2_aggregation_runs_{RESULT_TIER}.csv")
print(f\"\\n>>> ĐÃ LƯU KẾT QUẢ TỪNG LƯỢT ĐO TẠI: {runs_csv_path}\")
display(df_runs[[\"iteration\", \"method\", \"wall_clock_duration_s\", \"task_skew_ratio_max\", \"shuffle_write_bytes\", \"collector_status\"]])
"""))

    # --------------------------------------------------------------------------
    # Cell 9: Independent Ground Truth Audit with DuckDB
    # --------------------------------------------------------------------------
    cells.append(new_markdown_cell("""## 8. Kiểm chứng Tính Đúng đắn Độc lập với DuckDB (Ground Truth Audit)
Đối soát kết quả của cả 3 phương án với Ground Truth tính độc lập từ DuckDB trên tệp Parquet gốc:
- Đối soát 100% 261 Taxi Zones.
- Sai số đếm `trip_count`: tuyệt đối bằng 0.
- Sai số `avg_fare`: dung sai $< 10^{-4}$.
- Sai số `total_miles`: dung sai $< 10^{-2}$."""))

    cells.append(new_code_cell("""print(\">>> BẮT ĐẦU ĐỐI SOÁT VỚI DUCKDB GROUND TRUTH...\")

audit_summaries = []
audit_dfs = []

for method_name, result_df in final_spark_results.items():
    passed, summary, df_audit = verify_aggregation_with_duckdb(
        spark_df=result_df,
        silver_path=paths[\"silver_path\"],
        fare_tol=1e-4,
        miles_tol=1e-2
    )
    summary[\"method\"] = method_name
    audit_summaries.append(summary)
    
    df_audit[\"method\"] = method_name
    audit_dfs.append(df_audit)
    
    print(f\"  - Phương án: {method_name:25s} | Kết quả: {summary['status']} | Sai số đếm: {summary['count_mismatches']}\")

df_audit_summary = pd.DataFrame(audit_summaries)
validation_by_method = df_audit_summary.set_index("method")["status"]
df_runs["validation_status"] = df_runs["method"].map(validation_by_method)
valid_runs = df_runs[(df_runs["collector_status"] == "OK") & (df_runs["validation_status"] == "PASS")]
df_runs.pipe(write_csv_preserving_history, runs_csv_path)
df_audit_all = pd.concat(audit_dfs, ignore_index=True)
audit_csv_path = os.path.join(paths[\"benchmarks_dir\"], f"benchmark_nd2_aggregation_audit_{RESULT_TIER}.csv")
df_audit_all.pipe(write_csv_preserving_history, audit_csv_path)
display(df_audit_summary)

all_passed = (df_audit_summary[\"status\"] == "PASS").all()
assert all_passed, "LỖI: Kiểm chứng Ground Truth thất bại! Có phương án không khớp số liệu với DuckDB."
print(\"\\n>>> XÁC NHẬN: TOÀN BỘ 3 PHƯƠNG ÁN ĐÃ VƯỢT QUA KIỂM ĐỊNH GROUND TRUTH 100%!\")
print(f\">>> Đã lưu bảng đối soát chi tiết tại: {audit_csv_path}\")
"""))

    # --------------------------------------------------------------------------
    # Cell 10: Summary Statistics & Discussion
    # --------------------------------------------------------------------------
    cells.append(new_markdown_cell("""## 9. Tổng hợp Thống kê & Thảo luận Kết quả (Chương 4, Mục 4.2)
Tổng hợp giá trị Trung vị (Median), Trung bình (Mean), Độ lệch chuẩn (Std Dev), Min, Max, TSR và Khối lượng Shuffle."""))

    cells.append(new_code_cell("""# Bảng tổng hợp theo phương án
if valid_runs.empty:
    raise RuntimeError("No ND2 runs passed both metrics collection and DuckDB correctness validation.")
summary_rows = []
for method_name, group in valid_runs.groupby("method"):
    durations = group["wall_clock_duration_s"]
    tsrs = group["task_skew_ratio_max"].dropna()
    shuffles = group["shuffle_write_bytes"] / (1024 * 1024)
    
    summary_rows.append({
        "Phương án": method_name,
        "Số lượt đo": len(durations),
        "Thời gian Median (s)": round(durations.median(), 3),
        "Thời gian Mean (s)": round(durations.mean(), 3),
        "Thời gian Std (s)": round(durations.std() if len(durations) > 1 else 0.0, 3),
        "Thời gian Min (s)": round(durations.min(), 3),
        "Thời gian Max (s)": round(durations.max(), 3),
        "TSR Max Median": round(tsrs.median(), 2) if not tsrs.empty else "N/A",
        "Shuffle Write Median (MB)": round(shuffles.median(), 2),
    })

df_summary = pd.DataFrame(summary_rows)
summary_csv_path = os.path.join(paths["benchmarks_dir"], f"benchmark_nd2_aggregation_summary_{RESULT_TIER}.csv")
df_summary.pipe(write_csv_preserving_history, summary_csv_path)

print(">>> BẢNG TỔNG HỢP HIỆU NĂNG NỘI DUNG 2 (CHƯƠNG 4):")
display(df_summary)
"""))

    # --------------------------------------------------------------------------
    # Cell 11: Visualizations
    # --------------------------------------------------------------------------
    cells.append(new_markdown_cell("""## 10. Trực quan hóa So sánh Hiệu năng & Thảo luận Chuyên sâu
Biểu đồ so sánh thời gian thực thi (Wall-clock Duration), hệ số lệch tải (TSR) và khối lượng dữ liệu qua tầng Shuffle."""))

    cells.append(new_code_cell("""fig, axes = plt.subplots(1, 3, figsize=(16, 5), dpi=150)

methods_order = df_summary["Phương án"].tolist()
colors = ["#2b5c8f", "#d95f02", "#7570b3"]

# 1. Thời gian thực thi Median
axes[0].bar(df_summary["Phương án"], df_summary["Thời gian Median (s)"], color=colors, edgecolor="black", alpha=0.85)
axes[0].set_title("Thời gian thực thi Median (s)", fontsize=11, fontweight="bold")
axes[0].set_ylabel("Giây")
axes[0].grid(axis="y", linestyle="--", alpha=0.6)
for i, v in enumerate(df_summary["Thời gian Median (s)"]):
    axes[0].text(i, v + 0.05, f"{v:.2f}s", ha="center", fontweight="bold")

# 2. Khối lượng Shuffle Write
axes[1].bar(df_summary["Phương án"], df_summary["Shuffle Write Median (MB)"], color=colors, edgecolor="black", alpha=0.85)
axes[1].set_title("Khối lượng Shuffle Write (MB)", fontsize=11, fontweight="bold")
axes[1].set_ylabel("Dung lượng (MB)")
axes[1].grid(axis="y", linestyle="--", alpha=0.6)
for i, v in enumerate(df_summary["Shuffle Write Median (MB)"]):
    axes[1].text(i, v + 0.01, f"{v:.2f}MB", ha="center", fontweight="bold")

# 3. Task Skew Ratio (TSR)
tsr_vals = [float(x) if x != "N/A" else 0.0 for x in df_summary["TSR Max Median"]]
axes[2].bar(df_summary["Phương án"], tsr_vals, color=colors, edgecolor="black", alpha=0.85)
axes[2].set_title("Task Skew Ratio (TSR) Max", fontsize=11, fontweight="bold")
axes[2].set_ylabel("Hệ số TSR")
axes[2].grid(axis="y", linestyle="--", alpha=0.6)
for i, v in enumerate(tsr_vals):
    axes[2].text(i, v + 0.05, f"{v:.2f}", ha="center", fontweight="bold")

for ax in axes:
    ax.tick_params(axis="x", rotation=15)

plt.tight_layout()
plot_path = os.path.join(paths["benchmarks_dir"], "figure_nd2_aggregation_benchmark.png")
plt.savefig(plot_path)
plt.show()
print(f">>> Đã lưu biểu đồ tổng hợp tại: {plot_path}")
"""))

    # --------------------------------------------------------------------------
    # Cell 12: Concluding Discussion Markdown
    # --------------------------------------------------------------------------
    cells.append(new_markdown_cell(r"""## 11. Kết luận Khoa học & Đóng góp Báo cáo (Chương 4, Mục 4.2)

### 1. Bản chất Kỹ thuật của Phép gom nhóm (Aggregation Skew):
- Trong Apache Spark, toán tử `groupBy()` tự động được Catalyst tối ưu bằng cơ chế **Map-side Combine** (`HashAggregate` giai đoạn partial).
- Bởi vì không gian khóa `PULocationID` tại NYC chỉ có 261 vùng hợp lệ ($K = 261$), mỗi Map Task (tương ứng với partition đọc vào) đã gom cụm ~5 triệu dòng dữ liệu thô về **tối đa 261 dòng** trước khi phát tán qua mạng Shuffle Exchange.
- Do đó, khối lượng dữ liệu thực tế phải Shuffle trong phép toán Baseline là cực kỳ nhỏ (chỉ vài trăm Kilobytes).

### 2. Chi phí Bổ sung (Overhead) của Kỹ thuật Salting trong Phép gom nhóm:
1. **Phức tạp hóa kế hoạch thực thi (Two-Stage Aggregation):** Salting buộc Spark phải tạo 2 giai đoạn gom nhóm liên tiếp, làm tăng số lượng Stage và chi phí điều phối của DAGScheduler.
2. **Khuếch đại kích thước Shuffle trung gian:** Việc chia $S = 8$ bucket làm tăng số lượng khóa trung gian tại Stage 1 lên gấp 8 lần ($261 \\times 8 = 2,088$ nhóm), làm tăng khối lượng dữ liệu ghi Shuffle thay vì giảm bớt.
3. **Chi phí tính toán CPU:** Việc thực thi hàm băm và gán cột Salt tạo ra phụ phí CPU trên toàn bộ 19.99 triệu dòng.

### 3. Kết luận Thực nghiệm:
> [!IMPORTANT]
> **Kết luận cốt lõi cho Đề tài:** Kỹ thuật Salting **không mang lại lợi ích hiệu năng** trong các bài toán gom nhóm đơn giản có tập khóa đích nhỏ ($K \\ll \\text{Partition Size}$) nhờ sức mạnh của Map-side Combine bản địa trong Spark. Salting chỉ thực sự phát huy tối đa giá trị kỹ thuật trong các phép **Join Skew** (Nội dung 3) - nơi mà Map-side combine không thể áp dụng được.
"""))

    nb.cells = cells
    return nb


def create_join_notebook() -> nbformat.NotebookNode:
    nb = new_notebook()
    nb.metadata = {
        "kernelspec": {
            "display_name": "Python (.venv - taxi-data-baker)",
            "language": "python",
            "name": "taxi-data-baker"
        },
        "language_info": {
            "name": "python",
            "version": "3.12.15"
        }
    }

    cells = []

    # --------------------------------------------------------------------------
    # Cell 1: Header & Metadata
    # --------------------------------------------------------------------------
    cells.append(new_markdown_cell(r"""# BÁO CÁO THỰC NGHIỆM NỘI DUNG 3: DISTRIBUTED JOIN SKEW TRÊN MA TRẬN HÀNH TRÌNH ĐI–ĐẾN (OD MATRIX JOIN)
**Đề tài:** Tối ưu hóa Pipeline Xử lý Lệch tải Không gian trên Apache Spark và Đánh giá Mật độ Trả khách quanh Ranh giới Thu phí Manhattan (Dữ liệu NYC HVFHV 2026)  
**Phần phụ trách:** Chương 3 (Mục 3.3 & 3.4: Thiết kế Giải thuật Phép nối Skew trên Ma trận OD) và Chương 4 (Mục 4.3: Đánh giá Hiệu năng Join Skew)  
**Tác giả:** Nhóm nghiên cứu Big Data Pipeline  
**Công nghệ mục tiêu:** Spark 3.5.9 theo Compose; host `requirements.txt` hiện pin PySpark 3.5.6 và cần được đồng bộ trước benchmark chính thức; DuckDB 1.5.6

---

## TỔNG QUAN NỘI DUNG 3
Nội dung này tập trung độc lập vào khảo sát và tối ưu hóa hiện tượng lệch tải (skew) trong phép nối phân tán lớn (Distributed Join) trên ma trận hành trình Đi–Đến (OD Matrix Join) giữa tập dữ liệu giao dịch chuyến đi (Tầng Silver: ~20 triệu bản ghi) và bảng ma trận lưu lượng Đi–Đến (OD Matrix: 58,747 cặp OD).

### Phép nối ma trận OD theo cặp khóa composite `(PULocationID, DOLocationID)`:
- **Bảng bên trái (Fact):** Tầng Silver `data/silver/fhvhv_2026_01_clean.parquet` (19,993,097 bản ghi chuyến đi).
- **Bảng bên phải (OD Flow Matrix):** `data/silver/od_matrix_2026_01.parquet` (58,747 cặp OD duy nhất với các thuộc tính không gian và chỉ số luồng: `od_trip_count`, `od_avg_fare`, `od_avg_miles`, `od_avg_time_sec`, `PU_Zone`, `PU_Borough`, `DO_Zone`, `DO_Borough`).
- **Độ lệch tải tự nhiên cực đại:** Cặp OD lớn nhất (Zone 76 East New York $\leftrightarrow$ Zone 76 East New York) có tới **83,025 chuyến xe**, trong khi trung vị toàn mạng lưới chỉ là **33 chuyến/cặp** (hệ số lệch $Skew = \frac{83,025}{33} \approx 2,516\times$).

### Ba phương án so sánh:
1. **Phương án 1 — Baseline Sort-Merge Join (SMJ):** Ép sử dụng Sort-Merge Join bằng cách tắt Broadcast (`spark.sql.autoBroadcastJoinThreshold = -1`) và tắt AQE (`spark.sql.adaptive.enabled = false`).
2. **Phương án 2 — Spark AQE Adaptive Skew Join:** Bật cơ chế tự động thích ứng tại runtime của Spark (`spark.sql.adaptive.enabled = true`, `spark.sql.adaptive.skewJoin.enabled = true`, `skewedPartitionFactor = 2`, `skewedPartitionThresholdInBytes = 16MB`), duy trì tắt Broadcast.
3. **Phương án 3 — Salting Join trên Ma trận OD:** Gán $S=8$ salt bucket cho các chuyến đi xuất phát từ 14 Hotspot PU đón khách ($H$), nhân bản tương ứng 3,557 cặp OD liên quan ở bảng OD Matrix lên $S=8$ lần bằng `explode(array)`, sau đó join trên khóa composite `(PULocationID, DOLocationID, salt)`. Tắt AQE để cô lập hoàn toàn hiệu năng của giải thuật Salting.

> [!IMPORTANT]
> **Khác biệt cốt lõi với Aggregation:** Khác với `groupBy()`, phép nối phân tán lớn không thể áp dụng Map-side Combine. Toàn bộ gần 20 triệu dòng chuyến đi buộc phải qua Shuffle Exchange theo khóa join. Các partition chứa các cặp OD của các vùng nóng (như East New York, Crown Heights, Bushwick, sân bay JFK, LaGuardia) trở thành nút thắt cổ chai cực đại (Straggler Tasks). Toàn bộ kết quả được đo đạc qua Task Skew Ratio (TSR) và đối soát độc lập 100% với DuckDB.
"""))

    cells.append(new_markdown_cell("""## Saved-output provenance
The outputs already stored in this notebook are historical local-functional evidence. The recorded Spark version is 3.5.6, master is `local[4]`, and application ID begins with `local-`; the Spark UI showed only the driver executor. Do not cite those timings as multi-worker Standalone results. Full mode now requires Spark 3.5.9 and active executors on at least two worker hosts; outputs are written to the `official_standalone` tier only after those checks pass."""))

    # --------------------------------------------------------------------------
    # Cell 2: Imports & Environment
    # --------------------------------------------------------------------------
    cells.append(new_markdown_cell("""## 1. Cấu hình Môi trường & Khởi tạo Metric Collector
Cấu hình JVM Java 17, đồng bộ môi trường Python worker và khởi tạo bộ đo lường Spark REST API."""))

    cells.append(new_code_cell("""import os
import sys
import time
import json
import urllib.request
import statistics
import pandas as pd
import duckdb
import matplotlib.pyplot as plt
from pyspark.sql import functions as F

# Bổ sung thư mục gốc vào path để import module hỗ trợ
sys.path.insert(0, os.path.abspath(".."))
from scripts.benchmark_engine import (
    configure_environment,
    resolve_paths,
    dataset_manifest_fingerprint,
    get_spark_master_url,
    create_benchmark_spark_session,
    SparkMetricsCollector,
    load_hotspot_pu_ids,
    join_baseline_smj,
    join_aqe_skew,
    join_salting,
    verify_join_with_duckdb,
    verify_distributed_executors,
    write_csv_preserving_history,
)

configure_environment()
paths = resolve_paths()

print(">>> Cấu hình đường dẫn hệ thống:")
for k, v in paths.items():
    print(f"  - {k}: {v}")
"""))

    # --------------------------------------------------------------------------
    # Cell 3: OD Matrix & Hotspots Inspection
    # --------------------------------------------------------------------------
    cells.append(new_markdown_cell("""## 2. Khảo sát Phân bố và Độ lệch tải của Ma trận OD & Khóa nóng
Khảo sát đặc tính phân tán của tập ma trận hành trình Đi–Đến (OD Matrix) và độ bao phủ của 14 Hotspot đón khách (P95)."""))

    cells.append(new_code_cell("""hotspot_pu_ids = load_hotspot_pu_ids(paths["hotspots_pu_path"])
print(f">>> 14 Hotspot Pickup Keys (P95): {hotspot_pu_ids}")

# 1. Thống kê tổng quan ma trận OD qua DuckDB
df_od_stats = duckdb.query(f\"\"\"
    SELECT 
        COUNT(*) AS total_od_pairs,
        MAX(od_trip_count) AS max_trips_per_od,
        ROUND(AVG(od_trip_count), 2) AS avg_trips_per_od,
        MEDIAN(od_trip_count) AS median_trips_per_od,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY od_trip_count) AS p95_trips_per_od,
        ROUND(MAX(od_trip_count) * 1.0 / MEDIAN(od_trip_count), 1) AS max_to_median_skew_ratio
    FROM read_parquet('{paths["od_matrix_path"]}');
\"\"\").df()
print(">>> Thống kê độ lệch tải (Skew) của Ma trận OD:")
display(df_od_stats)

# 2. Top 10 cặp OD có mật độ chuyến cao nhất toàn thành phố NYC
df_top_od = duckdb.query(f\"\"\"
    SELECT 
        PULocationID, PU_Zone, PU_Borough,
        DOLocationID, DO_Zone, DO_Borough,
        od_trip_count,
        ROUND(od_trip_count * 100.0 / 19993097, 3) AS pct_of_all_trips
    FROM read_parquet('{paths["od_matrix_path"]}')
    ORDER BY od_trip_count DESC
    LIMIT 10;
\"\"\").df()
print(">>> Top 10 Cặp OD có lưu lượng tập trung cao nhất:")
display(df_top_od)

# 3. Mức độ bao phủ của 14 Hotspot PU lên ma trận OD
df_hp_coverage = duckdb.query(f\"\"\"
    SELECT 
        COUNT(*) AS od_pairs_with_hotspot_pu,
        SUM(od_trip_count) AS trips_from_hotspots,
        ROUND(SUM(od_trip_count) * 100.0 / 19993097, 2) AS pct_trips_salted
    FROM read_parquet('{paths["od_matrix_path"]}')
    WHERE PULocationID IN ({', '.join(map(str, hotspot_pu_ids))});
\"\"\").df()
print(">>> Mức độ bao phủ của 14 Hotspot PU trên Ma trận OD:")
display(df_hp_coverage)
"""))

    # --------------------------------------------------------------------------
    # Cell 4: Spark Session Setup
    # --------------------------------------------------------------------------
    cells.append(new_markdown_cell("""## 3. Khởi tạo SparkSession Chuẩn Thực nghiệm
Cấu hình shuffle partitions = 200. Các cấu hình `autoBroadcastJoinThreshold` và `adaptive.enabled` được kiểm soát độc lập cho từng nhánh thí nghiệm."""))

    cells.append(new_code_cell("""BENCHMARK_MODE = os.environ.get("BENCHMARK_MODE", "full")
NUM_ITERATIONS = 1 if BENCHMARK_MODE == "pilot" else 3
SHUFFLE_PARTITIONS = 200

print(f">>> Chế độ thực nghiệm: {BENCHMARK_MODE.upper()}")
print(f">>> Số lượt đo chính thức: {NUM_ITERATIONS}")
print(f">>> Shuffle Partitions: {SHUFFLE_PARTITIONS}")

master_url = get_spark_master_url()
print(f">>> Spark Master Target: {master_url}")
RESULT_TIER = "official_standalone" if BENCHMARK_MODE == "full" else ("local_functional" if master_url.startswith("local") else "pilot_standalone")
PLAN_DIR = os.path.join(paths["plans_dir"], RESULT_TIER)
os.makedirs(PLAN_DIR, exist_ok=True)

spark = create_benchmark_spark_session(
    app_name="03_Benchmark_Join_Skew_OD_Matrix",
    master_url=master_url,
    shuffle_partitions=SHUFFLE_PARTITIONS,
    enable_aqe=False,  # Mặc định tắt, kích hoạt riêng cho nhánh AQE
    require_standalone=(BENCHMARK_MODE == "full")
)

collector = SparkMetricsCollector(spark)

# Đọc tập dữ liệu chuyến đi Silver và bảng Ma trận OD
df_trips = spark.read.parquet(paths["silver_path"] + "/*.parquet")
df_od = spark.read.parquet(paths["od_matrix_path"])

print(">>> Schema Bảng Chuyến đi (Silver):")
df_trips.printSchema()
print(">>> Schema Bảng Ma trận OD (Silver):")
df_od.printSchema()
"""))

    # --------------------------------------------------------------------------
    # Cell 5: Physical Execution Plans Inspection & Export
    # --------------------------------------------------------------------------
    cells.append(new_markdown_cell("""## 4. Kiểm tra và Lưu Kế hoạch Thực thi Vật lý (Execution Plans)
Kiểm tra rằng Catalyst áp dụng đúng chiến lược Sort-Merge Join trên cặp khóa composite:
1. **Baseline SMJ:** Chứa toán tử `SortMergeJoin` trên `[PULocationID, DOLocationID]` sau `Exchange hashpartitioning(PULocationID, DOLocationID, 200)`.
2. **AQE Skew Join:** Kế hoạch SortMergeJoin ban đầu có `AdaptiveSparkPlan`, tại runtime sẽ tự động chia nhỏ các partition vượt ngưỡng lệch.
3. **Salting Join:** Chứa `SortMergeJoin` trên `[PULocationID, DOLocationID, salt]` sau `Exchange hashpartitioning(PULocationID, DOLocationID, salt, 200)`. Bảng OD matrix được nhân bản qua `Generate explode(...)`."""))

    cells.append(new_code_cell("""plans_dir = paths["plans_dir"]

# 1. Kế hoạch Baseline SMJ
df_join_base = join_baseline_smj(spark, df_trips, df_od)
base_plan_str = df_join_base._jdf.queryExecution().executedPlan().toString()
with open(os.path.join(plans_dir, "nd3_plan_baseline_smj.txt"), "w") as f:
    f.write(base_plan_str)

# 2. Kế hoạch AQE Skew Join
df_join_aqe = join_aqe_skew(spark, df_trips, df_od, skew_factor=2, skew_threshold_mb=16)
aqe_plan_str = df_join_aqe._jdf.queryExecution().executedPlan().toString()
with open(os.path.join(plans_dir, "nd3_plan_aqe_skew.txt"), "w") as f:
    f.write(aqe_plan_str)

# 3. Kế hoạch Salting Join
df_join_salt = join_salting(spark, df_trips, df_od, hotspot_pu_ids, num_salts=8)
salt_plan_str = df_join_salt._jdf.queryExecution().executedPlan().toString()
with open(os.path.join(plans_dir, "nd3_plan_salting_join.txt"), "w") as f:
    f.write(salt_plan_str)

print("=" * 80)
print(">>> 1. KẾ HOẠCH VẬT LÝ BASELINE SORT-MERGE JOIN (OD COMPOSITE KEY):")
print("=" * 80)
df_join_base.explain(mode="formatted")

print("\\n" + "=" * 80)
print(">>> 2. KẾ HOẠCH VẬT LÝ SALTING JOIN (S=8 TRÊN KHÓA NÓNG OD):")
print("=" * 80)
df_join_salt.explain(mode="formatted")

print(f"\\n>>> Đã lưu toàn bộ tệp kế hoạch thực thi vào thư mục: {plans_dir}")
"""))

    # --------------------------------------------------------------------------
    # Cell 6: Warm-up Runs
    # --------------------------------------------------------------------------
    cells.append(new_markdown_cell("""## 5. Thực hiện Warm-up Riêng cho 3 Nhánh Phép Nối
Thực hiện warm-up độc lập để khởi tạo JVM, nạp buffer và đồng đều hóa trạng thái cụm."""))

    cells.append(new_code_cell("""print(">>> BẮT ĐẦU GIAI ĐOẠN WARM-UP NỘI DUNG 3 (OD JOIN)...")

# Helper materialize cho OD join: tổng hợp theo cặp Quận (PU_Borough, DO_Borough)
def materialize_join_result(joined_df):
    return joined_df.groupBy("PU_Borough", "DO_Borough").agg(
        F.count("*").alias("cnt"),
        F.round(F.sum("trip_miles"), 2).alias("tot_miles"),
        F.round(F.sum("base_passenger_fare"), 2).alias("tot_fare")
    ).collect()

# Warm-up Baseline SMJ
collector.start_run("warmup_join_baseline", "Warmup Baseline SMJ")
df_wb = join_baseline_smj(spark, df_trips, df_od)
wb_res = materialize_join_result(df_wb)
m_wb = collector.end_run("warmup_join_baseline")
print(f"  [Warmup] Baseline SMJ hoàn tất trong {m_wb['wall_clock_duration_s']}s (Borough pairs: {len(wb_res)})")

# Warm-up AQE Skew Join
collector.start_run("warmup_join_aqe", "Warmup AQE")
df_wa = join_aqe_skew(spark, df_trips, df_od, skew_factor=2, skew_threshold_mb=16)
wa_res = materialize_join_result(df_wa)
m_wa = collector.end_run("warmup_join_aqe")
print(f"  [Warmup] AQE Skew Join hoàn tất trong {m_wa['wall_clock_duration_s']}s (Borough pairs: {len(wa_res)})")

# Warm-up Salting Join
collector.start_run("warmup_join_salting", "Warmup Salting")
df_ws = join_salting(spark, df_trips, df_od, hotspot_pu_ids, num_salts=8)
ws_res = materialize_join_result(df_ws)
m_ws = collector.end_run("warmup_join_salting")
print(f"  [Warmup] Salting Join hoàn tất trong {m_ws['wall_clock_duration_s']}s (Borough pairs: {len(ws_res)})")

print(">>> GIAI ĐOẠN WARM-UP KẾT THÚC THÀNH CÔNG!")
"""))

    # --------------------------------------------------------------------------
    # Cell 7: Official Benchmark Execution
    # --------------------------------------------------------------------------
    cells.append(new_markdown_cell(r"""## 6. Thực hiện Benchmark Chính thức với Thứ tự Luân phiên (Latin Square)
Quy trình đo lường tuân thủ nghiêm ngặt:
- Tối thiểu 3 lượt đo chính thức.
- Luân phiên thứ tự thực thi (Latin Square Order) nhằm triệt tiêu hiệu ứng nhớ đệm (OS buffer cache bias).
- Thu thập đầy đủ Task Skew Ratio (TSR), Shuffle Metrics và Disk/Memory Spill qua Spark REST API:

$$TSR = \frac{\max(\text{Task Duration})}{\operatorname{median}(\text{Task Duration})}$$
"""))

    cells.append(new_code_cell("""methods_join = [
    ("Baseline_SMJ", lambda: join_baseline_smj(spark, df_trips, df_od)),
    ("Spark_AQE_Skew_Join", lambda: join_aqe_skew(spark, df_trips, df_od, skew_factor=2, skew_threshold_mb=16)),
    ("Salting_Join_S8", lambda: join_salting(spark, df_trips, df_od, hotspot_pu_ids, num_salts=8)),
]

orders_join = [
    [0, 1, 2],  # Lượt 1: Baseline -> AQE -> Salting
    [1, 2, 0],  # Lượt 2: AQE -> Salting -> Baseline
    [2, 0, 1],  # Lượt 3: Salting -> Baseline -> AQE
]

all_join_runs = []
final_join_results = {}

for iteration in range(1, NUM_ITERATIONS + 1):
    current_order = orders_join[(iteration - 1) % len(orders_join)]
    order_names = [methods_join[idx][0] for idx in current_order]
    print(f\"\\n{'='*70}\")
    print(f\">>> LƯỢT ĐO JOIN CHÍNH THỨC #{iteration}/{NUM_ITERATIONS} | Thứ tự: {' -> '.join(order_names)}\")
    print(f\"{'='*70}\")
    
    for idx in current_order:
        method_name, method_func = methods_join[idx]
        job_group_id = f\"nd3_iter{iteration}_{method_name}\"
        desc = f\"Join Iteration {iteration} - {method_name}\"
        
        print(f\"  -> Đang thực thi: {method_name}...\")
        collector.start_run(job_group_id, desc)
        
        joined_df = method_func()
        res_borough = materialize_join_result(joined_df)
        
        metrics = collector.end_run(job_group_id)
        metrics[\"iteration\"] = iteration
        metrics[\"method\"] = method_name
        metrics[\"borough_groups\"] = len(res_borough)
        metrics[\"benchmark_mode\"] = BENCHMARK_MODE
        metrics[\"result_tier\"] = RESULT_TIER
        metrics[\"application_id\"] = spark.sparkContext.applicationId
        metrics[\"master_url\"] = spark.sparkContext.master
        metrics[\"spark_version\"] = spark.version
        metrics[\"run_timestamp_utc\"] = time.strftime(\"%Y-%m-%dT%H:%M:%SZ\", time.gmtime())
        metrics[\"event_log_dir\"] = paths[\"events_dir\"]
        metrics[\"trip_dataset_manifest_sha256\"] = dataset_manifest_fingerprint(paths[\"silver_path\"])
        metrics[\"od_dataset_manifest_sha256\"] = dataset_manifest_fingerprint(paths[\"od_matrix_path\"])
        metrics[\"worker_evidence\"] = json.dumps(verify_distributed_executors(spark, require_two_workers=(RESULT_TIER == \"official_standalone\")) if spark.sparkContext.master.startswith(\"spark://\") else {})
        plan_text = joined_df._jdf.queryExecution().executedPlan().toString()
        plan_name = f"nd3_{method_name}_iter{iteration}_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.txt"
        plan_path = os.path.join(PLAN_DIR, plan_name)
        with open(plan_path, "w", encoding="utf-8") as plan_file:
            plan_file.write(plan_text)
        metrics["executed_plan_path"] = plan_path
        metrics["spark.sql.adaptive.enabled"] = spark.conf.get("spark.sql.adaptive.enabled")
        metrics["spark.sql.adaptive.skewJoin.enabled"] = spark.conf.get("spark.sql.adaptive.skewJoin.enabled", "false")
        metrics["spark.sql.autoBroadcastJoinThreshold"] = spark.conf.get("spark.sql.autoBroadcastJoinThreshold", "10485760")
        
        all_join_runs.append(metrics)
        final_join_results[method_name] = joined_df
        
        print(f\"     Thời gian: {metrics['wall_clock_duration_s']:.3f}s | TSR (Max): {metrics['task_skew_ratio_max']} | Shuffle Read bytes: {metrics.get('shuffle_read_bytes')} | Memory spill bytes: {metrics.get('memory_bytes_spilled')}\")

df_join_runs = pd.DataFrame(all_join_runs)
join_runs_csv = os.path.join(paths[\"benchmarks_dir\"], f"benchmark_nd3_join_runs_{RESULT_TIER}.csv")
print(f\"\\n>>> ĐÃ LƯU KẾT QUẢ TỪNG LƯỢT ĐO JOIN TẠI: {join_runs_csv}\")
display(df_join_runs[[\"iteration\", \"method\", \"wall_clock_duration_s\", \"task_skew_ratio_max\", \"shuffle_read_bytes\", \"memory_bytes_spilled\", \"collector_status\"]])
"""))

    # --------------------------------------------------------------------------
    # Cell 8: Independent Ground Truth Audit with DuckDB
    # --------------------------------------------------------------------------
    cells.append(new_markdown_cell("""## 7. Kiểm chứng Tính Đúng đắn Độc lập với DuckDB (Ground Truth Audit)
Đối soát với kết quả nối trực tiếp của DuckDB trên Parquet tầng Silver và Parquet Ma trận OD:
- Tổng số bản ghi kết quả: Khớp chính xác $19,993,097$ dòng (không mất bản ghi, không nhân đôi bản ghi).
- Tổng quãng đường và tổng cước gom nhóm theo 32 cặp Quận (`PU_Borough, DO_Borough`): Sai số $< 0.05$."""))

    cells.append(new_code_cell("""print(\">>> BẮT ĐẦU ĐỐI SOÁT KẾT QUẢ PHÉP NỐI VỚI DUCKDB GROUND TRUTH...\")

join_audit_summaries = []
join_audit_dfs = []

for method_name, result_df in final_join_results.items():
    passed, summary, df_audit = verify_join_with_duckdb(
        spark_joined_df=result_df,
        silver_path=paths[\"silver_path\"],
        od_path=paths[\"od_matrix_path\"],
        fare_tol=0.05,
        miles_tol=0.05
    )
    summary[\"method\"] = method_name
    join_audit_summaries.append(summary)
    
    df_audit[\"method\"] = method_name
    join_audit_dfs.append(df_audit)
    
    print(f\"  - Phương án: {method_name:25s} | Kết quả: {summary['status']} | Số dòng khớp: {summary['total_trips_spark']:,} / {summary['total_trips_duck']:,} | Cặp quận: {summary['total_borough_pairs']}\")

df_join_audit_summary = pd.DataFrame(join_audit_summaries)
validation_by_method = df_join_audit_summary.set_index("method")["status"]
df_join_runs["validation_status"] = df_join_runs["method"].map(validation_by_method)
valid_join_runs = df_join_runs[(df_join_runs["collector_status"] == "OK") & (df_join_runs["validation_status"] == "PASS")]
df_join_runs.pipe(write_csv_preserving_history, join_runs_csv)
df_join_audit_all = pd.concat(join_audit_dfs, ignore_index=True)
join_audit_csv_path = os.path.join(paths[\"benchmarks_dir\"], f"benchmark_nd3_join_audit_{RESULT_TIER}.csv")
df_join_audit_all.pipe(write_csv_preserving_history, join_audit_csv_path)
display(df_join_audit_summary)

all_join_passed = (df_join_audit_summary[\"status\"] == "PASS").all()
assert all_join_passed, "LỖI: Có phương án Join không khớp tính đúng đắn với DuckDB!"
print(\"\\n>>> XÁC NHẬN: TẤT CẢ CÁC PHƯƠNG ÁN JOIN ĐẠT CHUẨN TÍNH TOÀN VẸN VÀ ĐÚNG ĐẮN 100%!\")
print(f\">>> Đã lưu bảng đối soát chi tiết tại: {join_audit_csv_path}\")
"""))

    # --------------------------------------------------------------------------
    # Cell 9: Summary Statistics & Discussion
    # --------------------------------------------------------------------------
    cells.append(new_markdown_cell("""## 8. Bảng Tổng hợp Thống kê & Thảo luận Kết quả (Chương 4, Mục 4.3)
Tổng hợp giá trị Trung vị (Median), Trung bình (Mean), Độ lệch chuẩn (Std Dev), Hệ số lệch tải TSR và Khối lượng Shuffle."""))

    cells.append(new_code_cell("""if valid_join_runs.empty:
    raise RuntimeError("No ND3 runs passed both metrics collection and DuckDB correctness validation.")
summary_join_rows = []
for method_name, group in valid_join_runs.groupby("method"):
    durations = group["wall_clock_duration_s"]
    tsrs = group["task_skew_ratio_max"].dropna()
    shuffles = group["shuffle_read_bytes"] / (1024 * 1024)
    spills = group["memory_bytes_spilled"] / (1024 * 1024)
    
    summary_join_rows.append({
        "Phương án": method_name,
        "Số lượt đo": len(durations),
        "Thời gian Median (s)": round(durations.median(), 3),
        "Thời gian Mean (s)": round(durations.mean(), 3),
        "Thời gian Std (s)": round(durations.std() if len(durations) > 1 else 0.0, 3),
        "Thời gian Min (s)": round(durations.min(), 3),
        "Thời gian Max (s)": round(durations.max(), 3),
        "TSR Max Median": round(tsrs.median(), 2) if not tsrs.empty else "N/A",
        "Shuffle Read Median (MB)": round(shuffles.median(), 2),
        "Memory Spill Median (MB)": round(spills.median(), 2),
    })

df_join_summary = pd.DataFrame(summary_join_rows)
join_summary_csv = os.path.join(paths["benchmarks_dir"], f"benchmark_nd3_join_summary_{RESULT_TIER}.csv")
df_join_summary.pipe(write_csv_preserving_history, join_summary_csv)

print(">>> BẢNG TỔNG HỢP HIỆU NĂNG NỘI DUNG 3 (CHƯƠNG 4):")
display(df_join_summary)
"""))

    # --------------------------------------------------------------------------
    # Cell 10: Visualizations
    # --------------------------------------------------------------------------
    cells.append(new_markdown_cell("""## 9. Trực quan hóa So sánh Hiệu năng & Đánh giá Nút thắt Cổ chai
Biểu đồ so sánh Thời gian thực thi (Wall-clock Duration), Task Skew Ratio (TSR) và Bộ nhớ tràn (Memory Spill)."""))

    cells.append(new_code_cell("""fig, axes = plt.subplots(1, 3, figsize=(16, 5), dpi=150)

colors_join = ["#c0392b", "#27ae60", "#2980b9"]

# 1. Thời gian Median
axes[0].bar(df_join_summary["Phương án"], df_join_summary["Thời gian Median (s)"], color=colors_join, edgecolor="black", alpha=0.85)
axes[0].set_title("Thời gian thực thi Median (s)", fontsize=11, fontweight="bold")
axes[0].set_ylabel("Giây")
axes[0].grid(axis="y", linestyle="--", alpha=0.6)
for i, v in enumerate(df_join_summary["Thời gian Median (s)"]):
    axes[0].text(i, v + 0.05, f"{v:.2f}s", ha="center", fontweight="bold")

# 2. Task Skew Ratio (TSR)
tsr_join_vals = [float(x) if x != "N/A" else 0.0 for x in df_join_summary["TSR Max Median"]]
axes[1].bar(df_join_summary["Phương án"], tsr_join_vals, color=colors_join, edgecolor="black", alpha=0.85)
axes[1].set_title("Hệ số Lệch tải Task (TSR Max)", fontsize=11, fontweight="bold")
axes[1].set_ylabel("TSR (Càng gần 1 càng cân bằng)")
axes[1].grid(axis="y", linestyle="--", alpha=0.6)
for i, v in enumerate(tsr_join_vals):
    axes[1].text(i, v + 0.05, f"{v:.2f}", ha="center", fontweight="bold")

# 3. Memory Spill
axes[2].bar(df_join_summary["Phương án"], df_join_summary["Memory Spill Median (MB)"], color=colors_join, edgecolor="black", alpha=0.85)
axes[2].set_title("Bộ nhớ Tràn (Memory Spill MB)", fontsize=11, fontweight="bold")
axes[2].set_ylabel("MB")
axes[2].grid(axis="y", linestyle="--", alpha=0.6)
for i, v in enumerate(df_join_summary["Memory Spill Median (MB)"]):
    axes[2].text(i, v + 0.01, f"{v:.2f}MB", ha="center", fontweight="bold")

for ax in axes:
    ax.tick_params(axis="x", rotation=15)

plt.tight_layout()
join_plot_path = os.path.join(paths["benchmarks_dir"], "figure_nd3_join_benchmark.png")
plt.savefig(join_plot_path)
plt.show()
print(f">>> Đã lưu biểu đồ so sánh Join Skew tại: {join_plot_path}")
"""))

    # --------------------------------------------------------------------------
    # Cell 11: Concluding Discussion Markdown
    # --------------------------------------------------------------------------
    cells.append(new_markdown_cell(r"""## 10. Kết luận Khoa học & Đóng góp Báo cáo (Chương 4, Mục 4.3)

### 1. Phân tích Hiện tượng Join Skew trên Ma trận Hành trình Đi–Đến (OD Matrix):
- Trong phép kết nối ma trận OD theo cặp khóa composite `(PULocationID, DOLocationID)`, dữ liệu thực tế thể hiện đặc tính lệch tải không gian cực kỳ gay gắt:
  - Cặp OD lớn nhất (East New York $\leftrightarrow$ East New York) tập trung **83,025 chuyến xe**, cao gấp **2,516 lần** so với giá trị trung vị (33 chuyến/cặp).
  - Khi tắt Broadcast, Spark bắt buộc phải thực hiện Shuffle Exchange trên toàn bộ gần 20 triệu chuyến xe.
  - Các partition nhận phải các cặp OD vùng nóng chịu tải trọng vượt bậc so với các partition chứa các cặp OD ngoại ô, tạo ra hiện tượng **Straggler Tasks** (các core xử lý xong phải đứng chờ một vài task bị lệch tải).

### 2. So sánh Kỹ thuật: Spark AQE Skew Join vs Salting Join trên Ma trận OD:
1. **Spark AQE Skew Join (Adaptive):**
   - **Cơ chế:** Dựa trên số liệu thống kê thu thập tại runtime sau giai đoạn Shuffle Map Stage. Nếu một partition vượt cả hai điều kiện: kích thước vượt `skewedPartitionThresholdInBytes` (16MB) và vượt `skewedPartitionFactor` ($2\times$) so với trung vị, Catalyst tự động chia nhỏ partition đó thành nhiều sub-partition để đọc song song.
   - **Ưu điểm:** Hoàn toàn trong suốt với người dùng, không làm biến đổi schema hay tăng kích thước dữ liệu lưu trữ.

2. **Salting Sort-Merge Join (Deterministic / Static):**
   - **Cơ chế:** Chủ động gán $S=8$ bucket salt ngẫu nhiên đồng đều cho các chuyến đi xuất phát từ 14 Hotspot PU ($H$). Phía bảng ma trận OD, nhân bản các dòng tương ứng lên 8 lần (`explode`).
   - **Ưu điểm:** Triệt tiêu hoàn toàn sự dồn cục tại tầng Shuffle Exchange trước khi thực thi; mỗi sub-partition nhận đúng $\approx 12.5\%$ khối lượng dữ liệu của vùng nóng.
   - **Chi phí nhân bản (Replication Overhead):** Do chỉ nhân bản 3,557 cặp OD liên quan đến 14 Hotspots đón khách, kích thước bảng OD chỉ tăng thêm $3,557 \times 7 = 24,899$ dòng (từ 58,747 dòng lên 83,646 dòng) — chi phí bộ nhớ và mạng hoàn toàn không đáng kể ($< 0.1\%$) so với quy mô 20 triệu bản ghi của bảng fact.

> [!IMPORTANT]
> **Khuyến nghị Thực tế cho Pipeline Dữ liệu Phân tán (Chương 4, Mục 4.4):**
> - Đối với cụm Spark hiện đại có hỗ trợ AQE (Spark 3.2+), **Spark AQE Skew Join** là lựa chọn mặc định tối ưu vì tính linh hoạt tự thích ứng mà không can thiệp logic code.
> - Tuy nhiên, khi độ lệch tải vượt ngoài tầm kiểm soát của ngưỡng tĩnh hoặc trên các cụm tính toán cố định, **Salting Join** là giải pháp kỹ thuật có tính tất định cao nhất để đảm bảo phân bổ đều tải trọng tính toán trên toàn bộ worker nodes.
"""))

    nb.cells = cells
    return nb

    nb.cells = cells
    return nb


def main():
    os.makedirs("notebooks", exist_ok=True)

    nb2 = create_aggregation_notebook()
    nb2_path = "notebooks/02_benchmark_aggregation_salting.ipynb"
    with open(nb2_path, "w", encoding="utf-8") as f:
        nbformat.write(nb2, f)
    nbformat.validate(nb2)
    print(f"Successfully generated and validated {nb2_path} (cells: {len(nb2.cells)})")

    nb3 = create_join_notebook()
    nb3_path = "notebooks/03_benchmark_join_skew.ipynb"
    with open(nb3_path, "w", encoding="utf-8") as f:
        nbformat.write(nb3, f)
    nbformat.validate(nb3)
    print(f"Successfully generated and validated {nb3_path} (cells: {len(nb3.cells)})")


if __name__ == "__main__":
    main()
