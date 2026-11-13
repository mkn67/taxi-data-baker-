#!/usr/bin/env python3
"""Xây dựng bộ dữ liệu Gold mật độ điểm trả khách theo Taxi Zone và khung thời gian.

Mục tiêu (Chương 3.6):
  - Tổng hợp số chuyến theo (DOLocationID, time_period) bằng DuckDB, không tải
    toàn bộ dữ liệu chuyến đi vào bộ nhớ driver.
  - Tính diện tích vùng (km²) trên hệ tọa độ chiếu EPSG:32618 (UTM 18N).
  - Tính mật độ: dropoff_trip_count / polygon_area_km2.
  - Xử lý tường minh các vùng có hình học không hợp lệ hoặc diện tích bằng 0.
  - Phân loại vùng Manhattan theo ranh giới xấp xỉ phố 60th Street (tái sử dụng
    logic từ 02b_zone_boundary_check.py, không nhân bản).
  - Xuất Gold parquet/CSV (Kepler.gl-ready) và báo cáo kiểm toán.
  - Vẽ biểu đồ mật độ (figure_3_1_dropoff_density.png).

Đầu ra:
  data/gold/dropoff_density_by_zone_period.parquet   ← Gold GeoParquet
  data/gold/dropoff_density_by_zone_period.csv       ← bản CSV
  data/benchmarks/spatial_analysis_audit.csv         ← kiểm toán
  data/benchmarks/figure_3_1_dropoff_density.png     ← biểu đồ

Biến môi trường (tuỳ chọn):
  SILVER_PATH          đường dẫn đến thư mục parquet Silver
  TAXI_ZONES_SHP       đường dẫn đến shapefile taxi_zones.shp
  SPATIAL_GOLD_PATH    ghi đè đường dẫn Gold parquet
  SPATIAL_GOLD_CSV     ghi đè đường dẫn Gold CSV
  SPATIAL_AUDIT_PATH   ghi đè đường dẫn kiểm toán CSV
  SPATIAL_FIGURE_PATH  ghi đè đường dẫn hình ảnh
  BOUNDARY_AUDIT_PATH  CSV nhóm biên cũ dùng để đối chiếu các zone ứng viên
"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile
import glob
from pathlib import Path

import duckdb
import geopandas as gpd
import matplotlib
matplotlib.use("Agg")  # Chế độ không có màn hình (headless)
import matplotlib.pyplot as plt
import pandas as pd
from shapely.geometry import GeometryCollection, LineString, MultiPolygon, Point, Polygon
from shapely.ops import unary_union
from shapely.validation import make_valid

# ---------------------------------------------------------------------------
# Đường dẫn mặc định (có thể ghi đè bằng biến môi trường)
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]

SILVER = Path(os.environ.get("SILVER_PATH",
                             ROOT / "data/silver/fhvhv_2026_01_clean.parquet"))
SHAPEFILE = Path(os.environ.get("TAXI_ZONES_SHP",
                                ROOT / "data/zones/shp/taxi_zones.shp"))
GOLD_PARQUET = Path(os.environ.get("SPATIAL_GOLD_PATH",
                                   ROOT / "data/gold/dropoff_density_by_zone_period.parquet"))
GOLD_CSV = Path(os.environ.get("SPATIAL_GOLD_CSV",
                               ROOT / "data/gold/dropoff_density_by_zone_period.csv"))
AUDIT = Path(os.environ.get("SPATIAL_AUDIT_PATH",
                            ROOT / "data/benchmarks/spatial_analysis_audit.csv"))
FIGURE = Path(os.environ.get("SPATIAL_FIGURE_PATH",
                             ROOT / "data/benchmarks/figure_3_1_dropoff_density.png"))

# Hệ tọa độ chiếu để tính diện tích (đơn vị mét)
PROJECTED_CRS = "EPSG:32618"  # UTM zone 18N

# Tọa độ xấp xỉ hai đầu phố 60th Street (khớp với 02b_zone_boundary_check.py)
EAST_PT = (40.7590, -73.9585)
WEST_PT = (40.7738, -73.9918)
AREA_SIDE_TOLERANCE_PCT = 5.0  # % diện tích cho phép nằm sai phía

PERIODS = ("Daytime", "Nighttime")
BOUNDARY_AUDIT = Path(os.environ.get(
    "BOUNDARY_AUDIT_PATH", ROOT / "data/zones/manhattan_60th_boundary_audit.csv"
))


# ---------------------------------------------------------------------------
# Tiện ích ghi file an toàn
# ---------------------------------------------------------------------------

def _backup_existing(path: Path) -> None:
    """Sao lưu file hiện có với tem thời gian trước khi ghi đè."""
    if path.exists():
        stamp = pd.Timestamp.utcnow().strftime("%Y%m%dT%H%M%SZ")
        backup = path.with_name(f"{path.stem}.bak_{stamp}{path.suffix}")
        suffix_n = 1
        while backup.exists():
            backup = path.with_name(f"{path.stem}.bak_{stamp}_{suffix_n}{path.suffix}")
            suffix_n += 1
        shutil.copy2(path, backup)


def _stage_dataframe(df: pd.DataFrame, path: Path, *, is_parquet: bool) -> Path:
    """Write one complete artifact to a same-filesystem temporary path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = _temporary_sibling(path)
    try:
        if is_parquet:
            df.to_parquet(tmp, index=False)
        else:
            df.to_csv(tmp, index=False, encoding="utf-8")
    except Exception:
        tmp.unlink(missing_ok=True)
        raise
    return tmp


def _temporary_sibling(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{path.stem}.", suffix=path.suffix, dir=path.parent)
    os.close(fd)
    os.unlink(tmp)
    return Path(tmp)


def _publish_staged_files(staged_files: list[tuple[Path, Path]]) -> None:
    """Publish completed files atomically one by one; callers put the audit last."""
    for staged, destination in staged_files:
        if not staged.is_file() or staged.stat().st_size == 0:
            raise RuntimeError(f"Staged artifact missing or empty: {staged}")
    for staged, destination in staged_files:
        destination.parent.mkdir(parents=True, exist_ok=True)
        _backup_existing(destination)
        os.replace(staged, destination)


def _sql_string(value: str) -> str:
    """Quote a filesystem path as a DuckDB SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def _parquet_source(path: Path) -> str:
    """Return a safe DuckDB read_parquet expression, including nested directories."""
    if path.is_file():
        files = [path]
    elif path.is_dir():
        files = sorted(path.rglob("*.parquet"))
    else:
        files = [Path(p) for p in sorted(glob.glob(str(path), recursive=True)) if p.endswith(".parquet")]
    if not files:
        raise FileNotFoundError(f"No parquet files found for Silver input: {path}")
    file_list = ", ".join(_sql_string(str(p.resolve())) for p in files)
    return f"read_parquet([{file_list}], union_by_name=true)"


# ---------------------------------------------------------------------------
# Phân loại Taxi Zone theo ranh giới xấp xỉ phố 60th Street
# (Tái sử dụng logic từ 02b_zone_boundary_check.py, không nhân bản hằng số)
# ---------------------------------------------------------------------------

def classify_zones(zones: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Thêm cột analytical_group và classification_method vào GeoDataFrame zones.

    Logic phân loại khớp hoàn toàn với 02b_zone_boundary_check.py:
      - SOUTH_OF_APPROX_60TH   : vùng Manhattan có >= 95% diện tích ở phía Nam
      - NORTH_OF_APPROX_60TH   : vùng Manhattan có >= 95% diện tích ở phía Bắc
      - BOUNDARY_AMBIGUOUS     : vùng Manhattan nằm giao nhau không rõ phía
      - OUTSIDE_MANHATTAN      : các quận khác

    Tham số hình học EAST_PT, WEST_PT, AREA_SIDE_TOLERANCE_PCT được dùng chung
    với module-level constants để đảm bảo nhất quán với 02b.
    """
    if zones.crs is None:
        raise ValueError("Shapefile không có CRS; không thể tính diện tích/mật độ.")
    borough_col = next((c for c in zones.columns if c.lower() == "borough"), None)
    if borough_col is None:
        raise ValueError("Shapefile cần có cột 'Borough'.")
    if "LocationID" not in zones.columns:
        raise ValueError("Shapefile cần có cột 'LocationID'.")
    if zones["LocationID"].isna().any():
        raise ValueError("LocationID có giá trị null trong shapefile.")

    # Giữ lại đủ cột cần thiết và chiếu sang EPSG:32618
    zone_col = next((c for c in zones.columns if c.lower() == "zone"), None)
    keep = ["LocationID", borough_col, "geometry"] + ([zone_col] if zone_col else [])
    zones = zones[keep].copy().to_crs(PROJECTED_CRS)

    # Hợp nhất đa giác phân mảnh theo LocationID
    agg_dict = {c: "first" for c in zones.columns if c not in ("LocationID", "geometry")}
    zones = zones.dissolve(by="LocationID", as_index=False, aggfunc=agg_dict)
    if zones["LocationID"].isna().any() or not zones["LocationID"].is_unique:
        raise ValueError("LocationID phải duy nhất sau khi dissolve theo zone.")

    # Sửa hình học không hợp lệ
    zones["geometry_type_before_repair"] = zones.geometry.geom_type
    invalid_mask = ~zones.geometry.is_valid
    invalid_count = int(invalid_mask.sum())
    if invalid_count:
        def repair_polygonal(geom):
            repaired = make_valid(geom)
            if isinstance(repaired, (Polygon, MultiPolygon)):
                return repaired
            if isinstance(repaired, GeometryCollection):
                polygon_parts = [
                    part for part in repaired.geoms
                    if isinstance(part, (Polygon, MultiPolygon)) and not part.is_empty
                ]
                return unary_union(polygon_parts) if polygon_parts else GeometryCollection()
            return GeometryCollection()

        zones.loc[invalid_mask, "geometry"] = zones.loc[invalid_mask, "geometry"].map(repair_polygonal)
    if not zones.geometry.is_valid.all():
        raise ValueError("Tồn tại Taxi Zone vẫn không hợp lệ sau khi make_valid.")
    zones["geometry_type_after_repair"] = zones.geometry.geom_type
    unsupported_geometry = (
        ~zones["geometry_type_after_repair"].isin(["Polygon", "MultiPolygon"])
        & ~zones.geometry.is_empty
    )
    if unsupported_geometry.any():
        bad_types = zones.loc[unsupported_geometry, "geometry_type_after_repair"].value_counts().to_dict()
        raise ValueError(f"Taxi Zone geometry must be polygonal after repair; found {bad_types}")

    # Tính diện tích — đơn vị km² trong EPSG:32618
    zones["polygon_area_km2"] = zones.geometry.area / 1_000_000.0
    # Ghi nhận nhưng không thay thế vùng có diện tích không dương
    zero_area_mask = (zones["polygon_area_km2"] <= 0) | zones.geometry.is_empty
    zero_area_count = int(zero_area_mask.sum())
    zones["area_valid"] = ~zero_area_mask  # True = có thể tính mật độ

    # Xây dựng đường tham chiếu phố 60th và nửa mặt phẳng phía Bắc
    pts = gpd.GeoSeries(
        [Point(EAST_PT[1], EAST_PT[0]), Point(WEST_PT[1], WEST_PT[0])],
        crs="EPSG:4326",
    ).to_crs(PROJECTED_CRS)
    (x1, y1), (x2, y2) = list(LineString(pts.tolist()).coords)
    dx, dy = x2 - x1, y2 - y1

    def side(pt: Point) -> float:
        return (x2 - x1) * (pt.y - y1) - (y2 - y1) * (pt.x - x1)

    # Dùng Zone 161 (Midtown Center) làm mốc phía Nam (khớp với 02b)
    is_manhattan = zones[borough_col].astype(str).str.strip().str.casefold().eq("manhattan")
    south_ref = zones.loc[zones["LocationID"].astype(int).eq(161) & is_manhattan, "geometry"]
    if south_ref.empty:
        raise ValueError("Zone 161 (Midtown Center) không tìm thấy — cần để xác định phía Nam.")
    south_sign = 1 if side(south_ref.iloc[0].centroid) > 0 else -1

    nx, ny = y2 - y1, -(x2 - x1)
    norm = (nx * nx + ny * ny) ** 0.5
    nx, ny = nx / norm, ny / norm
    if side(Point(x1 + nx, y1 + ny)) * south_sign > 0:
        nx, ny = -nx, -ny
    big = 100_000.0
    north_half = Polygon([
        (x1 - 5 * dx, y1 - 5 * dy),
        (x2 + 5 * dx, y2 + 5 * dy),
        (x2 + 5 * dx + nx * big, y2 + 5 * dy + ny * big),
        (x1 - 5 * dx + nx * big, y1 - 5 * dy + ny * big),
    ])

    groups, methods = [], []
    for zone_pos, (is_man, geom) in enumerate(zip(is_manhattan, zones.geometry)):
        if not is_man:
            groups.append("OUTSIDE_MANHATTAN")
            methods.append("borough attribute")
            continue
        if not bool(zones["area_valid"].iloc[zone_pos]):
            groups.append("BOUNDARY_AMBIGUOUS")
            methods.append("non-positive area — cannot classify by side")
            continue
        north_pct = 100.0 * geom.intersection(north_half).area / geom.area
        centroid_is_south = side(geom.centroid) * south_sign > 0
        south_dominant = north_pct <= AREA_SIDE_TOLERANCE_PCT
        north_dominant = north_pct >= 100.0 - AREA_SIDE_TOLERANCE_PCT
        if south_dominant and centroid_is_south:
            groups.append("SOUTH_OF_APPROX_60TH")
        elif north_dominant and not centroid_is_south:
            groups.append("NORTH_OF_APPROX_60TH")
        else:
            groups.append("BOUNDARY_AMBIGUOUS")
        methods.append(
            f"Manhattan polygon area split at approx 60th St; {AREA_SIDE_TOLERANCE_PCT}% tolerance"
        )

    zones["analytical_group"] = groups
    zones["classification_method"] = methods
    zones.attrs["invalid_geometry_count"] = invalid_count
    zones.attrs["zero_area_count"] = zero_area_count
    zones.attrs["geometry_type_after_repair_counts"] = zones["geometry_type_after_repair"].value_counts().to_dict()
    return zones


# ---------------------------------------------------------------------------
# Tổng hợp dữ liệu chuyến đi bằng DuckDB (không tải toàn bộ dữ liệu)
# ---------------------------------------------------------------------------

def validate_boundary_assignments(zones: gpd.GeoDataFrame, audit_path: Path) -> dict:
    """Compare the shared area/centroid rule with the existing 02b candidate audit."""
    if not audit_path.is_file():
        raise FileNotFoundError(f"Boundary verification CSV not found: {audit_path}")
    reference = pd.read_csv(audit_path)
    required = {"LocationID", "Assigned_Group"}
    if not required.issubset(reference.columns):
        raise ValueError(f"Boundary audit lacks required columns: {sorted(required - set(reference.columns))}")
    if reference["LocationID"].duplicated().any():
        raise ValueError("Boundary audit contains duplicate LocationID values.")

    reference_group_map = {
        "CRZ_SOUTH_ADJACENT": "SOUTH_OF_APPROX_60TH",
        "NON_CRZ_NORTH_ADJACENT": "NORTH_OF_APPROX_60TH",
    }
    reference["expected_group"] = reference["Assigned_Group"].map(
        lambda value: reference_group_map.get(value, "BOUNDARY_AMBIGUOUS")
    )
    observed = zones[["LocationID", "analytical_group"]]
    compared = reference.merge(observed, on="LocationID", how="left", validate="one_to_one")
    missing = compared["analytical_group"].isna()
    mismatch = missing | (compared["expected_group"] != compared["analytical_group"])
    mismatches = compared.loc[mismatch, ["LocationID", "Assigned_Group", "analytical_group"]]
    if not mismatches.empty:
        raise ValueError(
            "Zone-side classifications disagree with the existing 02b audit for "
            f"{len(mismatches)} candidate zone(s): {mismatches.to_dict('records')}"
        )
    return {"boundary_audit_path": str(audit_path), "boundary_audit_zones_compared": len(compared),
            "boundary_audit_mismatches": 0}


def aggregate_with_duckdb(silver_path: Path) -> tuple[pd.DataFrame, dict]:
    """Tổng hợp số chuyến theo (DOLocationID, time_period) qua DuckDB.

    Trả về:
      counts_df  : DataFrame (DOLocationID, time_period, trip_count)
      audit_info : dict chứa các số liệu kiểm toán từ DuckDB
    """
    source = _parquet_source(silver_path)
    con = duckdb.connect()
    try:
        columns = {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()}
        required = {"DOLocationID", "dropoff_datetime", "time_period"}
        if not required.issubset(columns):
            raise ValueError(f"Silver data lacks required columns: {sorted(required - columns)}")

        stats = con.execute(f"""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(*) FILTER (WHERE DOLocationID IS NULL) AS null_do_zone_rows,
                COUNT(*) FILTER (WHERE DOLocationID IS NOT NULL AND dropoff_datetime IS NULL)
                    AS excluded_null_datetime_rows,
                COUNT(*) FILTER (WHERE DOLocationID IS NOT NULL AND dropoff_datetime IS NOT NULL
                                  AND time_period IS NULL) AS excluded_null_period_rows,
                COUNT(*) FILTER (WHERE DOLocationID IS NOT NULL AND dropoff_datetime IS NOT NULL
                                  AND time_period IS NOT NULL
                                  AND time_period NOT IN ('Daytime', 'Nighttime')) AS excluded_invalid_period_rows,
                COUNT(*) FILTER (WHERE dropoff_datetime IS NULL) AS all_null_datetime_rows,
                COUNT(*) FILTER (WHERE time_period IS NULL) AS all_null_period_rows,
                COUNT(*) FILTER (
                    WHERE dropoff_datetime IS NOT NULL
                      AND time_period IN ('Daytime', 'Nighttime')
                      AND time_period <> CASE
                          WHEN date_part('hour', dropoff_datetime) BETWEEN 5 AND 20 THEN 'Daytime'
                          ELSE 'Nighttime' END
                ) AS period_timestamp_mismatch_rows
            FROM {source}
        """).fetchone()
        (total_rows, null_zone_rows, excluded_null_datetime_rows,
         excluded_null_period_rows, excluded_invalid_period_rows,
         all_null_datetime_rows, all_null_period_rows, period_mismatches) = map(int, stats)
        if total_rows == 0:
            raise ValueError(f"Silver parquet is empty: {silver_path}")
        if period_mismatches:
            raise ValueError(
                f"Silver time_period disagrees with dropoff_datetime hour for {period_mismatches:,} rows. "
                "Repair/rebuild Silver rather than publishing inconsistent Gold periods."
            )

        counts_df = con.execute(f"""
            SELECT DOLocationID, time_period, COUNT(*) AS trip_count
            FROM {source}
            WHERE DOLocationID IS NOT NULL
              AND dropoff_datetime IS NOT NULL
              AND time_period IN ('Daytime', 'Nighttime')
            GROUP BY DOLocationID, time_period
            ORDER BY DOLocationID, time_period
        """).df()
    finally:
        con.close()

    eligible_rows = int(counts_df["trip_count"].sum())
    excluded_rows = null_zone_rows + excluded_null_datetime_rows + excluded_null_period_rows + excluded_invalid_period_rows
    if excluded_rows + eligible_rows != total_rows:
        raise AssertionError(
            f"Trip-row reconciliation failed before zone matching: input={total_rows}, "
            f"eligible={eligible_rows}, disjoint_exclusions={excluded_rows}."
        )
    audit_info = {
        "silver_pattern": str(silver_path),
        "total_input_rows": total_rows,
        "null_do_zone_rows": null_zone_rows,
        "excluded_null_datetime_rows": excluded_null_datetime_rows,
        "excluded_null_period_rows": excluded_null_period_rows,
        "excluded_invalid_period_rows": excluded_invalid_period_rows,
        "all_null_datetime_rows": all_null_datetime_rows,
        "all_null_period_rows": all_null_period_rows,
        "period_timestamp_mismatch_rows": period_mismatches,
        "pre_zone_eligible_rows": eligible_rows,
        "pre_zone_excluded_rows": excluded_rows,
    }
    return counts_df, audit_info


# ---------------------------------------------------------------------------
# Vẽ biểu đồ mật độ điểm trả khách (figure_3_1)
# ---------------------------------------------------------------------------

def plot_density_figure(gold_gdf: gpd.GeoDataFrame, fig_path: Path) -> None:
    """Vẽ choropleth mật độ điểm trả khách theo thời gian và lưu PNG.

    Chỉ vẽ các vùng Manhattan (không bao gồm OUTSIDE_MANHATTAN) để bản đồ
    rõ ràng. Hai subplot cho Daytime và Nighttime.
    """
    fig_path.parent.mkdir(parents=True, exist_ok=True)

    manhattan = gold_gdf[gold_gdf["analytical_group"] != "OUTSIDE_MANHATTAN"].copy()
    # Chiếu lại sang EPSG:4326 để Kepler.gl / Matplotlib hiển thị trực quan
    manhattan = manhattan.to_crs("EPSG:4326")

    fig, axes = plt.subplots(1, 2, figsize=(14, 8))
    fig.suptitle(
        "Hình 3.1 — Mật độ điểm trả khách (chuyến/km²) theo vùng Taxi Zone Manhattan\n"
        "Tháng 1/2026 | EPSG:4326 | Cùng thang màu: P95 gộp Daytime/Nighttime",
        fontsize=11, y=1.01,
    )

    finite_density = manhattan["dropoff_density_trips_per_km2"].dropna()
    shared_vmax = float(finite_density.quantile(0.95)) if not finite_density.empty else 1.0
    if shared_vmax <= 0:
        shared_vmax = 1.0

    for ax, period in zip(axes, PERIODS):
        sub = manhattan[manhattan["time_period"] == period].copy()
        if sub.empty:
            ax.set_title(f"{period} — Không có dữ liệu")
            continue
        sub.plot(
            column="dropoff_density_trips_per_km2",
            ax=ax,
            legend=True,
            cmap="YlOrRd",
            vmin=0,
            vmax=shared_vmax,
            legend_kwds={
                "label": "Chuyến/km²",
                "shrink": 0.6,
            },
            edgecolor="grey",
            linewidth=0.3,
            missing_kwds={"color": "lightgrey", "label": "Không có chuyến"},
        )
        ax.set_title(f"{period}\n(05:00–20:59)" if period == "Daytime"
                     else f"{period}\n(21:00–04:59)")
        ax.set_xlabel("Kinh độ")
        ax.set_ylabel("Vĩ độ")
        ax.tick_params(labelsize=7)

    plt.tight_layout()
    fig.savefig(fig_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f">>> Đã lưu biểu đồ: {fig_path}")


# ---------------------------------------------------------------------------
# Hàm chính
# ---------------------------------------------------------------------------

def main() -> None:
    missing = []
    for label, path in (("Silver", SILVER), ("Shapefile", SHAPEFILE), ("Boundary audit", BOUNDARY_AUDIT)):
        if not path.exists():
            missing.append(f"{label}: {path}")
    if missing:
        sys.exit("Thiếu đầu vào:\n  " + "\n  ".join(missing))

    print(">>> Bước 1: Đọc shapefile và phân loại Taxi Zone...")
    raw_zones = gpd.read_file(SHAPEFILE)
    source_crs = str(raw_zones.crs)
    zones = classify_zones(raw_zones)
    invalid_geom_count = int(zones.attrs.get("invalid_geometry_count", 0))
    zero_area_count = int(zones.attrs.get("zero_area_count", 0))
    geometry_type_counts = zones.attrs.get("geometry_type_after_repair_counts", {})
    boundary_validation = validate_boundary_assignments(zones, BOUNDARY_AUDIT)
    print(f"    Tổng zone: {len(zones)} | hình học sửa: {invalid_geom_count} | diện tích 0: {zero_area_count}")
    print(f"    Phân bổ nhóm:\n{zones['analytical_group'].value_counts().to_string()}")

    print("\n>>> Bước 2: Tổng hợp Silver bằng DuckDB...")
    counts_df, audit_info = aggregate_with_duckdb(SILVER)
    if zones["LocationID"].isna().any() or not zones["LocationID"].is_unique:
        raise AssertionError("LocationID must be non-null and unique after dissolve.")
    if counts_df.duplicated(["DOLocationID", "time_period"]).any():
        raise AssertionError("DuckDB aggregation returned duplicate zone-period keys.")
    unexpected_periods = set(counts_df["time_period"].dropna().unique()) - set(PERIODS)
    if unexpected_periods:
        raise ValueError(f"Unexpected time_period values: {sorted(unexpected_periods)}")

    known_ids = set(zones["LocationID"].astype(int).tolist())
    counts_df["LocationID"] = counts_df["DOLocationID"].astype(int)
    unmatched_mask = ~counts_df["LocationID"].isin(known_ids)
    unmatched_ids = sorted(counts_df.loc[unmatched_mask, "LocationID"].unique().astype(int).tolist())
    unmatched_zone_rows = int(counts_df.loc[unmatched_mask, "trip_count"].sum())
    counts_matched = counts_df.loc[~unmatched_mask, ["LocationID", "time_period", "trip_count"]].copy()

    print(f"    Tổng input: {audit_info['total_input_rows']:,}")
    print(f"    Đủ trường và kỳ hợp lệ trước đối chiếu zone: {audit_info['pre_zone_eligible_rows']:,}")
    print(f"    Zone ID không khớp: {len(unmatched_ids)} zone / {unmatched_zone_rows:,} chuyến")

    print("\n>>> Bước 3: Ghép geometry, đối soát và tính mật độ...")
    full_index = pd.MultiIndex.from_product(
        [zones["LocationID"].astype(int).tolist(), PERIODS],
        names=["LocationID", "time_period"],
    ).to_frame(index=False)
    expected_gold_rows = len(zones) * len(PERIODS)
    if len(full_index) != expected_gold_rows or full_index.duplicated(["LocationID", "time_period"]).any():
        raise AssertionError("Zone-period scaffold size/key uniqueness validation failed.")

    merged = full_index.merge(
        counts_matched, on=["LocationID", "time_period"], how="left", validate="one_to_one"
    )
    if len(merged) != expected_gold_rows or merged.duplicated(["LocationID", "time_period"]).any():
        raise AssertionError("Zone-period count join changed expected size or key uniqueness.")
    merged["trip_count"] = merged["trip_count"].fillna(0).astype("int64")

    gold = merged.merge(zones, on="LocationID", how="left", validate="many_to_one")
    missing_geometry_rows = int(gold.geometry.isna().sum())
    if missing_geometry_rows:
        raise AssertionError(f"{missing_geometry_rows} Gold rows lack matched geometry.")
    density = pd.Series(float("nan"), index=gold.index, dtype="float64")
    valid_area = gold["area_valid"].fillna(False)
    density.loc[valid_area] = gold.loc[valid_area, "trip_count"] / gold.loc[valid_area, "polygon_area_km2"]
    gold["dropoff_density_trips_per_km2"] = density
    gold["analysis_month"] = "2026-01"
    gold["time_definition"] = (
        "Stored Silver time_period; validated against dropoff_datetime hour: "
        "Daytime 05:00-20:59; Nighttime 21:00-04:59"
    )
    gold["boundary_definition"] = (
        f"Approximate 60th Street reference EAST={EAST_PT}, WEST={WEST_PT}; "
        f"Manhattan polygon side area with {AREA_SIDE_TOLERANCE_PCT}% tolerance; not a legal boundary"
    )
    gold_gdf = gpd.GeoDataFrame(gold, geometry="geometry", crs=PROJECTED_CRS)

    if len(gold_gdf) != expected_gold_rows or gold_gdf.duplicated(["LocationID", "time_period"]).any():
        raise AssertionError("Gold row count or zone-period key uniqueness validation failed.")
    total_gold_trips = int(gold_gdf["trip_count"].sum())
    expected_matched_trips = audit_info["pre_zone_eligible_rows"] - unmatched_zone_rows
    if total_gold_trips != expected_matched_trips:
        raise AssertionError(
            f"Post-match trip reconciliation failed: Gold={total_gold_trips}, "
            f"eligible={audit_info['pre_zone_eligible_rows']}, unmatched={unmatched_zone_rows}."
        )
    excluded_total = audit_info["pre_zone_excluded_rows"] + unmatched_zone_rows
    if total_gold_trips + excluded_total != audit_info["total_input_rows"]:
        raise AssertionError(
            f"Full input reconciliation failed: input={audit_info['total_input_rows']}, "
            f"Gold={total_gold_trips}, excluded={excluded_total}."
        )

    included = gold_gdf[
        gold_gdf["analytical_group"].isin(["SOUTH_OF_APPROX_60TH", "NORTH_OF_APPROX_60TH"])
    ]
    included_trips = int(included["trip_count"].sum())

    # CSV geometry is transformed to lon/lat; Gold GeoParquet remains in UTM metres.
    gold_csv = gold_gdf.to_crs("EPSG:4326").copy()
    minx, miny, maxx, maxy = gold_csv.total_bounds
    if not (-180 <= minx <= maxx <= 180 and -90 <= miny <= maxy <= 90):
        raise AssertionError(f"CSV display geometry is outside EPSG:4326 bounds: {gold_csv.total_bounds}")
    gold_csv["geometry_wkt"] = gold_csv.geometry.to_wkt()
    gold_csv["geometry_crs"] = "EPSG:4326"
    gold_csv_out = gold_csv.drop(columns=["geometry"])

    audit_rows = [
        {"check": "input_silver_pattern", "value": audit_info["silver_pattern"], "status": "recorded"},
        {"check": "input_total_rows", "value": audit_info["total_input_rows"], "status": "measured by DuckDB"},
        {"check": "excluded_null_DOLocationID_rows", "value": audit_info["null_do_zone_rows"], "status": "disjoint quality-exclusion bucket"},
        {"check": "excluded_null_dropoff_datetime_rows", "value": audit_info["excluded_null_datetime_rows"], "status": "DOLocationID non-null; disjoint bucket"},
        {"check": "excluded_null_time_period_rows", "value": audit_info["excluded_null_period_rows"], "status": "zone/timestamp non-null; disjoint bucket"},
        {"check": "excluded_invalid_time_period_rows", "value": audit_info["excluded_invalid_period_rows"], "status": "zone/timestamp non-null; disjoint bucket"},
        {"check": "all_null_dropoff_datetime_rows", "value": audit_info["all_null_datetime_rows"], "status": "overlapping diagnostic; not summed in exclusions"},
        {"check": "all_null_time_period_rows", "value": audit_info["all_null_period_rows"], "status": "overlapping diagnostic; not summed in exclusions"},
        {"check": "period_timestamp_mismatch_rows", "value": audit_info["period_timestamp_mismatch_rows"], "status": "validated; must be zero"},
        {"check": "pre_zone_eligible_trip_rows", "value": audit_info["pre_zone_eligible_rows"], "status": "measured by DuckDB"},
        {"check": "unmatched_zone_id_count", "value": len(unmatched_ids), "status": str(unmatched_ids)},
        {"check": "unmatched_zone_trip_rows", "value": unmatched_zone_rows, "status": "excluded from Gold; absent from shapefile"},
        {"check": "gold_total_trip_count", "value": total_gold_trips, "status": "sum over all Gold zones and periods"},
        {"check": "total_excluded_trip_rows", "value": excluded_total, "status": "disjoint quality exclusions plus unmatched zones"},
        {"check": "input_gold_exclusion_reconciliation", "value": f"{total_gold_trips} + {excluded_total} = {audit_info['total_input_rows']}", "status": "validated exact equality"},
        {"check": "shapefile_source_crs", "value": source_crs, "status": "validated; projected to EPSG:32618 for area"},
        {"check": "area_computation_crs", "value": PROJECTED_CRS, "status": "UTM zone 18N metres; divided by 1e6 to km2"},
        {"check": "gold_output_crs", "value": str(gold_gdf.crs), "status": "GeoParquet CRS"},
        {"check": "csv_geometry_crs", "value": "EPSG:4326", "status": "WKT lon/lat for display; Kepler import still needs geometry-column selection"},
        {"check": "taxi_zone_polygon_count", "value": len(zones), "status": "measured after dissolve"},
        {"check": "expected_gold_zone_period_rows", "value": expected_gold_rows, "status": f"validated: {len(zones)} zones × {len(PERIODS)} periods"},
        {"check": "missing_geometry_rows", "value": missing_geometry_rows, "status": "must be zero"},
        {"check": "invalid_geometries_repaired", "value": invalid_geom_count, "status": "repaired with shapely make_valid"},
        {"check": "geometry_types_after_repair", "value": str(geometry_type_counts), "status": "polygonal components retained"},
        {"check": "zero_or_empty_area_zone_count", "value": zero_area_count, "status": "density is NaN; no substitute value"},
        {"check": "manhattan_comparison_trip_rows", "value": included_trips, "status": "south/north only; ambiguous and outside excluded"},
        {"check": "time_period_definition", "value": "Daytime 05:00-20:59; Nighttime 21:00-04:59", "status": "checked against dropoff_datetime hour"},
        {"check": "boundary_definition", "value": f"EAST={EAST_PT}; WEST={WEST_PT}; tolerance={AREA_SIDE_TOLERANCE_PCT}%", "status": "approximate analytical comparator, not legal boundary"},
        {"check": "boundary_audit_validation", "value": str(boundary_validation), "status": "all candidate groups match 02b audit"},
        {"check": "shared_figure_color_scale_vmax", "value": "pooled Manhattan 95th percentile", "status": "same scale for Daytime and Nighttime"},
        {"check": "output_gold_geoparquet", "value": str(GOLD_PARQUET), "status": "staged, checked, then published"},
        {"check": "output_gold_csv", "value": str(GOLD_CSV), "status": "staged; EPSG:4326 geometry_wkt column"},
        {"check": "output_figure", "value": str(FIGURE), "status": "staged and checked before audit publication"},
        {"check": "output_audit_csv", "value": str(AUDIT), "status": "published last as run-completion marker"},
    ]

    # Stage all output files before replacing any published artifact. Audit is the commit marker and is published last.
    staged: list[tuple[Path, Path]] = []
    try:
        staged.append((_stage_dataframe(gold_gdf, GOLD_PARQUET, is_parquet=True), GOLD_PARQUET))
        staged.append((_stage_dataframe(gold_csv_out, GOLD_CSV, is_parquet=False), GOLD_CSV))
        staged_figure = _temporary_sibling(FIGURE)
        plot_density_figure(gold_gdf, staged_figure)
        staged.append((staged_figure, FIGURE))
        staged_audit = _stage_dataframe(pd.DataFrame(audit_rows), AUDIT, is_parquet=False)
        staged.append((staged_audit, AUDIT))
        for temp_path, _ in staged:
            if not temp_path.is_file() or temp_path.stat().st_size == 0:
                raise RuntimeError(f"Generated artifact is missing or empty: {temp_path}")
        _publish_staged_files(staged)
    finally:
        for temp_path, _ in staged:
            temp_path.unlink(missing_ok=True)

    print("\n>>> Gold artifacts published:")
    for path in (GOLD_PARQUET, GOLD_CSV, FIGURE, AUDIT):
        if not path.is_file() or path.stat().st_size == 0:
            raise RuntimeError(f"Published artifact missing or empty: {path}")
        print(f"    {path}")
    print(f"    Input trips: {audit_info['total_input_rows']:,}; Gold trips: {total_gold_trips:,}; excluded: {excluded_total:,}")
    print(f"    Manhattan south/north comparison trips: {included_trips:,}")


if __name__ == "__main__":
    main()
