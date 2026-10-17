"""Ra soat hinh hoc toan bo cac Taxi Zone thuoc Manhattan quanh duong tham chieu
xap xi 60th Street trong ban kinh tim kiem 1 km (Muc 2.4.2).

Phan nhom: centroid VA ty le dien tich phai cung ung ho mot phia; neu mot trong hai khong
nhat quan, zone bi loai khoi hai nhom doi chung. Co Crosses_Line chi de nguoi doc kiem tra.
"""
import csv
import os
import sys

import geopandas as gpd
from shapely.geometry import LineString, Point, Polygon

SHAPEFILE = "data/zones/shp/taxi_zones.shp"
OUTPUT_AUDIT_CSV = "data/zones/manhattan_60th_boundary_audit.csv"

# Toa do (lat, lon) 2 dau duong tham chieu XAP XI 60th Street tai Manhattan (EPSG:4326).
# Day la toa do tham chieu xap xi, can doi chieu tren ban do truoc khi chot ket qua.
# Doi chieu: giao diem E 60th St & FDR Drive (Dong) va W 60th St & West Side Hwy (Tay)
EAST_PT = (40.7590, -73.9585)
WEST_PT = (40.7738, -73.9918)

# Ban kinh tim UNG VIEN quanh duong tham chieu (met, EPSG:32618). Day la tham so do nghien cuu
# chon, KHONG phai ranh gioi chinh sach; zone trong ban kinh nay khong mac nhien "tiep giap truc tiep".
SEARCH_RADIUS_M = 1000.0

# Ty le dien tich (%) toi da duoc phep nam SAI phia de zone van duoc xep vao nhom Nam/Bac.
SIDE_AREA_TOLERANCE_PCT = 5.0


def main():
    if not os.path.exists(SHAPEFILE):
        sys.exit(f"Khong tim thay shapefile tai: {SHAPEFILE}. Hay tai va giai nen taxi_zones.zip truoc.")

    zones = gpd.read_file(SHAPEFILE)
    zones = zones.to_crs("EPSG:32618")

    # Tim ten cot borough va zone (khong phan biet hoa/thuong)
    col_map = {c.lower(): c for c in zones.columns}
    borough_col = col_map.get("borough")
    name_col = col_map.get("zone")

    # Kiem tra cot bat buoc truoc khi xu ly
    if borough_col is None:
        sys.exit("Shapefile khong co cot Borough.")
    if "LocationID" not in zones.columns:
        sys.exit("Shapefile khong co cot LocationID.")

    # Hop nhat cac da giac con theo LocationID
    agg_dict = {c: "first" for c in zones.columns if c not in ["LocationID", "geometry"]}
    zones = zones.dissolve(by="LocationID", as_index=False, aggfunc=agg_dict)

    # 1. Loc toan bo cac polygon thuoc quan Manhattan
    manhattan = zones[zones[borough_col].astype(str).str.strip().str.lower() == "manhattan"].copy()

    # 2. Xay dung duong tham chieu xap xi 60th Street trong he toa do met EPSG:32618
    pts = gpd.GeoSeries(
        [Point(EAST_PT[1], EAST_PT[0]), Point(WEST_PT[1], WEST_PT[0])], crs="EPSG:4326"
    ).to_crs("EPSG:32618")
    line = LineString([pts.iloc[0], pts.iloc[1]])
    (x1, y1), (x2, y2) = list(line.coords)
    dx, dy = x2 - x1, y2 - y1
    long_line = LineString([(x1 - 5 * dx, y1 - 5 * dy), (x2 + 5 * dx, y2 + 5 * dy)])

    def side(pt):
        return (x2 - x1) * (pt.y - y1) - (y2 - y1) * (pt.x - x1)

    # Lay Zone 161 (Midtown Center) lam moc xac dinh dau phia Nam
    ref_geom = manhattan.loc[manhattan["LocationID"] == 161, "geometry"]
    if ref_geom.empty:
        sys.exit("Khong tim thay Zone 161 (Midtown Center) trong nhom Manhattan de lam moc phia Nam.")
    south_sign = 1 if side(ref_geom.iloc[0].centroid) > 0 else -1

    # Nua mat phang phia Bac de tinh ty le dien tich
    half_big = 100000.0
    nx, ny = (y2 - y1), -(x2 - x1)
    norm = (nx ** 2 + ny ** 2) ** 0.5
    nx, ny = nx / norm, ny / norm
    if side(Point(x1 + nx, y1 + ny)) * south_sign > 0:
        nx, ny = -nx, -ny

    north_half = Polygon([
        (x1 - 5 * dx, y1 - 5 * dy),
        (x2 + 5 * dx, y2 + 5 * dy),
        (x2 + 5 * dx + nx * half_big, y2 + 5 * dy + ny * half_big),
        (x1 - 5 * dx + nx * half_big, y1 - 5 * dy + ny * half_big),
    ])

    # 3. Quet tat ca cac zone Manhattan trong ban kinh SEARCH_RADIUS_M
    records = []
    for _, row in manhattan.iterrows():
        zid = int(row["LocationID"])
        geom = row["geometry"]
        zname = str(row[name_col]) if name_col else ""
        dist_m = geom.distance(long_line)

        if dist_m <= SEARCH_RADIUS_M:
            c_side = "Nam" if side(geom.centroid) * south_sign > 0 else "Bac"
            crosses = bool(geom.intersects(long_line))
            north_pct = 100.0 * geom.intersection(north_half).area / geom.area
            area_km2 = geom.area / 1e6

            # Quy tac phan loai: centroid VA ty le dien tich phai cung ung ho mot phia
            south_dominant = north_pct <= SIDE_AREA_TOLERANCE_PCT
            north_dominant = north_pct >= (100.0 - SIDE_AREA_TOLERANCE_PCT)

            if south_dominant and c_side == "Nam":
                assigned_group = "CRZ_SOUTH_ADJACENT"
            elif north_dominant and c_side == "Bac":
                assigned_group = "NON_CRZ_NORTH_ADJACENT"
            else:
                assigned_group = "LOAI_KHOI_DOI_CHUNG (Mo ho)"

            records.append({
                "LocationID": zid,
                "Zone": zname,
                "Area_km2": round(area_km2, 4),
                "Dist_to_60th_m": round(dist_m, 1),
                "Centroid_Side": c_side,
                "Crosses_Line": crosses,
                "North_Area_Pct": round(north_pct, 1),
                "Assigned_Group": assigned_group,
            })

    if not records:
        sys.exit(
            "Khong tim thay zone trong ban kinh tim kiem. "
            "Hay kiem tra duong tham chieu va shapefile."
        )

    # Sap xep theo phia Centroid va khoang cach den duong 60th St
    records.sort(key=lambda r: (r["Centroid_Side"], r["Dist_to_60th_m"], r["LocationID"]))

    print(f"\n>>> KET QUA RA SOAT TOAN BO MANHATTAN TRONG BAN KINH {SEARCH_RADIUS_M:.0f}m QUANH 60TH ST:")
    print(f"{'Zone':>4} | {'Ten Zone':28s} | {'KC (m)':>7s} | {'Centroid':>8s} | "
          f"{'Cat duong':>9s} | {'% DT Bac':>8s} | {'Nhom phan loai sau kiem tra'}")
    print("-" * 110)
    for r in records:
        print(f"{r['LocationID']:>4} | {r['Zone'][:28]:28s} | {r['Dist_to_60th_m']:>7.1f} | "
              f"{r['Centroid_Side']:>8s} | {str(r['Crosses_Line']):>9s} | "
              f"{r['North_Area_Pct']:>7.1f}% | {r['Assigned_Group']}")
    print(f"\nTham so: SEARCH_RADIUS_M={SEARCH_RADIUS_M:.0f}, SIDE_AREA_TOLERANCE_PCT={SIDE_AREA_TOLERANCE_PCT} "
          "-> ghi vao Muc 2.4.2.")

    # Luu bang ket qua ra CSV de dua vao bao cao
    out_dir = os.path.dirname(OUTPUT_AUDIT_CSV)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(OUTPUT_AUDIT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(records[0].keys()))
        writer.writeheader()
        writer.writerows(records)
    print(f">>> Da luu bang kiem tra hinh hoc tai: {OUTPUT_AUDIT_CSV}")


if __name__ == "__main__":
    main()