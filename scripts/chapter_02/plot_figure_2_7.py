"""Vẽ Hình 2.7: phạm vi dữ liệu Chương 2 và các chế độ benchmark tháng 01."""
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.patches as patches

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "data/zones/figure_2_7_data_hierarchy.png"

fig, ax = plt.subplots(figsize=(11, 5.2), dpi=300)
ax.set_xlim(0, 10)
ax.set_ylim(0, 8)
ax.axis("off")

boxes = [
    {
        "y": 4.35,
        "height": 2.25,
        "face": "#e3f2fd",
        "edge": "#1976d2",
        "title": "CHẾ ĐỘ CHẠY THỬ (PILOT MODE)",
        "body": (
            "• Đầu vào: cùng Silver tháng 01/2026 với benchmark chính thức\n"
            "• Một lượt đo để kiểm tra khả năng chạy và tính đúng đắn chức năng\n"
            "• Không phải mẫu 7 ngày, không dùng để kết luận hiệu năng"
        ),
    },
    {
        "y": 1.25,
        "height": 2.25,
        "face": "#e8f8f5",
        "edge": "#16a085",
        "title": "BENCHMARK CHÍNH THỨC (FULL MODE)",
        "body": (
            "• Đầu vào: cùng Silver tháng 01/2026\n"
            "• Một lượt warm-up và ba lượt đo cho mỗi phương án\n"
            "• Dùng so sánh thời gian, lệch tải, Shuffle và Spill"
        ),
    },
]

for item in boxes:
    rect = patches.FancyBboxPatch(
        (0.55, item["y"]), 8.9, item["height"],
        boxstyle="round,pad=0.16", facecolor=item["face"],
        edgecolor=item["edge"], linewidth=1.6,
    )
    ax.add_patch(rect)
    ax.text(0.85, item["y"] + 1.78, item["title"], fontsize=12,
            fontweight="bold", color=item["edge"])
    ax.text(0.85, item["y"] + 1.23, item["body"], fontsize=10.5,
            color="#2c3e50", linespacing=1.45, va="top")

ax.annotate("", xy=(5, 3.75), xytext=(5, 4.25),
            arrowprops=dict(facecolor="#7f8c8d", edgecolor="none", width=2, headwidth=8))
ax.text(5, 0.58,
        "Chương 2 chỉ chuẩn bị dữ liệu tháng 01/2026. Thí nghiệm nhiều tháng được trình bày riêng ở Chương 3.",
        ha="center", fontsize=9.5, fontstyle="italic", color="#555555")

fig.tight_layout()
OUT.parent.mkdir(parents=True, exist_ok=True)
fig.savefig(OUT, dpi=300, bbox_inches="tight")
plt.close(fig)
print(f"Saved: {OUT}")
