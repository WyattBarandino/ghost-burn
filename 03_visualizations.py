"""
=============================================================================
Project Ghost Burn v2 — Static Visualizations for Executive Report
=============================================================================
5 publication-quality Matplotlib charts for Ghost_Burn_Executive_Report.docx.
Design system: Ghost Burn palette, DM Sans typography, white/print-safe bg.

Charts:
  Fig 1 — 90-Day Burn Trend (bar + 7-day rolling avg)
  Fig 2 — SKU Cost Breakdown (horizontal bar, ranked)
  Fig 3 — Anomaly Cost Matrix (grouped bar, 90d vs annual)
  Fig 4 — GPU Utilization Scatter (bubble, cost vs util)
  Fig 5 — Remediation Waterfall (before → savings → net)

Author:  Wyatt Barandino
Version: 2.1.0 — March 2026 (professional revision)
=============================================================================
"""

import os
import json
import warnings
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.ticker as mticker
from matplotlib.lines import Line2D

warnings.filterwarnings("ignore")

# ── DATA ─────────────────────────────────────────────────────────────────────
df = pd.read_csv("ghost_burn_billing.csv", parse_dates=["usage_start_time"])
df["meta"] = df["usage_metadata"].apply(
    lambda x: json.loads(x) if pd.notna(x) else {}
)
df["date"] = df["usage_start_time"].dt.date
TOTAL_SPEND = df["estimated_cost_usd"].sum()
AUDIT_DAYS  = 90

print(f"Loaded {len(df):,} records | ${TOTAL_SPEND:,.2f} total spend\n")

# ── DESIGN TOKENS ────────────────────────────────────────────────────────────
C = {
    "burn":   "#FF3621",
    "burn_l": "#FFCBBF",
    "navy":   "#1B3A4B",
    "green":  "#15803D",
    "green_l":"#D1FAE5",
    "text":   "#111111",
    "sub":    "#555555",
    "dim":    "#999999",
    "grid":   "#EFEFEF",
    "border": "#E0E0E0",
    "white":  "#FFFFFF",
    "inter":  "#E05A3A",   # Interactive bars — distinct from AI/ML red
}

plt.rcParams.update({
    "font.family":        "DejaVu Sans",
    "font.size":          9,
    "axes.spines.top":    False,
    "axes.spines.right":  False,
    "axes.grid":          True,
    "axes.grid.axis":     "y",
    "grid.color":         C["grid"],
    "grid.linewidth":     0.6,
    "axes.axisbelow":     True,
    "figure.facecolor":   C["white"],
    "axes.facecolor":     C["white"],
    "text.parse_math":    False,
})

def clean_ax(ax):
    ax.spines["left"].set_color(C["border"])
    ax.spines["bottom"].set_color(C["border"])
    ax.tick_params(colors=C["sub"], labelsize=8.5, length=3)
    ax.yaxis.label.set_color(C["sub"])
    ax.xaxis.label.set_color(C["sub"])

def dollar_k(x, _):
    if x == 0:      return "$0"
    if x >= 1000:   return f"${x/1000:.0f}K"
    return f"${x:.0f}"

def pct_fmt(x, _):
    return f"{x:.0f}%"

def save_fig(fig, fname, dpi=200):
    path = fname
    fig.savefig(path, dpi=dpi, bbox_inches="tight",
                facecolor=C["white"], edgecolor="none")
    plt.close(fig)
    print(f"  ✅  {fname}  ({os.path.getsize(path)//1024} KB)")


# =============================================================================
# FIG 1 — 90-DAY BURN TREND
# =============================================================================
def fig1_burn_trend():
    print("Generating Fig 1 — Burn Trend...")

    daily = (
        df.groupby("date")["estimated_cost_usd"].sum()
        .reset_index().sort_values("date")
    )
    daily["dt"]    = pd.to_datetime(daily["date"])
    daily["roll7"] = daily["estimated_cost_usd"].rolling(7, min_periods=4).mean()

    SPIKE_S  = pd.Timestamp("2026-02-10").date()
    SPIKE_E  = pd.Timestamp("2026-02-13").date()
    is_spike = (daily["date"] >= SPIKE_S) & (daily["date"] <= SPIKE_E)

    fig, ax = plt.subplots(figsize=(11, 4.2))
    # Explicit margins so title block sits above axes without clipping
    fig.subplots_adjust(top=0.78, bottom=0.14, left=0.09, right=0.97)
    clean_ax(ax)
    ax.grid(axis="x", visible=False)

    bar_clrs = [C["burn"] if s else C["burn_l"] for s in is_spike]
    ax.bar(daily["dt"], daily["estimated_cost_usd"],
           color=bar_clrs, width=0.85, zorder=2, linewidth=0)

    ax.plot(daily["dt"], daily["roll7"],
            color=C["navy"], lw=1.8, ls="--", zorder=3)

    # Zombie baseline — confined to Jan, avoids mid-chart clutter
    z_avg  = df[df["workspace_id"]=="ws-zombie-008"]["estimated_cost_usd"].sum() / AUDIT_DAYS
    z_s    = pd.Timestamp("2026-01-01")
    z_e    = pd.Timestamp("2026-01-22")
    ax.plot([z_s, z_e], [z_avg, z_avg],
            color=C["burn"], lw=1.1, ls=":", alpha=0.65, zorder=2)
    ax.text(z_s + pd.Timedelta(days=0.8), z_avg + 48,
            "Zombie daily avg", fontsize=7.5, color=C["burn"], alpha=0.8)

    # Spike annotation — anchored safely above chart, arrow points to peak
    y_top      = daily["estimated_cost_usd"].max()
    annot_x    = pd.Timestamp("2026-01-10")   # left side, clear of legend
    annot_y    = y_top * 1.04
    spike_peak = daily.loc[is_spike, "estimated_cost_usd"].max()
    spike_x    = pd.Timestamp("2026-02-11")

    ax.annotate(
        "513% spike  ·  Feb 10–13\nServerless SQL runaway",
        xy=(spike_x, spike_peak),
        xytext=(annot_x, annot_y),
        fontsize=8, color=C["burn"], fontweight="bold",
        ha="left", va="bottom",
        arrowprops=dict(
            arrowstyle="-|>", color=C["burn"], lw=1.1,
            connectionstyle="arc3,rad=0.18",
        ),
    )

    y_lim = y_top * 1.45
    ax.set_ylim(0, y_lim)
    ax.set_xlim(pd.Timestamp("2025-12-30"), pd.Timestamp("2026-04-04"))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(dollar_k))
    ax.yaxis.set_major_locator(mticker.MaxNLocator(nbins=5, integer=False))
    ax.set_xlabel("Date  (Jan 1 – Mar 31, 2026)",
                  fontsize=8.5, color=C["sub"], labelpad=6)
    ax.set_ylabel("Daily Spend (USD)", fontsize=8.5, color=C["sub"], labelpad=8)

    # Legend — bottom-right, far from annotation
    leg = [
        mpatches.Patch(fc=C["burn_l"], label="Daily spend"),
        mpatches.Patch(fc=C["burn"],   label="Anomaly window  (Feb 10–13)"),
        Line2D([0],[0], color=C["navy"], ls="--", lw=1.8, label="7-day rolling avg"),
    ]
    ax.legend(handles=leg, loc="lower right", fontsize=8,
              frameon=True, framealpha=0.93,
              edgecolor=C["border"], labelcolor=C["sub"])

    # Title block — via fig.text so it lives above subplots_adjust top
    fig.text(0.09, 0.96, "90-Day Platform Burn Trend",
             fontsize=13, fontweight="bold", color=C["navy"],
             va="top", ha="left")
    fig.text(0.09, 0.90,
             f"Total: ${TOTAL_SPEND:,.0f}    Avg daily: "
             f"${TOTAL_SPEND/AUDIT_DAYS:,.0f}    Audit window: Q1 2026",
             fontsize=8, color=C["dim"], va="top", ha="left")

    save_fig(fig, "fig1_burn_trend.png")


# =============================================================================
# FIG 2 — SKU COST BREAKDOWN
# =============================================================================
def fig2_sku_breakdown():
    print("Generating Fig 2 — SKU Breakdown...")

    def cat(sku):
        if sku in ["GPU_SERVERLESS_COMPUTE","FOUNDATION_MODEL_TRAINING",
                   "SERVERLESS_REAL_TIME_INFERENCE","VECTOR_SEARCH_COMPUTE"]:
            return "AI / ML"
        if sku in ["JOBS_COMPUTE","JOBS_SERVERLESS_COMPUTE","SQL_PRO_COMPUTE",
                   "SERVERLESS_SQL_COMPUTE","DELTA_STORAGE"]:
            return "Production"
        if sku in ["STANDARD_ALL_PURPOSE_COMPUTE","PREMIUM_ALL_PURPOSE_COMPUTE"]:
            return "Interactive"
        return "Platform"

    df["cat"]  = df["sku_name"].apply(cat)
    cat_cost   = df.groupby("cat")["estimated_cost_usd"].sum().sort_values()
    total      = cat_cost.sum()
    cat_colors = {
        "AI / ML":    C["burn"],
        "Production": C["navy"],
        "Interactive":C["inter"],
        "Platform":   C["dim"],
    }

    fig, ax = plt.subplots(figsize=(9, 3.4))
    fig.subplots_adjust(top=0.78, bottom=0.14, left=0.15, right=0.97)
    clean_ax(ax)
    ax.grid(axis="x", color=C["grid"], lw=0.6)
    ax.grid(axis="y", visible=False)

    clrs = [cat_colors.get(c, C["dim"]) for c in cat_cost.index]
    bars = ax.barh(cat_cost.index, cat_cost.values,
                   color=clrs, height=0.48, zorder=2)

    # Smart labels: white inside wide bars, dark outside narrow bars
    for bar, val in zip(bars, cat_cost.values):
        pct  = val / total * 100
        txt  = f"${val:,.0f}  ({pct:.1f}%)"
        # threshold: bar fills at least 30% of axis width → label fits inside
        if val / total > 0.28:
            ax.text(val - total * 0.004,
                    bar.get_y() + bar.get_height() / 2,
                    txt, va="center", ha="right",
                    fontsize=9, color=C["white"], fontweight="600")
        else:
            ax.text(val + total * 0.006,
                    bar.get_y() + bar.get_height() / 2,
                    txt, va="center", ha="left",
                    fontsize=9, color=C["text"], fontweight="500")

    ax.set_xlim(0, cat_cost.max() * 1.28)  # tight to max bar + label room
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(dollar_k))
    ax.set_xlabel("90-Day Spend (USD)", fontsize=8.5, color=C["sub"], labelpad=6)
    ax.tick_params(axis="y", length=0, labelsize=10, labelcolor=C["text"])
    ax.spines["left"].set_visible(False)

    fig.text(0.15, 0.96, "Spend by Compute Category",
             fontsize=13, fontweight="bold", color=C["navy"], va="top", ha="left")
    fig.text(0.15, 0.90,
             f"90-day total: ${total:,.0f}   ·   4 categories   ·   12 SKUs",
             fontsize=8, color=C["dim"], va="top", ha="left")

    save_fig(fig, "fig2_sku_breakdown.png")


# =============================================================================
# FIG 3 — ANOMALY COST MATRIX
# =============================================================================
def fig3_anomaly_matrix():
    print("Generating Fig 3 — Anomaly Cost Matrix...")

    items = [
        ("Zombie\nCluster",   5007,  19289),
        ("GPU\nOverprov.",   11759,  23845),
        ("Serverless\nSpike", 1214,   4183),
        ("Weekend\nWaste",     946,   3645),
        ("DLT\nRunaway",      1510,   3673),
    ]
    labels  = [a[0] for a in items]
    cost90  = [a[1] for a in items]
    annual  = [a[2] for a in items]

    x     = np.arange(len(labels))
    W     = 0.35
    y_max = max(annual) * 1.32

    fig, ax = plt.subplots(figsize=(10, 4.6))
    fig.subplots_adjust(top=0.78, bottom=0.18, left=0.09, right=0.97)
    clean_ax(ax)

    bars1 = ax.bar(x - W/2, cost90, W, color=C["burn"],
                   label="90-day waste", zorder=2, linewidth=0)
    bars2 = ax.bar(x + W/2, annual, W, color=C["navy"],
                   label="Annual projection", zorder=2, linewidth=0, alpha=0.80)

    ax.set_ylim(0, y_max)

    # Value labels — above each bar, small gap
    gap = y_max * 0.011
    for b, v in zip(bars1, cost90):
        ax.text(b.get_x() + b.get_width()/2, b.get_height() + gap,
                f"${v:,}", ha="center", va="bottom",
                fontsize=7.8, color=C["burn"], fontweight="600")
    for b, v in zip(bars2, annual):
        ax.text(b.get_x() + b.get_width()/2, b.get_height() + gap,
                f"${v:,}", ha="center", va="bottom",
                fontsize=7.8, color=C["navy"], fontweight="600")

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=9.5, color=C["text"], linespacing=1.35)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(dollar_k))
    ax.set_ylabel("Cost (USD)", fontsize=8.5, color=C["sub"], labelpad=8)

    ax.legend(fontsize=9, frameon=True, framealpha=0.93,
              edgecolor=C["border"], labelcolor=C["sub"], loc="upper right")

    # Totals — inside axes upper-left, well away from legend
    ax.text(0.01, 0.97,
            f"90-day total: ${sum(cost90):,}   ·   Annual: ${sum(annual):,}",
            transform=ax.transAxes, fontsize=7.8, color=C["dim"],
            va="top", ha="left")

    fig.text(0.09, 0.96, "Anomaly Cost Matrix — 5 Findings",
             fontsize=13, fontweight="bold", color=C["navy"], va="top", ha="left")
    fig.text(0.09, 0.90,
             "90-day waste vs. annualized projection per anomaly type",
             fontsize=8, color=C["dim"], va="top", ha="left")

    save_fig(fig, "fig3_anomaly_matrix.png")


# =============================================================================
# FIG 4 — GPU UTILIZATION SCATTER  (numbered-badge version — no adjustText)
# =============================================================================
def fig4_gpu_utilization():
    print("Generating Fig 4 — GPU Utilization Scatter...")

    gpu_df = df[df["sku_name"] == "GPU_SERVERLESS_COMPUTE"].copy()
    gpu_df["gpu_util"] = gpu_df["meta"].apply(
        lambda x: float(x.get("gpu_utilization_pct", 50))
    )
    agg = (
        gpu_df.groupby("workspace_id")
        .agg(cost=("estimated_cost_usd", "sum"),
             util=("gpu_util", "mean"))
        .reset_index()
        .sort_values("cost", ascending=False)
        .reset_index(drop=True)
    )
    label_map = {
        "ws-retail-forecast-002":  "retail-forecast",
        "ws-fsi-trading-001":      "fsi-trading",
        "ws-ai-serving-006":       "ai-serving",
        "ws-prod-bi-004":          "prod-bi",
        "ws-health-analytics-003": "health-analytics",
        "ws-staging-007":          "staging",
        "ws-zombie-008":           "zombie-legacy",
        "ws-sandbox-ds-005":       "sandbox-ds",
    }
    agg["label"] = agg["workspace_id"].map(label_map)
    agg["num"]   = range(1, len(agg) + 1)

    # Display-only nudges — spread badges in the dense 58-65% util cluster
    nudge = {
        "retail-forecast":  ( 0,     0),
        "fsi-trading":      ( 0,    80),
        "ai-serving":       ( 1.2,  -30),
        "prod-bi":          ( 2.4,  -200),
        "health-analytics": (-2.0,  -80),
        "staging":          ( 2.2, -420),
        "zombie-legacy":    (-2.8, -510),
        "sandbox-ds":       ( 4.2, -570),
    }
    agg["plot_util"] = agg.apply(lambda r: r["util"] + nudge[r["label"]][0], axis=1)
    agg["plot_cost"] = agg.apply(lambda r: r["cost"] + nudge[r["label"]][1], axis=1)

    fig = plt.figure(figsize=(13, 6.0))
    fig.patch.set_facecolor(C["white"])

    ax = fig.add_axes([0.07, 0.13, 0.54, 0.64])
    ax.set_facecolor(C["white"])
    for sp in ["top", "right"]:   ax.spines[sp].set_visible(False)
    for sp in ["left", "bottom"]: ax.spines[sp].set_color(C["border"])
    ax.tick_params(colors=C["sub"], labelsize=8.5, length=3)
    ax.grid(color=C["grid"], linewidth=0.5, zorder=0)
    ax.set_axisbelow(True)

    sizes  = (agg["cost"] / agg["cost"].max() * 900) + 60
    colors = [C["burn"] if r["util"] < 40 else C["navy"] for _, r in agg.iterrows()]

    ax.scatter(agg["plot_util"], agg["plot_cost"],
               s=sizes, c=colors, alpha=0.83, zorder=3,
               linewidths=1.0, edgecolors="white")

    for _, row in agg.iterrows():
        ax.text(row["plot_util"], row["plot_cost"], str(row["num"]),
                ha="center", va="center",
                fontsize=7.0, color=C["white"], fontweight="bold", zorder=5)

    # Leader lines: nudged badge position -> true data point
    for _, row in agg.iterrows():
        if abs(row["plot_util"] - row["util"]) > 0.3 or abs(row["plot_cost"] - row["cost"]) > 50:
            ax.annotate("",
                xy=(row["util"],       row["cost"]),
                xytext=(row["plot_util"], row["plot_cost"]),
                arrowprops=dict(arrowstyle="-", color=C["border"],
                                lw=0.7, shrinkA=7, shrinkB=5),
                zorder=2)

    ax.axvline(x=40, color=C["burn"], lw=0.9, ls="--", alpha=0.40, zorder=2)
    ax.text(40.7, agg["cost"].max() * 1.22,
            "Right-size threshold (40%)", fontsize=7.5, color=C["burn"], va="top")

    outlier = agg[agg["label"] == "retail-forecast"].iloc[0]
    ax.annotate(
        f"  #1  Primary outlier
  27.9% util — ${outlier['cost']:,.0f}  ",
        xy=(outlier["util"], outlier["cost"]),
        xytext=(outlier["util"] - 17, outlier["cost"] - 3400),
        fontsize=7.8, color=C["burn"], ha="center", va="top",
        bbox=dict(boxstyle="round,pad=0.45", fc=C["white"],
                  ec=C["burn"], lw=0.9, alpha=0.96),
        arrowprops=dict(arrowstyle="-|>", color=C["burn"], lw=1.0,
                        connectionstyle="arc3,rad=-0.18"),
    )

    ax.xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(dollar_k))
    ax.set_xlabel("Average GPU Utilization (%)", fontsize=8.5, color=C["sub"], labelpad=6)
    ax.set_ylabel("90-Day Compute Cost (USD)",    fontsize=8.5, color=C["sub"], labelpad=8)
    ax.set_xlim(0, 100)
    ax.set_ylim(-400, agg["cost"].max() * 1.45)

    leg_handles = [
        mpatches.Patch(fc=C["burn"], label="Overprovisioned  (<40% util)"),
        mpatches.Patch(fc=C["navy"], label="Within threshold  (≥40% util)"),
    ]
    ax.legend(handles=leg_handles, loc="lower left", bbox_to_anchor=(0.01, 0.01),
              fontsize=8.5, frameon=True, framealpha=0.94,
              edgecolor=C["border"], labelcolor=C["sub"])

    tax = fig.add_axes([0.635, 0.13, 0.340, 0.64])
    tax.set_facecolor(C["white"])
    tax.axis("off")

    COL_X = [0.0, 0.08, 0.72, 1.0]
    ROW_H  = 1.0 / (len(agg) + 1.5)

    def draw_row(row_y, bg, cells, font_colors, font_weights):
        tax.add_patch(mpatches.FancyBboxPatch(
            (-0.02, row_y - ROW_H * 0.08), 1.04, ROW_H * 0.92,
            boxstyle="square,pad=0", transform=tax.transAxes,
            facecolor=bg, edgecolor=C["border"], linewidth=0.4,
            clip_on=False, zorder=1,
        ))
        aligns = ["center", "left", "right", "right"]
        for x, text, fc, fw, ha in zip(COL_X, cells, font_colors, font_weights, aligns):
            tax.text(x + (0.04 if ha == "center" else 0.02 if ha == "left" else -0.02),
                     row_y + ROW_H * 0.38, text,
                     ha=ha, va="center", fontsize=8.0, color=fc, fontweight=fw,
                     transform=tax.transAxes, zorder=2, clip_on=False)

    header_y = 1.0 - ROW_H
    tax.add_patch(mpatches.FancyBboxPatch(
        (-0.02, header_y - ROW_H * 0.08), 1.04, ROW_H * 0.92,
        boxstyle="square,pad=0", transform=tax.transAxes,
        facecolor=C["navy"], edgecolor="none", linewidth=0,
        clip_on=False, zorder=1,
    ))
    for x, text, ha in zip(COL_X, ["#", "Workspace", "Util", "90d Cost"],
                            ["center", "left", "right", "right"]):
        tax.text(x + (0.04 if ha == "center" else 0.02 if ha == "left" else -0.02),
                 header_y + ROW_H * 0.38, text,
                 ha=ha, va="center", fontsize=8.0, color=C["white"], fontweight="bold",
                 transform=tax.transAxes, zorder=2, clip_on=False)

    BURN_BG = "#FFF5F4"
    NAVY_L  = "#EAF1F6"
    for i, (_, row) in enumerate(agg.iterrows()):
        is_out = row["util"] < 40
        row_y  = header_y - (i + 1) * ROW_H
        bg     = BURN_BG if is_out else (C["white"] if i % 2 == 0 else NAVY_L)
        draw_row(
            row_y, bg,
            cells=[str(int(row["num"])), row["label"],
                   f"{row['util']:.1f}%", f"${row['cost']/1000:.1f}K"],
            font_colors=[C["burn"] if is_out else C["navy"],
                         C["burn"] if is_out else C["text"],
                         C["burn"] if is_out else C["sub"],
                         C["navy"]],
            font_weights=["bold", "bold" if is_out else "normal",
                          "bold" if is_out else "normal", "bold"],
        )

    tax.text(0.0, 1.0 - ROW_H * 0.15, "Workspace Index",
             ha="left", va="bottom", fontsize=9.5,
             color=C["navy"], fontweight="bold",
             transform=tax.transAxes, clip_on=False)

    fig.text(0.07, 0.91, "GPU Utilization vs. 90-Day Cost",
             fontsize=13, fontweight="bold", color=C["navy"], va="top", ha="left")
    fig.text(0.07, 0.855,
             "Bubble size proportional to cost.  "
             "Red = below right-size threshold.  "
             "Numbers correspond to index table.",
             fontsize=8, color=C["dim"], va="top", ha="left")

    save_fig(fig, "fig4_gpu_utilization.png")



# =============================================================================
# FIG 5 — REMEDIATION WATERFALL
# =============================================================================
def fig5_remediation_waterfall():
    print("Generating Fig 5 — Remediation Waterfall...")

    BASELINE = 56580
    steps = [
        ("Annual\nBaseline",  56580,  "base"),
        ("Zombie\nCluster",  -19289,  "save"),
        ("Serverless\nSpike", -4183,  "save"),
        ("GPU\nRight-size",  -23845,  "save"),
        ("Weekend\nWaste",    -3645,  "save"),
        ("DLT\nPipeline",     -3673,  "save"),
        ("Net Annual\nCost",   None,  "total"),
    ]

    # Build true waterfall geometry
    running   = 0
    bottoms   = []
    heights   = []
    clrs      = []

    for _, val, kind in steps:
        if kind == "base":
            bottoms.append(0); heights.append(val); clrs.append(C["navy"])
            running = val
        elif kind == "save":
            new = running + val
            bottoms.append(new); heights.append(abs(val)); clrs.append(C["green"])
            running = new
        else:
            bottoms.append(0); heights.append(running); clrs.append(C["navy"])

    labels = [s[0] for s in steps]
    NET    = running   # final running total after all saves
    x      = np.arange(len(labels))
    W      = 0.50
    Y_MAX  = BASELINE * 1.18

    fig, ax = plt.subplots(figsize=(11, 5.2))
    fig.subplots_adjust(top=0.78, bottom=0.17, left=0.09, right=0.88)
    clean_ax(ax)

    ax.bar(x, heights, bottom=bottoms, color=clrs,
           width=W, zorder=2, linewidth=0)

    # Connector lines: horizontal lines at the level where each saving lands
    run_c = BASELINE
    for i, (_, val, kind) in enumerate(steps):
        if kind == "save":
            new_c = run_c + val
            # line from right edge of current bar to left edge of next bar
            ax.plot([x[i] + W/2 + 0.03, x[i+1] - W/2 - 0.03],
                    [new_c, new_c],
                    color=C["border"], lw=1.1, zorder=3, solid_capstyle="butt")
            run_c = new_c

    # Value labels
    for i, (_, val, kind) in enumerate(steps):
        top = bottoms[i] + heights[i]
        mid = bottoms[i] + heights[i] / 2

        if kind == "base":
            ax.text(x[i], top + Y_MAX * 0.013,
                    f"${BASELINE:,}", ha="center", va="bottom",
                    fontsize=9.5, color=C["navy"], fontweight="bold")

        elif kind == "save":
            bar_h = heights[i]
            if bar_h > Y_MAX * 0.075:      # tall enough → label inside (white)
                ax.text(x[i], mid,
                        f"–${abs(val):,}", ha="center", va="center",
                        fontsize=8.5, color=C["white"], fontweight="600")
            else:                           # short bar → label below bottom edge
                ax.text(x[i], bottoms[i] - Y_MAX * 0.014,
                        f"–${abs(val):,}", ha="center", va="top",
                        fontsize=8, color=C["green"], fontweight="600")

        else:   # total
            ax.text(x[i], top + Y_MAX * 0.013,
                    f"${NET:,}", ha="center", va="bottom",
                    fontsize=9.5, color=C["navy"], fontweight="bold")

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=9.2, color=C["text"], linespacing=1.35)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(dollar_k))
    ax.set_ylabel("Annual Cost (USD)", fontsize=8.5, color=C["sub"], labelpad=8)
    ax.set_ylim(0, Y_MAX)

    # Savings callout — positioned in right margin via fig transform
    # Use axes-relative coords so it clears the last bar cleanly
    ax.annotate(
        f"  $54,635\n  saved / yr  ",
        xy=(x[-1] + W/2 + 0.08, NET / 2 + 500),
        xytext=(x[-1] + W/2 + 1.05, BASELINE * 0.46),
        fontsize=10.5, color=C["green"], fontweight="bold",
        ha="left", va="center",
        bbox=dict(boxstyle="round,pad=0.5", fc=C["green_l"],
                  ec=C["green"], lw=0.9),
        arrowprops=dict(arrowstyle="-|>", color=C["green"], lw=1.3),
        annotation_clip=False,
    )

    leg_handles = [
        mpatches.Patch(fc=C["navy"],  label="Cost retained"),
        mpatches.Patch(fc=C["green"], label="Savings recovered"),
    ]
    ax.legend(handles=leg_handles, loc="upper right",
              fontsize=9, frameon=True, framealpha=0.93,
              edgecolor=C["border"], labelcolor=C["sub"])

    fig.text(0.09, 0.96, "Annual Spend: Before vs. After Remediation",
             fontsize=13, fontweight="bold", color=C["navy"], va="top", ha="left")
    fig.text(0.09, 0.90,
             f"Baseline: ${BASELINE:,} / yr   ·   Net after 5 fixes: ${NET:,} / yr"
             f"   ·   ~8 hrs implementation",
             fontsize=8, color=C["dim"], va="top", ha="left")

    save_fig(fig, "fig5_remediation_waterfall.png")


# =============================================================================
# RUN ALL
# =============================================================================
print("=" * 60)
print("  Ghost Burn v2.1 — Professional Visualization Rebuild")
print("=" * 60)
print()

fig1_burn_trend()
fig2_sku_breakdown()
fig3_anomaly_matrix()
fig4_gpu_utilization()
fig5_remediation_waterfall()

print()
print("=" * 60)
print("  All 5 figures saved to repo root")
print("=" * 60)
