"""生成 4 张分析图表。

⚠️ 图表依赖 ADS 表，请先跑完 src.ads_metrics。
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from src.utils import get_logger, read_sql

log = get_logger("charts")
plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False
OUT = Path(__file__).resolve().parent / "images"
OUT.mkdir(exist_ok=True)


def chart_issue_trend():
    df = read_sql("""
        SELECT issue_month, COUNT(*) AS loan_cnt,
               ROUND(SUM(funded_amnt)/1e8, 4) AS amnt_yi
        FROM dwd_loan_fact GROUP BY issue_month ORDER BY issue_month
    """)
    if df.empty:
        log.warning("无数据，跳过 放款趋势")
        return
    fig, ax1 = plt.subplots(figsize=(14, 6))
    ax1.bar(df["issue_month"], df["loan_cnt"] / 10000, color="#4C72B0",
            alpha=.8, label="放款笔数(万)")
    ax1.set_ylabel("放款笔数（万笔）")
    step = max(1, len(df) // 18)
    ax1.set_xticks(range(0, len(df), step))
    ax1.set_xticklabels(df["issue_month"][::step], rotation=45, ha="right")
    ax2 = ax1.twinx()
    ax2.plot(df["issue_month"], df["amnt_yi"], color="#C44E52",
             marker="o", ms=3, label="放款金额(亿元)")
    ax2.set_ylabel("放款金额（亿元）")
    plt.title("放款趋势：月度放款笔数与金额")
    fig.tight_layout()
    fig.savefig(OUT / "01_放款趋势.png", dpi=150)
    plt.close(fig)
    log.info("✅ 01_放款趋势.png")


def chart_vintage():
    df = read_sql("""
        SELECT issue_month, mob, bad_rate FROM ads_vintage
        WHERE grade='ALL' AND mob BETWEEN 1 AND 24
    """)
    if df.empty:
        log.warning("无数据，跳过 Vintage（请先跑 dws_snapshot 与 ads_metrics）")
        return
    p = df.pivot(index="issue_month", columns="mob", values="bad_rate").dropna(how="all")
    fig, ax = plt.subplots(figsize=(16, max(6, len(p) * 0.32)))
    sns.heatmap(p * 100, cmap="YlOrRd", annot=False,
                cbar_kws={"label": "累计不良率 (%)"}, ax=ax)
    ax.set_title("Vintage 逾期率矩阵：放款批次 × 账龄 MOB")
    ax.set_xlabel("账龄 MOB（月）")
    ax.set_ylabel("放款批次")
    fig.tight_layout()
    fig.savefig(OUT / "02_Vintage矩阵.png", dpi=150)
    plt.close(fig)
    log.info("✅ 02_Vintage矩阵.png")


def chart_risk_segment():
    df = read_sql("""
        SELECT dim_value AS grade, loan_cnt, bad_rate, avg_int_rate
        FROM ads_risk_segment WHERE dim_type='grade' ORDER BY dim_value
    """)
    if df.empty:
        log.warning("无数据，跳过 风险分层")
        return
    fig, ax1 = plt.subplots(figsize=(10, 6))
    ax1.bar(df["grade"], df["bad_rate"].astype(float) * 100,
            color="#55A868", alpha=.85)
    ax1.set_ylabel("不良率 (%)")
    ax1.set_xlabel("信用评级")
    for i, (r, n) in enumerate(zip(df["bad_rate"].astype(float) * 100,
                                   df["loan_cnt"])):
        ax1.text(i, r + 0.3, f"{r:.1f}%\nn={int(n):,}", ha="center", fontsize=8)
    ax2 = ax1.twinx()
    ax2.plot(df["grade"], df["avg_int_rate"].astype(float),
             color="#C44E52", marker="s", label="加权平均利率")
    ax2.set_ylabel("加权平均利率 (%)")
    plt.title("风险分层：评级 vs 不良率 vs 加权平均利率")
    fig.tight_layout()
    fig.savefig(OUT / "03_风险分层.png", dpi=150)
    plt.close(fig)
    log.info("✅ 03_风险分层.png")


def chart_roll_rate():
    try:
        df = read_sql("""
            SELECT from_bucket, to_bucket, SUM(cnt) AS cnt
            FROM ads_roll_rate GROUP BY from_bucket, to_bucket
        """)
    except Exception as e:
        log.warning(f"迁徙率查询失败，跳过：{e}")
        return
    if df.empty:
        log.warning("无数据，跳过 迁徙率")
        return
    p = df.pivot(index="from_bucket", columns="to_bucket", values="cnt").fillna(0)
    p = p.div(p.sum(axis=1).replace(0, pd.NA), axis=0).astype(float) * 100
    fig, ax = plt.subplots(figsize=(10, 7))
    sns.heatmap(p, annot=True, fmt=".1f", cmap="Blues",
                cbar_kws={"label": "迁徙率 (%)"}, ax=ax)
    ax.set_title("迁徙率矩阵（基于月末快照，DPD 为规则推导值）")
    ax.set_xlabel("期末档位")
    ax.set_ylabel("期初档位")
    fig.tight_layout()
    fig.savefig(OUT / "04_迁徙率.png", dpi=150)
    plt.close(fig)
    log.info("✅ 04_迁徙率.png")


if __name__ == "__main__":
    chart_issue_trend()
    chart_vintage()
    chart_risk_segment()
    chart_roll_rate()
