"""ADS 层指标计算。

⭐ 关键口径（务必与指标口径字典一致）：
   1. 不良率必须限定 is_terminal = 1，否则 Current（占 38.85%）会稀释分母
   2. 加权平均利率用金额加权，不是简单平均
   3. 累计不良率用窗口函数取"曾核销"标志，避免同一笔被逐月重复计数
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text

from src.utils import get_engine, get_logger, read_sql

log = get_logger("ads_metrics")

SQL_RISK_SEGMENT = """
TRUNCATE TABLE ads_risk_segment;
INSERT INTO ads_risk_segment
  (dim_type, dim_value, loan_cnt, total_amnt, bad_cnt, bad_rate, avg_int_rate)
SELECT 'grade', f.grade,
       COUNT(*),
       ROUND(SUM(f.funded_amnt), 2),
       SUM(p.is_bad),
       ROUND(SUM(p.is_bad) / COUNT(*), 6),
       ROUND(SUM(f.funded_amnt * f.int_rate) / NULLIF(SUM(f.funded_amnt), 0), 4)
FROM dwd_loan_fact f
JOIN dwd_loan_perf_fact p ON f.loan_id = p.loan_id
WHERE p.is_terminal = 1
GROUP BY f.grade;

INSERT INTO ads_risk_segment
  (dim_type, dim_value, loan_cnt, total_amnt, bad_cnt, bad_rate, avg_int_rate)
SELECT 'income_band', a.income_band,
       COUNT(*),
       ROUND(SUM(f.funded_amnt), 2),
       SUM(p.is_bad),
       ROUND(SUM(p.is_bad) / COUNT(*), 6),
       ROUND(SUM(f.funded_amnt * f.int_rate) / NULLIF(SUM(f.funded_amnt), 0), 4)
FROM dwd_loan_fact f
JOIN dwd_loan_perf_fact p ON f.loan_id = p.loan_id
JOIN dim_applicant a ON f.loan_id = a.loan_id
WHERE p.is_terminal = 1
GROUP BY a.income_band;

INSERT INTO ads_risk_segment
  (dim_type, dim_value, loan_cnt, total_amnt, bad_cnt, bad_rate, avg_int_rate)
SELECT 'fico_band', a.fico_band,
       COUNT(*),
       ROUND(SUM(f.funded_amnt), 2),
       SUM(p.is_bad),
       ROUND(SUM(p.is_bad) / COUNT(*), 6),
       ROUND(SUM(f.funded_amnt * f.int_rate) / NULLIF(SUM(f.funded_amnt), 0), 4)
FROM dwd_loan_fact f
JOIN dwd_loan_perf_fact p ON f.loan_id = p.loan_id
JOIN dim_applicant a ON f.loan_id = a.loan_id
WHERE p.is_terminal = 1
GROUP BY a.fico_band;
"""

SQL_DELINQ_MONTHLY = """
TRUNCATE TABLE ads_delinq_monthly;
INSERT INTO ads_delinq_monthly
  (issue_month, mob, loan_cnt, bad_cnt, delinq_cnt, delinq_rate, bad_rate, outstanding_amt)
SELECT issue_month, mob, COUNT(*), SUM(is_bad_month), SUM(is_delinquent),
       ROUND(SUM(is_delinquent) / NULLIF(COUNT(*), 0), 6),
       ROUND(SUM(is_bad_month) / NULLIF(COUNT(*), 0), 6),
       ROUND(SUM(out_prncp), 2)
FROM dws_loan_snapshot_m
GROUP BY issue_month, mob;
"""

# ⭐ 累计不良率：用窗口函数取"曾核销"标志。
#    直接 SUM(is_bad_month) 会把同一笔逐月重复计数，导致累计不良率随 MOB 虚高。
SQL_VINTAGE = """
TRUNCATE TABLE ads_vintage;
INSERT INTO ads_vintage (issue_month, grade, mob, bad_rate, exposure_cnt)
SELECT issue_month, grade, mob,
       ROUND(SUM(ever_bad) / NULLIF(COUNT(*), 0), 6),
       COUNT(*)
FROM (
  SELECT s.issue_month,
         COALESCE(s.grade, 'UNKNOWN') AS grade,
         s.mob, s.loan_id,
         MAX(s.is_bad_month) OVER (
           PARTITION BY s.issue_month, s.loan_id ORDER BY s.mob
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
         ) AS ever_bad
  FROM dws_loan_snapshot_m s
  WHERE s.mob BETWEEN 1 AND 24
) t
GROUP BY issue_month, grade, mob;
"""

SQL_VINTAGE_ALL = """
INSERT INTO ads_vintage (issue_month, grade, mob, bad_rate, exposure_cnt)
SELECT issue_month, 'ALL', mob,
       ROUND(SUM(ever_bad) / NULLIF(COUNT(*), 0), 6),
       COUNT(*)
FROM (
  SELECT s.issue_month, s.mob, s.loan_id,
         MAX(s.is_bad_month) OVER (
           PARTITION BY s.issue_month, s.loan_id ORDER BY s.mob
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
         ) AS ever_bad
  FROM dws_loan_snapshot_m s
  WHERE s.mob BETWEEN 1 AND 24
) t
GROUP BY issue_month, mob;
"""

SQL_ROLL_RATE = """
TRUNCATE TABLE ads_roll_rate;
INSERT INTO ads_roll_rate (from_month, from_bucket, to_bucket, cnt, rate)
SELECT a.snapshot_month, a.dpd_bucket, b.dpd_bucket, COUNT(*),
       ROUND(COUNT(*) / SUM(COUNT(*)) OVER (
         PARTITION BY a.snapshot_month, a.dpd_bucket), 6)
FROM dws_loan_snapshot_m a
JOIN dws_loan_snapshot_m b
  ON a.loan_id = b.loan_id AND b.mob = a.mob + 1
GROUP BY a.snapshot_month, a.dpd_bucket, b.dpd_bucket;
"""


def run_block(name: str, sql: str) -> int:
    eng = get_engine()
    t0 = time.time()
    stmts = [s.strip() for s in sql.split(";") if s.strip()]
    with eng.begin() as conn:
        for s in stmts:
            conn.execute(text(s))
    log.info(f"  ✅ {name} 完成（{time.time() - t0:.1f}s，{len(stmts)} 条语句）")
    return len(stmts)


def show(title: str, df) -> None:
    log.info(f"\n{title}\n" + df.to_string(index=False))


def main():
    log.info("=" * 62)
    log.info("ADS 指标计算")
    log.info("=" * 62)

    run_block("风险分层 ads_risk_segment", SQL_RISK_SEGMENT)

    # 快照相关指标依赖 dws_loan_snapshot_m
    has_snap = read_sql(
        "SELECT COUNT(*) c FROM information_schema.tables "
        "WHERE table_schema='credit_dwh' AND table_name='dws_loan_snapshot_m'"
    ).iloc[0, 0]
    if not has_snap:
        log.warning("⚠️ 未找到 dws_loan_snapshot_m，跳过 Vintage/迁徙率/逾期月报")
    else:
        n = read_sql("SELECT COUNT(*) c FROM dws_loan_snapshot_m").iloc[0, 0]
        if n == 0:
            log.warning("⚠️ dws_loan_snapshot_m 为空，请先运行 src.dws_snapshot")
        else:
            log.info(f"快照表 {n:,} 行，开始计算依赖指标")
            run_block("逾期月报 ads_delinq_monthly", SQL_DELINQ_MONTHLY)
            run_block("Vintage（分评级）", SQL_VINTAGE)
            run_block("Vintage（ALL）", SQL_VINTAGE_ALL)
            run_block("迁徙率 ads_roll_rate", SQL_ROLL_RATE)

    log.info("\n" + "=" * 62)
    log.info("结果抽查")
    log.info("=" * 62)

    show("风险分层（按评级）",
         read_sql("SELECT * FROM ads_risk_segment WHERE dim_type='grade' "
                  "ORDER BY dim_value"))

    total = read_sql("SELECT COUNT(*) c, SUM(is_bad) b FROM dwd_loan_perf_fact "
                     "WHERE is_terminal=1").iloc[0]
    by_grade = read_sql("SELECT SUM(bad_cnt) s FROM ads_risk_segment "
                        "WHERE dim_type='grade'").iloc[0, 0]
    log.info(f"\n分散校验：终态不良总数={int(total['b']):,}  "
             f"按评级汇总={int(by_grade):,}  "
             f"{'✅ 一致' if int(total['b']) == int(by_grade) else '❌ 不一致'}")


if __name__ == "__main__":
    main()
