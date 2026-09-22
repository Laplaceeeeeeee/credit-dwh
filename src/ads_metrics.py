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

import pymysql

from src import config
from src.utils import get_logger, read_sql

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

# ⚠️ 迁徙率的计算代价（实测踩过）：
#    全量快照 4471 万行、141 个观测月。若对所有相邻月份对做自关联，
#    相当于对 4471 万行做 140 次双向扫描 —— 单机 MySQL 无法完成，作业会被拖死。
#
#    因此**只算最近 ROLL_RATE_MONTHS 个观测月**。
#    这不是偷懒：迁徙率矩阵的用途就是看"最近一期"的状态流转，
#    分析报告也只展示最近一个月。算全部历史没有业务意义，却有巨大的性能代价。
#
#    若确实需要更长历史，把 ROLL_RATE_MONTHS 调大（耗时近似线性增长）。
#
# ⚠️ 坑 2（排序规则，第二次踩）：
#    最初用 CREATE TEMPORARY TABLE 存"最近的月份"，结果临时表跟随**库默认排序规则**
#    utf8mb4_unicode_ci，而快照表是 utf8mb4_0900_ai_ci，JOIN 时报：
#        ERROR 1267 Illegal mix of collations
#    这正是排错手册 C-15 记过的坑 —— 加一张表就多一次踩坑机会。
#
#    教训：**能不加表就不加表**。改用子查询直接算出起始月份，纯范围过滤 + 自关联。
#
# ⚠️ 坑 3（日期函数对 CHAR 失效）：
#    snapshot_month 是 CHAR(7)（如 '2019-03'），DATE_SUB 直接作用于它**返回 NULL**，
#    于是过滤条件 `>= NULL` 恒为假，查询 0.1 秒返回 0 行（静默失败，不报错）。
#    这类"不报错但结果为空"的 bug 最危险，必须靠**验证行数**发现。
#
#    最终改用**字符串比较**：'YYYY-MM' 是定长格式，字典序恰好等于时间序，
#    所以 `snapshot_month >= DATE_FORMAT(DATE_SUB(..., INTERVAL n MONTH), '%Y-%m')`
#    可以简化为对 12 个月前的月份字符串做比较。既避开类型转换，又简单可靠。
ROLL_RATE_MONTHS = 12

SQL_ROLL_RATE = """
TRUNCATE TABLE ads_roll_rate;

-- 只取最近 {n} 个月的相邻月份对做自关联。
-- 起点 = 最大月份往前推 n 个月；用 CONCAT 把 CHAR(7) 补成 DATE 再运算，
-- 避免 DATE_SUB 直接作用于 CHAR 返回 NULL。
INSERT INTO ads_roll_rate (from_month, from_bucket, to_bucket, cnt, rate)
SELECT a.snapshot_month,
       a.dpd_bucket,
       b.dpd_bucket,
       COUNT(*) AS cnt,
       ROUND(COUNT(*) / SUM(COUNT(*)) OVER (
              PARTITION BY a.snapshot_month, a.dpd_bucket), 6) AS rate
FROM dws_loan_snapshot_m a
JOIN dws_loan_snapshot_m b
  ON  b.loan_id = a.loan_id
  AND b.snapshot_month = DATE_FORMAT(
        DATE_ADD(STR_TO_DATE(CONCAT(a.snapshot_month, '-01'), '%Y-%m-%d'),
                 INTERVAL 1 MONTH), '%Y-%m')
WHERE a.snapshot_month >= DATE_FORMAT(
        DATE_SUB(STR_TO_DATE(CONCAT(
          (SELECT MAX(snapshot_month) FROM dws_loan_snapshot_m), '-01'), '%Y-%m-%d'),
          INTERVAL {n} MONTH), '%Y-%m')
GROUP BY a.snapshot_month, a.dpd_bucket, b.dpd_bucket;
""".format(n=ROLL_RATE_MONTHS)


def run_block(name: str, sql: str) -> int:
    """执行一段 SQL（可含多条语句）。

    ⚠️ 必须**共用同一个连接**：本次新增了 CREATE TEMPORARY TABLE，
       而临时表只在创建它的会话里可见。若每条语句各开一个连接，
       后续 INSERT 会找不到临时表。
       同时临时表在会话结束时会自动消失，所以不要用连接池的自动归还逻辑。
    """
    t0 = time.time()
    stmts = [s.strip() for s in sql.split(";") if s.strip()]
    cfg = config.DB
    conn = pymysql.connect(host=cfg["host"], port=cfg["port"], user=cfg["user"],
                           password=cfg["password"], database=cfg["database"],
                           charset="utf8mb4", autocommit=True)
    try:
        with conn.cursor() as cur:
            for s in stmts:
                log.info(f"    · {s.splitlines()[0][:70]}")
                cur.execute(s)
    finally:
        conn.close()
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
