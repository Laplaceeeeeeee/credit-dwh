-- ============================================================
-- 第 14 步 · 复现 ads_delinq_monthly（与 MySQL 同口径）
-- 对应阶段二 src/ads_metrics.py 的 SQL_DELINQ_MONTHLY
--
-- ⭐ 硬约束 ③：v2 指导书的清单里写了这张表，但**通篇没有它的建表与插入语句** ——
--   这类"清单里有、文件里没有"的缺口，最后会变成"以为做了其实没做"。
--
-- ⚠️ 注意口径：这里是**逐月口径**（该月末处于什么状态），
--   不是 Vintage 的"曾核销"累计口径 —— 两者不要混用。
--
-- 执行：docker exec -i bd-spark /opt/spark/bin/spark-sql \
--         -f /workspace/07_bigdata/hive/07_ads_delinq_monthly.sql
-- ============================================================
USE credit_dwh_lc;

DROP TABLE IF EXISTS ads_delinq_monthly_lc;
CREATE TABLE ads_delinq_monthly_lc (
  issue_month     STRING,
  mob             INT,
  loan_cnt        BIGINT,
  bad_cnt         BIGINT,
  delinq_cnt      BIGINT,
  delinq_rate     DECIMAL(10,6),
  bad_rate        DECIMAL(10,6),
  outstanding_amt DECIMAL(18,2)
) STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE ads_delinq_monthly_lc
SELECT issue_month,
       mob,
       COUNT(*)                                            AS loan_cnt,
       SUM(is_bad_month)                                   AS bad_cnt,
       SUM(is_delinquent)                                  AS delinq_cnt,
       ROUND(SUM(is_delinquent) / NULLIF(COUNT(*), 0), 6)  AS delinq_rate,
       ROUND(SUM(is_bad_month) / NULLIF(COUNT(*), 0), 6)   AS bad_rate,
       ROUND(SUM(out_prncp), 2)                            AS outstanding_amt
FROM dws_loan_snapshot_lc
GROUP BY issue_month, mob;

SELECT COUNT(*) AS rows_total,
       COUNT(DISTINCT issue_month) AS months
FROM ads_delinq_monthly_lc;
-- 基准（MySQL 实测）：3,105 行
