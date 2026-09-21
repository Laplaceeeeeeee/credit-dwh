-- ============================================================
-- 第 14 步 · 复现 ads_roll_rate（与 MySQL 同口径）
-- 对应阶段二 src/ads_metrics.py 的 SQL_ROLL_RATE（ROLL_RATE_MONTHS = 12）
--
-- ⭐⭐ 硬约束 ②：必须限定"最近 12 个观测月"
--   阶段二只算最近 12 个观测月 —— 全量相邻月自关联 = 4471 万行 × 140 个月对，
--   单机 MySQL 会被拖死（阶段二实测过）。
--   更关键的是：**口径不同就不能比对**（全量 vs 12 个月，行数天然不等）。
--
-- 注意两个函数的对应关系：
--   MySQL  DATE_ADD(STR_TO_DATE(CONCAT(a.snapshot_month,'-01'),'%Y-%m-%d'), INTERVAL 1 MONTH)
--   Spark  add_months(to_date(concat(a.snapshot_month,'-01')), 1)
--   起点月同理：max(snapshot_month) 往前推 12 个月（含端点 → 13 个月，但 a 只能是 max-1 之前）
--
-- 执行：docker exec -i bd-spark /opt/spark/bin/spark-sql \
--         -f /workspace/07_bigdata/hive/06_ads_roll_rate.sql
-- ============================================================
USE credit_dwh_lc;

DROP TABLE IF EXISTS ads_roll_rate_lc;
CREATE TABLE ads_roll_rate_lc (
  from_month  STRING,
  from_bucket STRING,
  to_bucket   STRING,
  cnt         BIGINT,
  rate        DECIMAL(10,6)
) STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE ads_roll_rate_lc
SELECT a.snapshot_month                              AS from_month,
       a.dpd_bucket                                  AS from_bucket,
       b.dpd_bucket                                  AS to_bucket,
       COUNT(*)                                      AS cnt,
       ROUND(COUNT(*) / SUM(COUNT(*)) OVER (
             PARTITION BY a.snapshot_month, a.dpd_bucket), 6) AS rate
FROM dws_loan_snapshot_lc a
JOIN dws_loan_snapshot_lc b
  ON  a.loan_id = b.loan_id
  AND b.snapshot_month = date_format(
        add_months(to_date(concat(a.snapshot_month, '-01')), 1), 'yyyy-MM')
WHERE a.snapshot_month >= (
        SELECT date_format(
                 add_months(to_date(concat(MAX(snapshot_month), '-01')), -12),
                 'yyyy-MM')
        FROM dws_loan_snapshot_lc
      )
GROUP BY a.snapshot_month, a.dpd_bucket, b.dpd_bucket;

SELECT COUNT(*) AS rows_total,
       COUNT(DISTINCT from_month) AS months,
       COUNT(DISTINCT from_bucket) AS from_buckets
FROM ads_roll_rate_lc;
-- 基准（MySQL 实测）：108 行 / 12 个 from_month
