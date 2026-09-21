-- ============================================================
-- 第 14 步 · 复现 ads_vintage（与 MySQL 同口径）
-- 对应阶段二 src/ads_metrics.py 的 SQL_VINTAGE + SQL_VINTAGE_ALL（两段 INSERT）
--
-- ⭐⭐ 硬约束 ①：必须生成 grade='ALL' 行
--   阶段二就是两段 INSERT（分评级 + 全量）。只在 Hive 侧按 grade 分组 → 少 1/8 的行，
--   逐行比对第一步就挂（这正是 v2 指导书漏掉的地方）。
--
-- ⭐ 累计不良率：用窗口函数取"曾核销"标志，避免同一笔被逐月重复计数
--   关于窗口帧：默认帧是 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW，不是 ROWS。
--   ORDER BY 键有重复值时两者语义不同；本数据 (issue_month, loan_id) 内 mob 唯一，两者等价，
--   但**显式写 ROWS** 可以避免"以后数据粒度变了、结论静默变化"。
--
-- 执行：docker exec -i bd-spark /opt/spark/bin/spark-sql \
--         -f /workspace/07_bigdata/hive/05_ads_vintage.sql
-- ============================================================
USE credit_dwh_lc;

DROP TABLE IF EXISTS ads_vintage_lc;
CREATE TABLE ads_vintage_lc (
  issue_month  STRING,
  grade        STRING,
  mob          INT,
  bad_rate     DECIMAL(10,6),
  exposure_cnt BIGINT
) STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- ---------- ① 分评级（对应 SQL_VINTAGE）----------
INSERT OVERWRITE TABLE ads_vintage_lc
SELECT issue_month,
       grade,
       mob,
       ROUND(SUM(ever_bad) / NULLIF(COUNT(*), 0), 6) AS bad_rate,
       COUNT(*)                                      AS exposure_cnt
FROM (
    SELECT s.issue_month,
           COALESCE(s.grade, 'UNKNOWN') AS grade,   -- ⭐ 与 MySQL 侧写法保持一致
           s.mob,
           s.loan_id,
           MAX(s.is_bad_month) OVER (
               PARTITION BY s.issue_month, s.loan_id ORDER BY s.mob
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS ever_bad
    FROM dws_loan_snapshot_lc s
    WHERE s.mob BETWEEN 1 AND 24
) t
GROUP BY issue_month, grade, mob;

-- ---------- ② ⭐ ALL 行（对应 SQL_VINTAGE_ALL）----------
INSERT INTO TABLE ads_vintage_lc
SELECT issue_month,
       'ALL' AS grade,
       mob,
       ROUND(SUM(ever_bad) / NULLIF(COUNT(*), 0), 6) AS bad_rate,
       COUNT(*)                                      AS exposure_cnt
FROM (
    SELECT s.issue_month, s.mob, s.loan_id,
           MAX(s.is_bad_month) OVER (
               PARTITION BY s.issue_month, s.loan_id ORDER BY s.mob
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS ever_bad
    FROM dws_loan_snapshot_lc s
    WHERE s.mob BETWEEN 1 AND 24
) t
GROUP BY issue_month, mob;

SELECT COUNT(*) AS rows_total,
       COUNT(DISTINCT grade) AS grades,
       SUM(CASE WHEN grade = 'ALL' THEN 1 ELSE 0 END) AS rows_all
FROM ads_vintage_lc;
-- 基准（MySQL 实测）：24,768 行；grade 去重后含 'ALL'（A~G + UNKNOWN + ALL）
