-- ============================================================
-- 第 14 步 · 复现 ads_risk_segment（与 MySQL 同口径）
-- 对应阶段二 src/ads_metrics.py 的 SQL_RISK_SEGMENT（三段 INSERT）
-- 执行：docker exec -i bd-spark /opt/spark/bin/spark-sql \
--         -f /workspace/07_bigdata/hive/04_ads_risk_segment.sql
-- ============================================================
USE credit_dwh_lc;

DROP TABLE IF EXISTS ads_risk_segment_lc;
CREATE TABLE ads_risk_segment_lc (
  dim_type     STRING,
  dim_value    STRING,
  loan_cnt     BIGINT,
  total_amnt   DECIMAL(18,2),
  bad_cnt      BIGINT,
  bad_rate     DECIMAL(10,6),
  avg_int_rate DECIMAL(8,4)
) STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- ---------- ① grade 维度 ----------
INSERT OVERWRITE TABLE ads_risk_segment_lc
SELECT 'grade'                                    AS dim_type,
       f.grade                                    AS dim_value,
       COUNT(*)                                   AS loan_cnt,
       ROUND(SUM(f.funded_amnt), 2)               AS total_amnt,
       SUM(p.is_bad)                              AS bad_cnt,
       ROUND(SUM(p.is_bad) / COUNT(*), 6)         AS bad_rate,
       ROUND(SUM(f.funded_amnt * f.int_rate)
             / NULLIF(SUM(f.funded_amnt), 0), 4)  AS avg_int_rate
FROM dwd_loan_fact_lc f
JOIN dwd_loan_perf_fact_lc p ON f.loan_id = p.loan_id
WHERE p.is_terminal = 1        -- ⭐ 关键：只算终态，否则分母被 Current 稀释
GROUP BY f.grade;

-- ---------- ② income_band 维度 ----------
INSERT INTO TABLE ads_risk_segment_lc
SELECT 'income_band', a.income_band, COUNT(*),
       ROUND(SUM(f.funded_amnt), 2), SUM(p.is_bad),
       ROUND(SUM(p.is_bad) / COUNT(*), 6),
       ROUND(SUM(f.funded_amnt * f.int_rate) / NULLIF(SUM(f.funded_amnt), 0), 4)
FROM dwd_loan_fact_lc f
JOIN dwd_loan_perf_fact_lc p ON f.loan_id = p.loan_id
JOIN dim_applicant_lc a      ON f.loan_id = a.loan_id
WHERE p.is_terminal = 1
GROUP BY a.income_band;

-- ---------- ③ fico_band 维度 ----------
INSERT INTO TABLE ads_risk_segment_lc
SELECT 'fico_band', a.fico_band, COUNT(*),
       ROUND(SUM(f.funded_amnt), 2), SUM(p.is_bad),
       ROUND(SUM(p.is_bad) / COUNT(*), 6),
       ROUND(SUM(f.funded_amnt * f.int_rate) / NULLIF(SUM(f.funded_amnt), 0), 4)
FROM dwd_loan_fact_lc f
JOIN dwd_loan_perf_fact_lc p ON f.loan_id = p.loan_id
JOIN dim_applicant_lc a      ON f.loan_id = a.loan_id
WHERE p.is_terminal = 1
GROUP BY a.fico_band;

SELECT COUNT(*) AS rows_total,
       COUNT(DISTINCT dim_type) AS dim_types
FROM ads_risk_segment_lc;
-- 基准（MySQL 实测）：18 行 / 3 个 dim_type
