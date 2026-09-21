-- ============================================================
-- 实验 E5 · 数据量阶梯（Spark 组）
-- 语义与 E5_baseline.sql / bench_mysql.py **完全一致**（按 grade 分层的终态不良率）
-- 样本用同一个确定性谓词：CAST(loan_id AS BIGINT) % N = 0
--   ⚠️ v2 只在 Spark 侧造样本，MySQL 侧那些格子永远填不出来 —— 两侧必须都建。
-- ============================================================

-- @case spark_25pct 25% 样本
SELECT f.grade, COUNT(*) AS loan_cnt,
       ROUND(SUM(f.funded_amnt), 2) AS total_amnt,
       SUM(p.is_bad) AS bad_cnt,
       ROUND(SUM(p.is_bad) / COUNT(*), 6) AS bad_rate,
       ROUND(SUM(f.funded_amnt * f.int_rate) / NULLIF(SUM(f.funded_amnt), 0), 4) AS avg_int_rate
FROM e5_spark_25 f JOIN e5_spark_perf_25 p ON f.loan_id = p.loan_id
WHERE p.is_terminal = 1 GROUP BY f.grade;

-- @case spark_10pct 10% 样本
SELECT f.grade, COUNT(*) AS loan_cnt,
       ROUND(SUM(f.funded_amnt), 2) AS total_amnt,
       SUM(p.is_bad) AS bad_cnt,
       ROUND(SUM(p.is_bad) / COUNT(*), 6) AS bad_rate,
       ROUND(SUM(f.funded_amnt * f.int_rate) / NULLIF(SUM(f.funded_amnt), 0), 4) AS avg_int_rate
FROM e5_spark_10 f JOIN e5_spark_perf_10 p ON f.loan_id = p.loan_id
WHERE p.is_terminal = 1 GROUP BY f.grade;
