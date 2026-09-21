-- ============================================================
-- 实验 E4 · 第二步：复现倾斜（看到症状）
-- 倾斜的症状**不是"慢"，而是 task 耗时严重不均**。
--
-- ⚠️ 先把 AQE 关掉：不是为了"否则看不到倾斜"（聚合倾斜照样本存在），
--    而是为了**确定性**：分区数固定、不会被自动转成广播连接、三次跑的是一样的计划。
-- ============================================================

-- @case baseline_A_by_status 按状态分组（单一键·严重倾斜）
-- @conf spark.sql.adaptive.enabled=false
-- @conf spark.sql.adaptive.skewJoin.enabled=false
-- @conf spark.sql.shuffle.partitions=20
SELECT p.loan_status,
       COUNT(*)                       AS loan_cnt,
       ROUND(SUM(f.funded_amnt), 2)   AS total_amnt,
       SUM(f.funded_amnt * f.int_rate) AS rate_numerator,
       SUM(p.is_bad)                  AS bad_cnt
FROM dwd_loan_fact_lc f
JOIN dwd_loan_perf_fact_lc p ON f.loan_id = p.loan_id
GROUP BY p.loan_status;

-- @case baseline_B_composite 复合键分组（对照组·倾斜被摊薄）
-- @conf spark.sql.adaptive.enabled=false
-- @conf spark.sql.shuffle.partitions=20
SELECT p.loan_status, f.grade, f.purpose,
       COUNT(*)                       AS loan_cnt,
       ROUND(SUM(f.funded_amnt), 2)   AS total_amnt,
       SUM(f.funded_amnt * f.int_rate) AS rate_numerator,
       SUM(p.is_bad)                  AS bad_cnt
FROM dwd_loan_fact_lc f
JOIN dwd_loan_perf_fact_lc p ON f.loan_id = p.loan_id
GROUP BY p.loan_status, f.grade, f.purpose;
