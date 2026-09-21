-- ============================================================
-- 实验 E4 · 第五步：AQE 与手动加盐的边界（必须做）
-- 否则面试会问："Spark 3.x 的 AQE 不是能自动处理倾斜吗？为什么还要手动加盐？"
-- ============================================================

-- @case aqe_on_groupby 聚合倾斜·AQE 开（skewJoin 管不了聚合）
-- @conf spark.sql.adaptive.enabled=true
-- @conf spark.sql.adaptive.skewJoin.enabled=true
-- @conf spark.sql.shuffle.partitions=20
SELECT p.loan_status, COUNT(*) AS loan_cnt,
       ROUND(SUM(f.funded_amnt), 2) AS total_amnt,
       SUM(f.funded_amnt * f.int_rate) AS rate_numerator, SUM(p.is_bad) AS bad_cnt
FROM dwd_loan_fact_lc f JOIN dwd_loan_perf_fact_lc p ON f.loan_id = p.loan_id
GROUP BY p.loan_status;

-- @case rebalance_hint 聚合倾斜·REBALANCE 提示（引擎原生"自动加盐"）
-- @conf spark.sql.adaptive.enabled=true
-- @conf spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled=true
-- @conf spark.sql.shuffle.partitions=20
SELECT /*+ REBALANCE(loan_status) */
       p.loan_status, COUNT(*) AS loan_cnt,
       ROUND(SUM(f.funded_amnt), 2) AS total_amnt,
       SUM(f.funded_amnt * f.int_rate) AS rate_numerator, SUM(p.is_bad) AS bad_cnt
FROM dwd_loan_fact_lc f JOIN dwd_loan_perf_fact_lc p ON f.loan_id = p.loan_id
GROUP BY p.loan_status;

-- @case join_skew_aqe_on_default 强制 shuffle join·AQE 默认阈值
-- @conf spark.sql.adaptive.enabled=true
-- @conf spark.sql.adaptive.skewJoin.enabled=true
-- @conf spark.sql.autoBroadcastJoinThreshold=-1
-- @conf spark.sql.adaptive.autoBroadcastJoinThreshold=-1
-- @conf spark.sql.shuffle.partitions=20
SELECT l.grade, COUNT(*) AS cnt, ROUND(SUM(l.funded_amnt), 2) AS amt
FROM dwd_loan_fact_lc l JOIN e4_grade_dim r ON l.grade = r.grade
GROUP BY l.grade;

-- @case join_skew_aqe_on_low_threshold 调低阈值让它真的触发
-- @conf spark.sql.adaptive.enabled=true
-- @conf spark.sql.adaptive.skewJoin.enabled=true
-- @conf spark.sql.autoBroadcastJoinThreshold=-1
-- @conf spark.sql.adaptive.autoBroadcastJoinThreshold=-1
-- @conf spark.sql.adaptive.skewJoin.skewedPartitionFactor=2
-- @conf spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=8MB
-- @conf spark.sql.shuffle.partitions=20
SELECT l.grade, COUNT(*) AS cnt, ROUND(SUM(l.funded_amnt), 2) AS amt
FROM dwd_loan_fact_lc l JOIN e4_grade_dim r ON l.grade = r.grade
GROUP BY l.grade;
