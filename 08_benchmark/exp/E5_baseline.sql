-- ============================================================
-- 实验 E5：三引擎基准（Spark 组）
-- 语义必须与 08_benchmark/bench_mysql.py 完全一致：
--   按 grade 分层的 **终态** 不良率 + 金额 + 加权平均利率
--
-- 执行（容器内）：
--   docker exec -i bd-spark /opt/spark/bin/spark-submit \
--     --master spark://spark:7077 --executor-memory 1500m --total-executor-cores 4 \
--     --conf spark.eventLog.enabled=true \
--     --conf spark.eventLog.dir=file:///workspace/08_benchmark/_spark_events \
--     /workspace/08_benchmark/run_experiment.py \
--     /workspace/08_benchmark/exp/E5_baseline.sql --reps 3
-- ============================================================

-- @case spark_unoptimized 未优化（AQE 关）
-- @conf spark.sql.adaptive.enabled=false
-- @conf spark.sql.adaptive.skewJoin.enabled=false
-- @conf spark.sql.shuffle.partitions=20
SELECT f.grade, COUNT(*) AS loan_cnt,
       ROUND(SUM(f.funded_amnt), 2) AS total_amnt,
       SUM(p.is_bad) AS bad_cnt,
       ROUND(SUM(p.is_bad) / COUNT(*), 6) AS bad_rate,
       ROUND(SUM(f.funded_amnt * f.int_rate) / NULLIF(SUM(f.funded_amnt), 0), 4) AS avg_int_rate
FROM dwd_loan_fact_lc f
JOIN dwd_loan_perf_fact_lc p ON f.loan_id = p.loan_id
WHERE p.is_terminal = 1
GROUP BY f.grade;

-- @case spark_optimized 优化后（AQE 开）
-- @conf spark.sql.adaptive.enabled=true
-- @conf spark.sql.adaptive.skewJoin.enabled=true
-- @conf spark.sql.shuffle.partitions=20
SELECT f.grade, COUNT(*) AS loan_cnt,
       ROUND(SUM(f.funded_amnt), 2) AS total_amnt,
       SUM(p.is_bad) AS bad_cnt,
       ROUND(SUM(p.is_bad) / COUNT(*), 6) AS bad_rate,
       ROUND(SUM(f.funded_amnt * f.int_rate) / NULLIF(SUM(f.funded_amnt), 0), 4) AS avg_int_rate
FROM dwd_loan_fact_lc f
JOIN dwd_loan_perf_fact_lc p ON f.loan_id = p.loan_id
WHERE p.is_terminal = 1
GROUP BY f.grade;

-- @case spark_optimized_rebalance 优化后 + REBALANCE 提示（E4 实测最快的聚合方案）
-- @conf spark.sql.adaptive.enabled=true
-- @conf spark.sql.shuffle.partitions=20
SELECT /*+ REBALANCE(grade) */
       f.grade, COUNT(*) AS loan_cnt,
       ROUND(SUM(f.funded_amnt), 2) AS total_amnt,
       SUM(p.is_bad) AS bad_cnt,
       ROUND(SUM(p.is_bad) / COUNT(*), 6) AS bad_rate,
       ROUND(SUM(f.funded_amnt * f.int_rate) / NULLIF(SUM(f.funded_amnt), 0), 4) AS avg_int_rate
FROM dwd_loan_fact_lc f
JOIN dwd_loan_perf_fact_lc p ON f.loan_id = p.loan_id
WHERE p.is_terminal = 1
GROUP BY f.grade;

-- ---------- 数据量阶梯：两侧用同一个确定性谓词（loan_id 取模）----------
-- ⚠️ v2 只在 Spark 侧造样本，MySQL 侧那些格子永远填不出来 —— 这里两侧都建。
-- @case ladder_build_25pct 建样本 25%
DROP TABLE IF EXISTS e5_spark_25;
CREATE TABLE e5_spark_25 STORED AS ORC AS
SELECT * FROM dwd_loan_fact_lc WHERE CAST(loan_id AS BIGINT) % 4 = 0;

-- @case ladder_rows_25pct 样本行数（要与 MySQL 侧相等）
SELECT COUNT(*) AS spark_25 FROM e5_spark_25;
