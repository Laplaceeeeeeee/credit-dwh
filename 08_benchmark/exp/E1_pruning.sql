-- ============================================================
-- 实验 E1：分区裁剪到底省了多少 IO
--
-- 对照组设计的关键：**同一个谓词，在分区表与非分区表上分别跑** ——
-- 这样才能证明"省 IO 的是分区，而不是那个 WHERE 条件"。
--
-- 执行（容器内）：
--   docker exec -i bd-spark /opt/spark/bin/spark-submit \
--     --master spark://spark:7077 --executor-memory 1500m --total-executor-cores 4 \
--     --conf spark.eventLog.enabled=true \
--     --conf spark.eventLog.dir=file:///workspace/08_benchmark/_spark_events \
--     /workspace/08_benchmark/run_experiment.py \
--     /workspace/08_benchmark/exp/E1_pruning.sql --reps 3
-- ============================================================

-- ---------- 准备：非分区表（同一份数据，只去掉分区）----------
-- @case prep_nopart 建非分区对照表
DROP TABLE IF EXISTS tmp_nopart;
CREATE TABLE tmp_nopart STORED AS ORC AS
SELECT * FROM dwd_loan_fact_lc;

-- ---------- Baseline：分区表全表扫描 ----------
-- @case baseline_full_scan 分区表·全表
SELECT COUNT(*) AS cnt, ROUND(SUM(funded_amnt), 2) AS amt FROM dwd_loan_fact_lc;

-- ---------- Treatment：分区表 + 单分区过滤 ----------
-- @case treatment_one_partition 分区表·单分区
SELECT COUNT(*) AS cnt, ROUND(SUM(funded_amnt), 2) AS amt
FROM dwd_loan_fact_lc WHERE issue_month = '2018-06';

-- ---------- Control：非分区表 + 同样的谓词 ⭐ v3 新增的对照组 ----------
-- @case control_nopart_same_predicate 非分区表·同谓词
SELECT COUNT(*) AS cnt, ROUND(SUM(funded_amnt), 2) AS amt
FROM tmp_nopart WHERE issue_month = '2018-06';

-- ---------- 机制证据：分区裁剪下推 ----------
-- @case explain_partition_pruning 查看下推
EXPLAIN EXTENDED
SELECT COUNT(*) FROM dwd_loan_fact_lc WHERE issue_month = '2018-06';

-- @case explain_cost_estimate 估算扫描量
EXPLAIN COST
SELECT COUNT(*) FROM dwd_loan_fact_lc WHERE issue_month = '2018-06';
