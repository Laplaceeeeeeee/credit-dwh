-- ============================================================
-- 实验 E4 · 第一步：量化倾斜（不要急着"优化"）
--
-- ⭐ 关键认知：本项目的倾斜是天然存在的，不需要造数据。
-- 执行：
--   docker exec -i bd-spark /opt/spark/bin/spark-submit \
--     --master spark://spark:7077 --executor-memory 1500m --total-executor-cores 4 \
--     --conf spark.eventLog.enabled=true \
--     --conf spark.eventLog.dir=file:///workspace/08_benchmark/_spark_events \
--     /workspace/08_benchmark/run_experiment.py \
--     /workspace/08_benchmark/exp/E4_skew_quant.sql --reps 1
-- ============================================================

-- ---------- ① 各 key 占比与倾斜比 ----------
-- 🚨 v2 指导书在这里有一个会让结论失真的写法：
--    它把 COUNT(DISTINCT loan_status) 写进了**同一个 GROUP BY loan_status 的查询**里，
--    而那个表达式在该作用域下**恒等于 1** → 算出的"倾斜比"其实是占比本身（0.48），
--    会让人误判"这里没有倾斜"。正确做法是把 key 总数从另一个作用域取出来（下面用 CTE+CROSS JOIN）。
-- @case quant_key_distribution 各key占比与倾斜比
WITH agg AS (
  SELECT loan_status, COUNT(*) AS cnt FROM dwd_loan_perf_fact_lc GROUP BY loan_status
), tot AS (
  SELECT COUNT(*) AS n_keys, SUM(cnt) AS total FROM agg
)
SELECT a.loan_status,
       a.cnt,
       ROUND(a.cnt / t.total, 4)                 AS pct,
       ROUND((a.cnt / t.total) * t.n_keys, 2)    AS skew_ratio
FROM agg a CROSS JOIN tot t
ORDER BY a.cnt DESC;

-- ---------- ② 哈希后落到哪个 shuffle 分区（决定 task 有多不均）----------
-- @case quant_hash_partition_distribution 哈希分区分布
SELECT pmod(hash(loan_status), 20) AS shuffle_part, COUNT(*) AS cnt
FROM dwd_loan_perf_fact_lc
GROUP BY pmod(hash(loan_status), 20)
ORDER BY cnt DESC;

-- ---------- ③ ⭐ 粒度决定倾斜严重程度：分组键越细，倾斜越被摊薄 ----------
-- @case quant_granularity_comparison 键粒度对照
SELECT 'loan_status' AS key_granularity, MAX(pct) AS max_key_pct, COUNT(*) AS n_keys
FROM (SELECT loan_status, COUNT(*)/SUM(COUNT(*)) OVER () AS pct
      FROM dwd_loan_perf_fact_lc GROUP BY loan_status) t
UNION ALL
SELECT 'loan_status+grade', MAX(pct), COUNT(*)
FROM (SELECT loan_status, grade, COUNT(*)/SUM(COUNT(*)) OVER () AS pct
      FROM dwd_loan_perf_fact_lc l JOIN dwd_loan_fact_lc f ON l.loan_id = f.loan_id
      GROUP BY loan_status, grade) t
UNION ALL
SELECT 'status+grade+purpose', MAX(pct), COUNT(*)
FROM (SELECT loan_status, grade, purpose, COUNT(*)/SUM(COUNT(*)) OVER () AS pct
      FROM dwd_loan_perf_fact_lc l JOIN dwd_loan_fact_lc f ON l.loan_id = f.loan_id
      GROUP BY loan_status, grade, purpose) t;
