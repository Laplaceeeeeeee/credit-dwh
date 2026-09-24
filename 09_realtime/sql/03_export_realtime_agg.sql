-- ============================================================
-- M3 对账导出：把**实时指标表**（Paimon 主键表）导出成 CSV，
-- 供 check_batch_stream.py 与离线侧（MySQL dwd_loan_fact）逐月比对。
--
-- 为什么要导出而不是让 Python 直接读 Paimon：
--   Python 侧没有 Paimon 读取器（PK 表的 LSM 语义不能靠直接读 parquet 复现）。
--   所以用 Flink 自己的 batch 读 + filesystem sink 落 CSV，
--   再 docker exec cat 出来 —— 这也让"读取"用的是**引擎的正确语义**。
--
-- 产物：/results/rt_loan_month_agg/part-*（容器内），由脚本读出。
-- ============================================================

SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'batch';
SET 'parallelism.default' = '1';

CREATE CATALOG paimon WITH (
  'type'      = 'paimon',
  'warehouse' = 'file:///warehouse'
);

-- ⚠️ funded_sum 转成 STRING 再落 CSV：避免 DECIMAL 在文本化时出现
--    "1E+3" 这类科学计数（实测 debezium-json 就出现过），
--    让两侧的比对是**字符串级精确**而不是靠 float 近似。
CREATE TABLE rt_month_agg_csv (
  issue_month STRING,
  loan_cnt    BIGINT,
  funded_sum  STRING
) WITH (
  'connector' = 'filesystem',
  'path'      = 'file:///results/rt_loan_month_agg',
  'format'    = 'csv'
);

INSERT INTO rt_month_agg_csv
SELECT
  issue_month,
  loan_cnt,
  CAST(funded_sum AS STRING)
FROM paimon.rt.rt_loan_month_agg;
