-- ============================================================
-- 只读验证脚本：在 sql-client 里跑，用来核对实时链路的产出。
-- 用法：
--   docker exec rt-jobmanager /opt/flink/bin/sql-client.sh -f /sql/99_verify.sql
--
-- ⚠️ 用 batch 模式读 Paimon 表（快照读，不等流），脚本能立刻退出。
-- ============================================================

SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'batch';

CREATE CATALOG paimon WITH (
  'type'      = 'paimon',
  'warehouse' = 'file:///warehouse'
);

-- 1. 实时层表清单
SHOW TABLES IN paimon.rt;

-- 2. sink1 总量
SELECT COUNT(*) AS loan_cnt, SUM(funded_amnt) AS funded_sum
FROM paimon.rt.rt_loan_fact_latest;

-- 3. ⭐ 端到端延迟分布（核心指标）
--    latency_ms = sink_ts(Paimon 落库时刻) - op_ts(binlog 变更时刻)
--    含 CDC 抓取 + Kafka + Flink 处理 + Paimon 提交，全程无观测开销。
--    ⚠️ Flink 的 TIMESTAMPDIFF 不接受 MILLISECOND（只接受
--       SECOND/MECROSECOND 一档），实测报 ParseException —— 因此用微秒再除 1000。
SELECT
  COUNT(*)                                                      AS n,
  CAST(MIN(TIMESTAMPDIFF(MICROSECOND, op_ts, sink_ts)) / 1000 AS BIGINT) AS min_ms,
  CAST(AVG(TIMESTAMPDIFF(MICROSECOND, op_ts, sink_ts)) / 1000 AS BIGINT) AS avg_ms,
  CAST(MAX(TIMESTAMPDIFF(MICROSECOND, op_ts, sink_ts)) / 1000 AS BIGINT) AS max_ms
FROM paimon.rt.rt_loan_fact_latest;

-- 4. 最近落库的明细（看 op_ts 与 sink_ts 的先后关系）
SELECT loan_id, funded_amnt, grade, issue_month, op_ts, sink_ts,
       CAST(TIMESTAMPDIFF(MICROSECOND, op_ts, sink_ts) / 1000 AS BIGINT) AS latency_ms
FROM paimon.rt.rt_loan_fact_latest
ORDER BY sink_ts DESC
LIMIT 10;

-- 5. sink2：1 分钟窗口聚合（事件时间 + Watermark 的产出）
SELECT window_start, window_end, write_cnt, funded_sum
FROM paimon.rt.rt_loan_change_1min
ORDER BY window_start DESC
LIMIT 10;
