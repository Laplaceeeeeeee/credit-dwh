-- ============================================================
-- 开发用重置脚本：删掉实时层的两张 Paimon 表。
--
-- 何时用：改了 sink 表结构（例如后来给 rt_loan_fact_latest 加了
--   sink_ts 列做延迟测量），而 CREATE TABLE IF NOT EXISTS 不会改已存在的表。
-- ⚠️ 这是**破坏性**操作，只在开发/演练时用，不要放进流水线。
-- 用法（先取消 M1b 作业，再跑本脚本）：
--   docker exec rt-jobmanager /opt/flink/bin/sql-client.sh -f /sql/98_reset_rt_tables.sql
-- ============================================================

CREATE CATALOG paimon WITH (
  'type'      = 'paimon',
  'warehouse' = 'file:///warehouse'
);

DROP TABLE IF EXISTS paimon.rt.rt_loan_fact_latest;
DROP TABLE IF EXISTS paimon.rt.rt_loan_change_1min;

SHOW TABLES IN paimon.rt;
