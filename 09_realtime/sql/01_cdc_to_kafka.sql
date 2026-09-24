-- ============================================================
-- M1a · MySQL binlog 变更流 -> Kafka
--
-- 为什么要有这一跳（而不是 CDC 直连 Paimon）：
--   变更流先进消息队列，才能解耦出"多个消费方"的形态（实时指标、
--   对账、告警各自订阅同一份流）；这也是实时岗筛简历时的标准关键词。
--   取舍与理由见 09_realtime/技术决策.md 的 D1。
--
-- ⭐ 关键设计：Kafka 里存的是**带 op 的变更日志**（debezium-json），
--   不是"最新状态"。
--   试过两条错路（都留下了报错记录）：
--     ① 源表不声明主键 -> 报 chunk.key-column is required（增量快照需要主键）
--     ② 去掉主键后 -> planner 报 "sink doesn't support consuming update and
--        delete changes"：CDC 源**始终**产出 update/delete 变更，
--        普通 append-only 的 json sink 接不住。
--   正解是让 sink 用 changelog 格式。`debezium-json` 由 Flink 自带的
--   flink-json-1.20.5.jar 提供（已实测 /opt/flink/lib 里有
--   DebeziumJsonFormatFactory），**不需要额外的 connector jar**。
--
-- ⭐ op_ts 在 sink 表里是**物理列**（不是 VIRTUAL）：debezium-json 的
--   metadata 不会把 source timestamp 带过去，事件时间必须靠 after.op_ts
--   逐条携带，下游才能重建 Watermark。
--
-- ⚠️ scan.startup.mode = latest-offset：dwd_loan_fact 有 226 万行，
--   M1 只验证链路与延迟，不做全量快照（那是 M3 批流对账的事）。
-- ============================================================

SET 'execution.runtime-mode' = 'streaming';
SET 'parallelism.default' = '1';
SET 'execution.checkpointing.interval' = '10s';
SET 'pipeline.name' = 'M1a-cdc-mysql-to-kafka';

-- ---------- 源：MySQL CDC ----------
CREATE TABLE src_loan_fact (
  loan_id       STRING,
  funded_amnt   DECIMAL(14,2),
  grade         STRING,
  issue_month   STRING,
  etl_load_time TIMESTAMP(0),
  -- ⭐ binlog 里这条变更**发生的时间**，不是 Flink 处理它的时间
  op_ts         TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
  PRIMARY KEY (loan_id) NOT ENFORCED
) WITH (
  'connector'            = 'mysql-cdc',
  'hostname'             = 'credit-dwh-mysql',
  'port'                 = '3306',
  -- ⭐ 用最小权限复制账号，不用 root：
  --    root 是 caching_sha2_password，非 SSL 下需要 RSA 公钥交换，
  --    驱动默认禁止（allowPublicKeyRetrieval=false），报
  --      "Public Key Retrieval is not allowed"
  --    cdc 账号是 mysql_native_password，不需要公钥交换，问题从根上消失。
  --    建号脚本：09_realtime/init/01_create_cdc_user.sql
  -- ⚠️ 不要试图用 'jdbc.properties.allowPublicKeyRetrieval' 绕 ——
  --    报错来自 MySqlValidator，它走 dbzProperties（Debezium 属性），
  --    不是 jdbc.properties 那张表，实测无效。
  'username'             = 'cdc',
  'password'             = 'cdc_pwd_2026',
  'database-name'        = 'credit_dwh',
  'table-name'           = 'dwd_loan_fact',
  'scan.startup.mode'    = 'latest-offset',
  'scan.incremental.snapshot.enabled' = 'false',
  'server-time-zone'     = 'Asia/Shanghai'
);

-- ---------- 汇：Kafka（debezium-json 变更日志）----------
CREATE TABLE sink_loan_fact_kafka (
  loan_id       STRING,
  funded_amnt   DECIMAL(14,2),
  grade         STRING,
  issue_month   STRING,
  etl_load_time TIMESTAMP(0),
  op_ts         TIMESTAMP_LTZ(3)
) WITH (
  'connector'                             = 'kafka',
  'topic'                                 = 'credit_dwd_loan_fact',
  'properties.bootstrap.servers'          = 'kafka:9092',
  'format'                                = 'debezium-json',
  'debezium-json.timestamp-format.standard' = 'ISO-8601',
  'sink.partitioner'                      = 'fixed'
);

INSERT INTO sink_loan_fact_kafka
SELECT loan_id, funded_amnt, grade, issue_month, etl_load_time, op_ts
FROM src_loan_fact;
