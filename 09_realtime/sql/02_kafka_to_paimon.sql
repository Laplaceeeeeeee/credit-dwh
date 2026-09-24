-- ============================================================
-- M1b · Kafka -> Flink -> Paimon
--
-- 一个作业装两个 sink，分别回答两个问题：
--   sink 1  rt_loan_fact_latest  —— Paimon 主键表镜像最新状态。
--           **不经过窗口缓冲**，用来测「MySQL 改一行 -> Paimon 可见」
--           的真实端到端延迟（M1 的核心指标）。
--   sink 2  rt_loan_change_1min  —— 1 分钟滚动窗口聚合，
--           用**事件时间 + Watermark**，证明时间语义是真的（D3），
--           而不是用处理时间凑数。
--
-- ⚠️ 事件时间用 after.op_ts（binlog 变更发生时间），允许乱序 5 秒。
--    迟到超过 Watermark 的记录会被丢弃 —— 这是**故意暴露**的行为，
--    M2 会用乱序数据专门验证它与 allowed-lateness 的边界。
--
-- ⚠️ 窗口指标的语义（想清楚再写，这里是 changelog，不是 append 流）：
--    输入是 changelog：INSERT 是 +I，UPDATE 是 -U/+U，DELETE 是 -D。
--    Flink 的窗口聚合作用在"动态表"上，所以窗口的内容 =
--      **该时间窗内发生过变更的那些贷款（当前状态）**
--    UPDATE 会把该行**从旧窗口移到新窗口**（因为 op_ts 变了）。
--    于是 COUNT(*) = 本窗口内有过写入的贷款笔数，SUM = 这些贷款的放款金额合计。
--    ⭐ 这比"数原始事件条数"更严谨：后者会被 -U/+U 相互抵消而失去意义。
--
--    ⚠️ 曾有两条错路（留档避免重走）：
--      ① 想在 Kafka 源上用 `op` 元数据列过滤 → 报 Invalid metadata key 'op'。
--         Kafka 源只暴露 value.* 前缀的格式元数据（value.schema /
--         value.ingestion-timestamp / value.source.* 等），**debezium-json
--         的 op 不在其中**（已从报错里的权威清单确认）。
--      ② 想用 `value.source.timestamp` 当事件时间 → 需要 Job A 的 sink
--         声明 source 元数据列才会写出该字段；本项目直接让 op_ts 作为
--         物理列随 after 携带，更直接可靠。
-- ============================================================

SET 'execution.runtime-mode' = 'streaming';
SET 'parallelism.default' = '1';
SET 'execution.checkpointing.interval' = '10s';
-- ⚠️ Paimon sink 必需：不关掉 sink materializer 会报
--    "Sink materializer must not be used with Paimon sink"（实测踩到）。
--    这里显式写一遍，让本 SQL 换到任何集群都能直接跑（不依赖 flink-conf）。
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'pipeline.name' = 'M1b-kafka-to-paimon';

-- ---------- 源：Kafka（debezium-json 读取，得到 changelog）----------
CREATE TABLE default_catalog.default_database.src_loan_fact_kafka (
  loan_id       STRING,
  funded_amnt   DECIMAL(14,2),
  grade         STRING,
  issue_month   STRING,
  etl_load_time TIMESTAMP(0),
  op_ts         TIMESTAMP_LTZ(3),
  -- ⭐ 事件时间属性 + Watermark：容忍 5 秒乱序
  WATERMARK FOR op_ts AS op_ts - INTERVAL '5' SECOND
) WITH (
  'connector'                               = 'kafka',
  'topic'                                   = 'credit_dwd_loan_fact',
  'properties.bootstrap.servers'            = 'kafka:9092',
  'properties.group.id'                     = 'rt-loan-m1',
  'scan.startup.mode'                       = 'earliest-offset',
  'format'                                  = 'debezium-json',
  'debezium-json.timestamp-format.standard' = 'ISO-8601',
  'debezium-json.schema-include'            = 'false'
);

-- ---------- Paimon 目录 ----------
CREATE CATALOG paimon WITH (
  'type'      = 'paimon',
  -- 本地文件系统：D4 决策（不引入 MinIO），JM/TM 共享同一个宿主机目录
  'warehouse' = 'file:///warehouse'
);
CREATE DATABASE IF NOT EXISTS paimon.rt;

-- ---------- sink 1：主键表，镜像最新状态 ----------
-- ⭐ sink_ts 是**为延迟测量刻意加的列**：
--    如果靠外部轮询去测"什么时候能在 Paimon 里查到"，测到的其实是
--    "sql-client 启动时间 + 查询时间"，把链路延迟完全淹没了。
--    正确做法是在链路内部打时间戳，让延迟由表自己算：
--        latency_ms = sink_ts - op_ts
--                    = (Flink 落库时刻) - (binlog 变更时刻)
--    这是**端到端**的：含 CDC 抓取、Kafka、Flink 处理、Paimon 提交。
CREATE TABLE IF NOT EXISTS paimon.rt.rt_loan_fact_latest (
  loan_id       STRING,
  funded_amnt   DECIMAL(14,2),
  grade         STRING,
  issue_month   STRING,
  etl_load_time TIMESTAMP(0),
  op_ts         TIMESTAMP_LTZ(3),
  sink_ts       TIMESTAMP_LTZ(3),
  PRIMARY KEY (loan_id) NOT ENFORCED
) WITH (
  'bucket'             = '1',
  -- 主键去重 = 收敛到最新状态（这正是 Paimon 主键表相对纯文件表的核心价值）
  'merge-engine'       = 'deduplicate',
  -- 生成 changelog，供下游流读
  'changelog-producer' = 'input'
);

-- ---------- sink 2：1 分钟滚动窗口聚合 ----------
CREATE TABLE IF NOT EXISTS paimon.rt.rt_loan_change_1min (
  window_start TIMESTAMP(3),
  window_end   TIMESTAMP(3),
  write_cnt    BIGINT,
  funded_sum   DECIMAL(18,2),
  PRIMARY KEY (window_start, window_end) NOT ENFORCED
) WITH (
  'bucket'             = '1',
  'merge-engine'       = 'deduplicate',
  'changelog-producer' = 'input'
);

-- ---------- 两个 sink 放在同一个作业里 ----------
-- ⭐ 用 EXECUTE STATEMENT SET 而不是两条独立 INSERT：
--    · 两条独立 INSERT 会被 sql-client 提交成**两个作业**，各占一个 slot，
--      并且各自重复读一遍 Kafka（多一份源读取与状态）；
--    · STATEMENT SET 提交为**一个作业**，两个 sink 共享同一个源，
--      省 slot、省重复消费、语义上更干净。
--    实测教训：2 个 slot 的集群如果按两个作业提交，
--    加上 M1a 就会撞 NoResourceAvailableException。
EXECUTE STATEMENT SET
BEGIN

  -- sink 1：变更日志 -> 最新状态镜像
  -- CURRENT_TIMESTAMP 在流模式下**逐条求值**，得到该记录被处理落库的时刻
  INSERT INTO paimon.rt.rt_loan_fact_latest
  SELECT loan_id, funded_amnt, grade, issue_month, etl_load_time, op_ts, CURRENT_TIMESTAMP
  FROM default_catalog.default_database.src_loan_fact_kafka;

  -- sink 2：事件时间窗口聚合
  INSERT INTO paimon.rt.rt_loan_change_1min
  SELECT
    window_start,
    window_end,
    COUNT(*)         AS write_cnt,
    SUM(funded_amnt) AS funded_sum
  FROM TABLE(
    TUMBLE(TABLE default_catalog.default_database.src_loan_fact_kafka,
           DESCRIPTOR(op_ts), INTERVAL '1' MINUTE)
  )
  GROUP BY window_start, window_end;

END;
