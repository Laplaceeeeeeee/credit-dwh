-- ============================================================
-- 阶段三 · Hive/Spark 层 DDL（第 13 步）
-- 执行：docker exec -i bd-spark /opt/spark/bin/spark-sql \
--         -f /workspace/07_bigdata/hive/01_ddl.sql
-- ============================================================

CREATE DATABASE IF NOT EXISTS credit_dwh_lc
COMMENT '信贷数仓 - Hive/Spark 大数据层（与 MySQL 层同口径）';

USE credit_dwh_lc;

-- ============================================================
-- ⭐ Hive/Spark 与 MySQL 的 DDL 差异（面试会问，这段话要能顺口说）
--   1. 不设 PRIMARY KEY 约束 —— Hive 不强制主键，唯一性靠 ETL + 校验保证
--   2. 不用 AUTO_INCREMENT 代理键 —— 直接用 loan_id 业务键
--   3. DECIMAL 精度显式声明，且不宜过大（影响列存编码效率）
--   4. PARTITIONED BY 的列**不能**出现在字段列表里（分区是目录结构，不是行数据）
--   5. 用 STORED AS ORC 指定列存格式（E3 会做格式对比）
-- ============================================================

-- ---------- ① 申请时点事实表（按放款月分区）----------
DROP TABLE IF EXISTS dwd_loan_fact_lc;
CREATE TABLE dwd_loan_fact_lc (
  loan_id             STRING        COMMENT '贷款ID',
  loan_amnt           DECIMAL(14,2) COMMENT '申请金额',
  funded_amnt         DECIMAL(14,2) COMMENT '放款金额',
  term_months         INT           COMMENT '期限月数',
  int_rate            DECIMAL(8,4)  COMMENT '利率（已去%）',
  grade               STRING        COMMENT '信用评级',
  sub_grade           STRING        COMMENT '子评级',
  emp_length_years    INT           COMMENT '工作年限',
  home_ownership      STRING        COMMENT '住房情况',
  annual_inc          DECIMAL(16,2) COMMENT '年收入',
  verification_status STRING        COMMENT '收入核验',
  purpose             STRING        COMMENT '借款用途',
  addr_state          STRING        COMMENT '州',
  dti                 DECIMAL(10,4) COMMENT '债务收入比',
  fico_low            DECIMAL(8,2)  COMMENT 'FICO下限',
  fico_high           DECIMAL(8,2)  COMMENT 'FICO上限',
  issue_date          DATE          COMMENT '放款日期'
)
PARTITIONED BY (issue_month STRING COMMENT '放款月 YYYY-MM')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');


-- ---------- ② 表现期事实表（不分区：按贷款粒度，没有自然的时间分区）----------
DROP TABLE IF EXISTS dwd_loan_perf_fact_lc;
CREATE TABLE dwd_loan_perf_fact_lc (
  loan_id         STRING,
  loan_status     STRING,
  is_terminal     INT,
  is_bad          INT,
  dpd_bucket      STRING,
  total_rec_prncp DECIMAL(14,2),
  total_rec_int   DECIMAL(14,2),
  recoveries      DECIMAL(14,2),
  out_prncp       DECIMAL(14,2),
  total_pymnt     DECIMAL(14,2),
  last_pymnt_d    DATE,
  mob_final       INT
) STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY');


-- ---------- ③ 月末快照事实表（按观测月分区）----------
-- ⚠️ 4471 万行，是本次迁移最大的一张表
DROP TABLE IF EXISTS dws_loan_snapshot_lc;
CREATE TABLE dws_loan_snapshot_lc (
  loan_id                 STRING,
  snapshot_date           DATE,
  issue_month             STRING,
  mob                     INT,
  months_since_last_pymnt INT,
  dpd_bucket              STRING,
  is_delinquent           INT,
  is_bad_month            INT,
  out_prncp               DECIMAL(14,2),
  total_rec_prncp         DECIMAL(14,2),
  loan_amnt               DECIMAL(14,2),
  grade                   STRING
)
PARTITIONED BY (snapshot_month STRING COMMENT '观测月末 YYYY-MM')
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');


-- ---------- ④ 申请人画像维 ----------
DROP TABLE IF EXISTS dim_applicant_lc;
CREATE TABLE dim_applicant_lc (
  loan_id          STRING,
  annual_inc       DECIMAL(16,2),
  income_band      STRING,
  home_ownership   STRING,
  emp_length_years INT,
  addr_state       STRING,
  fico_low         DECIMAL(8,2),
  fico_band        STRING,
  dti              DECIMAL(10,4)
) STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY');

SHOW TABLES;
