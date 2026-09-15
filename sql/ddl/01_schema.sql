-- ============================================================
-- 信贷数仓 DDL（阶段二）
-- MySQL 8.0 / utf8mb4
--
-- ⚠️ 命名规范（踩过的坑）：
--    所有列名一律用反引号包裹。MySQL 8 对保留字很严格，例如
--    `year_month` 属保留字（YEAR_MONTH 是 interval 单位），不加反引号直接报：
--      ERROR 1064 (42000): ... near 'year_month CHAR(7)'
--    统一加反引号可从根上免疫这类问题。
--
-- ⚠️ 数据事实（实测，见 data/clean/clean_meta.json）：
--    物理 2,260,702 行 = 表头 1 + 页脚 33 + 真实数据 2,260,668
--    issue_d 范围 Apr-2008 ~ Sep-2018；137 列（14 列整列为空已剔除）
--    终态不良率 19.9807%
-- ============================================================
CREATE DATABASE IF NOT EXISTS credit_dwh
  DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
USE credit_dwh;

-- ============================================================
-- ODS 层：原样落地（保留分析用关键列）
-- ============================================================
DROP TABLE IF EXISTS ods_loan_raw;
CREATE TABLE ods_loan_raw (
  `id`                   VARCHAR(32)   NOT NULL COMMENT '贷款ID(业务主键,纯数字)',
  `loan_amnt`            DECIMAL(14,2) COMMENT '申请金额',
  `funded_amnt`          DECIMAL(14,2) COMMENT '放款金额',
  `term`                 VARCHAR(24)   COMMENT '期限原文',
  `int_rate`             DECIMAL(8,4)  COMMENT '利率(原始已去%)',
  `installment`          DECIMAL(14,2) COMMENT '月供',
  `grade`                VARCHAR(4)    COMMENT '信用评级A-G',
  `sub_grade`            VARCHAR(8)    COMMENT '子评级',
  `emp_length`           VARCHAR(32)   COMMENT '工作年限原文',
  `home_ownership`       VARCHAR(32)   COMMENT '住房情况',
  `annual_inc`           DECIMAL(16,2) COMMENT '年收入',
  `verification_status`  VARCHAR(48)   COMMENT '收入核验状态',
  `issue_d`              VARCHAR(16)   COMMENT '放款月原文',
  `loan_status`          VARCHAR(80)   COMMENT '贷款状态(表现期!)',
  `purpose`              VARCHAR(64)   COMMENT '借款用途',
  `title`                VARCHAR(128)  COMMENT '借款标题',
  `addr_state`           VARCHAR(8)    COMMENT '州',
  `zip_code`             VARCHAR(16)   COMMENT '邮编前3位',
  `dti`                  DECIMAL(10,4) COMMENT '债务收入比',
  `delinq_2yrs`          DECIMAL(8,2)  COMMENT '近2年逾期次数',
  `earliest_cr_line`     VARCHAR(16)   COMMENT '最早信用记录月',
  `fico_range_low`       DECIMAL(8,2)  COMMENT 'FICO下限',
  `fico_range_high`      DECIMAL(8,2)  COMMENT 'FICO上限',
  `inq_last_6mths`       DECIMAL(8,2)  COMMENT '近6月查询次数',
  `open_acc`             DECIMAL(8,2)  COMMENT '开放账户数',
  `pub_rec`              DECIMAL(8,2)  COMMENT '负面公开记录数',
  `revol_bal`            DECIMAL(16,2) COMMENT '循环余额',
  `revol_util`           DECIMAL(10,4) COMMENT '循环额度使用率',
  `total_acc`            DECIMAL(8,2)  COMMENT '总账户数',
  `application_type`     VARCHAR(32)   COMMENT '申请类型',
  `mort_acc`             DECIMAL(8,2)  COMMENT '房贷账户数',
  `pub_rec_bankruptcies` DECIMAL(8,2)  COMMENT '破产记录数',
  `total_rec_prncp`      DECIMAL(14,2) COMMENT '累计已收本金(表现期)',
  `total_rec_int`        DECIMAL(14,2) COMMENT '累计已收利息(表现期)',
  `recoveries`           DECIMAL(14,2) COMMENT '回收金额(表现期)',
  `out_prncp`            DECIMAL(14,2) COMMENT '未偿本金(表现期)',
  `total_pymnt`          DECIMAL(14,2) COMMENT '累计还款额(表现期)',
  `last_pymnt_d`         VARCHAR(16)   COMMENT '最后还款月(表现期)',
  `last_pymnt_amnt`      DECIMAL(14,2) COMMENT '最后还款额(表现期)',
  `etl_load_time`        DATETIME      COMMENT '装载时间',
  PRIMARY KEY (`id`),
  KEY `idx_issue_d` (`issue_d`),
  KEY `idx_loan_status` (`loan_status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='ODS-原始放款流水(关键列)';


-- ============================================================
-- DWD 层：申请时点事实表
-- ⭐ 严禁出现任何表现期字段 —— 由 dq_check Q8 自动校验
-- ============================================================
DROP TABLE IF EXISTS dwd_loan_fact;
CREATE TABLE dwd_loan_fact (
  `loan_id`             VARCHAR(32)   NOT NULL COMMENT '贷款ID',
  `loan_amnt`           DECIMAL(14,2) COMMENT '申请金额',
  `funded_amnt`         DECIMAL(14,2) COMMENT '放款金额',
  `term_months`         INT           COMMENT '期限月数',
  `int_rate`            DECIMAL(8,4)  COMMENT '利率(已去%)',
  `installment`         DECIMAL(14,2) COMMENT '月供',
  `grade`               VARCHAR(4)    COMMENT '信用评级',
  `sub_grade`           VARCHAR(8)    COMMENT '子评级',
  `emp_length_years`    INT           COMMENT '工作年限(10+记10)',
  `home_ownership`      VARCHAR(32)   COMMENT '住房情况',
  `annual_inc`          DECIMAL(16,2) COMMENT '年收入',
  `verification_status` VARCHAR(48)   COMMENT '收入核验状态',
  `purpose`             VARCHAR(64)   COMMENT '借款用途(申请时点信息!)',
  `addr_state`          VARCHAR(8)    COMMENT '州',
  `dti`                 DECIMAL(10,4) COMMENT '债务收入比',
  `fico_low`            DECIMAL(8,2)  COMMENT 'FICO下限',
  `fico_high`           DECIMAL(8,2)  COMMENT 'FICO上限',
  `issue_date`          DATE          COMMENT '放款日期(当月1日)',
  `issue_month`         CHAR(7)       COMMENT '放款月 YYYY-MM',
  `vintage`             CHAR(7)       COMMENT '放款批次',
  `dq_level`            VARCHAR(16)   COMMENT '数据质量等级',
  `etl_load_time`       DATETIME      COMMENT '装载时间',
  PRIMARY KEY (`loan_id`),
  KEY `idx_issue_month` (`issue_month`),
  KEY `idx_grade` (`grade`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='DWD-贷款申请时点事实表(无表现期字段)';


-- ============================================================
-- DWD 层：表现期事实表（与上表物理隔离）
-- ============================================================
DROP TABLE IF EXISTS dwd_loan_perf_fact;
CREATE TABLE dwd_loan_perf_fact (
  `loan_id`         VARCHAR(32)   NOT NULL COMMENT '贷款ID',
  `loan_status`     VARCHAR(80)   COMMENT '最终状态',
  `is_terminal`     TINYINT(1)    COMMENT '是否终态',
  `is_bad`          TINYINT(1)    COMMENT '是否不良',
  `dpd_bucket`      VARCHAR(16)   COMMENT 'DPD档位',
  `total_rec_prncp` DECIMAL(14,2) COMMENT '累计已收本金',
  `total_rec_int`   DECIMAL(14,2) COMMENT '累计已收利息',
  `recoveries`      DECIMAL(14,2) COMMENT '回收金额',
  `out_prncp`       DECIMAL(14,2) COMMENT '未偿本金',
  `total_pymnt`     DECIMAL(14,2) COMMENT '累计还款总额',
  `last_pymnt_d`    DATE          COMMENT '最后还款日',
  `last_pymnt_amnt` DECIMAL(14,2) COMMENT '最后还款额',
  `mob_final`       INT           COMMENT '最终账龄(月)',
  `etl_load_time`   DATETIME      COMMENT '装载时间',
  PRIMARY KEY (`loan_id`),
  KEY `idx_is_bad` (`is_bad`),
  KEY `idx_terminal` (`is_terminal`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='DWD-贷款表现期事实表(仅表现期字段)';


-- ============================================================
-- 维度表
-- ⭐ 无客户维：member_id 整列为空（实测），不虚构主键
-- ============================================================
DROP TABLE IF EXISTS dim_applicant;
CREATE TABLE dim_applicant (
  `loan_id`          VARCHAR(32)   NOT NULL COMMENT '贷款ID(粒度=一笔贷款)',
  `annual_inc`       DECIMAL(16,2) COMMENT '年收入',
  `income_band`      VARCHAR(16)   COMMENT '收入档位',
  `home_ownership`   VARCHAR(32)   COMMENT '住房情况',
  `emp_length_years` INT           COMMENT '工作年限',
  `addr_state`       VARCHAR(8)    COMMENT '州',
  `fico_low`         DECIMAL(8,2)  COMMENT 'FICO下限',
  `fico_band`        VARCHAR(16)   COMMENT 'FICO档位',
  `dti`              DECIMAL(10,4) COMMENT '债务收入比',
  PRIMARY KEY (`loan_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='申请人画像维(member_id整列缺失,不建客户维)';


DROP TABLE IF EXISTS dim_product;
CREATE TABLE dim_product (
  `product_code` VARCHAR(32)  NOT NULL COMMENT '产品编码=期限_评级',
  `term_months`  INT          COMMENT '期限月数',
  `grade`        VARCHAR(4)   COMMENT '评级',
  `avg_int_rate` DECIMAL(8,4) COMMENT '平均利率',
  `rate_band`    VARCHAR(16)  COMMENT '利率档位',
  PRIMARY KEY (`product_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='产品维';


DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date (
  `date_key`     INT         NOT NULL COMMENT 'YYYYMMDD',
  `full_date`    DATE        NOT NULL,
  `year_num`     INT,
  `quarter_num`  INT,
  `month_num`    INT,
  `month_name`   CHAR(3),
  `year_month`   CHAR(7)     COMMENT 'YYYY-MM(保留字,必须反引号)',
  `week_of_year` INT,
  `day_of_week`  INT,
  `is_weekend`   TINYINT(1),
  PRIMARY KEY (`date_key`),
  KEY `idx_year_month` (`year_month`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='日期维';


-- ============================================================
-- DWS 层：月末贷款快照事实表（周期快照）
-- ============================================================
DROP TABLE IF EXISTS dws_loan_snapshot_m;
CREATE TABLE dws_loan_snapshot_m (
  `loan_id`                 VARCHAR(32) NOT NULL,
  `snapshot_month`          CHAR(7)     NOT NULL COMMENT '观测月末',
  `snapshot_date`           DATE        COMMENT '观测月末日期',
  `issue_month`             CHAR(7)     COMMENT '放款月',
  `mob`                     INT         COMMENT '账龄',
  `months_since_last_pymnt` INT         COMMENT '距最后还款月数(推导)',
  `dpd_bucket`              VARCHAR(16) COMMENT '该月末DPD档位',
  `is_delinquent`           TINYINT(1)  COMMENT '该月末是否逾期',
  `is_bad_month`            TINYINT(1)  COMMENT '该月末是否已核销',
  `out_prncp`               DECIMAL(14,2) COMMENT '期末未偿本金',
  `total_rec_prncp`         DECIMAL(14,2) COMMENT '累计已收本金',
  `loan_amnt`               DECIMAL(14,2) COMMENT '放款金额',
  `grade`                   VARCHAR(4)  COMMENT '评级(冗余)',
  `etl_load_time`           DATETIME,
  -- ⭐ 主键顺序是 (snapshot_month, loan_id) 而不是反过来：
  --    数据是**按观测月批量写入**的，这个顺序让插入变成聚簇索引的顺序追加。
  --    用 (loan_id, snapshot_month) 会导致同一笔贷款的各月记录分散在
  --    索引各处，随机 I/O 把吞吐从 40k 行/秒压到 5k 行/秒。
  PRIMARY KEY (`snapshot_month`, `loan_id`),
  KEY `idx_snapshot_mob` (`snapshot_month`, `mob`),
  KEY `idx_issue_mob` (`issue_month`, `mob`),
  KEY `idx_dpd` (`snapshot_month`, `dpd_bucket`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='DWS-月末贷款快照事实表';


-- ============================================================
-- ADS 层
-- ============================================================
DROP TABLE IF EXISTS ads_risk_segment;
CREATE TABLE ads_risk_segment (
  `dim_type`     VARCHAR(24)  NOT NULL COMMENT '分层维度',
  `dim_value`    VARCHAR(48)  NOT NULL COMMENT '维度取值',
  `loan_cnt`     BIGINT       COMMENT '笔数',
  `total_amnt`   DECIMAL(18,2) COMMENT '金额合计',
  `bad_cnt`      BIGINT       COMMENT '不良笔数(终态口径)',
  `bad_rate`     DECIMAL(10,6) COMMENT '不良率',
  `avg_int_rate` DECIMAL(8,4) COMMENT '加权平均利率',
  PRIMARY KEY (`dim_type`, `dim_value`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='ADS-风险分层';


DROP TABLE IF EXISTS ads_delinq_monthly;
CREATE TABLE ads_delinq_monthly (
  `issue_month`     CHAR(7) NOT NULL,
  `mob`             INT     NOT NULL,
  `loan_cnt`        INT     COMMENT '该批次该MOB笔数',
  `bad_cnt`         INT     COMMENT '累计不良笔数',
  `delinq_cnt`      INT     COMMENT '逾期笔数',
  `delinq_rate`     DECIMAL(10,6) COMMENT '逾期率',
  `bad_rate`        DECIMAL(10,6) COMMENT '累计不良率',
  `outstanding_amt` DECIMAL(18,2) COMMENT '期末未偿余额',
  PRIMARY KEY (`issue_month`, `mob`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='ADS-放款批次×账龄';


DROP TABLE IF EXISTS ads_vintage;
CREATE TABLE ads_vintage (
  `issue_month`  CHAR(7) NOT NULL COMMENT '放款批次',
  `grade`        VARCHAR(8) NOT NULL COMMENT '评级(ALL=全部)',
  `mob`          INT     NOT NULL COMMENT '账龄',
  `bad_rate`     DECIMAL(10,6) COMMENT '累计不良率',
  `exposure_cnt` INT     COMMENT '暴露笔数',
  PRIMARY KEY (`issue_month`, `grade`, `mob`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='ADS-Vintage逾期率矩阵';


DROP TABLE IF EXISTS ads_roll_rate;
CREATE TABLE ads_roll_rate (
  `from_month`  CHAR(7)     NOT NULL,
  `from_bucket` VARCHAR(16) NOT NULL,
  `to_bucket`   VARCHAR(16) NOT NULL,
  `cnt`         INT         COMMENT '迁徙笔数',
  `rate`        DECIMAL(10,6) COMMENT '迁徙率',
  PRIMARY KEY (`from_month`, `from_bucket`, `to_bucket`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='ADS-迁徙率矩阵';


-- ============================================================
-- 运维与质量
-- ============================================================
DROP TABLE IF EXISTS etl_watermark;
CREATE TABLE etl_watermark (
  `job_name`        VARCHAR(64) NOT NULL COMMENT '任务名',
  `watermark_value` VARCHAR(32) COMMENT '水位值',
  `last_run_time`   DATETIME    COMMENT '上次运行时间',
  `last_run_rows`   INT         COMMENT '上次处理行数',
  PRIMARY KEY (`job_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='ETL增量水位表';


DROP TABLE IF EXISTS dq_check_result;
CREATE TABLE dq_check_result (
  `check_id`     BIGINT AUTO_INCREMENT,
  `check_name`   VARCHAR(64),
  `check_level`  VARCHAR(8) COMMENT 'ERROR/WARN',
  `target_table` VARCHAR(64),
  `rule_desc`    VARCHAR(255),
  `actual_value` VARCHAR(128),
  `expect_value` VARCHAR(64),
  `passed`       TINYINT(1),
  `check_time`   DATETIME,
  `batch_id`     VARCHAR(32) COMMENT '批次标识',
  PRIMARY KEY (`check_id`),
  KEY `idx_check_time` (`check_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='数据质量校验结果';
