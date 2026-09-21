-- ============================================================
-- 阶段三 · 装载 快照表 + 维表（第 13 步）
-- 与 02_load.sql 分开：4471 万行的快照是最慢的一步，
-- 单独一个文件便于失败后只重跑这一段。
-- 执行：docker exec -i bd-spark /opt/spark/bin/spark-sql \
--         -f /workspace/07_bigdata/hive/02b_load_snapshot_dim.sql
-- ============================================================
USE credit_dwh_lc;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET spark.sql.sources.partitionOverwriteMode = dynamic;
SET spark.sql.shuffle.partitions = 20;

-- ---------- ③ 月末快照事实表（4471 万行，最慢的一步）----------
DROP TABLE IF EXISTS stg_snap;
CREATE TABLE stg_snap (
  loan_id                 STRING,
  snapshot_month          STRING,
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
USING csv OPTIONS (
  path '/workspace/07_bigdata/exchange/dws_loan_snapshot_m.tsv',
  sep '\t', nullValue '\\N', emptyValue '\\N', header 'false', mode 'FAILFAST'
);

-- ⭐⭐ REPARTITION(20, snapshot_month)：按分区列做 hash 重分区
--    这样"每个月只会落在某一个 task 里"，文件数 ≈ 20 × (141/20) ≈ 141 个（每分区 1 个）
--    不加这一步：输入 CSV 的每个 split 都会写出 141 个月的文件 → 上千个小文件
INSERT OVERWRITE TABLE dws_loan_snapshot_lc PARTITION (snapshot_month)
SELECT /*+ REPARTITION(20, snapshot_month) */
       loan_id, snapshot_date, issue_month, mob, months_since_last_pymnt,
       dpd_bucket, is_delinquent, is_bad_month, out_prncp,
       total_rec_prncp, loan_amnt, grade,
       snapshot_month
FROM stg_snap;

SELECT COUNT(*) AS dws_loan_snapshot_lc_rows FROM dws_loan_snapshot_lc;

-- ---------- ④ 申请人画像维 ----------
DROP TABLE IF EXISTS stg_applicant;
CREATE TABLE stg_applicant (
  loan_id          STRING,
  annual_inc       DECIMAL(16,2),
  income_band      STRING,
  home_ownership   STRING,
  emp_length_years INT,
  addr_state       STRING,
  fico_low         DECIMAL(8,2),
  fico_band        STRING,
  dti              DECIMAL(10,4)
)
USING csv OPTIONS (
  path '/workspace/07_bigdata/exchange/dim_applicant.tsv',
  sep '\t', nullValue '\\N', emptyValue '\\N', header 'false', mode 'FAILFAST'
);

INSERT OVERWRITE TABLE dim_applicant_lc
SELECT /*+ COALESCE(4) */ * FROM stg_applicant;

SELECT COUNT(*) AS dim_applicant_lc_rows FROM dim_applicant_lc;
