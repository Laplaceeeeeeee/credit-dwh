-- ============================================================
-- 阶段三 · 装载 事实表（第 13 步）
-- 执行：docker exec -i bd-spark /opt/spark/bin/spark-sql \
--         -f /workspace/07_bigdata/hive/02_load.sql
-- ============================================================
USE credit_dwh_lc;

-- ============================================================
-- ⭐ 两个设置都要写，它们管的是**两条不同的写入路径**：
--   ① hive.exec.dynamic.partition.mode=nonstrict
--      Spark **确实会读它**（SQLConf 会被拷进 Hadoop 配置，Hive 写入校验从那里取）。
--      对 Hive serde 写入路径（`INSERT ... PARTITION(动态列)` 且没有静态分区列），
--      默认的 strict 模式会直接抛 DYNAMIC_PARTITION_STRICT_MODE。
--   ② spark.sql.sources.partitionOverwriteMode=dynamic
--      `STORED AS ORC` 的表默认会被转换成数据源写入路径
--      （convertMetastoreOrc / convertInsertingPartitionedTable 默认 true），
--      这条路径下默认的 STATIC 会让 `INSERT OVERWRITE ... PARTITION(动态列)`
--      **把整张表清空再重写**。
-- ============================================================
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET spark.sql.sources.partitionOverwriteMode = dynamic;
SET spark.sql.shuffle.partitions = 20;

-- ---------- ① 申请时点事实表 ----------
-- ⚠️ 显式给 schema：不让 Spark 猜类型
--    （可空整数一旦被猜成 double，后面很难查出来）
DROP TABLE IF EXISTS stg_loan_fact;
CREATE TABLE stg_loan_fact (
  loan_id             STRING,
  loan_amnt           DECIMAL(14,2),
  funded_amnt         DECIMAL(14,2),
  term_months         INT,
  int_rate            DECIMAL(8,4),
  grade               STRING,
  sub_grade           STRING,
  emp_length_years    INT,
  home_ownership      STRING,
  annual_inc          DECIMAL(16,2),
  verification_status STRING,
  purpose             STRING,
  addr_state          STRING,
  dti                 DECIMAL(10,4),
  fico_low            DECIMAL(8,2),
  fico_high           DECIMAL(8,2),
  issue_date          DATE,
  issue_month         STRING
)
USING csv
OPTIONS (
  path '/workspace/07_bigdata/exchange/dwd_loan_fact.tsv',
  sep '\t',
  nullValue '\\N',        -- mysql --batch 的空值标记（这里 \\N 在 SQL 里表示字面量 \N）
  emptyValue '\\N',
  header 'false',
  mode 'FAILFAST'         -- ⭐ 宁可直接失败，也不要静默把坏行变成 NULL
);

-- ⭐ 关键：分区列必须放在 SELECT 的**最后**
-- ⭐⭐ 用 REPARTITION(20, issue_month) 控制文件数：
--    写文件时"一个 task 对它持有的每个分区值各写一个文件"，
--    文件数 ≈ 任务数 × 每个任务持有的分区值数。
--    不加这一步，输入 CSV 的 ~N 个 split 会各自写出 129 个月的文件，产生上千个小文件。
INSERT OVERWRITE TABLE dwd_loan_fact_lc PARTITION (issue_month)
SELECT /*+ REPARTITION(20, issue_month) */
       loan_id, loan_amnt, funded_amnt, term_months, int_rate,
       grade, sub_grade, emp_length_years, home_ownership, annual_inc,
       verification_status, purpose, addr_state, dti,
       fico_low, fico_high, issue_date,
       issue_month
FROM stg_loan_fact;

SELECT COUNT(*) AS dwd_loan_fact_lc_rows FROM dwd_loan_fact_lc;

-- ---------- ② 表现期事实表（不分区）----------
DROP TABLE IF EXISTS stg_perf;
CREATE TABLE stg_perf (
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
)
USING csv OPTIONS (
  path '/workspace/07_bigdata/exchange/dwd_loan_perf_fact.tsv',
  sep '\t', nullValue '\\N', emptyValue '\\N', header 'false', mode 'FAILFAST'
);

INSERT OVERWRITE TABLE dwd_loan_perf_fact_lc
SELECT /*+ COALESCE(8) */ * FROM stg_perf;

SELECT COUNT(*) AS dwd_loan_perf_fact_lc_rows FROM dwd_loan_perf_fact_lc;
