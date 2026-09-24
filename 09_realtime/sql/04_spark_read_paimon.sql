-- ============================================================
-- M4 · Spark 直读 Paimon：证明"同一份存储、两个引擎"
--
-- 用法（宿主机）：
--   docker exec rt-spark-query /opt/spark/bin/spark-sql \
--     --jars /opt/paimon/paimon-spark-3.5_2.12-1.4.2.jar \
--     --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
--     --conf spark.sql.catalog.paimon.warehouse=file:///warehouse \
--     --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions \
--     -f /sql/04_spark_read_paimon.sql
--
-- 注意：Paimon 的 Spark catalog 在 Spark 里是通过
--   spark.sql.catalog.<name>=org.apache.paimon.spark.SparkCatalog 注册的，
--   不是 Flink 里那种 CREATE CATALOG 语法。
-- ============================================================

SHOW DATABASES IN paimon;
SHOW TABLES IN paimon.rt;

-- 1. 实时指标表：放款月粒度（应与离线 MySQL 侧逐月一致）
SELECT COUNT(*) AS months, SUM(loan_cnt) AS total_cnt, ROUND(SUM(funded_sum),2) AS total_amt
FROM paimon.rt.rt_loan_month_agg;

-- 2. 抽样看几个月的明细
SELECT issue_month, loan_cnt, funded_sum
FROM paimon.rt.rt_loan_month_agg
ORDER BY issue_month
LIMIT 5;

-- 3. 变更日志镜像表的总量（应与离线 dwd_loan_fact 一致：2,260,668 / 34,004,208,600.00）
SELECT COUNT(*) AS n, ROUND(SUM(funded_amnt),2) AS amt
FROM paimon.rt.rt_loan_fact_latest;
