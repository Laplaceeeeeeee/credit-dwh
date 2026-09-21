-- ============================================================
-- 实验 E3：列存格式对比（TextFile / Parquet / ORC）
--
-- ⭐ 控制变量：三张表**同一份数据、同样的 10 列**，只换存储格式。
-- ⚠️ 故意用 10 列而不是 5 列：列数太少时"少列查询"的优势测不出来。
--   本表 226 万行 × 10 列，三种格式的体积与两类查询耗时都可量化。
--
-- 两类查询：
--   · 少列查询：只碰 1 列（grade）→ 列存优势最大
--   · 全列查询：**让 10 列全部参与计算**（而不是 `SELECT COUNT(*) WHERE grade='A'`）
--     → 列存优势收窄，这就是"边界"
--
-- 执行（容器内）：
--   docker exec -i bd-spark /opt/spark/bin/spark-submit \
--     --master spark://spark:7077 --executor-memory 1500m --total-executor-cores 4 \
--     --conf spark.eventLog.enabled=true \
--     --conf spark.eventLog.dir=file:///workspace/08_benchmark/_spark_events \
--     /workspace/08_benchmark/run_experiment.py \
--     /workspace/08_benchmark/exp/E3_formats.sql --reps 1
-- ============================================================

-- @case build_text TextFile
DROP TABLE IF EXISTS fmt_text;
CREATE TABLE fmt_text (
  loan_id STRING, loan_amnt DECIMAL(14,2), funded_amnt DECIMAL(14,2),
  term_months INT, int_rate DECIMAL(8,4), grade STRING, sub_grade STRING,
  purpose STRING, addr_state STRING, annual_inc DECIMAL(16,2)
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
INSERT OVERWRITE TABLE fmt_text
SELECT loan_id, loan_amnt, funded_amnt, term_months, int_rate, grade,
       sub_grade, purpose, addr_state, annual_inc FROM dwd_loan_fact_lc;

-- @case build_parquet Parquet
DROP TABLE IF EXISTS fmt_parquet;
CREATE TABLE fmt_parquet (
  loan_id STRING, loan_amnt DECIMAL(14,2), funded_amnt DECIMAL(14,2),
  term_months INT, int_rate DECIMAL(8,4), grade STRING, sub_grade STRING,
  purpose STRING, addr_state STRING, annual_inc DECIMAL(16,2)
) STORED AS PARQUET;
INSERT OVERWRITE TABLE fmt_parquet
SELECT loan_id, loan_amnt, funded_amnt, term_months, int_rate, grade,
       sub_grade, purpose, addr_state, annual_inc FROM dwd_loan_fact_lc;

-- @case build_orc ORC
DROP TABLE IF EXISTS fmt_orc;
CREATE TABLE fmt_orc (
  loan_id STRING, loan_amnt DECIMAL(14,2), funded_amnt DECIMAL(14,2),
  term_months INT, int_rate DECIMAL(8,4), grade STRING, sub_grade STRING,
  purpose STRING, addr_state STRING, annual_inc DECIMAL(16,2)
) STORED AS ORC TBLPROPERTIES ('orc.compress' = 'SNAPPY');
INSERT OVERWRITE TABLE fmt_orc
SELECT loan_id, loan_amnt, funded_amnt, term_months, int_rate, grade,
       sub_grade, purpose, addr_state, annual_inc FROM dwd_loan_fact_lc;

-- @case sanity_rowcounts 三表行数必须一致
SELECT 'text' AS fmt, COUNT(*) AS cnt FROM fmt_text
UNION ALL SELECT 'parquet', COUNT(*) FROM fmt_parquet
UNION ALL SELECT 'orc', COUNT(*) FROM fmt_orc;

-- ---------- 少列查询（只读 grade 一列）----------
-- @case narrow_text 少列·TextFile
SELECT grade, COUNT(*) AS cnt FROM fmt_text GROUP BY grade ORDER BY grade;
-- @case narrow_parquet 少列·Parquet
SELECT grade, COUNT(*) AS cnt FROM fmt_parquet GROUP BY grade ORDER BY grade;
-- @case narrow_orc 少列·ORC
SELECT grade, COUNT(*) AS cnt FROM fmt_orc GROUP BY grade ORDER BY grade;

-- ---------- 全列查询（让 10 列全部参与计算）----------
-- @case wide_text 全列·TextFile
SELECT COUNT(*) AS n, SUM(loan_amnt) AS s1, SUM(funded_amnt) AS s2,
       SUM(term_months) AS s3, SUM(int_rate) AS s4, MAX(sub_grade) AS s5,
       MAX(purpose) AS s6, MAX(addr_state) AS s7, SUM(annual_inc) AS s8,
       MAX(LENGTH(loan_id)) AS s9
FROM fmt_text;

-- @case wide_parquet 全列·Parquet
SELECT COUNT(*) AS n, SUM(loan_amnt) AS s1, SUM(funded_amnt) AS s2,
       SUM(term_months) AS s3, SUM(int_rate) AS s4, MAX(sub_grade) AS s5,
       MAX(purpose) AS s6, MAX(addr_state) AS s7, SUM(annual_inc) AS s8,
       MAX(LENGTH(loan_id)) AS s9
FROM fmt_parquet;

-- @case wide_orc 全列·ORC
SELECT COUNT(*) AS n, SUM(loan_amnt) AS s1, SUM(funded_amnt) AS s2,
       SUM(term_months) AS s3, SUM(int_rate) AS s4, MAX(sub_grade) AS s5,
       MAX(purpose) AS s6, MAX(addr_state) AS s7, SUM(annual_inc) AS s8,
       MAX(LENGTH(loan_id)) AS s9
FROM fmt_orc;
