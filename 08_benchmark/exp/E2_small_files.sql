-- ============================================================
-- 实验 E2：小文件合并
--
-- ⭐ 先讲清"文件数从哪来"（这是本实验的知识核心，也是面试可讲的机制）：
--   ① 写文件时，**一个 task 对"它持有的每一个分区值"各写一个文件**
--      → 文件数 ≈ 任务数 × 每个任务持有的分区值数
--   ② 写分区表**不会**按分区列再 shuffle，写阶段只做"分区内局部排序"，
--      所以 hint 的并行度会一直保留到 writer
--   ③ `REPARTITION(数字)` **不会被 AQE 合并**（它的 shuffle origin 是 REPARTITION_BY_NUM，
--      被明确排除在"可合并分区"之外）→ 文件数可复现
--
-- 执行（容器内）：
--   docker exec -i bd-spark /opt/spark/bin/spark-submit \
--     --master spark://spark:7077 --executor-memory 1500m --total-executor-cores 4 \
--     --conf spark.eventLog.enabled=true \
--     --conf spark.eventLog.dir=file:///workspace/08_benchmark/_spark_events \
--     /workspace/08_benchmark/run_experiment.py \
--     /workspace/08_benchmark/exp/E2_small_files.sql --reps 1
-- ============================================================

-- ---------- 制造小文件：200 个任务各写一份 ----------
-- @case build_many_files 分区表·200并行
-- @conf spark.sql.adaptive.enabled=false
-- @conf spark.sql.shuffle.partitions=20
-- @conf hive.exec.dynamic.partition.mode=nonstrict
--   ⭐ 必须加：否则写分区表时报
--   "Dynamic partition strict mode requires at least one static partition column"
--   （来自 V1WritesHiveUtils.getDynamicPartitionColumn —— 这说明 Spark **确实会读**
--     hive.exec.dynamic.partition.mode，之前"Spark 不认这个参数"的说法是错的）
DROP TABLE IF EXISTS tmp_files_many;
CREATE TABLE tmp_files_many
PARTITIONED BY (issue_month)     -- ⚠️ CTAS 里**不能写类型**：写 STRING 会报
                                 -- "Partition column types may not be specified in CTAS"
STORED AS ORC
AS
SELECT /*+ REPARTITION(200) */ *
FROM dwd_loan_fact_lc
WHERE issue_month BETWEEN '2017-01' AND '2017-12';

-- ---------- 按记录数切文件（生产里更常用的手段）----------
-- @case build_maxrecords_20000 每2万行一个文件
-- @conf spark.sql.adaptive.enabled=false
-- @conf spark.sql.files.maxRecordsPerFile=20000
DROP TABLE IF EXISTS tmp_files_many2;
CREATE TABLE tmp_files_many2 STORED AS ORC AS
SELECT * FROM dwd_loan_fact_lc
WHERE issue_month BETWEEN '2017-01' AND '2017-12';

-- ---------- 合并对照组：每个分区恰好 1 个文件 ----------
-- @case build_one_file_per_partition 分区表·COALESCE(1)
-- @conf spark.sql.adaptive.enabled=false
-- @conf spark.sql.files.maxRecordsPerFile=0
-- @conf hive.exec.dynamic.partition.mode=nonstrict
DROP TABLE IF EXISTS tmp_files_one_per_part;
CREATE TABLE tmp_files_one_per_part
PARTITIONED BY (issue_month)     -- ⚠️ CTAS 里**不能写类型**：写 STRING 会报
                                 -- "Partition column types may not be specified in CTAS"
STORED AS ORC
AS
SELECT /*+ COALESCE(1) */ *
FROM dwd_loan_fact_lc
WHERE issue_month BETWEEN '2017-01' AND '2017-12';

-- ---------- 测量：同一查询在小文件表 vs 合并表上的表现 ----------
-- @case query_many_files 查询·小文件表
SELECT grade, COUNT(*) AS cnt, ROUND(SUM(funded_amnt), 2) AS amt
FROM tmp_files_many GROUP BY grade ORDER BY grade;

-- @case query_one_file_per_partition 查询·合并表
SELECT grade, COUNT(*) AS cnt, ROUND(SUM(funded_amnt), 2) AS amt
FROM tmp_files_one_per_part GROUP BY grade ORDER BY grade;

-- @case query_maxrecords 查询·maxRecordsPerFile 表
SELECT grade, COUNT(*) AS cnt, ROUND(SUM(funded_amnt), 2) AS amt
FROM tmp_files_many2 GROUP BY grade ORDER BY grade;
