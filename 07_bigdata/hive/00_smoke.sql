-- ============================================================
-- 阶段三环境功能验收（第 12 步 §8）
-- 执行：docker exec -i bd-spark /opt/spark/bin/spark-sql \
--         -f /workspace/07_bigdata/hive/00_smoke.sql
-- ============================================================

-- ① 建库：能成功说明 Spark → Metastore → 元数据库 整条链通了
CREATE DATABASE IF NOT EXISTS credit_dwh_lc COMMENT '信贷数仓 - 大数据层';
USE credit_dwh_lc;
SHOW DATABASES;

-- ② ORC 表 + 插入 + 聚合
DROP TABLE IF EXISTS tmp_wordcount;
CREATE TABLE tmp_wordcount (word STRING, cnt BIGINT) STORED AS ORC;
INSERT INTO tmp_wordcount VALUES ('a', 1), ('b', 2), ('a', 3);
SELECT word, SUM(cnt) AS total FROM tmp_wordcount GROUP BY word ORDER BY word;
-- 期望：a=4, b=2

-- ③ 确认是真正的 Hive 表：看 Location 是否落在共享卷上
DESCRIBE FORMATTED tmp_wordcount;

-- ④ 分区表（阶段三所有性能实验的基础）
DROP TABLE IF EXISTS tmp_part;
CREATE TABLE tmp_part (id STRING, amt DECIMAL(14,2))
PARTITIONED BY (issue_month STRING)
STORED AS ORC;

INSERT INTO tmp_part PARTITION (issue_month='2018-06') VALUES ('L1', 1000);
INSERT INTO tmp_part PARTITION (issue_month='2018-07') VALUES ('L2', 2000);

SHOW PARTITIONS tmp_part;
-- 期望：2018-06 / 2018-07

SELECT * FROM tmp_part WHERE issue_month = '2018-06';
-- 期望：只有 L1

-- ⑤ ⭐ 分区裁剪下推（E1 实验的地基）
EXPLAIN EXTENDED SELECT COUNT(*) FROM tmp_part WHERE issue_month = '2018-06';
-- 关注输出里有没有 PartitionFilters: [... (issue_month = 2018-06)]
