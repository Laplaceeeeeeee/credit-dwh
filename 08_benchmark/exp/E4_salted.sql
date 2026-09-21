-- ============================================================
-- 实验 E4 · 第三/四步：加盐两阶段聚合 + 正确性验证
--
-- 思路：
--   阶段 1  给每行加散列后缀 → 大 key 被拆成 N 份 → 分散到 N 个 reducer
--   阶段 2  去盐，对阶段 1 的小结果再聚合一次
--   ⭐ 关键：把"大数据的 shuffle"和"小结果的聚合"分开 ——
--      阶段 1 的 shuffle 被盐摊平；阶段 2 虽然仍按 loan_status 分区、仍然不均，
--      但输入只有几百行，不均是免费的。
--
-- ⭐ 用 pmod(hash(loan_id), N) 作**确定性盐**，而不是 RAND()：
--    RAND() 每次运行都不同，会让"同一实验跑三次"的对比失去意义。
-- ============================================================

-- @case salted_5 加盐·5桶（单一键）
-- @conf spark.sql.adaptive.enabled=false
-- @conf spark.sql.adaptive.skewJoin.enabled=false
-- @conf spark.sql.shuffle.partitions=20
-- @var salt_buckets=5
WITH salted AS (
  SELECT f.loan_id, p.loan_status, f.funded_amnt, f.int_rate, p.is_bad,
         pmod(hash(f.loan_id), ${salt_buckets}) AS salt
  FROM dwd_loan_fact_lc f JOIN dwd_loan_perf_fact_lc p ON f.loan_id = p.loan_id
), stage1 AS (
  SELECT loan_status, salt,
         COUNT(*)                    AS part_cnt,
         SUM(funded_amnt)            AS part_amnt,
         SUM(funded_amnt * int_rate) AS part_rate_num,
         SUM(is_bad)                 AS part_bad
  FROM salted GROUP BY loan_status, salt
)
SELECT loan_status, SUM(part_cnt) AS loan_cnt,
       ROUND(SUM(part_amnt), 2) AS total_amnt,
       SUM(part_rate_num) AS rate_numerator, SUM(part_bad) AS bad_cnt
FROM stage1 GROUP BY loan_status;

-- @case salted_10 加盐·10桶
-- @conf spark.sql.adaptive.enabled=false
-- @conf spark.sql.shuffle.partitions=20
-- @var salt_buckets=10
WITH salted AS (
  SELECT f.loan_id, p.loan_status, f.funded_amnt, f.int_rate, p.is_bad,
         pmod(hash(f.loan_id), ${salt_buckets}) AS salt
  FROM dwd_loan_fact_lc f JOIN dwd_loan_perf_fact_lc p ON f.loan_id = p.loan_id
), stage1 AS (
  SELECT loan_status, salt,
         COUNT(*) AS part_cnt, SUM(funded_amnt) AS part_amnt,
         SUM(funded_amnt * int_rate) AS part_rate_num, SUM(is_bad) AS part_bad
  FROM salted GROUP BY loan_status, salt
)
SELECT loan_status, SUM(part_cnt) AS loan_cnt,
       ROUND(SUM(part_amnt), 2) AS total_amnt,
       SUM(part_rate_num) AS rate_numerator, SUM(part_bad) AS bad_cnt
FROM stage1 GROUP BY loan_status;

-- @case salted_20 加盐·20桶
-- @conf spark.sql.adaptive.enabled=false
-- @conf spark.sql.shuffle.partitions=20
-- @var salt_buckets=20
WITH salted AS (
  SELECT f.loan_id, p.loan_status, f.funded_amnt, f.int_rate, p.is_bad,
         pmod(hash(f.loan_id), ${salt_buckets}) AS salt
  FROM dwd_loan_fact_lc f JOIN dwd_loan_perf_fact_lc p ON f.loan_id = p.loan_id
), stage1 AS (
  SELECT loan_status, salt,
         COUNT(*) AS part_cnt, SUM(funded_amnt) AS part_amnt,
         SUM(funded_amnt * int_rate) AS part_rate_num, SUM(is_bad) AS part_bad
  FROM salted GROUP BY loan_status, salt
)
SELECT loan_status, SUM(part_cnt) AS loan_cnt,
       ROUND(SUM(part_amnt), 2) AS total_amnt,
       SUM(part_rate_num) AS rate_numerator, SUM(part_bad) AS bad_cnt
FROM stage1 GROUP BY loan_status;

-- ---------- 正确性验证：加盐前后必须逐行一致 ----------
-- @case verify_materialize_base 落表·基线（复合键）
-- @conf spark.sql.adaptive.enabled=false
DROP TABLE IF EXISTS e4_base_result;
CREATE TABLE e4_base_result STORED AS ORC AS
SELECT p.loan_status, f.grade, f.purpose,
       COUNT(*) AS loan_cnt, ROUND(SUM(f.funded_amnt), 2) AS total_amnt,
       SUM(f.funded_amnt * f.int_rate) AS rate_numerator, SUM(p.is_bad) AS bad_cnt
FROM dwd_loan_fact_lc f JOIN dwd_loan_perf_fact_lc p ON f.loan_id = p.loan_id
GROUP BY p.loan_status, f.grade, f.purpose;

-- @case verify_materialize_salted10 落表·加盐10桶（同分组键）
-- @conf spark.sql.adaptive.enabled=false
-- @var salt_buckets=10
DROP TABLE IF EXISTS e4_salted_result;
CREATE TABLE e4_salted_result STORED AS ORC AS
WITH salted AS (
  SELECT f.loan_id, f.grade, f.purpose, p.loan_status, f.funded_amnt, f.int_rate, p.is_bad,
         pmod(hash(f.loan_id), ${salt_buckets}) AS salt
  FROM dwd_loan_fact_lc f JOIN dwd_loan_perf_fact_lc p ON f.loan_id = p.loan_id
), stage1 AS (
  SELECT loan_status, grade, purpose, salt,
         COUNT(*) AS part_cnt, SUM(funded_amnt) AS part_amnt,
         SUM(funded_amnt * int_rate) AS part_rate_num, SUM(is_bad) AS part_bad
  FROM salted GROUP BY loan_status, grade, purpose, salt
)
SELECT loan_status, grade, purpose,
       SUM(part_cnt) AS loan_cnt, ROUND(SUM(part_amnt), 2) AS total_amnt,
       SUM(part_rate_num) AS rate_numerator, SUM(part_bad) AS bad_cnt
FROM stage1 GROUP BY loan_status, grade, purpose;

-- @case verify_row_match ⭐逐行比对（期望：全部 mismatch = 0 且 total_rows > 0）
-- 🚨 关键：NULL 安全连接。直接 = 会漏掉 purpose 为 NULL 的行，产生"看似一致"的假象。
--    同时必须要求 total_rows > 0 —— 否则"0 差异"是假的通过（两边都没数据）。
SELECT COUNT(*) AS total_rows,
       SUM(CASE WHEN a.loan_cnt  <> b.loan_cnt  THEN 1 ELSE 0 END) AS cnt_mismatch,
       SUM(CASE WHEN ABS(a.total_amnt - b.total_amnt) > 0.01 THEN 1 ELSE 0 END) AS amnt_mismatch,
       SUM(CASE WHEN a.bad_cnt   <> b.bad_cnt   THEN 1 ELSE 0 END) AS bad_mismatch,
       SUM(CASE WHEN ABS(a.rate_numerator - b.rate_numerator) > 0.01 THEN 1 ELSE 0 END) AS rate_mismatch
FROM e4_base_result a
FULL OUTER JOIN e4_salted_result b
  ON  a.loan_status = b.loan_status
  AND a.grade       = b.grade
  AND COALESCE(a.purpose, '__NULL__') = COALESCE(b.purpose, '__NULL__');
