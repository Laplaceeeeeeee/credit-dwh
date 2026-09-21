-- ============================================================
-- 阶段三 · 装载后四项校验（第 13 步）
-- 期望：① 行数与阶段二一致 ② 主键唯一（0 行）③ 关键列 NULL 全为 0 ④ 分区数正确
-- 执行：docker exec -i bd-spark /opt/spark/bin/spark-sql \
--         -f /workspace/07_bigdata/hive/03_verify_load.sql
-- ============================================================
USE credit_dwh_lc;

-- ---------- ① 行数：必须与阶段二完全一致 ----------
SELECT 'dwd_loan_fact'      AS tbl, COUNT(*) AS cnt FROM dwd_loan_fact_lc
UNION ALL SELECT 'dwd_loan_perf_fact', COUNT(*) FROM dwd_loan_perf_fact_lc
UNION ALL SELECT 'dws_loan_snapshot',  COUNT(*) FROM dws_loan_snapshot_lc
UNION ALL SELECT 'dim_applicant',      COUNT(*) FROM dim_applicant_lc;
-- 期望：2,260,668 / 2,260,668 / 44,718,091 / 2,260,668

-- ---------- ② 主键唯一性：Hive 不强制主键，必须自己验（期望 0 行）----------
SELECT 'fact_dup' AS chk, COUNT(*) AS bad_rows FROM (
  SELECT loan_id FROM dwd_loan_fact_lc GROUP BY loan_id HAVING COUNT(*) > 1
) t
UNION ALL
SELECT 'perf_dup', COUNT(*) FROM (
  SELECT loan_id FROM dwd_loan_perf_fact_lc GROUP BY loan_id HAVING COUNT(*) > 1
) t
UNION ALL
SELECT 'snapshot_dup', COUNT(*) FROM (
  SELECT loan_id, snapshot_month FROM dws_loan_snapshot_lc
  GROUP BY loan_id, snapshot_month HAVING COUNT(*) > 1
) t;

-- ---------- ③ ⭐ 关键列的 NULL 数：与 MySQL 对不上说明类型被猜错了 ----------
SELECT
  SUM(CASE WHEN loan_id          IS NULL THEN 1 ELSE 0 END) AS null_loan_id,
  SUM(CASE WHEN funded_amnt      IS NULL THEN 1 ELSE 0 END) AS null_funded_amnt,
  SUM(CASE WHEN issue_month      IS NULL THEN 1 ELSE 0 END) AS null_issue_month,
  SUM(CASE WHEN grade            IS NULL THEN 1 ELSE 0 END) AS null_grade,
  SUM(CASE WHEN issue_date       IS NULL THEN 1 ELSE 0 END) AS null_issue_date
FROM dwd_loan_fact_lc;

SELECT
  SUM(CASE WHEN loan_id       IS NULL THEN 1 ELSE 0 END) AS null_loan_id,
  SUM(CASE WHEN loan_status   IS NULL THEN 1 ELSE 0 END) AS null_loan_status,
  SUM(CASE WHEN is_terminal   IS NULL THEN 1 ELSE 0 END) AS null_is_terminal,
  SUM(CASE WHEN is_bad        IS NULL THEN 1 ELSE 0 END) AS null_is_bad
FROM dwd_loan_perf_fact_lc;

SELECT
  SUM(CASE WHEN loan_id        IS NULL THEN 1 ELSE 0 END) AS null_loan_id,
  SUM(CASE WHEN snapshot_month IS NULL THEN 1 ELSE 0 END) AS null_snapshot_month,
  SUM(CASE WHEN mob            IS NULL THEN 1 ELSE 0 END) AS null_mob,
  SUM(CASE WHEN dpd_bucket     IS NULL THEN 1 ELSE 0 END) AS null_dpd_bucket,
  SUM(CASE WHEN is_bad_month   IS NULL THEN 1 ELSE 0 END) AS null_is_bad_month
FROM dws_loan_snapshot_lc;

-- ---------- ④ 分区数 / 分区列表抽查 ----------
SELECT COUNT(DISTINCT issue_month)    AS issue_months    FROM dwd_loan_fact_lc;
SELECT COUNT(DISTINCT snapshot_month) AS snapshot_months FROM dws_loan_snapshot_lc;
SHOW PARTITIONS dwd_loan_fact_lc;
