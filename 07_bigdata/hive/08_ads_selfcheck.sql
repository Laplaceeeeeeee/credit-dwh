-- ============================================================
-- 第 14 步 · 口径自检（三条，期望全部返回 0）
-- 这是"用 SQL 自己做回归校验"——比只看总数严格得多。
-- 执行：docker exec -i bd-spark /opt/spark/bin/spark-sql \
--         -f /workspace/07_bigdata/hive/08_ads_selfcheck.sql
-- ============================================================
USE credit_dwh_lc;

-- ---------- ① 累计不良率必须随 MOB 单调不减（期望 0 行）----------
SELECT 'vintage_monotonic_violation' AS chk, COUNT(*) AS bad_rows FROM (
    SELECT issue_month, grade, mob, bad_rate,
           LAG(bad_rate) OVER (PARTITION BY issue_month, grade ORDER BY mob) AS prev_rate
    FROM ads_vintage_lc
) x
WHERE prev_rate IS NOT NULL AND bad_rate < prev_rate;

-- ---------- ② ALL 行的暴露笔数 = 各评级之和（期望 0 行）----------
SELECT 'vintage_all_row_mismatch' AS chk, COUNT(*) AS bad_rows FROM (
    SELECT a.issue_month, a.mob, a.exposure_cnt AS cnt_all, g.cnt_sum
    FROM (SELECT issue_month, mob, exposure_cnt
          FROM ads_vintage_lc WHERE grade = 'ALL') a
    JOIN (SELECT issue_month, mob, SUM(exposure_cnt) AS cnt_sum
          FROM ads_vintage_lc WHERE grade <> 'ALL'
          GROUP BY issue_month, mob) g
      ON a.issue_month = g.issue_month AND a.mob = g.mob
    WHERE a.exposure_cnt <> g.cnt_sum
) t;

-- ---------- ③ 每个 (from_month, from_bucket) 的迁徙率之和必须为 1（期望 0 行）----------
SELECT 'roll_rate_not_normalized' AS chk, COUNT(*) AS bad_rows FROM (
    SELECT from_month, from_bucket, ROUND(SUM(rate), 4) AS s
    FROM ads_roll_rate_lc
    GROUP BY from_month, from_bucket
) t
WHERE ABS(s - 1) > 0.001;

-- ---------- ④ 风险分层：终态不良总数 = 按评级汇总（期望 0 行，或差异 0）----------
SELECT 'risk_segment_bad_total_mismatch' AS chk,
       ABS((SELECT SUM(bad_cnt) FROM ads_risk_segment_lc WHERE dim_type = 'grade')
           - (SELECT SUM(is_bad) FROM dwd_loan_perf_fact_lc WHERE is_terminal = 1)) AS diff;
