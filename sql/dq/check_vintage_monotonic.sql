-- ============================================================
-- 口径自检：累计不良率必须随 MOB 单调不减
-- 期望返回 0 行（违反单调性说明"曾核销"标志算错了）
-- ============================================================
USE credit_dwh;

SELECT issue_month, grade, mob, bad_rate, prev_rate
FROM (
  SELECT issue_month, grade, mob, bad_rate,
         LAG(bad_rate) OVER (PARTITION BY issue_month, grade ORDER BY mob) AS prev_rate
  FROM ads_vintage
  WHERE grade = 'ALL'
) t
WHERE prev_rate IS NOT NULL AND bad_rate < prev_rate
LIMIT 20;
