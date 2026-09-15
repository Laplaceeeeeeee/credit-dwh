-- ============================================================
-- 各层行数一览（最常用的日常检查）
-- ============================================================
USE credit_dwh;

SELECT 'ods_loan_raw'           AS table_name, COUNT(*) AS row_cnt FROM ods_loan_raw
UNION ALL SELECT 'dwd_loan_fact',        COUNT(*) FROM dwd_loan_fact
UNION ALL SELECT 'dwd_loan_perf_fact',   COUNT(*) FROM dwd_loan_perf_fact
UNION ALL SELECT 'dim_applicant',        COUNT(*) FROM dim_applicant
UNION ALL SELECT 'dim_product',          COUNT(*) FROM dim_product
UNION ALL SELECT 'dim_date',             COUNT(*) FROM dim_date
UNION ALL SELECT 'dws_loan_snapshot_m',  COUNT(*) FROM dws_loan_snapshot_m
UNION ALL SELECT 'ads_risk_segment',     COUNT(*) FROM ads_risk_segment
UNION ALL SELECT 'ads_vintage',          COUNT(*) FROM ads_vintage
UNION ALL SELECT 'ads_roll_rate',        COUNT(*) FROM ads_roll_rate
UNION ALL SELECT 'ads_delinq_monthly',   COUNT(*) FROM ads_delinq_monthly;
