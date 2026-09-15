-- ============================================================
-- ⭐ 防数据泄漏校验：申请时点事实表绝不允许出现表现期字段
-- 期望返回 0 行。返回非空即为数据泄漏，流水线必须中止。
-- ============================================================
USE credit_dwh;

SELECT COLUMN_NAME AS leaked_column,
       '申请时点表出现表现期字段' AS problem
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = 'credit_dwh'
  AND TABLE_NAME = 'dwd_loan_fact'
  AND COLUMN_NAME IN (
    'loan_status', 'pymnt_plan',
    'total_pymnt', 'total_pymnt_inv', 'total_rec_prncp', 'total_rec_int',
    'total_rec_late_fee', 'recoveries', 'collection_recovery_fee',
    'last_pymnt_d', 'last_pymnt_amnt', 'next_pymnt_d',
    'last_credit_pull_d', 'last_fico_range_high', 'last_fico_range_low',
    'out_prncp', 'out_prncp_inv',
    'total_bal_ex_prncp', 'total_bal_il', 'il_util', 'max_bal_bc', 'all_util',
    'hardship_flag', 'hardship_type', 'hardship_reason',
    'hardship_amount', 'hardship_dpd', 'hardship_loan_status',
    'debt_settlement_flag', 'settlement_status', 'settlement_amount'
  );
