"""统一定义：路径、数据库连接、字段口径常量。所有脚本从这里取值，禁止硬编码。"""
from pathlib import Path
import os

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
LOG_DIR = PROJECT_ROOT / "06_ops" / "logs"

RAW_FILE = DATA_RAW / "accepted_2007_to_2018Q4.csv.gz"

DB = {
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "root123456"),
    "database": os.getenv("DB_NAME", "credit_dwh"),
    "charset": "utf8mb4",
}

# ============ 口径常量：口径字典的代码化，改口径只改这里 ============
BAD_STATUSES = ["Charged Off", "Default", "Does not meet the credit policy. Status:Charged Off"]

# 只有这些行是"终态"，才能计算最终不良率（其余仍在观察中）
TERMINAL_STATUSES = [
    "Fully Paid", "Charged Off", "Default",
    "Does not meet the credit policy. Status:Fully Paid",
    "Does not meet the credit policy. Status:Charged Off",
]

# 表现期字段黑名单：绝不允许出现在 dwd_loan_fact（申请时点事实表）中
LEAKAGE_BLACKLIST = [
    "loan_status", "pymnt_plan", "purpose",
    "total_pymnt", "total_pymnt_inv", "total_rec_prncp", "total_rec_int",
    "total_rec_late_fee", "recoveries", "collection_recovery_fee",
    "last_pymnt_d", "last_pymnt_amnt", "next_pymnt_d",
    "last_credit_pull_d", "last_fico_range_high", "last_fico_range_low",
    "out_prncp", "out_prncp_inv",
    "total_bal_ex_prncp", "total_bal_il", "il_util", "max_bal_bc", "all_util",
    "total_rev_hi_lim", "hardship_flag", "hardship_type", "hardship_reason",
    "hardship_status", "deferral_term", "hardship_amount", "hardship_start_date",
    "hardship_end_date", "payment_plan_start_date", "hardship_length",
    "hardship_dpd", "hardship_loan_status", "orig_projected_additional_accrued_interest",
    "hardship_payoff_balance_amount", "hardship_last_payment_amount",
    "debt_settlement_flag", "debt_settlement_flag_date", "settlement_status",
    "settlement_date", "settlement_amount", "settlement_percentage", "settlement_term",
]

# 申请时点字段白名单（DWD 事实表只允许这些 + 派生字段）
# ⚠️ 不含 member_id：本地实测该列 226 万行全部为空，不可用（见 1.3 节）
APPLY_COLS = [
    "id", "loan_amnt", "funded_amnt", "term", "int_rate", "installment",
    "grade", "sub_grade", "emp_length", "home_ownership", "annual_inc",
    "verification_status", "issue_d", "addr_state", "purpose",
    "dti", "delinq_2yrs", "earliest_cr_line", "fico_range_low", "fico_range_high",
    "inq_last_6mths", "open_acc", "pub_rec", "revol_bal", "revol_util",
    "total_acc", "application_type", "mort_acc", "pub_rec_bankruptcies",
]

# loan_status -> DPD 档位映射（口径字典的一部分）
DPD_MAP = {
    "Fully Paid": "PAID",
    "Current": "CURRENT",
    "In Grace Period": "DPD_1_30",
    "Late (16-30 days)": "DPD_1_30",
    "Late (31-120 days)": "DPD_31_120",
    "Charged Off": "CHARGED_OFF",
    "Default": "CHARGED_OFF",
    "Does not meet the credit policy. Status:Fully Paid": "PAID",
    "Does not meet the credit policy. Status:Charged Off": "CHARGED_OFF",
}