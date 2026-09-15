"""统一定义：路径、数据库连接、字段口径常量。所有脚本从这里取值，禁止硬编码。"""
from pathlib import Path
import os

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
DATA_CLEAN = PROJECT_ROOT / "data" / "clean"       # 🆕 已剔除页脚的干净数据
LOG_DIR = PROJECT_ROOT / "06_ops" / "logs"

RAW_FILE = DATA_RAW / "accepted_2007_to_2018Q4.csv.gz"

# 🆕 干净数据（由 prepare_raw.py 生成）。所有后续脚本读这里，不再碰页脚问题。
CLEAN_PARQUET = DATA_CLEAN / "loans_full.parquet"
CLEAN_PARQUET_SAMPLE = DATA_CLEAN / "loans_sample.parquet"
CLEAN_TSV = DATA_CLEAN / "loans_full.tsv"          # 给 mysql LOAD DATA 用
FOOTER_TXT = DATA_CLEAN / "footer_line.txt"        # 被剔除的页脚，留证

# 🆕 规模开关：先用子集跑通，最后切全量。
#    None      = 全量 2,260,700 行
#    200_000   = 20 万行子集（调流程用）
MAX_ROWS = None
# 当前：全量模式（2,260,668 行）。改回 200_000 可快速调流程。

DB = {
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    # ⚠️ 必须与 docker-compose.yml 的端口映射一致（3307:3306）
    "port": int(os.getenv("DB_PORT", 3307)),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "root123456"),
    "database": os.getenv("DB_NAME", "credit_dwh"),
    "charset": "utf8mb4",
}

# ============ 口径常量：口径字典的代码化，改口径只改这里 ============
BAD_STATUSES = [
    "Charged Off",
    "Default",
    "Does not meet the credit policy. Status:Charged Off",
]

# 只有这些行是"终态"，才能计算最终不良率（其余仍在观察中）
TERMINAL_STATUSES = [
    "Fully Paid", "Charged Off", "Default",
    "Does not meet the credit policy. Status:Fully Paid",
    "Does not meet the credit policy. Status:Charged Off",
]

# 表现期字段黑名单：绝不允许出现在 dwd_loan_fact（申请时点事实表）中
# ⚠️ 注意：purpose（借款用途）是【申请时点】信息，绝不能放进黑名单，
#    否则 Q8 校验会把合法字段判成泄漏。曾在早期版本误列，已修正。
LEAKAGE_BLACKLIST = [
    "loan_status", "pymnt_plan",
    "total_pymnt", "total_pymnt_inv", "total_rec_prncp", "total_rec_int",
    "total_rec_late_fee", "recoveries", "collection_recovery_fee",
    "last_pymnt_d", "last_pymnt_amnt", "next_pymnt_d",
    "last_credit_pull_d", "last_fico_range_high", "last_fico_range_low",
    "out_prncp", "out_prncp_inv",
    "total_bal_ex_prncp", "total_bal_il", "il_util", "max_bal_bc", "all_util",
    "hardship_flag", "hardship_type", "hardship_reason",
    "hardship_status", "deferral_term", "hardship_amount", "hardship_start_date",
    "hardship_end_date", "payment_plan_start_date", "hardship_length",
    "hardship_dpd", "hardship_loan_status",
    "orig_projected_additional_accrued_interest",
    "hardship_payoff_balance_amount", "hardship_last_payment_amount",
    "debt_settlement_flag", "debt_settlement_flag_date", "settlement_status",
    "settlement_date", "settlement_amount", "settlement_percentage",
    "settlement_term",
    # ⚠️ 下面 4 个字段经过复核，确认属于【申请时点】信息，已从黑名单移除：
    #    dti / revol_util / tot_cur_bal / total_rev_hi_lim
    #    它们来自放款时的征信快照（dti 是申请时算出的债务收入比），
    #    只有 last_fico_range_* 这种带 "last" 前缀的才是拉取时点信息、才算泄漏。
    #    区分原则：看字段语义是"申请那一刻的画像"还是"放款后的表现"。
]

# 申请时点字段白名单（dwd_loan_fact 只允许这些 + 派生字段）
# ⚠️ 不含 member_id：实测该列 226 万行全部为空，不可用
APPLY_COLS = [
    "id", "loan_amnt", "funded_amnt", "term", "int_rate", "installment",
    "grade", "sub_grade", "emp_length", "home_ownership", "annual_inc",
    "verification_status", "issue_d", "addr_state", "purpose",
    "dti", "delinq_2yrs", "earliest_cr_line", "fico_range_low",
    "fico_range_high", "inq_last_6mths", "open_acc", "pub_rec", "revol_bal",
    "revol_util", "total_acc", "application_type", "mort_acc",
    "pub_rec_bankruptcies", "emp_title", "title", "zip_code",
]

# 表现期字段（dwd_loan_perf_fact 专用）
PERF_COLS = [
    "id", "loan_status", "total_rec_prncp", "total_rec_int", "recoveries",
    "out_prncp", "total_pymnt", "last_pymnt_d", "last_pymnt_amnt",
    "issue_d", "term", "loan_amnt",
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
