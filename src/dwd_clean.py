"""DWD 层：清洗与标准化，输出申请时点事实表 dwd_loan_fact。

⭐ 数据泄漏防线之二：本脚本**只**产出申请时点字段，
   任何表现期字段（loan_status / out_prncp / total_rec_pncp ...）都不得进入。
   防线之一是 DDL 物理分表，防线之三是 dq_check Q8 自动校验。
"""
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd
from sqlalchemy import text

from src import config
from src.utils import clean_source_name, get_engine, get_logger, read_clean

log = get_logger("dwd_clean")

READ_COLS = [
    "id", "loan_amnt", "funded_amnt", "term", "int_rate", "installment",
    "grade", "sub_grade", "emp_length", "home_ownership", "annual_inc",
    "verification_status", "purpose", "addr_state", "dti",
    "fico_range_low", "fico_range_high", "issue_d",
]
BATCH = 20_000


def parse_term(s: pd.Series) -> pd.Series:
    """'36 months' -> 36"""
    return pd.to_numeric(
        s.astype("string").str.extract(r"(\d+)")[0], errors="coerce").astype("Int64")


def parse_rate(s: pd.Series) -> pd.Series:
    """'13.56%' -> 13.56（原始文件里已是数值，这里兼容带 % 的情况）"""
    return pd.to_numeric(
        s.astype("string").str.replace("%", "", regex=False), errors="coerce")


def parse_month(s: pd.Series) -> pd.Series:
    """'Dec-2018' -> Timestamp('2018-12-01')"""
    return pd.to_datetime(s, format="%b-%Y", errors="coerce")


def parse_emp_length(s: pd.Series) -> pd.Series:
    """'10+ years'->10, '< 1 year'->0, 'n/a'->NA"""
    txt = s.astype("string")
    v = pd.to_numeric(txt.str.extract(r"(\d+)")[0], errors="coerce")
    is_na = txt.str.contains("n/a", case=False, na=False)
    return v.mask(is_na).astype("Int64")


def clean(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    out["loan_id"] = df["id"].astype("string")
    out["loan_amnt"] = pd.to_numeric(df["loan_amnt"], errors="coerce")
    out["funded_amnt"] = pd.to_numeric(df["funded_amnt"], errors="coerce")
    out["term_months"] = parse_term(df["term"])
    out["int_rate"] = parse_rate(df["int_rate"])
    out["installment"] = pd.to_numeric(df["installment"], errors="coerce")
    out["grade"] = df["grade"].astype("string").str.strip()
    out["sub_grade"] = df["sub_grade"].astype("string").str.strip()
    out["emp_length_years"] = parse_emp_length(df["emp_length"])
    out["home_ownership"] = df["home_ownership"].astype("string").str.upper().str.strip()
    out["annual_inc"] = pd.to_numeric(df["annual_inc"], errors="coerce")
    out["verification_status"] = df["verification_status"].astype("string").str.strip()
    # ⭐ purpose 是【申请时点】信息，允许进入本表（它不在 LEAKAGE_BLACKLIST 中）
    out["purpose"] = (df["purpose"].astype("string")
                      .str.replace("_", " ", regex=False).str.strip())
    out["addr_state"] = df["addr_state"].astype("string").str.upper().str.strip()
    out["dti"] = pd.to_numeric(df["dti"], errors="coerce")
    out["fico_low"] = pd.to_numeric(df["fico_range_low"], errors="coerce")
    out["fico_high"] = pd.to_numeric(df["fico_range_high"], errors="coerce")

    issue_dt = parse_month(df["issue_d"])
    out["issue_date"] = issue_dt.dt.date
    out["issue_month"] = issue_dt.dt.strftime("%Y-%m")
    out["vintage"] = out["issue_month"]

    # 数据质量标记
    flags = {
        "F1_主键缺失": out["loan_id"].isna(),
        "F2_金额非法": ~(out["loan_amnt"] > 0),
        "F3_日期非法": issue_dt.isna(),
        "F4_利率非法": ~out["int_rate"].between(0, 40),
        "F5_评级非法": ~out["grade"].isin(list("ABCDEFG")),
    }
    dq = pd.Series("PASS", index=df.index, dtype="object")
    for name, mask in flags.items():
        dq = dq.mask(mask.fillna(True), "WARN")
    # 主键缺失 / 金额非法 / 日期非法 -> REJECT（不写入 DWD）
    reject = (out["loan_id"].isna() | ~(out["loan_amnt"] > 0)
              | issue_dt.isna()).fillna(True)
    dq = dq.mask(reject, "REJECT")
    out["dq_level"] = dq
    return out


def main(full_reload: bool = True):
    eng = get_engine()
    log.info(f"读取干净数据：{clean_source_name()}")
    df = read_clean(usecols=READ_COLS)
    log.info(f"读入 {len(df):,} 行")

    clean_df = clean(df)
    n_in = len(clean_df)
    reject = clean_df[clean_df["dq_level"] == "REJECT"]
    keep = (clean_df[clean_df["dq_level"] != "REJECT"]
            .drop_duplicates(subset=["loan_id"])
            .copy())
    dup = n_in - len(reject) - len(keep)
    keep["etl_load_time"] = datetime.now()

    log.info(f"数据质量分级：REJECT {len(reject):,} | 重复 {dup:,} | 保留 {len(keep):,}")
    if len(reject):
        log.info("  REJECT 原因分布：\n"
                 + reject["dq_level"].value_counts().to_string())

    with eng.begin() as conn:
        if full_reload:
            conn.execute(text("TRUNCATE TABLE dwd_loan_fact"))

    for i in range(0, len(keep), BATCH):
        part = keep.iloc[i:i + BATCH]
        part.to_sql("dwd_loan_fact", eng, if_exists="append",
                    index=False, chunksize=2000, method="multi")

    with get_engine().connect() as conn:
        after = conn.execute(text("SELECT COUNT(*) FROM dwd_loan_fact")).scalar()
        dup_in_db = conn.execute(text(
            "SELECT COUNT(*) FROM (SELECT loan_id FROM dwd_loan_fact "
            "GROUP BY loan_id HAVING COUNT(*)>1) t")).scalar()

    log.info(f"DWD 完成：{after:,} 行，主键重复 {dup_in_db} 组")
    log.info(f"行数守恒登记：ODS {n_in:,} − REJECT {len(reject):,} = {len(keep):,}")
    assert after == len(keep)
    assert dup_in_db == 0, "存在重复主键"
    log.info("✅ 行数守恒 + 主键唯一 校验通过")


if __name__ == "__main__":
    main()
