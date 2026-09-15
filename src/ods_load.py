"""ODS 层：把干净 Parquet 落地到 MySQL（原样，不做加工）。

⭐ 设计要点：
   1. 读 prepare_raw.py 产出的干净 Parquet，**不再碰 skipfooter**
      （skipfooter 与 chunksize 互斥，原设计在本机 pandas 上直接报错）
   2. 分批 to_sql，避免 226 万行一次性进内存
   3. 可重复运行（幂等）：默认先 TRUNCATE
"""
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from sqlalchemy import text

from src import config
from src.utils import clean_source_name, get_engine, get_logger, read_clean

log = get_logger("ods_load")

# ODS 只落地分析要用的关键列（与 sql/ddl/01_schema.sql 的 ods_loan_raw 一一对应）
ODS_COLS = [
    "id", "loan_amnt", "funded_amnt", "term", "int_rate", "installment",
    "grade", "sub_grade", "emp_length", "home_ownership", "annual_inc",
    "verification_status", "issue_d", "loan_status", "purpose", "title",
    "addr_state", "zip_code", "dti", "delinq_2yrs", "earliest_cr_line",
    "fico_range_low", "fico_range_high", "inq_last_6mths", "open_acc",
    "pub_rec", "revol_bal", "revol_util", "total_acc", "application_type",
    "mort_acc", "pub_rec_bankruptcies", "total_rec_prncp", "total_rec_int",
    "recoveries", "out_prncp", "total_pymnt", "last_pymnt_d",
    "last_pymnt_amnt",
]

BATCH = 20_000


def cast_for_ods(df: pd.DataFrame) -> pd.DataFrame:
    """按 DDL 做类型对齐。

    ⚠️ 数值列统一用 pd.to_numeric 强制转换：
       Parquet 里这些列是 float64，但若原文件某列混入了非数字（如页脚），
       会在写入时报 DataError。显式转换可让问题在源头暴露。
    """
    out = df.copy()

    # 整数/金额/比率类 -> 数值
    num_cols = [
        "loan_amnt", "funded_amnt", "int_rate", "installment", "annual_inc",
        "dti", "delinq_2yrs", "fico_range_low", "fico_range_high",
        "inq_last_6mths", "open_acc", "pub_rec", "revol_bal", "revol_util",
        "total_acc", "mort_acc", "pub_rec_bankruptcies", "total_rec_prncp",
        "total_rec_int", "recoveries", "out_prncp", "total_pymnt",
        "last_pymnt_amnt",
    ]
    for c in num_cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    # 字符串列 -> 去首尾空格；空串转 NA，避免写入空串
    str_cols = [
        "id", "term", "grade", "sub_grade", "emp_length", "home_ownership",
        "verification_status", "issue_d", "loan_status", "purpose", "title",
        "addr_state", "zip_code", "earliest_cr_line", "application_type",
        "last_pymnt_d",
    ]
    for c in str_cols:
        if c in out.columns:
            s = out[c].astype("string").str.strip()
            out[c] = s.mask(s == "", pd.NA)

    out["etl_load_time"] = datetime.now()
    return out


def load_ods(full_reload: bool = True) -> int:
    eng = get_engine()
    t0 = time.time()

    log.info(f"读取干净数据：{clean_source_name()}")
    df = read_clean(usecols=ODS_COLS)
    log.info(f"读入 {len(df):,} 行 × {df.shape[1]} 列")

    df = cast_for_ods(df)

    # 防御性校验：id 必须是纯数字（页脚行会破坏这一点）
    bad_id = (~df["id"].astype(str).str.fullmatch(r"\d+")).sum()
    if bad_id:
        raise ValueError(
            f"发现 {bad_id} 行 id 不是纯数字 —— 干净数据可能被污染，"
            f"请重跑 python -m src.prepare_raw"
        )

    with eng.begin() as conn:
        if full_reload:
            log.info("full_reload=True -> TRUNCATE ods_loan_raw")
            conn.execute(text("TRUNCATE TABLE ods_loan_raw"))

    n = 0
    for i in range(0, len(df), BATCH):
        part = df.iloc[i:i + BATCH]
        part.to_sql("ods_loan_raw", eng, if_exists="append",
                    index=False, chunksize=2000, method="multi")
        n += len(part)
        if n % 200_000 < BATCH:
            log.info(f"  已写入 {n:,} / {len(df):,}")

    with get_engine().connect() as conn:
        after = conn.execute(text("SELECT COUNT(*) FROM ods_loan_raw")).scalar()

    log.info(f"ODS 完成：{after:,} 行，耗时 {time.time() - t0:.1f}s")
    assert after == len(df), f"ODS 行数不符：库内 {after}，期望 {len(df)}"
    log.info("✅ 行数守恒校验通过")
    return after


if __name__ == "__main__":
    load_ods()
