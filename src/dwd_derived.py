"""DWD 表现期事实表：派生 is_bad / dpd_bucket / mob_final。

⭐ 数据泄漏防线：所有表现期字段集中于此表，与申请时点表 dwd_loan_fact 物理隔离。
   本表的字段来自 loan_status / out_prncp / total_rec_prncp 等"放款后才知道"的信息。
"""
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from sqlalchemy import text

from src import config
from src.utils import clean_source_name, get_engine, get_logger, read_clean

log = get_logger("dwd_derived")

READ_COLS = [
    "id", "loan_status", "total_rec_prncp", "total_rec_int", "recoveries",
    "out_prncp", "total_pymnt", "last_pymnt_d", "last_pymnt_amnt", "issue_d",
]
BATCH = 20_000
SNAPSHOT_MONTH = pd.Timestamp("2019-03-01")   # 数据快照月（晚于最晚放款 2018-09）


def build(full_reload: bool = True) -> int:
    eng = get_engine()
    log.info(f"读取干净数据：{clean_source_name()}")
    df = read_clean(usecols=READ_COLS)
    log.info(f"读入 {len(df):,} 行")

    out = pd.DataFrame(index=df.index)
    out["loan_id"] = df["id"].astype("string")
    status = df["loan_status"].astype("string").str.strip()
    out["loan_status"] = status

    # ⭐ 口径来自 config：口径字典的代码化，改口径只改一处
    out["is_terminal"] = status.isin(config.TERMINAL_STATUSES).astype(int)
    out["is_bad"] = status.isin(config.BAD_STATUSES).astype(int)
    out["dpd_bucket"] = status.map(config.DPD_MAP).fillna("UNKNOWN")

    for c in ["total_rec_prncp", "total_rec_int", "recoveries", "out_prncp",
              "total_pymnt", "last_pymnt_amnt"]:
        out[c] = pd.to_numeric(df[c], errors="coerce").round(2)

    last = pd.to_datetime(df["last_pymnt_d"], format="%b-%Y", errors="coerce")
    out["last_pymnt_d"] = last.dt.date
    issue = pd.to_datetime(df["issue_d"], format="%b-%Y", errors="coerce")

    # 最终账龄：终态用最后还款月，非终态用数据快照月
    ref = last.where(out["is_terminal"] == 1, SNAPSHOT_MONTH).fillna(SNAPSHOT_MONTH)
    mob = ((ref.dt.year - issue.dt.year) * 12 + (ref.dt.month - issue.dt.month))
    out["mob_final"] = mob.clip(lower=0).astype("Int64")

    out["etl_load_time"] = datetime.now()
    out = out.drop_duplicates(subset=["loan_id"])

    with eng.begin() as conn:
        if full_reload:
            conn.execute(text("TRUNCATE TABLE dwd_loan_perf_fact"))

    for i in range(0, len(out), BATCH):
        out.iloc[i:i + BATCH].to_sql(
            "dwd_loan_perf_fact", eng, if_exists="append",
            index=False, chunksize=2000, method="multi")

    with get_engine().connect() as conn:
        n = conn.execute(text("SELECT COUNT(*) FROM dwd_loan_perf_fact")).scalar()

    log.info(f"表现期事实表完成：{n:,} 行")
    log.info("状态分布：\n" + out["loan_status"].value_counts().to_string())
    term_bad = out.loc[out["is_terminal"] == 1, "is_bad"]
    log.info(f"终态口径不良率 = {100 * term_bad.mean():.4f}%"
             f"（{int(term_bad.sum()):,} / {len(term_bad):,}）")
    assert n == len(out)
    return n


if __name__ == "__main__":
    build()
