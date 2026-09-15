"""DWS 层：月末贷款快照事实表（周期快照事实表）。

⭐ 为什么用周期快照而不是 SCD2 拉链表：
   本数据集是**放款时点的静态快照**，不存在真实的客户属性变更序列。
   硬做 SCD2 只能造假数据。月末快照是标准数仓模式，数据真实可校验，
   并且天然支撑 Vintage 与迁徙率分析。

⭐ 四个必须注意的实现细节（都是踩过的坑）：

   1. 月份字符串用 pd.DateOffset 生成，不要手工拼 year/month
      （手工公式在 12 月边界极易写错，曾在早期版本写错过）

   2. months_since_last_pymnt 必须先 fillna 再比较：
      Int64 的 NA 参与比较得到 NA，mask 时**不会覆盖原值**，
      会把"从未还款"的坏账静默判成 CURRENT（= 未曾逾期），严重失真。
      实测全量有 148 笔这样的贷款。

   3. mob 上限必须限制：
      行数 ≈ 贷款数 × 观察月数，不限制会是上亿行。

   4. ⭐ 插入性能：**必须先删二级索引、灌完数再重建**。
      本表有 1 主键 + 3 个二级索引，逐行插入时每行要维护 4 个索引，
      随着表变大索引页分裂，速度从 5000 行/秒一路掉到 2000 行/秒。
      实测：先删索引后批量灌数可把 44.7M 行从 2.5 小时降到约 20 分钟。
"""
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd
import pymysql

from src import config
from src.utils import clean_source_name, get_logger, read_clean

log = get_logger("dws_snapshot")

SNAPSHOT_END = pd.Timestamp("2019-03-01")   # 数据快照月

# 最长观察期（月）
#   12 → 覆盖首年表现（年化不良率的关键窗口）
#   24 → 覆盖 2 年期（36 months）贷款完整生命周期  ← 本项目采用
#   36 → 覆盖 3 年期，但行数增至 5900 万，边际收益低
# 实测行数：MOB12=2521万 / MOB24=4472万 / MOB36=5916万
MOB_MAX = 24

READ_COLS = ["id", "issue_d", "loan_status", "last_pymnt_d",
             "out_prncp", "total_rec_prncp", "loan_amnt", "grade"]

TARGET = "dws_loan_snapshot_m"
COLUMNS = ["loan_id", "snapshot_month", "snapshot_date", "issue_month", "mob",
           "months_since_last_pymnt", "dpd_bucket", "is_delinquent",
           "is_bad_month", "out_prncp", "total_rec_prncp", "loan_amnt",
           "grade", "etl_load_time"]

# 灌数前要先删掉的二级索引（主键无法删，只能留）
SECONDARY_INDEXES = {
    "idx_snapshot_mob": "(`snapshot_month`, `mob`)",
    "idx_issue_mob": "(`issue_month`, `mob`)",
    "idx_dpd": "(`snapshot_month`, `dpd_bucket`)",
}

INSERT_BATCH = 5_000


def month_index(ts: pd.Series) -> pd.Series:
    """时间 -> 绝对月序号 (year*12+month)，便于做月份差。"""
    return (ts.dt.year * 12 + ts.dt.month).astype("Int64")


def mi_to_month_str(mi: int) -> str:
    """月序号 -> 'YYYY-MM'。

    ⚠️ 用 epoch 基准 + DateOffset，不要手工写 mi//12 / mi%12 那套公式：
       手工公式在 12 月边界上极易写错。
    """
    epoch = pd.Timestamp("2000-01-01")
    base_mi = 2000 * 12 + 1
    return (epoch + pd.DateOffset(months=int(mi) - base_mi)).strftime("%Y-%m")


def py_conn(local_infile: bool = False):
    c = config.DB
    return pymysql.connect(host=c["host"], port=c["port"], user=c["user"],
                           password=c["password"], database=c["database"],
                           charset="utf8mb4", local_infile=local_infile,
                           autocommit=False)


def drop_secondary_indexes():
    conn = py_conn()
    try:
        cur = conn.cursor()
        for name in SECONDARY_INDEXES:
            try:
                cur.execute(f"ALTER TABLE {TARGET} DROP INDEX {name}")
                log.info(f"  已删除索引 {name}")
            except pymysql.err.OperationalError as e:
                if e.args[0] == 1091:      # 索引不存在
                    log.info(f"  索引 {name} 不存在，跳过")
                else:
                    raise
        conn.commit()
    finally:
        conn.close()


def rebuild_secondary_indexes():
    conn = py_conn()
    try:
        cur = conn.cursor()
        for name, cols in SECONDARY_INDEXES.items():
            t0 = time.time()
            log.info(f"  正在重建索引 {name} ...")
            cur.execute(f"ALTER TABLE {TARGET} ADD INDEX {name} {cols}")
            conn.commit()
            log.info(f"  ✅ {name} 重建完成（{time.time() - t0:.1f}s）")
    finally:
        conn.close()


def build(full_reload: bool = True) -> int:
    t0 = time.time()

    log.info(f"读取干净数据：{clean_source_name()}")
    df = read_clean(usecols=READ_COLS)
    log.info(f"读入 {len(df):,} 行")

    # ---------- 解析与标准化 ----------
    df["loan_id"] = df["id"].astype("string")
    df["issue_dt"] = pd.to_datetime(df["issue_d"], format="%b-%Y", errors="coerce")
    df["last_pymnt_dt"] = pd.to_datetime(df["last_pymnt_d"], format="%b-%Y",
                                        errors="coerce")
    status = df["loan_status"].astype("string").str.strip()
    df["is_bad_loan"] = status.isin(config.BAD_STATUSES)

    df = df[df["issue_dt"].notna()].copy()
    df["issue_mi"] = month_index(df["issue_dt"])
    df["last_pymnt_mi"] = month_index(df["last_pymnt_dt"])
    df["out_prncp"] = pd.to_numeric(df["out_prncp"], errors="coerce").fillna(0)
    df["total_rec_prncp"] = pd.to_numeric(df["total_rec_prncp"],
                                          errors="coerce").fillna(0)
    df["loan_amnt"] = pd.to_numeric(df["loan_amnt"], errors="coerce")

    n_no_pymnt = int(df["last_pymnt_mi"].isna().sum())
    log.info(f"last_pymnt_d 为空的行数：{n_no_pymnt:,}"
             f"（无还款记录，需 fillna 处理，否则误判为 CURRENT）")

    # 推定核销月：终态核销 -> 最后还款月 + 6；无还款记录则用放款月 + 12
    df["charge_off_mi"] = np.where(
        df["is_bad_loan"],
        df["last_pymnt_mi"].fillna(df["issue_mi"] + 12).astype("float64") + 6,
        np.nan,
    )

    min_mi = int(df["issue_mi"].min())
    max_mi = int(month_index(pd.Series([SNAPSHOT_END]))[0])
    log.info(f"观测月范围：{mi_to_month_str(min_mi + 1)} ~ {mi_to_month_str(max_mi)}"
             f"（共 {max_mi - min_mi} 个月）｜ mob <= {MOB_MAX}")

    # 预估行数（用于进度百分比）
    total_avail = int((max_mi - df["issue_mi"]).clip(upper=MOB_MAX).clip(lower=0).sum())
    log.info(f"预估快照总行数：{total_avail:,}")

    # ---------- 清空 + 删二级索引 ----------
    if full_reload:
        conn = py_conn()
        try:
            with conn.cursor() as cur:
                log.info(f"TRUNCATE {TARGET}")
                cur.execute(f"TRUNCATE TABLE {TARGET}")
            conn.commit()
        finally:
            conn.close()
    drop_secondary_indexes()

    # ---------- 逐月生成并灌数 ----------
    conn = py_conn()
    cur = conn.cursor()
    placeholders = ",".join(["%s"] * len(COLUMNS))
    insert_sql = (f"INSERT INTO {TARGET} ({','.join(COLUMNS)}) "
                  f"VALUES ({placeholders})")

    total = 0
    t_load = time.time()
    try:
        for mi in range(min_mi + 1, max_mi + 1):
            snap = df[df["issue_mi"] < mi]
            if snap.empty:
                continue
            snap = snap[(mi - snap["issue_mi"]) <= MOB_MAX]
            if snap.empty:
                continue
            snap = snap.copy()
            snap["mob"] = (mi - snap["issue_mi"]).astype(int)

            # ⚠️ 先 fillna(-1) 再比较：Int64 的 NA 比较得到 NA，mask 不覆盖
            ms = (mi - snap["last_pymnt_mi"]).fillna(-1)

            bucket = pd.Series("CURRENT", index=snap.index, dtype="object")
            bucket = bucket.mask(ms >= 1, "DPD_1_30")
            bucket = bucket.mask(ms >= 2, "DPD_31_120")
            bucket = bucket.mask(ms >= 4, "DPD_120_PLUS")
            charged = snap["is_bad_loan"] & (snap["charge_off_mi"] <= mi)
            bucket = bucket.mask(charged.fillna(False), "CHARGED_OFF")
            snap["dpd_bucket"] = bucket

            snap["is_delinquent"] = (
                ~snap["dpd_bucket"].isin(["CURRENT", "PAID"])).astype(int)
            snap["is_bad_month"] = (snap["dpd_bucket"] == "CHARGED_OFF").astype(int)
            snap["months_since_last_pymnt"] = ms.clip(lower=0).astype(int)

            month_str = mi_to_month_str(mi)
            snap["snapshot_month"] = month_str
            snap["snapshot_date"] = (pd.Timestamp(month_str + "-01")
                                     + pd.offsets.MonthEnd(0)).strftime("%Y-%m-%d")
            snap["issue_month"] = snap["issue_dt"].dt.strftime("%Y-%m")
            snap["etl_load_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            out = snap[COLUMNS]
            rows = [tuple(r) for r in out.itertuples(index=False, name=None)]
            for i in range(0, len(rows), INSERT_BATCH):
                cur.executemany(insert_sql, rows[i:i + INSERT_BATCH])
            conn.commit()
            total += len(out)

            pct = 100 * total / total_avail if total_avail else 0
            rate = total / max(time.time() - t_load, 1e-9)
            log.info(f"  {month_str}: +{len(out):>9,}  累计 {total:>12,}"
                     f"  ({pct:5.1f}%)  {rate:>8,.0f} 行/秒")
    finally:
        cur.close()
        conn.close()

    load_sec = time.time() - t_load
    log.info(f"灌数完成：{total:,} 行，耗时 {load_sec / 60:.1f} 分钟"
             f"（{total / max(load_sec, 1e-9):,.0f} 行/秒）")

    # ---------- 重建二级索引 ----------
    log.info("开始重建二级索引 ...")
    rebuild_secondary_indexes()

    # ---------- 校验 ----------
    conn = py_conn()
    try:
        with conn.cursor() as c2:
            c2.execute(f"SELECT COUNT(*) FROM {TARGET}")
            n = c2.fetchone()[0]
            c2.execute(f"SELECT MIN(snapshot_month), MAX(snapshot_month) FROM {TARGET}")
            rng = c2.fetchone()
    finally:
        conn.close()

    log.info("=" * 62)
    log.info(f"✅ 快照表完成：{n:,} 行，观测月 {rng[0]} ~ {rng[1]}")
    log.info(f"总耗时 {(time.time() - t0) / 60:.1f} 分钟")
    log.info("=" * 62)
    assert n == total, f"行数不符：库内 {n}，写入 {total}"
    return n


if __name__ == "__main__":
    build()
