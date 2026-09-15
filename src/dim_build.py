"""构建维表：dim_applicant / dim_product / dim_date。

⭐ dim_applicant 的由来：
   原设计是 dim_customer（客户维），但实测 member_id **整列为空**，
   不存在识别"同一客户多笔贷款"的键。因此不虚构客户主键，
   改为**申请人画像维**：粒度 = 一笔贷款，主键 = loan_id，只承载申请时点属性。
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd

from src.utils import clean_source_name, df_to_table, get_logger, read_clean

log = get_logger("dim_build")
BATCH = 20_000


def income_band(x):
    if pd.isna(x):
        return "UNKNOWN"
    if x < 30000:
        return "L0_<3w"
    if x < 60000:
        return "L1_3-6w"
    if x < 100000:
        return "L2_6-10w"
    if x < 200000:
        return "L3_10-20w"
    return "L4_>=20w"


def fico_band(x):
    if pd.isna(x):
        return "UNKNOWN"
    if x < 660:
        return "F0_<660"
    if x < 700:
        return "F1_660-699"
    if x < 740:
        return "F2_700-739"
    if x < 780:
        return "F3_740-779"
    return "F4_>=780"


def build_dim_applicant():
    log.info("构建 dim_applicant ...")
    df = read_clean(usecols=["id", "annual_inc", "home_ownership", "emp_length",
                             "addr_state", "fico_range_low", "dti"])
    out = pd.DataFrame({
        "loan_id": df["id"].astype("string"),
        "annual_inc": pd.to_numeric(df["annual_inc"], errors="coerce"),
        "home_ownership": df["home_ownership"].astype("string").str.upper().str.strip(),
        "emp_length_years": pd.to_numeric(
            df["emp_length"].astype("string").str.extract(r"(\d+)")[0],
            errors="coerce").astype("Int64"),
        "addr_state": df["addr_state"].astype("string").str.upper().str.strip(),
        "fico_low": pd.to_numeric(df["fico_range_low"], errors="coerce"),
        "dti": pd.to_numeric(df["dti"], errors="coerce"),
    })
    out["income_band"] = out["annual_inc"].apply(income_band)
    out["fico_band"] = out["fico_low"].apply(fico_band)
    out = out.dropna(subset=["loan_id"]).drop_duplicates(subset=["loan_id"])

    df_to_table(out, "dim_applicant", if_exists="replace", chunksize=2000)
    log.info(f"  ✅ dim_applicant {len(out):,} 行（loan_id 唯一 "
             f"{out['loan_id'].nunique():,}）")
    return len(out)


def build_dim_product():
    log.info("构建 dim_product ...")
    df = read_clean(usecols=["term", "grade", "int_rate"])
    term_months = pd.to_numeric(
        df["term"].astype("string").str.extract(r"(\d+)")[0], errors="coerce")
    rate = pd.to_numeric(
        df["int_rate"].astype("string").str.replace("%", "", regex=False),
        errors="coerce")
    g = (pd.DataFrame({"term_months": term_months,
                       "grade": df["grade"].astype("string").str.strip(),
                       "int_rate": rate})
         .dropna(subset=["term_months", "grade"])
         .groupby(["term_months", "grade"], as_index=False)
         .agg(avg_int_rate=("int_rate", "mean")))
    g["product_code"] = (g["term_months"].astype(int).astype(str) + "_" + g["grade"])
    g["term_months"] = g["term_months"].astype(int)
    g["avg_int_rate"] = g["avg_int_rate"].round(4)

    def rate_band(r):
        if pd.isna(r):
            return "UNKNOWN"
        if r < 8:
            return "R0_<8"
        if r < 12:
            return "R1_8-12"
        if r < 16:
            return "R2_12-16"
        if r < 20:
            return "R3_16-20"
        return "R4_>=20"

    g["rate_band"] = g["avg_int_rate"].apply(rate_band)
    out = g[["product_code", "term_months", "grade", "avg_int_rate", "rate_band"]]
    df_to_table(out, "dim_product", if_exists="replace")
    log.info(f"  ✅ dim_product {len(out)} 行")
    return len(out)


def build_dim_date(start="2008-01-01", end="2019-12-31"):
    """日期维覆盖范围按实测 issue_d（Apr-2008 ~ Sep-2018）前后各留余量。"""
    log.info(f"构建 dim_date（{start} ~ {end}）...")
    d = pd.date_range(start, end, freq="D")
    out = pd.DataFrame({
        "date_key": d.strftime("%Y%m%d").astype(int),
        "full_date": d.date,
        "year_num": d.year,
        "quarter_num": d.quarter,
        "month_num": d.month,
        "month_name": d.strftime("%b"),
        "year_month": d.strftime("%Y-%m"),
        "week_of_year": d.isocalendar().week.astype(int),
        "day_of_week": d.dayofweek,
        "is_weekend": (d.dayofweek >= 5).astype(int),
    })
    df_to_table(out, "dim_date", if_exists="replace")
    log.info(f"  ✅ dim_date {len(out):,} 行")
    return len(out)


def unify_collation(tables=("dim_applicant", "dim_product", "dim_date")):
    """把 to_sql 建出来的表统一到 utf8mb4_0900_ai_ci。

    ⚠️ 为什么必须做这一步：
       DDL 显式声明了 utf8mb4_unicode_ci，但 pandas 的 to_sql 建表会跟随
       MySQL 8 的库默认排序规则 utf8mb4_0900_ai_ci。两套规则不同的列做 JOIN 时，
       MySQL 直接拒绝执行：

         (1267, "Illegal mix of collations (utf8mb4_0900_ai_ci,IMPLICIT) and
                 (utf8mb4_unicode_ci,IMPLICIT) for operation '='")

       典型触发点：ads_risk_segment 里
       `JOIN dim_applicant a ON f.loan_id = a.loan_id`。

       get_engine() 里还做了 SET NAMES ... COLLATE，两层保险。
    """
    from sqlalchemy import text as _text

    from src.utils import get_engine
    eng = get_engine()
    with eng.begin() as conn:
        for t in tables:
            conn.execute(_text(
                f"ALTER TABLE {t} CONVERT TO CHARACTER SET utf8mb4 "
                f"COLLATE utf8mb4_0900_ai_ci"))
    log.info(f"  ✅ 排序规则已统一为 utf8mb4_0900_ai_ci：{list(tables)}")


if __name__ == "__main__":
    log.info(f"数据源：{clean_source_name()}")
    build_dim_applicant()
    build_dim_product()
    build_dim_date()
    unify_collation()
    log.info("✅ 三张维表构建完成")
