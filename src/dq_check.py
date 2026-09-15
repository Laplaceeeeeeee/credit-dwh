"""数据质量校验：8 项规则，结果写入 dq_check_result。

⭐ 其中 Q8（无未来信息）是本项目的核心亮点：
   用字段黑名单**自动**校验申请时点事实表里没有混入表现期字段。
   数据泄漏不靠自觉，靠机制。
"""
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from sqlalchemy import text

from src import config
from src.utils import get_engine, get_logger, read_sql

log = get_logger("dq_check")
RESULTS = []
BATCH_ID = datetime.now().strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:6]


def record(name, level, table, rule, actual, expect, passed):
    RESULTS.append({
        "check_name": name, "check_level": level, "target_table": table,
        "rule_desc": rule, "actual_value": str(actual)[:128],
        "expect_value": str(expect)[:64], "passed": int(bool(passed)),
        "check_time": datetime.now(), "batch_id": BATCH_ID,
    })
    flag = "✅" if passed else ("❌" if level == "ERROR" else "⚠️")
    log.info(f"{flag} [{level:5}] {name}: 实际={actual} 期望={expect}")


def q1_row_conservation():
    """行数守恒：ODS 应等于 DWD（本数据集已确认无 REJECT 行）"""
    ods = read_sql("SELECT COUNT(*) c FROM ods_loan_raw").iloc[0, 0]
    dwd = read_sql("SELECT COUNT(*) c FROM dwd_loan_fact").iloc[0, 0]
    diff = ods - dwd
    record("Q1_行数守恒", "ERROR", "ods→dwd",
           "ODS 行数 = DWD 行数（差值为 REJECT 行数）",
           f"ODS={ods:,}, DWD={dwd:,}, 差={diff}", "差=0", diff == 0)
    return ods, dwd


def q2_pk_unique():
    """主键唯一"""
    for tbl, col in [("ods_loan_raw", "id"), ("dwd_loan_fact", "loan_id"),
                     ("dwd_loan_perf_fact", "loan_id"), ("dim_applicant", "loan_id")]:
        dup = read_sql(f"SELECT COUNT(*) c FROM (SELECT {col} FROM {tbl} "
                       f"GROUP BY {col} HAVING COUNT(*)>1) t").iloc[0, 0]
        record(f"Q2_主键唯一[{tbl}]", "ERROR", tbl, f"{col} 无重复", dup, 0, dup == 0)


def q3_aggregate_consistency():
    """汇总一致性 ⭐ 最核心：ADS 汇总 vs DWD 明细重算"""
    dwd = read_sql("SELECT COUNT(*) c, SUM(is_bad) b FROM dwd_loan_perf_fact "
                   "WHERE is_terminal=1").iloc[0]
    ads_cnt = read_sql("SELECT SUM(loan_cnt) c FROM ads_risk_segment "
                       "WHERE dim_type='grade'").iloc[0, 0] or 0
    ads_bad = read_sql("SELECT SUM(bad_cnt) c FROM ads_risk_segment "
                       "WHERE dim_type='grade'").iloc[0, 0] or 0
    ok = (int(dwd["c"]) == int(ads_cnt)) and (int(dwd["b"]) == int(ads_bad))
    record("Q3_汇总一致性", "ERROR", "ads_risk_segment",
           "ADS 按评级汇总 = DWD 明细重算（笔数+不良数）",
           f"ADS=({int(ads_cnt):,},{int(ads_bad):,}) DWD=({int(dwd['c']):,},{int(dwd['b']):,})",
           "两者相等", ok)


def q4_amount_non_negative():
    bad = read_sql("SELECT COUNT(*) c FROM dwd_loan_fact WHERE loan_amnt <= 0 "
                   "OR funded_amnt < 0").iloc[0, 0]
    record("Q4_金额非负", "ERROR", "dwd_loan_fact",
           "loan_amnt > 0 且 funded_amnt >= 0", bad, 0, bad == 0)


def q5_enum_valid():
    df = read_sql("SELECT DISTINCT loan_status FROM dwd_loan_perf_fact")
    known = set(config.DPD_MAP.keys())
    unknown = sorted(set(df["loan_status"].dropna()) - known)
    record("Q5_枚举合法", "ERROR", "dwd_loan_perf_fact",
           "loan_status 全部在已知口径集合内",
           f"未知值={unknown if unknown else '无'}", "无未知值", not unknown)


def q6_date_valid():
    bad = read_sql("SELECT COUNT(*) c FROM dwd_loan_fact WHERE issue_date > CURDATE()"
                   ).iloc[0, 0]
    record("Q6_时间合理", "WARN", "dwd_loan_fact",
           "放款日期 <= 当前日期", bad, 0, bad == 0)


def q7_partition_complete():
    """分区（放款月）完整性：月份序列无空洞"""
    df = read_sql("SELECT DISTINCT issue_month FROM dwd_loan_fact ORDER BY 1")
    months = pd.to_datetime(df["issue_month"] + "-01")
    expected = pd.date_range(months.min(), months.max(), freq="MS")
    holes = sorted(set(expected) - set(months))
    record("Q7_分区完整性", "WARN", "dwd_loan_fact",
           "放款月序列无空洞",
           f"缺失 {len(holes)} 个月" + (f"（如 {[str(x)[:7] for x in holes[:3]]}）"
                                     if holes else ""),
           0, len(holes) == 0)


def q8_no_leakage():
    """⭐ 无未来信息：申请时点表不得含任何表现期字段"""
    df = read_sql("SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                  "WHERE TABLE_SCHEMA='credit_dwh' AND TABLE_NAME='dwd_loan_fact'")
    cols = set(df["COLUMN_NAME"])
    leaked = sorted(cols & set(config.LEAKAGE_BLACKLIST))
    record("Q8_无未来信息", "ERROR", "dwd_loan_fact",
           "不含任何表现期字段（防数据泄漏）",
           f"泄漏字段={leaked if leaked else '无'}", "无", not leaked)


def main():
    t0 = time.time()
    log.info("=" * 66)
    log.info(f"数据质量校验开始  batch_id={BATCH_ID}")
    log.info("=" * 66)

    q1_row_conservation()
    q2_pk_unique()
    q3_aggregate_consistency()
    q4_amount_non_negative()
    q5_enum_valid()
    q6_date_valid()
    q7_partition_complete()
    q8_no_leakage()

    eng = get_engine()
    df = pd.DataFrame(RESULTS)
    with eng.begin() as conn:
        conn.execute(text("DELETE FROM dq_check_result WHERE batch_id=:b"),
                     {"b": BATCH_ID})
    df.to_sql("dq_check_result", eng, if_exists="append",
              index=False, chunksize=500)

    n_err = int(((df["check_level"] == "ERROR") & (df["passed"] == 0)).sum())
    n_warn = int(((df["check_level"] == "WARN") & (df["passed"] == 0)).sum())
    log.info("=" * 66)
    log.info(f"校验完成：{int(df['passed'].sum())}/{len(df)} 通过 | "
             f"ERROR 失败 {n_err} | WARN 失败 {n_warn} | 耗时 {time.time()-t0:.1f}s")
    log.info("=" * 66)

    if n_err:
        log.error("❌ 存在 ERROR 级失败，流水线应中止")
        sys.exit(1)
    log.info("✅ 全部 ERROR 级校验通过")


if __name__ == "__main__":
    main()
