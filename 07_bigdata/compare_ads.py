"""第 14 步 · 双引擎逐行比对（Hive 侧 ADS 表 vs MySQL 侧 ADS 表）。

⭐ 为什么不能只比总数：总数一致但分组错位很常见
   （比如 A 级和 B 级的数互换了，总数完全一样）。**必须按唯一键逐行比对。**

⭐ 三个关键设计：
   1. **NULL 安全的连接**：直接 `=` 会漏掉 NULL 行，产生"看似一致"的假象；
      这里两侧都把 NULL 归一成 '__NULL__' 再连接，并用 outer merge 检查是否有未匹配行。
   2. **按列类型给容差**，每一档都有原因（不是"差不多就行"）：
      · 计数：两侧都是整数 → 0 误差
      · 金额：Hive 走 double 累加、MySQL 走 DECIMAL → 末位可能差 1 分
      · 比率：ROUND(x,6) 后允许 1e-6（double vs decimal 的末位差）
      · 利率：ROUND(x,4) 后允许 1e-4
   3. **顺序**：清理 → 导出 → 读取（v2 把"先删后读"写反了，永远读不到数据）。

用法（宿主侧，必须用 venv 的 python）：
    .\\.venv\\Scripts\\python.exe 07_bigdata\\compare_ads.py
"""
from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402

from src.utils import get_logger, read_sql  # noqa: E402

log = get_logger("compare_ads")

EXP = ROOT / "08_benchmark" / "_exp"
REPORT = ROOT / "08_benchmark" / "ads_row_diff.csv"

TOL_INT = 0.0        # 计数类
TOL_AMOUNT = 0.01    # 金额类（DECIMAL(18,2)）
TOL_RATE6 = 5e-5 + 1e-9   # 比率类：见下方"量化"说明（+1e-9 是给"上界本身"的浮点表示留的余量）
TOL_RATE4 = 1e-4     # 利率类 ROUND(x,4)

# ⭐⭐ 为什么比率列的容差是 5e-5 而不是 1e-6（本机逐行比对时实测出来的根因）：
#
#   MySQL 的 **DECIMAL 除法**受 `div_precision_increment`（默认 **4**）限制 ——
#   结果最多保留 4 位小数。所以 MySQL 侧 `ROUND(SUM(is_bad)/COUNT(*), 6)` 算出来的
#   其实是个**已经在第 4 位小数上量化过**的值：
#       MySQL: A 级 bad_rate = 0.060400      （真实分布是 0.060429…）
#       Spark: A 级 bad_rate = 0.060429      （double 除法 + ROUND(,6)）
#   两者最大可能差 **5e-5**（4 位小数量化误差的一半）—— 实测最大差异正好 4.8e-5，吻合。
#
#   所以这里做**两层校验**，而不是简单把容差放大：
#     ① 差异必须 ≤ 5e-5（量化上界，超出就说明是真的算错了）
#     ② **量化到 4 位小数后必须完全相等**（这才是严格的那一层 —— 宽松容差藏不住的
#        真实错误会被它抓住）
#   同样的道理：`avg_int_rate`（目标列 4 位小数 ≤ MySQL 除法精度）两侧**完全一致（0.0）**。
#   —— 这也解释了"为什么只有比率列有差异、金额与利率列没有"。
QUANTIZE4 = {"bad_rate", "rate", "delinq_rate"}

# (显示名, Hive 表, MySQL 表, 唯一键, {列: 容差})
PAIRS = [
    ("风险分层", "ads_risk_segment_lc", "ads_risk_segment",
     ["dim_type", "dim_value"],
     {"loan_cnt": TOL_INT, "total_amnt": TOL_AMOUNT, "bad_cnt": TOL_INT,
      "bad_rate": TOL_RATE6, "avg_int_rate": TOL_RATE4}),
    ("Vintage", "ads_vintage_lc", "ads_vintage",
     ["issue_month", "grade", "mob"],
     {"bad_rate": TOL_RATE6, "exposure_cnt": TOL_INT}),
    ("迁徙率", "ads_roll_rate_lc", "ads_roll_rate",
     ["from_month", "from_bucket", "to_bucket"],
     {"cnt": TOL_INT, "rate": TOL_RATE6}),
    ("逾期月报", "ads_delinq_monthly_lc", "ads_delinq_monthly",
     ["issue_month", "mob"],
     {"loan_cnt": TOL_INT, "bad_cnt": TOL_INT, "delinq_cnt": TOL_INT,
      "delinq_rate": TOL_RATE6, "bad_rate": TOL_RATE6,
      "outstanding_amt": TOL_AMOUNT}),
]


def export_hive() -> None:
    """⭐ 顺序：先清理 → 再导出（v2 把这两步写反了）。"""
    if EXP.exists():
        shutil.rmtree(EXP)
    log.info("已清理旧导出目录，开始导出 Hive 侧 ADS 表 ...")
    r = subprocess.run(
        ["docker", "exec", "bd-spark", "/opt/spark/bin/spark-submit",
         "--master", "local[2]", "/workspace/07_bigdata/export_ads_csv.py"],
        capture_output=True, text=True, encoding="utf-8", errors="replace")
    for line in r.stdout.splitlines():
        if line.startswith("OK "):
            log.info("  " + line)
    if r.returncode != 0:
        log.error(f"导出失败：\n{r.stderr[-1500:]}")
        sys.exit(1)


def read_hive_table(table: str) -> pd.DataFrame:
    d = EXP / table
    parts = sorted(d.glob("*.csv")) if d.exists() else []
    if not parts:
        return pd.DataFrame()
    # ⚠️ dtype=str + keep_default_na=False：避免 pandas 把 '' 与 'NA' 都当成缺失值
    return pd.concat([pd.read_csv(p, dtype=str, keep_default_na=False)
                      for p in parts], ignore_index=True)


def norm_keys(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """唯一键统一成字符串，并把 NULL/空值归一成同一个哨兵值（两侧都要做）。"""
    out = df.copy()
    for k in keys:
        s = out[k].astype(str).str.strip()
        out[k] = s.where(~s.isin(["", "None", "nan", "NaN", "<NA>"]), "__NULL__")
    return out


def main() -> int:
    export_hive()

    all_ok = True
    detail_rows = []

    for name, ht, mt, keys, tolmap in PAIRS:
        log.info("=" * 66)
        log.info(f"比对 {name}：{ht} vs {mt}")
        a = read_hive_table(ht)
        if a.empty:
            log.error(f"❌ {name}: Hive 侧无数据（确认导出是否成功）")
            all_ok = False
            continue
        b = read_sql(f"SELECT * FROM {mt}")
        log.info(f"  行数：Hive={len(a):,}  MySQL={len(b):,}")

        a = norm_keys(a, keys)
        b = norm_keys(b, keys)

        m = a.merge(b, on=keys, how="outer", suffixes=("_lc", "_my"),
                    indicator=True)
        only_lc = int((m["_merge"] == "left_only").sum())
        only_my = int((m["_merge"] == "right_only").sum())
        log.info(f"  未匹配：仅 Hive={only_lc}  仅 MySQL={only_my}")
        if only_lc or only_my:
            all_ok = False
            log.error("  ❌ 存在未匹配的唯一键组合")
            log.error(f"     样例：\n{m.loc[m['_merge'] != 'both', keys].head(5).to_string()}")

        m = m[m["_merge"] == "both"]
        for col, tol in tolmap.items():
            ca, cb = f"{col}_lc", f"{col}_my"
            if ca not in m.columns or cb not in m.columns:
                log.warning(f"  ⚠️ 列缺失：{col}")
                continue
            x = pd.to_numeric(m[ca], errors="coerce")
            y = pd.to_numeric(m[cb], errors="coerce")
            diff = (x - y).abs()
            bad = int((diff > tol).sum())
            maxd = float(diff.max()) if len(diff) else 0.0
            ok = bad == 0

            # ⭐ 第二层（严格）：量化到 4 位小数后，
            #    **不允许出现任何"非平局"的不等行**；
            #    允许的只有落在 4 位小数平局点上的行（其 hive 值 ×10⁴ 的小数部分 ≈ 0.5）。
            #    理由：平局点上 MySQL 用精确十进制 half-up 进位，而 Spark 的 double
            #    可能差一丝而向下舍 —— 这是**表示精度**问题，不是算错。
            q_bad = tie_bad = None
            if col in QUANTIZE4:
                x4, y4 = x * 10000, y * 10000
                q_bad = int((x4.round() != y4.round()).sum())
                frac = x4 - x4.apply(lambda v: int(v) if pd.notna(v) else v)
                is_tie = (frac - 0.5).abs() < 1e-6
                tie_bad = int(((x4.round() != y4.round()) & ~is_tie).sum())
                ok = ok and tie_bad == 0
            all_ok &= ok

            log.info(f"  {'✅' if ok else '❌'} {col:16} 最大差异={maxd:.8f} "
                     f"超容差行数={bad}  容差={tol}"
                     + (f"  量化不等={q_bad}（其中非平局={tie_bad}，须为 0）"
                        if q_bad is not None else ""))
            detail_rows.append({"table": name, "column": col, "max_diff": maxd,
                                "over_tol": bad, "tol": tol,
                                "quantized_mismatch": q_bad,
                                "non_tie_mismatch": tie_bad, "passed": ok})
            if not ok:
                log.error(f"     样例：\n"
                          f"{m.loc[diff > tol, [*keys, ca, cb]].head(3).to_string()}")

    df = pd.DataFrame(detail_rows)
    df.to_csv(REPORT, index=False, encoding="utf-8-sig")
    log.info("=" * 66)
    log.info(f"明细已写入 {REPORT}")
    if all_ok:
        log.info("✅ 全部一致：口径在两种引擎上完全对齐")
        return 0
    log.error("❌ 存在不一致 —— 必须在进入性能实验前修复")
    return 1


if __name__ == "__main__":
    sys.exit(main())
