"""双引擎一致性校验：把 Hive 侧指标与 MySQL 侧基准值逐项比对。

⭐ 为什么这么写（v2 的写法在这里是跑不通的）：
   旧写法是 `docker exec ... spark-sql -e "SELECT ..."` 抓标准输出再逐行解析，有两个硬伤：
     ① `--showHeader=false` 是 **Beeline** 的参数，spark-sql CLI 不认；
     ② 解析时跳过"以 2 开头的行"会把 `2260668` 这种返回值当噪音跳掉 → 永远返回 None。
   本脚本把两侧都放在 Spark 里算、在 Spark 里比，失败时进程退出码非 0，可挂进流水线。

在容器内执行：
    /opt/spark/bin/spark-submit --master local[4] /workspace/07_bigdata/check_consistency.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession

BASE = Path("/workspace/08_benchmark/_baseline/mysql_baseline.json")
# ⚠️ 约定：机读产物用 ASCII 文件名与 ASCII 表头（验收脚本/CI 要能稳定读）
OUT = Path("/workspace/08_benchmark/consistency_result.csv")

# 指标名 -> (Hive 侧 SQL, 容差)
# ⭐ 容差不是"随便给个小数"，每一档都有原因：
#    · 计数类：两侧都是整数，必须 0 误差
#    · 金额类：Hive 走 double 累加、MySQL 走 DECIMAL，末位可能差 1 分
#    · 比率类：ROUND(x,6) 之后允许 1e-6 的末位差（double vs decimal 语义差异）
CHECKS: dict[str, tuple[str, float]] = {
    "总放款笔数":   ("SELECT COUNT(*) FROM dwd_loan_fact_lc", 0.0),
    "总放款金额":   ("SELECT ROUND(SUM(funded_amnt),2) FROM dwd_loan_fact_lc", 0.01),
    "终态笔数":     ("SELECT COUNT(*) FROM dwd_loan_perf_fact_lc "
                     "WHERE is_terminal=1", 0.0),
    "不良笔数":     ("SELECT SUM(is_bad) FROM dwd_loan_perf_fact_lc "
                     "WHERE is_terminal=1", 0.0),
    "快照表行数":   ("SELECT COUNT(*) FROM dws_loan_snapshot_lc", 0.0),
    "加权平均利率": ("SELECT ROUND(SUM(funded_amnt*int_rate)/SUM(funded_amnt),6) "
                     "FROM dwd_loan_fact_lc", 1e-6),
    "放款月数":     ("SELECT COUNT(DISTINCT issue_month) FROM dwd_loan_fact_lc", 0.0),
}


def main() -> int:
    if not BASE.exists():
        print(f"❌ 找不到基准文件 {BASE}")
        print("   先在宿主侧跑：.\\.venv\\Scripts\\python.exe 07_bigdata\\export_mysql_tsv.py")
        return 2

    baseline = json.loads(BASE.read_text(encoding="utf-8"))

    spark = (SparkSession.builder
             .appName("stage3-consistency-check")
             .config("spark.sql.catalogImplementation", "hive")
             .enableHiveSupport()
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # ⭐ 切库要用 setCurrentDatabase，**不能**写 spark.sql("USE x; SELECT ...")
    #   —— PySpark 的 spark.sql() 只接受**单条语句**（spark-sql CLI 才支持分号拼接）。
    #   写成 "USE x; SELECT ..." 会报：
    #     ParseException: [PARSE_SYNTAX_ERROR] Syntax error at or near 'SELECT'
    spark.catalog.setCurrentDatabase("credit_dwh_lc")

    print("=" * 74)
    print("Hive/Spark 层 vs MySQL 层 指标一致性校验")
    print("=" * 74)

    rows, all_ok = [], True
    for name, (sql, tol) in CHECKS.items():
        my = float(baseline[name])
        v = spark.sql(sql).collect()[0][0]
        sv = float(v) if v is not None else None
        diff = None if sv is None else abs(my - sv)
        ok = sv is not None and diff <= tol
        all_ok &= ok

        def fmt(x):
            return "None" if x is None else format(x, ",.4f")

        print(f"{'✅' if ok else '❌'} {name:12} MySQL={fmt(my):>18}  "
              f"Spark={fmt(sv):>18}  容差={tol:<8} 差异={diff}")
        rows.append({"metric": name, "mysql": my, "spark": sv,
                     "tol": tol, "diff": diff, "matched": ok})

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as fh:
        fh.write("metric,mysql,spark,tol,diff,matched\n")
        for r in rows:
            fh.write(f"{r['metric']},{r['mysql']},{r['spark']},"
                     f"{r['tol']},{r['diff']},{r['matched']}\n")

    print("=" * 74)
    if all_ok:
        print(f"✅ 全部 {len(rows)} 项指标一致 —— 可以进入第 14 步（口径复现）")
    else:
        print("❌ 存在不一致 —— 必须先查口径，不要带着错误数据做性能实验")
        print("   排查顺序：① 行数是否导全 ② 终态过滤条件是否一致 "
              "③ NULL 处理是否一致 ④ 容差是否给对")
    print(f"明细已写入 {OUT}")
    spark.stop()
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
