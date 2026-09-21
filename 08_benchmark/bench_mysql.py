"""实验 E5 · MySQL 侧计时：同一查询跑 3 次取中位数。

⚠️ 为什么单独写这个脚本：E5 的 MySQL 组必须与 Spark 组**语义等价**，
   而"语义等价"最容易错的地方是 WHERE 与除零处理 —— 所以把它写成显式 SQL。

用法（宿主侧，venv）：
    .\\.venv\\Scripts\\python.exe 08_benchmark\\bench_mysql.py
    .\\.venv\\Scripts\\python.exe 08_benchmark\\bench_mysql.py e5_mysql_25 e5_mysql_perf_25
"""
from __future__ import annotations

import statistics
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pymysql  # noqa: E402

from src import config  # noqa: E402
from src.utils import get_logger, setup_console_encoding  # noqa: E402

setup_console_encoding()
log = get_logger("bench_mysql")

SQL = """
SELECT f.grade,
       COUNT(*)                                              AS loan_cnt,
       ROUND(SUM(f.funded_amnt), 2)                          AS total_amnt,
       SUM(p.is_bad)                                         AS bad_cnt,
       ROUND(SUM(p.is_bad) / COUNT(*), 6)                    AS bad_rate,
       ROUND(SUM(f.funded_amnt * f.int_rate)
             / NULLIF(SUM(f.funded_amnt), 0), 4)             AS avg_int_rate
FROM {fact} f
JOIN {perf} p ON f.loan_id = p.loan_id
WHERE p.is_terminal = 1
GROUP BY f.grade
ORDER BY f.grade
"""


def run_once(sql: str):
    cfg = config.DB
    conn = pymysql.connect(host=cfg["host"], port=cfg["port"], user=cfg["user"],
                           password=cfg["password"], database=cfg["database"],
                           charset="utf8mb4")
    try:
        t0 = time.perf_counter()
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()      # ⭐ 必须取回结果，否则测的只是"发出查询"
        return time.perf_counter() - t0, rows
    finally:
        conn.close()


def main(fact_table: str = "dwd_loan_fact",
         perf_table: str = "dwd_loan_perf_fact") -> int:
    sql = SQL.replace("{fact}", fact_table).replace("{perf}", perf_table)
    times, rows = [], []
    for i in range(3):
        el, rows = run_once(sql)
        times.append(el)
        log.info(f"第 {i+1} 次：{el:.3f}s，返回 {len(rows)} 行")
    log.info(f"✅ {fact_table}: 三次={['%.3f' % t for t in times]}  "
             f"中位数={statistics.median(times):.3f}s")
    log.info(f"   结果抽查：{rows[:2]}")
    return 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:3]))
