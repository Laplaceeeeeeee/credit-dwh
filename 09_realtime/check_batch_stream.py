#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
M3 · 批流对账：离线链路 vs 实时链路，同一个指标两条独立路径。

规则来源：`docs/SLA与监控.md` §5.3 的第四类 SLA（跨链路对账）。
那条规则从阶段三就写好了，但一直**只有判据没有实现**（因为当时没有实时链路）。
本脚本把它实现掉。

对账口径
--------
  指标：每个放款月的（放款笔数, 放款金额合计）
  离线侧：MySQL `credit_dwh.dwd_loan_fact`  —— 阶段二/三那套 T+1 批处理的结果
  实时侧：Paimon `rt.rt_loan_month_agg`     —— 流上按 issue_month 维护的 upsert 聚合

  ⚠️ 粒度是**放款月**（issue_month, YYYY-MM），不是日。
     SLA 文档原文写的是"日粒度"，那是为"有日事件"的系统写的判据；
     本数据集的 issue_date 一律是"当月 1 日"，根本没有真正的日粒度。
     这里按数据实情降到月粒度，并已在 SLA 文档里回写说明 ——
     **不许**含糊成"做过日粒度对账"。

容差（每一条都要有理由，不写"差不多就行"）
------------------------------------------
  · 计数类：**必须严格相等**（diff = 0）。两侧都在数同一个整数，没有舍入空间。
  · 金额类：目标 diff = 0.00。若出现非 0，脚本会打印最大差值并按
    `AMOUNT_TOLERANCE` 判定。设置该容差是因为两侧的求和实现不同
    （MySQL DECIMAL(14,2) 精确十进制 vs Flink DECIMAL 求和），
    理论上精确一致；一旦出现差异，差异本身就说明有口径或数据问题，
    所以容差只作为"是否失败"的阈值，**最大差值一定会打印出来接受审视**。
  · 月份集合：两侧的 issue_month 集合必须**完全相同**（多一个月/少一个月都算失败）。

产物
----
  09_realtime/_results/batch_stream_diff.csv   逐月明细（含两侧值与差值）
  退出码 0 = 对账通过；1 = 有超容差项
"""

from __future__ import annotations

import csv
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

# ---------------------------------------------------------------- 常量

REPO_ROOT = Path(__file__).resolve().parent.parent
RESULT_DIR = Path(__file__).resolve().parent / "_results"
DIFF_CSV = RESULT_DIR / "batch_stream_diff.csv"

JM_CONTAINER = "rt-jobmanager"
FLINK_USER = "flink"                 # ⚠️ 必须 -u flink，理由见 init/02_prepare_volumes.ps1
SQL_FILE = "/sql/03_export_realtime_agg.sql"
EXPORT_DIR = "/results/rt_loan_month_agg"

MYSQL = dict(host="127.0.0.1", port=3307, user="root", password="root123456",
             database="credit_dwh", charset="utf8mb4")

AMOUNT_TOLERANCE = Decimal("0.00")   # 见文件头"容差"一节
CNT_TOLERANCE = 0                    # 计数必须严格相等


# ---------------------------------------------------------------- 工具

def _setup_console_encoding() -> None:
    """阶段二的老坑：Windows 控制台默认 GBK，直接 print ✅ 会 UnicodeEncodeError。"""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
        except Exception:
            pass


def run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8",
                          errors="replace")
    if check and proc.returncode != 0:
        raise RuntimeError(
            f"命令失败（exit={proc.returncode}）：{' '.join(cmd)}\n"
            f"--- stdout ---\n{proc.stdout}\n--- stderr ---\n{proc.stderr}"
        )
    return proc


def dexec(args: list[str], as_flink: bool = False) -> subprocess.CompletedProcess:
    cmd = ["docker", "exec"]
    if as_flink:
        cmd += ["-u", FLINK_USER]
    cmd.append(JM_CONTAINER)
    return run(cmd + args)


# ---------------------------------------------------------------- 实时侧

def export_realtime_agg() -> list[dict]:
    """用 Flink batch 读 Paimon 实时指标表，经 filesystem sink 落 CSV，再读回来。"""
    print("[1/4] 导出实时侧指标（Flink batch + filesystem sink）")
    dexec(["rm", "-rf", EXPORT_DIR], as_flink=True)

    proc = dexec(["/opt/flink/bin/sql-client.sh", "-f", SQL_FILE], as_flink=True)
    if "ERROR" in proc.stdout or "Exception" in proc.stdout:
        raise RuntimeError("导出 SQL 执行失败：\n" + proc.stdout[-3000:])

    cat = dexec(["sh", "-c", f"cat {EXPORT_DIR}/part-*"])
    rows: list[dict] = []
    for line in cat.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(",")
        if len(parts) != 3:
            raise RuntimeError(f"实时侧 CSV 列数异常（期望 3）：{line!r}")
        month, cnt, amt = parts
        rows.append({"issue_month": month, "loan_cnt": int(cnt),
                     "funded_sum": Decimal(amt)})
    print(f"      实时侧拿到 {len(rows)} 个放款月")
    return rows


# ---------------------------------------------------------------- 离线侧

def query_offline_agg() -> list[dict]:
    """离线侧：直接问 MySQL（阶段二/三那套批处理的结果就落在这里）。"""
    print("[2/4] 查询离线侧指标（MySQL dwd_loan_fact）")
    import pymysql

    sql = """
        SELECT issue_month,
               COUNT(*)                      AS loan_cnt,
               ROUND(SUM(funded_amnt), 2)    AS funded_sum
        FROM dwd_loan_fact
        WHERE issue_month IS NOT NULL
        GROUP BY issue_month
        ORDER BY issue_month
    """
    conn = pymysql.connect(**MYSQL)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = [{"issue_month": m, "loan_cnt": int(c),
                     "funded_sum": Decimal(str(s))} for m, c, s in cur.fetchall()]
    finally:
        conn.close()
    print(f"      离线侧拿到 {len(rows)} 个放款月")
    return rows


# ---------------------------------------------------------------- 比对

def compare(offline: list[dict], realtime: list[dict]) -> tuple[list[dict], dict]:
    off = {r["issue_month"]: r for r in offline}
    rt = {r["issue_month"]: r for r in realtime}

    only_off = sorted(set(off) - set(rt))
    only_rt = sorted(set(rt) - set(off))

    details: list[dict] = []
    for month in sorted(set(off) & set(rt)):
        o, r = off[month], rt[month]
        cnt_diff = r["loan_cnt"] - o["loan_cnt"]
        sum_diff = r["funded_sum"] - o["funded_sum"]
        ok = abs(cnt_diff) <= CNT_TOLERANCE and abs(sum_diff) <= AMOUNT_TOLERANCE
        details.append({
            "issue_month": month,
            "offline_cnt": o["loan_cnt"], "realtime_cnt": r["loan_cnt"],
            "cnt_diff": cnt_diff,
            "offline_sum": f"{o['funded_sum']:.2f}", "realtime_sum": f"{r['funded_sum']:.2f}",
            "sum_diff": f"{sum_diff:.2f}",
            "status": "OK" if ok else "OVER_TOLERANCE",
        })

    total_off = sum(r["loan_cnt"] for r in offline)
    total_rt = sum(r["loan_cnt"] for r in realtime)
    amt_off = sum((r["funded_sum"] for r in offline), Decimal("0"))
    amt_rt = sum((r["funded_sum"] for r in realtime), Decimal("0"))

    summary = {
        "months_offline": len(offline),
        "months_realtime": len(realtime),
        "only_offline": only_off,
        "only_realtime": only_rt,
        "over_tolerance": [d["issue_month"] for d in details
                           if d["status"] != "OK"],
        "max_cnt_diff": max((abs(d["cnt_diff"]) for d in details), default=0),
        "max_sum_diff": max((abs(Decimal(d["sum_diff"])) for d in details),
                            default=Decimal("0")),
        "total_cnt_offline": total_off, "total_cnt_realtime": total_rt,
        "total_sum_offline": amt_off, "total_sum_realtime": amt_rt,
    }
    return details, summary


# ---------------------------------------------------------------- 主流程

def main() -> int:
    _setup_console_encoding()
    RESULT_DIR.mkdir(parents=True, exist_ok=True)

    realtime = export_realtime_agg()
    offline = query_offline_agg()

    print("[3/4] 逐月比对")
    details, s = compare(offline, realtime)

    with DIFF_CSV.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(details[0].keys()))
        w.writeheader()
        w.writerows(details)

    print("[4/4] 结论")
    print(f"      放款月数：离线 {s['months_offline']} / 实时 {s['months_realtime']}")
    print(f"      仅在离线：{s['only_offline'] or '无'}")
    print(f"      仅在实时：{s['only_realtime'] or '无'}")
    print(f"      最大计数差：{s['max_cnt_diff']}")
    print(f"      最大金额差：{s['max_sum_diff']}")
    print(f"      总额：离线 {s['total_sum_offline']} vs 实时 {s['total_sum_realtime']}"
          f"  （笔数 {s['total_cnt_offline']} vs {s['total_cnt_realtime']}）")
    print(f"      逐月明细已写入 {DIFF_CSV.relative_to(REPO_ROOT)}")

    failed = bool(s["only_offline"] or s["only_realtime"] or s["over_tolerance"])
    if failed:
        print(f"\n❌ 批流对账未通过（超容差月份：{s['over_tolerance']}）")
        return 1
    print("\n✅ 批流对账通过：逐月笔数与金额两侧完全一致")
    return 0


if __name__ == "__main__":
    sys.exit(main())
