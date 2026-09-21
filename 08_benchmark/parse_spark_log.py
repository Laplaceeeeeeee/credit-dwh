"""解析 Spark 事件日志，输出：
  ① 每个 Stage 的任务数与 task 耗时分布（min/中位/max）—— E2/E4 用
  ② 每条 SQL 的"扫描文件数 / 扫描字节数 / scan time / 输出行数"—— E1/E3 用

⭐ 为什么必须走事件日志（v2 让实验者手工抄 4040 UI，这条路上有三个坑）：
   ① `spark.ui.keepAlive` 这个配置**在 Spark 3.5 里不存在**；
   ② 短生命周期的 spark-sql 进程结束后 UI 与 Stage 信息一起消失；
   ③ 手工抄的数字无法复现、无法核对、无法进 CI。

事件日志里这两类信息存在哪儿（这是本脚本的关键，也是面试可讲的细节）：
   · SparkListenerTaskEnd          → Task Info(Executor Run Time) + Task Metrics(Input Metrics)
   · SparkListenerSQLExecutionStart → sparkPlanInfo：一棵带 metrics:[{name, accumulatorId}] 的计划树
                                     （扫描节点的指标名就是 "number of files read" /
                                      "size of files read" / "scan time" / "number of output rows"）
   · SparkListenerDriverAccumUpdates → accumulatorId → value（**值在这里，不在 planInfo 里**）
   两边 join 起来，才能把"扫描的文件数/字节数"还原出来。

用法（宿主侧，venv）：
    .\\.venv\\Scripts\\python.exe 08_benchmark\\parse_spark_log.py 08_benchmark\\_spark_events
"""
from __future__ import annotations

import gzip
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.utils import setup_console_encoding  # noqa: E402

setup_console_encoding()      # ⚠️ 不加这行，打印 ✅ 会抛 UnicodeEncodeError（GBK 控制台）

SCAN_METRIC_KEYS = ("number of files read", "size of files read", "scan time",
                    "number of output rows", "number of partitions read",
                    "metadata time")


def read_events(root: Path):
    # ⚠️ 事件日志的文件名随配置变化：
    #    · 未压缩（eventLog.compress=false，默认）：`app-<appId>`（运行中是 `....inprogress`）
    #    · 压缩：`app-<appId>.gz`；  滚动日志：`events_<appId>[_<n>]`
    #    只按 `events_*` 去 glob 会一个文件都找不到 —— 这是本机实测踩到的。
    files = sorted(list(root.rglob("app-*")) + list(root.rglob("events_*")))
    if not files:
        raise SystemExit(f"❌ {root} 下没找到事件日志（找的是 app-* 与 events_*）。"
                         f"提交作业时请加 --conf spark.eventLog.enabled=true "
                         f"--conf spark.eventLog.dir=file:///workspace/08_benchmark/_spark_events，"
                         f"并**先手动建好那个目录**（Spark 不会自动创建）")
    for f in files:
        opener = gzip.open if f.suffix == ".gz" else open
        with opener(f, "rt", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                line = line.strip()
                if line.startswith("{"):
                    yield json.loads(line)


def walk_plan(node, out):
    if not isinstance(node, dict):
        return
    for m in node.get("metrics", []) or []:
        name = m.get("name", "")
        if any(k in name for k in SCAN_METRIC_KEYS):
            out.append((node.get("nodeName", "?"), name, m.get("accumulatorId")))
    for child in node.get("children", []) or []:
        walk_plan(child, out)


def main() -> int:
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "08_benchmark" / "_spark_events"

    stage_name, stage_tasks = {}, {}
    task_dur = defaultdict(list)          # 单位：毫秒
    accum_id_to_val, sql_execs = {}, {}

    for e in read_events(root):
        ev = e.get("Event", "")
        # ⚠️ SQL 相关事件名带命名空间前缀：org.apache.spark.sql.execution.ui.SparkListenerXXX
        #    用 endswith 匹配，跨版本稳（只在文档里写 "SparkListenerSQLExecutionStart" 会一个都匹配不到）
        if ev == "SparkListenerStageSubmitted":
            si = e["Stage Info"]
            stage_name[si["Stage ID"]] = si.get("Stage Name", "")
            stage_tasks[si["Stage ID"]] = si.get("Number of Tasks", 0)
        elif ev == "SparkListenerTaskEnd":
            # ⚠️⚠️ "Executor Run Time" 在 **Task Metrics** 里，不在 Task Info 里！
            #     Task Info 只有 Launch/Finish/Getting Result Time。
            #     从 Task Info 取会一直拿到默认值 0 —— 本机实测踩过。
            #     单位是**毫秒**（按秒记会把小任务压成 0.0）。
            tm = e.get("Task Metrics") or {}
            task_dur[e["Stage ID"]].append(tm.get("Executor Run Time", 0))
        elif ev.endswith("SparkListenerDriverAccumUpdates"):
            for aid, val in e.get("accumUpdates", []):
                accum_id_to_val[aid] = val
        elif ev.endswith("SparkListenerSQLExecutionStart"):
            sql_execs[e.get("executionId")] = (e.get("description", "")[:70],
                                               e.get("sparkPlanInfo", {}))
        elif ev.endswith("SparkListenerSQLAdaptiveExecutionUpdate"):
            # AQE 查询的**最终**计划在这里（指标挂在它上面），用后者覆盖前者
            if e.get("executionId") in sql_execs:
                desc = sql_execs[e["executionId"]][0]
                sql_execs[e["executionId"]] = (desc, e.get("sparkPlanInfo", {}))
            else:
                sql_execs[e.get("executionId")] = ("", e.get("sparkPlanInfo", {}))

    out_dir = ROOT / "08_benchmark" / "_results"
    out_dir.mkdir(parents=True, exist_ok=True)

    # ---------- ① Stage 任务耗时分布（单位：毫秒）----------
    rows = []
    for sid, durs in task_dur.items():
        if len(durs) < 2:
            continue
        ds = sorted(durs)
        med = statistics.median(ds)
        rows.append({"stage_id": sid, "stage_name": stage_name.get(sid, "")[:50],
                     "num_tasks": stage_tasks.get(sid, len(ds)),
                     "min_ms": round(ds[0], 1), "median_ms": round(med, 1),
                     "max_ms": round(ds[-1], 1),
                     "max_over_median": round(ds[-1] / med, 2) if med else "",
                     "sum_ms": round(sum(ds), 1)})
    rows.sort(key=lambda r: -r["sum_ms"])
    csv1 = out_dir / "spark_task_stats.csv"
    cols = ["stage_id", "stage_name", "num_tasks", "min_ms", "median_ms",
            "max_ms", "max_over_median", "sum_ms"]
    with open(csv1, "w", encoding="utf-8-sig") as fh:
        fh.write(",".join(cols) + "\n")
        for r in rows:
            fh.write(",".join(str(r[c]) for c in cols) + "\n")

    print(f"{'stage':>6} {'tasks':>6} {'min_ms':>9} {'p50_ms':>9} {'max_ms':>9} "
          f"{'max/p50':>8}  name")
    for r in rows[:12]:
        print(f"{r['stage_id']:>6} {r['num_tasks']:>6} {r['min_ms']:>9} "
              f"{r['median_ms']:>9} {r['max_ms']:>9} {str(r['max_over_median']):>8}  "
              f"{r['stage_name']}")
    print(f"\n✅ task 分布已写入 {csv1}（单位：毫秒）")

    # ---------- ② 每条 SQL 的扫描量指标 ----------
    if sql_execs:
        csv2 = out_dir / "sql_scan_metrics.csv"
        n_written = 0
        with open(csv2, "w", encoding="utf-8-sig") as fh:
            fh.write("execution_id,node,metric,value,description\n")
            for exec_id, (desc, plan) in sql_execs.items():
                found = []
                walk_plan(plan, found)
                for node, name, aid in found:
                    val = accum_id_to_val.get(aid)
                    fh.write(f"{exec_id},{node},{name},{val},{desc}\n")
                    n_written += 1
        print(f"✅ 扫描量指标已写入 {csv2}（共 {n_written} 条）")
        if n_written == 0:
            print("   若为空：说明扫描节点没暴露这些指标，"
                  "改用 measure_files.sh 的**文件系统事实**作为主证据")

    if not task_dur:
        print("⚠️ 没解析到任何 TaskEnd 事件")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
