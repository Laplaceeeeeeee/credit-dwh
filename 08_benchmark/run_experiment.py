"""实验测量 harness：跑 SQL 实验文件并自动记录指标。

⭐ 为什么需要它（v2 的做法为什么不行）：
   旧文档让实验者打开 4040 UI 手工抄 Input Size / Files Read / Stage 耗时。
   三个问题：① `spark.ui.keepAlive` 这个配置**在 Spark 3.5 里根本不存在**；
   ② 短命令跑完 UI 就关了；③ 手工抄的数字无法复现、无法核对、无法进 CI。
   本 harness 把"实验定义"留在 SQL 里（单一事实来源），把"测量"自动化。

用法（容器内）：
  docker exec -i bd-spark /opt/spark/bin/spark-submit \
    --master spark://spark:7077 --executor-memory 1500m --total-executor-cores 4 \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=file:///workspace/08_benchmark/_spark_events \
    /workspace/08_benchmark/run_experiment.py \
    /workspace/08_benchmark/exp/E1_pruning.sql [--reps 3] [--dump-metrics]

SQL 文件里的标记语法：
  -- @case <名字>          开始一个实验组（名字里不要有空格）
  -- @conf <k>=<v>         该组生效的 Spark 配置
  -- @var  <k>=<v>         该组生效的文本参数（SQL 里写 ${k}，由 harness 替换）
  -- @reps <n>             该组重复次数（默认 3，取中位数）
每组里**最后一条语句**是被测量的语句（用 collect() 真正触发执行）。

⚠️ 被测量的 case 最好是**只读查询**或 CTAS；建表类 DDL 放单独 case，
   否则第 2、3 次重复会因为"表已存在"而失败（本项目的 case 都自带 DROP IF EXISTS）。
⚠️ 参数替换是 harness 干的（${k} → 值），不是 Spark 自己的变量替换。
⚠️⭐ 用 `spark.eventLog.dir` 之前**必须先建好那个目录**，否则 Spark 起不来：
     java.io.FileNotFoundException: File file:/workspace/08_benchmark/_spark_events does not exist
   宿主侧先跑：New-Item -ItemType Directory -Force 08_benchmark\_spark_events
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession

OUT_DIR = Path("/workspace/08_benchmark/_results")


def parse_exp_file(path: Path) -> list[dict]:
    """把 SQL 文件解析成 [{name, confs, vars, reps, stmts}, ...]。"""
    cases: list[dict] = []
    cur: dict | None = None
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line.startswith("-- @case"):
            # 名字后面可以跟中文说明；把空格换成下划线，避免 CSV 里出现裸空格
            name = line.split(None, 2)[2].strip().replace(" ", "_")
            cur = {"name": name, "confs": {}, "vars": {}, "reps": 3, "stmts": []}
            cases.append(cur)
        elif line.startswith("-- @conf") and cur is not None:
            k, v = line.split(None, 2)[2].strip().split("=", 1)
            cur["confs"][k.strip()] = v.strip()
        elif line.startswith("-- @var") and cur is not None:
            k, v = line.split(None, 2)[2].strip().split("=", 1)
            cur["vars"][k.strip()] = v.strip()
        elif line.startswith("-- @reps") and cur is not None:
            cur["reps"] = int(line.split()[2])
        elif cur is not None:
            cur["stmts"].append(raw)
    for c in cases:                       # 按 ; 切分语句
        text = "\n".join(c["stmts"])
        parts = []
        for s in text.split(";"):
            # ⚠️ 必须先**按行剥掉纯注释行**再判断是否为空：
            #    否则 "-- ---------- 分隔注释 ----------" 这种"只有注释"的片段会被当成
            #    一条 SQL 交给 spark.sql() → ParseException: Syntax error at or near end of input
            body = "\n".join(ln for ln in s.splitlines()
                             if not ln.strip().startswith("--")).strip()
            if body:
                parts.append(body)
        c["stmts"] = parts
    return [c for c in cases if c["stmts"]]


def _walk(node):
    yield node
    for child in node.children:
        yield from _walk(child)


def metric_dump(df) -> dict:
    """尽力抓取物理计划上的 SQL 指标。

    ⚠️ 指标名随 Spark 版本变化，所以第一次请先用 --dump-metrics 看一眼真实名字，
       再把要用的名字写进报告 —— 不要凭记忆猜。
    """
    out = {}
    try:
        plan = df.queryExecution.executedPlan
    except Exception:
        return out
    for node in _walk(plan):
        for k, v in (getattr(node, "metrics", None) or {}).items():
            try:
                key = f"{node.nodeName}:{k}"
                if key not in out:
                    out[key] = v.value
            except Exception:
                continue
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("exp_file")
    ap.add_argument("--reps", type=int, default=None)
    ap.add_argument("--dump-metrics", action="store_true")
    args = ap.parse_args()

    exp_path = Path(args.exp_file)
    exp_name = exp_path.stem

    spark = (SparkSession.builder
             .appName(f"exp-{exp_name}")
             .config("spark.sql.catalogImplementation", "hive")
             .enableHiveSupport()
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    spark.catalog.setCurrentDatabase("credit_dwh_lc")

    rows = []
    for case in parse_exp_file(exp_path):
        reps = args.reps or case["reps"]
        for k, v in case["confs"].items():
            spark.conf.set(k, v)      # ⚠️ 每组前重设，避免上一组污染下一组
        subst = lambda s: _subst(s, case["vars"])          # noqa: E731
        print(f"\n=== {case['name']}（{reps} 次） conf={case['confs']} "
              f"vars={case['vars']} ===")

        times, out_rows, dump = [], None, {}
        stmts = [subst(s) for s in case["stmts"]]
        ok = True
        for i in range(reps):
            for j, stmt in enumerate(stmts):
                last = (j == len(stmts) - 1)
                t0 = time.time()
                try:
                    df = spark.sql(stmt)
                    if last:
                        out_rows = len(df.collect())     # ⭐ 唯一能真正触发执行的写法
                        times.append(time.time() - t0)
                        if args.dump_metrics and i == 0:
                            dump = metric_dump(df)
                        spark.catalog.clearCache()
                        break
                    df.collect()
                except Exception as e:                   # noqa: BLE001
                    ok = False
                    print(f"  ❌ 第 {i+1} 次执行失败：{type(e).__name__}: "
                          f"{str(e)[:300]}")
                    break
            if not ok:
                break

        if times:
            med = statistics.median(times)
            print(f"  耗时: {[round(t, 2) for t in times]}  中位数={med:.2f}s  "
                  f"输出行数={out_rows:,}")
        else:
            med = None
            print("  ⚠️ 没有成功的测量")
        rows.append({"case": case["name"], "reps": reps,
                     "times_s": ";".join(f"{t:.2f}" for t in times),
                     "median_s": round(med, 2) if med is not None else "",
                     "output_rows": out_rows if out_rows is not None else "",
                     "ok": ok})
        if dump:
            print("  --- SQL 指标（前 12 项）---")
            for k, v in list(dump.items())[:12]:
                print(f"      {k} = {v}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_csv = OUT_DIR / f"{exp_name}.csv"
    with open(out_csv, "w", encoding="utf-8") as fh:
        fh.write("case,reps,times_s,median_s,output_rows,ok\n")
        for r in rows:
            fh.write(f"{r['case']},{r['reps']},{r['times_s']},{r['median_s']},"
                     f"{r['output_rows']},{r['ok']}\n")
    print(f"\n✅ 结果已写入 {out_csv}")
    print(json.dumps(rows, ensure_ascii=False))
    spark.stop()
    return 0 if all(r["ok"] for r in rows) else 1


def _subst(s: str, vars_: dict) -> str:
    for k, v in vars_.items():
        s = s.replace("${" + k + "}", v)
    return s


if __name__ == "__main__":
    sys.exit(main())
