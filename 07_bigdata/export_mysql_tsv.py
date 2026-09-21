r"""把阶段二 MySQL 的关键表导出为 TSV，供 Spark 装载；并落地 MySQL 侧基准值。

⚠️ 两种导出方式，按表大小选（这是实测踩出来的结论）：
  ① **小表（几亿字节以内）**：`mysql --batch` 流式导出，最快（288 MB / 5 秒）。
  ② **大表（GB 级）**：用 **服务端 `SELECT ... INTO OUTFILE` + `docker cp`**。
     为什么不用流式？实测导出 4471 万行的快照表（约 5 GB）时，
     GB 级结果集流经 `docker exec` 的 hijacked stream 把 **Docker daemon 卡死**了 ——
     之后 `docker ps` / `docker info` / `docker logs` **全部返回 500**，只能重启 Docker Desktop。
     而 `INTO OUTFILE` 由 MySQL 自己写文件，再 `docker cp` 拷出来，稳得多。

⚠️ 关于 NULL 的表示（**别照抄旧文档**）：
  · `mysql --batch` 在 MySQL 8 客户端下把 NULL 打成**字面量 `NULL`**，不是 `\N`。
    （实测：`SELECT NULL,'a',''` → `NULL<TAB>a<TAB><TAB>`）
  · `INTO OUTFILE ... ESCAPED BY '\\'` 才用 **`\N`** 表示 NULL。
  两种方式在 Spark 侧的 `nullValue` 选项必须对应改，否则会静默变成字符串 "NULL"。

其他设计要点：
· 显式列清单：只导 Hive 侧需要的列（阶段二的 vintage / dq_level 等不迁）
· 导出前后各查一次行数，不一致直接报错退出 —— 不做"看起来成功"的迁移
· 用 Python 直接接管子进程 stdout 写文件：PowerShell 5.1 的 `>` 默认写 UTF-16

用法（宿主侧，必须用 venv 的 python）：
    .\\.venv\\Scripts\\python.exe 07_bigdata\\export_mysql_tsv.py [表名 ...]
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.utils import get_logger, read_sql  # noqa: E402

log = get_logger("export_mysql_tsv")

OUT = ROOT / "07_bigdata" / "exchange"
OUT.mkdir(parents=True, exist_ok=True)
BASELINE_DIR = ROOT / "08_benchmark" / "_baseline"
BASELINE_DIR.mkdir(parents=True, exist_ok=True)

MYSQL_CONTAINER = "credit-dwh-mysql"
MYSQL_USER, MYSQL_PWD, MYSQL_DB = "root", "root123456", "credit_dwh"

# 表 -> 导出列（必须与 07_bigdata/hive/01_ddl.sql 的字段列表严格一一对应）
# ⚠️ 只导 Hive 侧需要的列：阶段二的 vintage / dq_level / etl_load_time / installment 不迁
EXPORTS: dict[str, list[str]] = {
    "dwd_loan_fact": [
        "loan_id", "loan_amnt", "funded_amnt", "term_months", "int_rate",
        "grade", "sub_grade", "emp_length_years", "home_ownership", "annual_inc",
        "verification_status", "purpose", "addr_state", "dti",
        "fico_low", "fico_high", "issue_date", "issue_month",
    ],
    "dwd_loan_perf_fact": [
        "loan_id", "loan_status", "is_terminal", "is_bad", "dpd_bucket",
        "total_rec_prncp", "total_rec_int", "recoveries", "out_prncp",
        "total_pymnt", "last_pymnt_d", "mob_final",
    ],
    "dws_loan_snapshot_m": [
        "loan_id", "snapshot_month", "snapshot_date", "issue_month", "mob",
        "months_since_last_pymnt", "dpd_bucket", "is_delinquent",
        "is_bad_month", "out_prncp", "total_rec_prncp", "loan_amnt", "grade",
    ],
    "dim_applicant": [
        "loan_id", "annual_inc", "income_band", "home_ownership",
        "emp_length_years", "addr_state", "fico_low", "fico_band", "dti",
    ],
}


def mysql_scalar(sql: str) -> str:
    """在容器里执行一条返回单值的 SQL。"""
    r = subprocess.run(
        ["docker", "exec", MYSQL_CONTAINER, "mysql", f"-u{MYSQL_USER}",
         f"-p{MYSQL_PWD}", "--batch", "--skip-column-names", MYSQL_DB,
         "-e", sql],
        capture_output=True, text=True, encoding="utf-8", errors="replace")
    if r.returncode != 0:
        raise RuntimeError(f"MySQL 查询失败：{sql}\n{r.stderr[-800:]}")
    return r.stdout.strip()


def export_table(table: str, cols: list[str]) -> tuple[int, int]:
    r"""流式导出为 TSV（小表用），返回 (MySQL 行数, 落盘行数)。

    ⚠️ NULL 表示：MySQL 8 客户端在 batch 模式下把 NULL 打成**字面量 `NULL`**（不是 `\N`），
       所以 Spark 侧的表选项要写 nullValue 'NULL'。
    """
    sql = f"SELECT {', '.join(cols)} FROM {table}"
    expect = int(mysql_scalar(f"SELECT COUNT(*) FROM {table}"))

    tsv = OUT / f"{table}.tsv"
    t0 = time.time()
    log.info(f"导出 {table}（MySQL 侧 {expect:,} 行）-> {tsv.name} [流式]")

    # ⭐ 二进制写：不做任何编码转换，字节原样落盘
    rows = 0
    with open(tsv, "wb") as fh:
        proc = subprocess.Popen(
            # ⚠️ 必须带 --skip-column-names(-N)：`--batch` **仍会输出表头**，
            #    不加就会多出一行表头，导致"TSV 行数 = MySQL 行数 + 1"。
            #    （本脚本的行数自校验正是为了抓这种事，这次真抓到了。）
            ["docker", "exec", MYSQL_CONTAINER, "mysql", f"-u{MYSQL_USER}",
             f"-p{MYSQL_PWD}", "--batch", "--raw", "--skip-column-names",
             "--default-character-set=utf8mb4", MYSQL_DB, "-e", sql],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        buf = bytearray()
        assert proc.stdout is not None
        while True:
            chunk = proc.stdout.read(1 << 20)          # 1 MB 一块
            if not chunk:
                break
            rows += chunk.count(b"\n")
            buf += chunk
            if len(buf) >= (1 << 22):                  # 攒到 4 MB 落一次盘
                fh.write(bytes(buf))
                buf.clear()
            if rows and rows % 5_000_000 < 1000:
                log.info(f"  ... 已写 {rows:,} 行")
        if buf:
            fh.write(bytes(buf))
        _, err = proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"导出 {table} 失败：{err.decode('utf-8', 'replace')[-800:]}")

    size_mb = tsv.stat().st_size / 1024 / 1024
    log.info(f"  ✅ {table}: {rows:,} 行 / {size_mb:,.1f} MB / {time.time()-t0:.1f}s")
    if rows != expect:
        raise RuntimeError(f"❌ {table} 行数不一致：MySQL={expect:,} TSV={rows:,}")
    return expect, rows


# ⭐ 默认**全部**走 INTO OUTFILE + docker cp：
#   目的是统一 NULL 表示为 `\N`。两种方式混用会出现"同一批 TSV 里
#   一个用字面量 NULL、一个用 \N"，Spark 侧 nullValue 只要写错一处就会静默变脏。
#   想对某张表改回流式：把它从下面这个集合里去掉（然后注意改 nullValue）。
OUTFILE_TABLES = set(EXPORTS)
CONTAINER_TMP = "/var/lib/mysql-files"


def export_table_outfile(table: str, cols: list[str]) -> tuple[int, int]:
    """服务端 `SELECT ... INTO OUTFILE` + `docker cp`（大表用）。

    ⚠️ 这种方式 NULL 是 **`\\N`**（因为 ESCAPED BY '\\\\'），
       与流式方式的字面量 `NULL` 不同 —— Spark 侧 nullValue 选项要对应改。
    """
    expect = int(mysql_scalar(f"SELECT COUNT(*) FROM {table}"))
    inner = f"{CONTAINER_TMP}/{table}.tsv"
    tsv = OUT / f"{table}.tsv"
    t0 = time.time()
    log.info(f"导出 {table}（MySQL 侧 {expect:,} 行）-> {tsv.name} [OUTFILE + docker cp]")

    # OUTFILE 要求目标文件**不存在**，先清掉容器内的旧文件
    subprocess.run(["docker", "exec", MYSQL_CONTAINER, "rm", "-f", inner],
                   capture_output=True)

    sql = (f"SELECT {', '.join(cols)} FROM {table} "
           f"INTO OUTFILE '{inner}' "
           f"FIELDS TERMINATED BY '\\t' ESCAPED BY '\\\\' "
           f"LINES TERMINATED BY '\\n'")
    r = subprocess.run(
        ["docker", "exec", MYSQL_CONTAINER, "mysql", f"-u{MYSQL_USER}",
         f"-p{MYSQL_PWD}", "--default-character-set=utf8mb4", MYSQL_DB, "-e", sql],
        capture_output=True, text=True, encoding="utf-8", errors="replace")
    if r.returncode != 0:
        raise RuntimeError(f"{table} INTO OUTFILE 失败：{r.stderr[-800:]}")
    log.info(f"  服务端写文件完成（{time.time()-t0:.1f}s），开始 docker cp ...")

    t1 = time.time()
    r = subprocess.run(["docker", "cp", f"{MYSQL_CONTAINER}:{inner}", str(tsv)],
                       capture_output=True, text=True, encoding="utf-8",
                       errors="replace")
    if r.returncode != 0:
        raise RuntimeError(f"{table} docker cp 失败：{r.stderr[-800:]}")
    log.info(f"  docker cp 完成（{time.time()-t1:.1f}s）")

    rows = 0
    with open(tsv, "rb") as fh:
        while True:
            chunk = fh.read(1 << 22)
            if not chunk:
                break
            rows += chunk.count(b"\n")

    size_mb = tsv.stat().st_size / 1024 / 1024
    log.info(f"  ✅ {table}: {rows:,} 行 / {size_mb:,.1f} MB / 总耗时 {time.time()-t0:.1f}s")

    # 清掉容器内的临时文件（不占容器可写层）
    subprocess.run(["docker", "exec", MYSQL_CONTAINER, "rm", "-f", inner],
                   capture_output=True)

    if rows != expect:
        raise RuntimeError(f"❌ {table} 行数不一致：MySQL={expect:,} TSV={rows:,}")
    return expect, rows


# MySQL 侧基准指标 —— check_consistency.py 按这些 key 读取
BASELINE_CHECKS: dict[str, str] = {
    "总放款笔数":   "SELECT COUNT(*) FROM dwd_loan_fact",
    "总放款金额":   "SELECT ROUND(SUM(funded_amnt),2) FROM dwd_loan_fact",
    "终态笔数":     "SELECT COUNT(*) FROM dwd_loan_perf_fact WHERE is_terminal=1",
    "不良笔数":     "SELECT SUM(is_bad) FROM dwd_loan_perf_fact WHERE is_terminal=1",
    "快照表行数":   "SELECT COUNT(*) FROM dws_loan_snapshot_m",
    "加权平均利率": ("SELECT ROUND(SUM(funded_amnt*int_rate)/SUM(funded_amnt),6) "
                     "FROM dwd_loan_fact"),
    "放款月数":     "SELECT COUNT(DISTINCT issue_month) FROM dwd_loan_fact",
    "Vintage行数":  "SELECT COUNT(*) FROM ads_vintage",
    "迁徙率行数":   "SELECT COUNT(*) FROM ads_roll_rate",
    "分层行数":     "SELECT COUNT(*) FROM ads_risk_segment",
    "逾期月报行数": "SELECT COUNT(*) FROM ads_delinq_monthly",
}


def export_baseline() -> None:
    baseline: dict[str, object] = {}
    log.info("导出 MySQL 侧基准指标 ...")
    for name, sql in BASELINE_CHECKS.items():
        v = read_sql(sql).iloc[0, 0]
        # Decimal / numpy 类型转成 JSON 能吃的原生类型
        if hasattr(v, "as_integer_ratio") or type(v).__name__ in ("Decimal",):
            v = float(v)
        elif hasattr(v, "item"):
            v = v.item()
        baseline[name] = v
        log.info(f"  基准 {name:14} = {v}")
    out = BASELINE_DIR / "mysql_baseline.json"
    out.write_text(json.dumps(baseline, ensure_ascii=False, indent=2),
                   encoding="utf-8")
    log.info(f"✅ 基准值已写入 {out}")


def main(argv: list[str]) -> int:
    tables = argv or list(EXPORTS)
    unknown = [t for t in tables if t not in EXPORTS]
    if unknown:
        log.error(f"未知表名：{unknown}；可选：{list(EXPORTS)}")
        return 2

    log.info("=" * 66)
    log.info("MySQL → TSV 导出（阶段三迁移第 1 步）")
    log.info("=" * 66)
    t0 = time.time()
    total = 0
    for t in tables:
        # ⭐ 按表大小选方式：GB 级的表走 INTO OUTFILE + docker cp（见文件头说明）
        fn = export_table_outfile if t in OUTFILE_TABLES else export_table
        _, n = fn(t, EXPORTS[t])
        total += n
    log.info("-" * 66)
    export_baseline()
    log.info("=" * 66)
    log.info(f"✅ 导出完成：{len(tables)} 张表 / {total:,} 行 / "
             f"{time.time()-t0:.1f}s")
    log.info(f"   TSV 在 {OUT}（体积大，装载校验通过后可删）")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
