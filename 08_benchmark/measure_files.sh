#!/usr/bin/env bash
# 统计 Hive 表在磁盘上的文件数与大小 —— E1/E2/E3 的硬事实来源。
#
# 用法（宿主侧）：
#   docker exec bd-spark bash /workspace/08_benchmark/measure_files.sh dwd_loan_fact_lc
#   docker exec bd-spark bash /workspace/08_benchmark/measure_files.sh dwd_loan_fact_lc 2007-06
#   docker exec bd-spark bash /workspace/08_benchmark/measure_files.sh a b c --all
#
# 输出：CSV 行  table,files,total_mb,avg_kb,min_kb,max_kb
# ⚠️ 本文件必须是 LF 换行（CRLF 会让 bash 报 "\r: command not found"）。
set -u

BASE=/opt/spark/warehouse/credit_dwh_lc.db
OUT=/workspace/08_benchmark/_results/file_stats.csv
mkdir -p "$(dirname "$OUT")"
[ -f "$OUT" ] || echo "table,files,total_mb,avg_kb,min_kb,max_kb" > "$OUT"

measure() {
  local t="$1" p="${2:-}" dir label row
  dir="$BASE/$t"
  [ -n "$p" ] && dir="$BASE/$t/issue_month=$p"
  label="$t${p:+/$p}"

  if [ ! -d "$dir" ]; then
    echo "❌ 目录不存在：$dir" >&2
    return 1
  fi

  # ⭐ 直接数文件系统：与 Spark 版本无关，数出来的就是引擎要读的文件。
  # ⚠️⚠️ 不能用 `-name '*.orc'` 统计！实测两种写路径的文件命名不同：
  #    · 数据源写路径（非分区 CTAS / INSERT 到转换表）→ `part-xxx.c000.snappy.orc`
  #    · Hive serde 写路径（分区表 CTAS）      → `part-xxx.c000`（**没有扩展名**）
  #    只按 *.orc 统计会把分区表数成 0 个文件（本机实测踩到）。
  #    另外每个数据文件旁边还有一个隐藏的 `.<name>.crc` 校验文件，必须排除。
  row=$(find "$dir" -type f -name 'part-*' ! -name '*.crc' -printf '%s\n' 2>/dev/null | awk '
    {n++; s+=$1; if (n==1 || $1<mn) mn=$1; if ($1>mx) mx=$1}
    END {printf "%d,%.2f,%.1f,%.1f,%.1f", n, s/1048576,
         (n?s/n/1024:0), (n?mn/1024:0), (n?mx/1024:0)}')
  echo "$label,$row" | tee -a "$OUT"
}

# --all：一次性统计四张主表
if [ "${1:-}" = "--all" ]; then
  for t in dwd_loan_fact_lc dwd_loan_perf_fact_lc dws_loan_snapshot_lc dim_applicant_lc; do
    measure "$t"
  done
  echo "--- 仓库合计 ---"
  du -sh "$BASE"
  exit 0
fi

if [ $# -eq 0 ]; then
  echo "用法: $0 <表名> [分区值] | --all" >&2
  exit 2
fi

measure "${1}" "${2:-}"
