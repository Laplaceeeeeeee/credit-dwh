"""预处理：一次性剔除页脚，产出干净数据。

为什么需要这一步（这是本项目第一个真实工程决策）：
  pandas 的 skipfooter 与 chunksize / nrows **互斥**，无法在分块读取时跳页脚。
  若坚持每次读取都用 skipfooter，就只能用 python 引擎一次性全量读，
  既慢（226 万行要 20-40 分钟）又无法分块。
  正确做法：**只在这一个脚本里处理页脚**，之后所有下游脚本读干净文件。

产出：
  data/clean/loans_full.parquet    全量（剔除页脚）
  data/clean/loans_sample.parquet  子集（默认 20 万行，调流程用）
  data/clean/footer_line.txt       被剔除的页脚原文（留证）
  data/clean/clean_meta.json       行数等元信息
"""
import gzip
import json
import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src import config          # noqa: E402
from src.utils import get_logger  # noqa: E402

log = get_logger("prepare_raw")

SAMPLE_ROWS = 200_000
CHUNK_LINES = 200_000


def read_raw_stream(skiprows_fn=None, chunksize=CHUNK_LINES, usecols=None,
                    dtype=None):
    """流式读取原始 gz（C 引擎 + 分块）。

    ⚠️ 不使用 skipfooter —— 它与 chunksize 互斥，会抛
       ValueError: 'skipfooter' not supported for iteration
       页脚改用 skiprows 剔除。

    ⚠️ dtype 必须传：分块读取时 pandas 逐块推断类型，同一列在不同块可能
       推断成不同类型（本文件里 verification_status_joint 就是如此），
       导致写 Parquet 时报 schema 不匹配。传死 dtype 从源头消除该问题。
    """
    return pd.read_csv(
        config.RAW_FILE,
        compression="gzip",
        chunksize=chunksize,
        usecols=usecols,
        dtype=dtype,
        skiprows=skiprows_fn,
        low_memory=False,
    )


def scan_physical_lines() -> tuple[int, list[str], set[int], int]:
    """单次物理扫描：统计总行数，并找出**全部**页脚行。

    ⚠️⚠️ 这是本项目最重要的一条数据事实，务必理解：

        这个 374MB 的文件**不是一份单纯的导出**，而是多个年度分段拼接而成，
        段与段之间夹着章节标题与汇总行。实测（2026-09-15）共找到 **33 行**非数据行：

          · 16 对汇总行（32 行），每对形如：
              Total amount funded in policy code 1: 6417608175,,,,...
              Total amount funded in policy code 2: 1944088810,,,,...
          · 1 行章节标题：
              Loans that do not meet the credit policy,,,,...

        关键点：这些行**散布在文件中部**（首个出现在第 421,096 行，
        间隔约 10~23 万行出现一次），**不是只在文件末尾**！

        严重后果（这也是本项目踩过的最大的坑）：
          - 用 skipfooter=1 只能删掉最后一行，**另外 32 行会混进数据**
          - 它们落在 id 列里，值形如 'Total amount funded in policy code 1: ...'
          - 更糟：pandas 会因这些非数字值把 id 推断成字符串，
            在分块写入时引发 dtype/schema 不一致，
            报错 "could not convert string to float: 'Total amount funded...'"
          - 所以真实数据行数是 2,260,668，**不是** 2,260,700

        判定规则（简单且稳健）：**id 列（首字段）必须是纯数字**，
        凡不满足者皆为非数据行。这样无需枚举所有页脚文案。

    返回 (总行数, 页脚行原文列表, 页脚行 0-based 行号集合, 数据行数)
    """
    log.info("单次扫描整个文件：统计总行数并定位【全部】页脚行 ...")
    n = 0
    footer_texts: list[str] = []
    footer_rows: set[int] = set()
    with gzip.open(config.RAW_FILE, "rt", encoding="utf-8", errors="replace") as f:
        for line in f:
            idx = n            # 0-based 行号
            n += 1
            if idx == 0:
                continue       # 表头
            first_field = line.split(",", 1)[0].strip()
            if (first_field.lower().startswith("total")
                    or not first_field.replace(".", "", 1).isdigit()):
                footer_rows.add(idx)
                footer_texts.append(line.rstrip("\r\n"))
            if n % 500_000 == 0:
                log.info(f"  已扫描 {n:,} 行，累计发现页脚 {len(footer_rows)} 行")
    data_rows = n - 1 - len(footer_rows)
    log.info(f"扫描完成：物理 {n:,} 行 = 表头 1 + 页脚 {len(footer_rows)}"
             f" + 数据 {data_rows:,}")
    return n, footer_texts, footer_rows, data_rows


def detect_schema(skiprows_fn, drop_all_null: bool = True,
                  drop_all_empty: bool = True):
    """用首块探测 schema：确定要保留的列与每列 dtype。

    返回 (columns, dtype_map, dropped_all_null, dropped_all_empty)
    """
    log.info("用首块探测 schema ...")
    it = read_raw_stream(skiprows_fn=skiprows_fn, chunksize=CHUNK_LINES)
    first = next(iter(it))
    del it

    dropped_null, dropped_empty = [], []
    for col in list(first.columns):
        s = first[col]
        if drop_all_null and s.isna().all():
            dropped_null.append(col)
            first = first.drop(columns=[col])
            continue
        if drop_all_empty and s.dtype == object:
            nn = s.dropna().astype(str).str.strip()
            if len(nn) == 0 or bool((nn == "").all()):
                dropped_empty.append(col)
                first = first.drop(columns=[col])

    # dtype 映射：数值列用 float64，其余一律 string。
    # 这样任何块都不会再出现"这块 double、那块 string"的分裂。
    #
    # ⚠️ 主键 id 必须强制为 string，不能跟着数值推断走：
    #    若存成 float64，读出来会是 68407277.0，既不好看也会在拼接/比较时出问题，
    #    更要紧的是数值型主键存在精度隐患（未来的长 ID 会丢精度）。
    #    业务主键一律按字符串处理，这是通用规范。
    STRING_KEYS = {"id", "zip_code"}
    dtype_map = {}
    for col in first.columns:
        if col in STRING_KEYS:
            dtype_map[col] = "string"
        elif pd.api.types.is_numeric_dtype(first[col].dtype):
            dtype_map[col] = "float64"
        else:
            dtype_map[col] = "string"

    return list(first.columns), dtype_map, dropped_null, dropped_empty


def write_chunks(path, skiprows_fn, columns, dtype_map,
                 max_rows: int | None = None, progress_every: int = 500_000):
    """按既定 schema 分块写入 Parquet，返回写入行数。

    ⭐ 关键：columns 与 dtype_map 在读取**之前**就定好，并传给每一次 read_csv。
       这样每个 chunk 的 schema 天然一致，不会出现
       "Table schema does not match schema used to create file"。
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    n_written = 0
    writer = None
    try:
        for chunk in read_raw_stream(skiprows_fn=skiprows_fn,
                                     usecols=lambda c: c in columns,
                                     dtype=dtype_map):
            chunk = chunk[columns]
            if max_rows is not None:
                need = max_rows - n_written
                if need <= 0:
                    break
                chunk = chunk.iloc[:need]

            if writer is None:
                writer = pq.ParquetWriter(
                    path,
                    pa.Table.from_pandas(chunk, preserve_index=False).schema,
                    compression="snappy")
            writer.write_table(pa.Table.from_pandas(chunk, preserve_index=False))
            n_written += len(chunk)
            if n_written % progress_every < CHUNK_LINES:
                log.info(f"  进度 {n_written:,} 行")
    finally:
        if writer is not None:
            writer.close()
    return n_written


def main():
    config.DATA_CLEAN.mkdir(parents=True, exist_ok=True)
    t0 = time.time()

    total_lines, footer_texts, footer_rows, data_rows = scan_physical_lines()
    if not footer_rows:
        log.warning("⚠️ 未识别出任何页脚行，请人工确认数据格式")
    log.info(f"发现 {len(footer_rows)} 行页脚（散布在文件中），示例：")
    for line in footer_texts[:3]:
        log.info(f"    {line[:72]!r}")
    if len(footer_texts) > 3:
        log.info(f"    ... 共 {len(footer_texts)} 行")

    config.FOOTER_TXT.write_text("\n".join(footer_texts), encoding="utf-8")
    log.info(f"页脚原文已留证 -> {config.FOOTER_TXT}")

    # ⚠️ skiprows 用 0-based 行号；表头是第 0 行
    skip_fn = lambda i: i in footer_rows          # noqa: E731

    columns, dtype_map, dropped_null, dropped_empty = detect_schema(skip_fn)
    log.info(f"保留列数 {len(columns)}"
             f"（原始 151 − 全空 {len(dropped_null)} − 全空字符串 {len(dropped_empty)}）")
    if dropped_null:
        log.info(f"  剔除的【全空】列：{dropped_null}")
    if dropped_empty:
        log.info(f"  剔除的【全空字符串】列：{dropped_empty}")

    expect_rows = data_rows
    log.info(f"预期干净行数 = {total_lines:,} − 1(表头) − {len(footer_rows)}(页脚)"
             f" = {expect_rows:,}")

    log.info("写入全量 Parquet ...")
    n_total = write_chunks(config.CLEAN_PARQUET, skip_fn, columns, dtype_map)
    log.info(f"✅ 全量 {n_total:,} 行 -> {config.CLEAN_PARQUET}")

    log.info(f"写入子集 Parquet（前 {SAMPLE_ROWS:,} 行）...")
    n_sample = write_chunks(config.CLEAN_PARQUET_SAMPLE, skip_fn, columns,
                            dtype_map, max_rows=SAMPLE_ROWS,
                            progress_every=10**9)
    log.info(f"✅ 子集 {n_sample:,} 行 -> {config.CLEAN_PARQUET_SAMPLE}")

    meta = {
        "file_physical_lines": total_lines,
        "header_lines": 1,
        "footer_lines_skipped": len(footer_rows),
        "footer_row_indices": sorted(footer_rows),
        "real_data_rows": data_rows,
        "clean_rows_full": n_total,
        "clean_rows_sample": n_sample,
        "clean_columns": len(columns),
        "dropped_all_null_columns": dropped_null,
        "dropped_all_empty_string_columns": dropped_empty,
        "footer_text_sample": footer_texts[:4],
        "elapsed_sec": round(time.time() - t0, 1),
    }
    (config.DATA_CLEAN / "clean_meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    log.info("=" * 60)
    log.info(f"文件总行数 {total_lines:,} − 1(表头) − {len(footer_rows)}(页脚)"
             f" = 应为 {expect_rows:,}")
    log.info(f"实际产出 {n_total:,} 行  "
             f"{'✅ 一致' if n_total == expect_rows else '❌ 不一致，需排查'}")
    log.info(f"保留列数 {len(columns)}；耗时 {meta['elapsed_sec']}s")
    log.info("=" * 60)

    assert n_total == expect_rows, (
        f"行数不符：应为 {expect_rows}（{total_lines} − 1 − {len(footer_rows)}），"
        f"实际 {n_total}"
    )


if __name__ == "__main__":
    main()
