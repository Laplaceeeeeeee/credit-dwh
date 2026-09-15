"""公共函数：日志、数据库连接、读取干净数据。

⚠️ 重要设计变更（修正 pandas 限制）：
   pandas 的 skipfooter 与 chunksize/nrows **互斥**，会抛 ValueError：
       ValueError: 'skipfooter' not supported for iteration
       ValueError: 'skipfooter' not supported with 'nrows'
   因此本项目不再在每次读取时跳页脚，而是**先用 prepare_raw.py 把页脚剔除一次**，
   产出 data/clean/loans_full.parquet。之后所有脚本读 Parquet，用 C 引擎，
   既没有页脚问题，也不受 python 引擎的性能拖累。
"""
import logging
import os
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

from src import config


def setup_console_encoding() -> None:
    """修正 Windows 控制台编码。

    ⚠️ 中文 Windows 上 sys.stdout 默认是 GBK（cp936），而脚本里大量使用
       ✅ ❌ ⚠️ 等字符。一旦输出到终端就会抛：
           UnicodeEncodeError: 'gbk' codec can't encode character '\\u2705'
       注意：这不是脚本逻辑错，是环境问题 —— 写文件没事，print 到终端才炸。
       在导入期尽早调用本函数即可全局解决。
    """
    if os.name != "nt":
        return
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass
    # 让子进程也吃到 UTF-8
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")


# 导入即生效：保证后续所有 print / logging 都不会因编码崩溃
setup_console_encoding()


def get_logger(name: str) -> logging.Logger:
    config.LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    fh = logging.FileHandler(config.LOG_DIR / f"{name}.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger


def get_engine():
    """创建数据库引擎。

    ⚠️ 关于排序规则（collation）—— 一个很难查的坑：

        DDL 若显式声明 utf8mb4_unicode_ci，而 pandas 的 to_sql 建表跟随
        MySQL 8 的库默认 utf8mb4_0900_ai_ci，两套规则不同的列做 JOIN 时
        MySQL 会**直接拒绝执行**（不是警告）：

          (1267, "Illegal mix of collations (utf8mb4_0900_ai_ci,IMPLICIT)
                  and (utf8mb4_unicode_ci,IMPLICIT) for operation '='")

        典型触发点：ads_risk_segment 里
        `JOIN dim_applicant a ON f.loan_id = a.loan_id`。

    本项目的处置：**全库统一用 MySQL 8 原生默认 utf8mb4_0900_ai_ci**，
    不在这里 SET NAMES 强制改规则（那样反而会与既有表打架）。
    统一由 sql/ddl/01_schema.sql 与 src/dim_build.py 的 unify_collation 保证。
    """
    cfg = config.DB
    url = (f"mysql+pymysql://{cfg['user']}:{cfg['password']}"
           f"@{cfg['host']}:{cfg['port']}/{cfg['database']}?charset={cfg['charset']}")
    return create_engine(url, pool_pre_ping=True, pool_recycle=3600)


def clean_source() -> Path:
    """返回当前规模下应读取的干净数据文件。"""
    if config.MAX_ROWS is None:
        f = config.CLEAN_PARQUET
    else:
        f = config.CLEAN_PARQUET_SAMPLE
    if not f.exists():
        raise FileNotFoundError(
            f"干净数据不存在：{f}\n"
            f"请先运行：python -m src.prepare_raw"
        )
    return f


def clean_source_name() -> str:
    """当前干净数据文件的可读描述（用于日志）。"""
    return (f"{clean_source().name}"
            f"（MAX_ROWS={config.MAX_ROWS if config.MAX_ROWS else '全量'}）")


def apply_max_rows(df: pd.DataFrame) -> pd.DataFrame:
    """按 MAX_ROWS 截断（全量 Parquet 读取后统一走这里）。"""
    if config.MAX_ROWS is not None and len(df) > config.MAX_ROWS:
        return df.iloc[:config.MAX_ROWS].copy()
    return df


def read_clean(usecols=None, nrows=None, chunksize=None):
    """读取已剔除页脚的干净数据（Parquet，C 引擎）。

    参数与 pd.read_csv 对齐，可传 usecols / nrows / chunksize。
    """
    return pd.read_parquet(
        clean_source(), columns=usecols, engine="pyarrow",
    ) if chunksize is None else _read_clean_iter(usecols, chunksize, nrows)


def _read_clean_iter(usecols, chunksize, nrows):
    # Parquet 原生不支持按行分块迭代，用 pyarrow 的 iter_batches 实现
    import pyarrow.parquet as pq
    pf = pq.ParquetFile(clean_source())
    remaining = nrows
    for batch in pf.iter_batches(batch_size=chunksize, columns=usecols):
        df = batch.to_pandas()
        if remaining is not None:
            if remaining <= 0:
                break
            df = df.iloc[:remaining]
            remaining -= len(df)
        yield df


def row_limit_reached(seen: int) -> bool:
    """配合 MAX_ROWS 使用：判断是否已读够。"""
    return config.MAX_ROWS is not None and seen >= config.MAX_ROWS


def run_sql(sql: str, params: dict | None = None):
    eng = get_engine()
    with eng.begin() as conn:
        return conn.execute(text(sql), params or {})


def read_sql(sql: str, params: dict | None = None) -> pd.DataFrame:
    return pd.read_sql(text(sql), get_engine(), params=params or {})


def df_to_table(df: pd.DataFrame, table: str, if_exists="append", chunksize=5000):
    df.to_sql(table, get_engine(), if_exists=if_exists,
              index=False, chunksize=chunksize, method="multi")


def describe_scale() -> str:
    n = config.MAX_ROWS
    return f"全量（{n if n else '2,260,700'} 行）" if n else "全量 2,260,700 行"
