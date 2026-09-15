"""公共函数：日志、数据库连接、读取原始文件。"""
import logging
import sys
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from src import config

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
    cfg = config.DB
    url = (f"mysql+pymysql://{cfg['user']}:{cfg['password']}"
           f"@{cfg['host']}:{cfg['port']}/{cfg['database']}?charset={cfg['charset']}")
    return create_engine(url, pool_pre_ping=True, pool_recycle=3600)


def read_raw_csv(usecols=None, nrows=None) -> pd.DataFrame:
    """
    读取原始 gz 文件。
    ⚠️ 关键：原始文件最后一行是页脚文本（Total amount funded in policy code 2: ...），
    必须用 skipfooter=1 跳过，否则会被当成数据行；skipfooter 需要 engine='python'。
    """
    df = pd.read_csv(
        config.RAW_FILE,
        compression="gzip",
        usecols=usecols,
        nrows=nrows,
        skipfooter=1,          # ⭐ 跳过页脚
        engine="python",       # skipfooter 仅 python 引擎支持
    )
    return df


def run_sql(sql: str, params: dict | None = None):
    eng = get_engine()
    with eng.begin() as conn:
        return conn.execute(text(sql), params or {})


def read_sql(sql: str, params: dict | None = None) -> pd.DataFrame:
    return pd.read_sql(text(sql), get_engine(), params=params or {})


def df_to_table(df: pd.DataFrame, table: str, if_exists="append", chunksize=5000):
    df.to_sql(table, get_engine(), if_exists=if_exists,
              index=False, chunksize=chunksize, method="multi")