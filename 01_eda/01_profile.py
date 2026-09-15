"""EDA 第一步：字段画像（缺失率、类型、取值样例）。

⭐ 修正要点：
   原版直接在原始 gz 上分块读取，但 pandas 的 skipfooter 与 chunksize 互斥，
   会直接抛 ValueError，脚本根本跑不起来。
   现在改为读取 prepare_raw.py 产出的干净 Parquet：
     - 无页脚问题（已剔除 33 行页脚）
     - 用 C 引擎，速度提升一个量级
     - 类型已固定，不会出现块间 schema 漂移
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from src import config
from src.utils import clean_source_name, get_logger, read_clean

log = get_logger("eda_profile")


def profile():
    log.info(f"读取干净数据：{clean_source_name()}")
    df = read_clean()
    n_rows, n_cols = df.shape
    log.info(f"数据规模：{n_rows:,} 行 × {n_cols} 列")

    rows = []
    for c in df.columns:
        s = df[c]
        n_missing = int(s.isna().sum())
        try:
            uniq = s.dropna().unique()[:5]
            sample = " | ".join(map(str, uniq))[:120]
        except Exception:
            sample = ""
        rows.append({
            "字段": c,
            "dtype": str(s.dtype),
            "缺失数": n_missing,
            "缺失率": round(n_missing / n_rows, 4),
            "唯一值数": int(s.nunique(dropna=True)),
            "样例值": sample,
        })

    res = pd.DataFrame(rows).sort_values("缺失率", ascending=False).reset_index(drop=True)
    out = config.PROJECT_ROOT / "01_eda" / "字段画像.csv"
    res.to_csv(out, index=False, encoding="utf-8-sig")
    log.info(f"✅ 字段画像已写出 -> {out}")

    print(f"\n=== 总行数: {n_rows:,} | 总字段: {n_cols} ===")
    print(f"\n=== 缺失率 > 50% 的字段（{int((res['缺失率'] > 0.5).sum())} 个，不入指标）===")
    print(res[res["缺失率"] > 0.5][["字段", "缺失率"]].to_string(index=False))
    print(f"\n=== 缺失率 = 0 的字段（{int((res['缺失率'] == 0).sum())} 个，最可靠）===")
    print(", ".join(res[res["缺失率"] == 0]["字段"].tolist()))
    return res


if __name__ == "__main__":
    profile()
