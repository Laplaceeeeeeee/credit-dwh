"""EDA 第二步：主键、枚举、时间跨度、异常值、体量实测。"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from src import config
from src.utils import clean_source_name, get_logger, read_clean

log = get_logger("eda_keys")


def main():
    log.info(f"读取干净数据：{clean_source_name()}")
    df = read_clean()
    log.info(f"读入 {len(df):,} 行 × {df.shape[1]} 列")

    out_lines = []

    def emit(title, body):
        print(f"\n{'=' * 62}\n{title}\n{'=' * 62}")
        print(body)
        out_lines.append(f"{'=' * 62}\n{title}\n{'=' * 62}\n{body}\n")

    # ① 主键
    n = len(df)
    emit("① 主键检查", "\n".join([
        f"id 唯一值数: {df['id'].nunique():,} / 总行数 {n:,}",
        f"id 是否唯一: {df['id'].nunique() == n}",
        f"id 重复行数: {int(df['id'].duplicated().sum())}",
        f"id 缺失数: {int(df['id'].isna().sum())}",
    ]))

    # ② loan_status
    vc = df["loan_status"].value_counts(dropna=False)
    pct = (vc / len(df) * 100).round(3)
    body = pd.DataFrame({"笔数": vc, "占比%": pct}).to_string()
    emit("② loan_status 取值分布（决定不良口径）", body)

    # ③ 枚举
    parts = []
    for c in ["grade", "term", "home_ownership", "purpose", "verification_status"]:
        if c in df.columns:
            parts.append(f"\n{c}（{df[c].nunique()} 个取值）:")
            parts.append(df[c].value_counts(dropna=False).head(15).to_string())
    emit("③ 关键枚举字段", "\n".join(parts))

    # ④ 时间跨度
    parts = []
    for c in ["issue_d", "earliest_cr_line", "last_pymnt_d"]:
        if c not in df.columns:
            continue
        s = df[c].dropna()
        parsed = pd.to_datetime(s, format="%b-%Y", errors="coerce")
        parts.append(f"{c}: 解析成功 {parsed.notna().sum():,}/{len(s):,}"
                     f"（失败 {int(parsed.isna().sum())}）")
        parts.append(f"   范围: {parsed.min()} ~ {parsed.max()}")
    emit("④ 时间跨度（决定能否做趋势与 Vintage）", "\n".join(parts))

    # ⑤ 缺失交叉
    no_issue = int(df["issue_d"].isna().sum())
    no_status = int(df["loan_status"].isna().sum())
    emit("⑤ 关键字段缺失（决定 DWD 行数）", "\n".join([
        f"issue_d 为空: {no_issue:,}",
        f"loan_status 为空: {no_status:,}",
        f"→ 预期 DWD 行数 = {n:,} - {no_issue:,} = {n - no_issue:,}",
    ]))

    # ⑥ 异常值
    parts = [
        f"loan_amnt <= 0: {int((df['loan_amnt'] <= 0).sum())}",
        f"loan_amnt 范围: {df['loan_amnt'].min():,.0f} ~ {df['loan_amnt'].max():,.0f}",
        f"annual_inc <= 0: {int((df['annual_inc'] <= 0).sum())}",
        f"annual_inc 分位数:\n{df['annual_inc'].quantile([.5, .9, .99, .999, 1]).to_string()}",
    ]
    if "dti" in df.columns:
        parts.append(f"dti 分位数:\n{df['dti'].quantile([.5, .9, .99, 1]).to_string()}")
    emit("⑥ 异常值检查", "\n".join(parts))

    # ⑦ 体量实测
    mem = df.memory_usage(deep=True).sum() / 1024 ** 2
    emit("⑦ 体量实测（写简历用）", "\n".join([
        f"DataFrame 内存占用: {mem:,.0f} MB",
        f"行数: {n:,} | 列数: {df.shape[1]}",
    ]))

    out = config.PROJECT_ROOT / "01_eda" / "key_checks_output.txt"
    out.write_text("\n".join(out_lines), encoding="utf-8")
    log.info(f"✅ 输出已保存 -> {out}")


if __name__ == "__main__":
    main()
