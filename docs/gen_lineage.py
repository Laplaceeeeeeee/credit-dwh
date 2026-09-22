"""读 docs/lineage.yaml，校验并生成 Mermaid 血缘图 -> docs/数据血缘.md

用法：
    python docs/gen_lineage.py            # 校验 + 生成 docs/数据血缘.md
    python docs/gen_lineage.py --check    # 只校验，不写文件（CI 用这个）

⭐ 为什么要有 --check：血缘图最大的问题是"写完就烂"——
   表改了名、加了新链路，yaml 却没人更新，图就变成了误导。
   所以这里把两件事变成断言：
     ① 所有非外部引用（不带 `kind:` 前缀的）必须是已定义的表；
     ② upstream / downstream 必须**互为反向**，不能只写一边。
   CI 里跑 `--check`，血缘一脱节就红。
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
# 让 `python docs/gen_lineage.py` 也能 import src（sys.path[0] 是 docs/）
sys.path.insert(0, str(ROOT))

from src.utils import setup_console_encoding  # noqa: E402  # 必须在上面的 path 之后

setup_console_encoding()   # 中文 Windows 控制台默认 GBK，print ✅/❌ 会炸

YAML_PATH = ROOT / "docs" / "lineage.yaml"
DOC_PATH = ROOT / "docs" / "数据血缘.md"

# ⚠️ 不要在源码里写三个反引号的字面量（在 markdown 文档里会被当成代码块结束）
FENCE = "`" * 3

LAYER_ORDER = ["ODS", "DWD", "DIM", "DWS", "ADS", "OPS", "EXP", "OTHER"]
LAYER_TITLE = {
    "ODS": "ODS 原始层",
    "DWD": "DWD 明细层",
    "DIM": "DIM 维表层",
    "DWS": "DWS 汇总层",
    "ADS": "ADS 应用层",
    "OPS": "OPS 运维/质量层",
    "EXP": "EXP 实验临时表",
    "OTHER": "其他",
}


def is_external(ref: str) -> bool:
    """`file:` / `kafka:` / `report:` 这类引用是外部系统，不在 tables 里定义。"""
    return ":" in ref


def layer_group(layer: str) -> str:
    """'DWD(Hive, ORC, 分区 issue_month)' -> 'DWD'"""
    m = re.match(r"[A-Za-z]+", layer or "")
    g = m.group(0).upper() if m else "OTHER"
    return g if g in LAYER_TITLE else "OTHER"


def node_id(name: str) -> str:
    """Mermaid 节点 id 只留字母数字下划线 —— 冒号/点/中文都可能让渲染失败。"""
    return "n_" + re.sub(r"[^0-9A-Za-z_]", "_", name)


def external_ids(refs: list[str]) -> dict[str, str]:
    """外部节点的 id：`n_ext_<kind>_<序号>`。

    ⚠️ 不能直接把中文标签"消毒"成 id：`report:分层风险画像` 与 `report:逾期月报`
       会消毒成不同长度的下划线串，**长度一旦相同就会撞 id、两个节点被静默合并**
       （图上看不出来，但血缘已经错了）。所以外部节点一律用序号保证唯一。
    """
    ids: dict[str, str] = {}
    for ref in sorted(refs):
        kind = re.sub(r"[^0-9A-Za-z_]", "_", ref.split(":", 1)[0]) or "ext"
        ids[ref] = f"n_ext_{kind}_{len(ids) + 1}"
    return ids


def load() -> dict:
    return yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))


def check(cfg: dict) -> tuple[list[str], list[str]]:
    """返回 (问题列表, 规划中节点列表)。问题为空 = 校验通过。"""
    tables = cfg.get("tables") or {}
    if not tables:
        return ["lineage.yaml 里没有解析出任何表"], []

    problems: list[str] = []

    # ① 所有非外部引用必须是已定义的表
    for tbl, meta in tables.items():
        for edge in ("upstream", "downstream"):
            for ref in meta.get(edge) or []:
                if not is_external(ref) and ref not in tables:
                    problems.append(f"❌ {tbl}.{edge} 引用了未定义的表：{ref}")

    # ② upstream / downstream 必须互为反向（只写一边 = 图会漏边）
    for tbl, meta in tables.items():
        for up in meta.get("upstream") or []:
            if is_external(up) or up not in tables:
                continue
            if tbl not in (tables[up].get("downstream") or []):
                problems.append(
                    f"❌ 单向边：{up} -> {tbl} 只写在 {tbl}.upstream，"
                    f"没写进 {up}.downstream")

    planned = [t for t, m in tables.items() if m.get("status") == "planned"]
    return problems, planned


def mermaid(cfg: dict) -> list[str]:
    tables = cfg["tables"]

    groups: dict[str, list[str]] = {}
    for tbl, meta in tables.items():
        groups.setdefault(layer_group(meta.get("layer", "")), []).append(tbl)

    externals: list[str] = []
    for meta in tables.values():
        for edge in ("upstream", "downstream"):
            for ref in meta.get(edge) or []:
                if is_external(ref) and ref not in externals:
                    externals.append(ref)

    ext = external_ids(externals)

    def nid(name: str) -> str:
        return ext[name] if name in ext else node_id(name)

    lines = ["flowchart LR"]
    for g in LAYER_ORDER:
        if g not in groups:
            continue
        lines.append(f'    subgraph sg_{g}["{LAYER_TITLE[g]}"]')
        for tbl in sorted(groups[g]):
            planned = tables[tbl].get("status") == "planned"
            label = f"{tbl}（规划中）" if planned else tbl
            lines.append(f'        {node_id(tbl)}["{label}"]')
        lines.append("    end")

    if externals:
        lines.append('    subgraph sg_EXT["外部系统 / 产物"]')
        for ref in sorted(externals):
            lines.append(f'        {ext[ref]}(["{ref}"])')
        lines.append("    end")

    lines.append("")
    for tbl, meta in tables.items():
        dashed_from = meta.get("status") == "planned"
        for ds in meta.get("downstream") or []:
            # 规划中的链路画成虚线，免得把"没做"画成"做了"
            dashed_to = ds in tables and tables[ds].get("status") == "planned"
            arrow = "-.->" if (dashed_from or dashed_to) else "-->"
            lines.append(f"    {nid(tbl)} {arrow} {nid(ds)}")

    # ⚠️ 外部节点不会作为"表"被上面那个循环遍历到，所以它们的**出边**
    #    （file: → 表、kafka: → 表、calendar: → 表）必须单独补一遍，
    #    否则图里 12 个外部节点会全是孤岛 —— 看起来"没有上游"，实际是漏画。
    for tbl, meta in tables.items():
        for up in meta.get("upstream") or []:
            if is_external(up):
                arrow = "-.->" if meta.get("status") == "planned" else "-->"
                lines.append(f"    {nid(up)} {arrow} {node_id(tbl)}")

    for t, m in tables.items():
        if m.get("status") == "planned":
            lines.append(f"    style {node_id(t)} stroke-dasharray: 4 4")
    return lines


def render(cfg: dict) -> str:
    tables = cfg["tables"]
    planned = {t: m for t, m in tables.items() if m.get("status") == "planned"}
    real = {t: m for t, m in tables.items() if m.get("status") != "planned"}

    out = [
        "# 数据血缘图",
        "",
        "> 由 `docs/gen_lineage.py` 从 `docs/lineage.yaml` 自动生成，**不要手改**。",
        "> 生成：`python docs/gen_lineage.py` ｜"
        " 校验：`python docs/gen_lineage.py --check`",
        "> 元数据按**代码实测**登记，不是照抄文档模板（见文末「读图须知」）。",
        "",
        f"共登记 **{len(tables)}** 个节点：已实现 **{len(real)}** 个、"
        f"规划中 **{len(planned)}** 个。",
        "",
        f"{FENCE}mermaid",
        *mermaid(cfg),
        FENCE,
        "",
        "## 分层清单",
        "",
        "| 表 | 层 | 上游 | 下游 | SLA | 说明 |",
        "|---|---|---|---|---|---|",
    ]
    for tbl, meta in tables.items():
        ups = "<br>".join(meta.get("upstream") or []) or "—"
        downs = "<br>".join(meta.get("downstream") or []) or "—"
        layer = meta.get("layer", "")
        if meta.get("status") == "planned":
            layer = f"{layer} ⚠️规划中"
        out.append(f"| `{tbl}` | {layer} | {ups} | {downs} | "
                   f"{meta.get('sla', '—')} | {meta.get('note', '')} |")

    if planned:
        out += ["", "## ⚠️ 规划中（图中虚线，**表并不存在**）", ""]
        for tbl, meta in planned.items():
            out.append(f"- `{tbl}`：{meta.get('note', '')}")

    out += [
        "",
        "## 读图须知（诚实说明）",
        "",
        "1. **MySQL 层是扇出，不是串接**：`ods_loan_raw` / `dwd_*` / `dim_*` / `dws_*` "
        "都直接读同一份 `data/clean/loans_full.parquet`（`src/utils.py::read_clean`）。"
        "所以 ODS 表是**留证 / 回滚点，不是 DWD 的输入** —— 生产里通常是 ODS→DWD 串接，"
        "这里为了单机可重跑改成了同源扇出，被问到要如实讲。",
        "2. **Hive 层才是真正的串接**：`*_lc` 表由 MySQL 表经 "
        "`07_bigdata/export_mysql_tsv.py` 导出 TSV 后装载；4 张 `ads_*_lc` "
        "由 Hive 表在库内计算得出。",
        "3. `dim_product` / `dim_date` 目前**没有下游**（已建但未进 ADS 汇总），"
        "这是诚实的空白，不在图里编下游。",
        "4. `e5_ladder` 等 `e4_*` / `e5_*` 是实验用的**临时表**，"
        "由 `08_benchmark/run_experiment.py` 按 `08_benchmark/exp/*.sql` 建；"
        "这里只登记被血缘引用到的 `e5_ladder`。",
    ]
    return "\n".join(out) + "\n"


def main(argv: list[str]) -> int:
    cfg = load()
    problems, planned = check(cfg)
    if problems:
        print("\n".join(problems))
        print(f"\n❌ 血缘校验未通过：{len(problems)} 个问题")
        return 1

    print(f"✅ 血缘校验通过：{len(cfg['tables'])} 个节点，"
          f"引用与双向边全部一致（规划中 {len(planned)} 个已标注）")

    if "--check" in argv:
        return 0

    DOC_PATH.write_text(render(cfg), encoding="utf-8")
    print(f"✅ {DOC_PATH.relative_to(ROOT)} 已生成")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
