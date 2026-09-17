# -*- coding: utf-8 -*-
"""核查项目文件完备性：对照手册 11 步的产出物清单逐项检查。"""
import os
import sys

sys.stdout.reconfigure(encoding="utf-8")
ROOT = r"F:\BUPT\Internship_Preparation\credit-dwh"

# (相对路径, 说明, 是否必需)
ITEMS = [
    ("README.md", "项目说明", True),
    ("docker-compose.yml", "容器编排", True),
    ("requirements.txt", "依赖清单", True),
    (".gitignore", "git 忽略规则", True),

    ("src/__init__.py", "包标记", True),
    ("src/config.py", "配置与口径常量", True),
    ("src/utils.py", "公共工具", True),
    ("src/prepare_raw.py", "预处理（剔页脚）", True),
    ("src/metrics.py", "口径纯函数", False),
    ("src/ods_load.py", "ODS 落地", True),
    ("src/dwd_clean.py", "DWD 清洗", True),
    ("src/dwd_derived.py", "DWD 表现期派生", True),
    ("src/dim_build.py", "维表构建", True),
    ("src/dws_snapshot.py", "DWS 月末快照", True),
    ("src/ads_metrics.py", "ADS 指标", True),
    ("src/watermark.py", "增量水位", False),
    ("src/dq_check.py", "数据质量校验", True),

    ("sql/ddl/01_schema.sql", "13 张表 DDL", True),
    ("sql/dq/check_no_leakage.sql", "防泄漏校验 SQL", True),
    ("sql/dq/check_row_counts.sql", "行数核对 SQL", False),
    ("sql/dq/check_vintage_monotonic.sql", "Vintage 单调性 SQL", False),

    ("docs/信贷数仓实施手册.md", "阶段二主手册", True),
    ("docs/名词讲解.md", "名词词典", True),
    ("docs/项目指导书v2(阶段三).md", "阶段三手册", True),
    ("docs/archive", "历史文档归档", False),

    ("01_eda/01_profile.py", "字段画像脚本", True),
    ("01_eda/02_key_checks.py", "关键检查脚本", True),
    ("01_eda/字段画像.csv", "字段画像产出", True),
    ("01_eda/key_checks_output.txt", "关键检查产出", True),
    ("01_eda/数据探查报告.md", "数据探查报告", True),

    ("03_metrics/指标口径字典.md", "指标口径字典", True),
    ("03_metrics/数据边界与偏差说明.md", "数据边界说明", True),

    ("04_analysis/01_charts.py", "图表脚本", True),
    ("04_analysis/分析报告.md", "分析报告", True),
    ("04_analysis/images/01_放款趋势.png", "图1", True),
    ("04_analysis/images/02_Vintage矩阵.png", "图2", True),
    ("04_analysis/images/03_风险分层.png", "图3", True),
    ("04_analysis/images/04_迁徙率.png", "图4", True),

    ("06_ops/shell/run_pipeline.ps1", "一键流水线", True),
    ("06_ops/runbook.md", "运维手册", True),

    ("data/clean/loans_full.parquet", "干净数据（全量）", True),
    ("data/clean/loans_sample.parquet", "干净数据（子集）", False),
    ("data/clean/footer_line.txt", "页脚留证", True),
    ("data/clean/clean_meta.json", "清洗元信息", True),
    ("data/raw/accepted_2007_to_2018Q4.csv.gz", "原始数据", True),
]

print("=" * 78)
print("项目文件完备性核查")
print("=" * 78)

missing_required, missing_optional = [], []
ok = 0
for rel, desc, required in ITEMS:
    p = os.path.join(ROOT, rel.replace("/", os.sep))
    exists = os.path.exists(p)
    if exists:
        ok += 1
        if os.path.isdir(p):
            size = "目录/%d 项" % len(os.listdir(p))
        else:
            b = os.path.getsize(p)
            size = f"{b/1024/1024:.2f} MB" if b > 1024 * 1024 else f"{b/1024:.1f} KB"
        mark = "OK"
    else:
        mark = "缺失"
        size = "-"
        (missing_required if required else missing_optional).append(rel)
    tag = "必需" if required else "可选"
    print(f"  [{mark:4}] [{tag}] {rel:44} {size:>12}  {desc}")

print()
print("=" * 78)
print(f"结果：{ok}/{len(ITEMS)} 存在")
if missing_required:
    print(f"[缺失] 必需文件 {len(missing_required)} 个：")
    for m in missing_required:
        print(f"     {m}")
else:
    print("[OK] 所有必需文件齐备")
if missing_optional:
    print(f"[可选] 缺失：{missing_optional}")

print()
print("=" * 78)
print("项目目录结构总览")
print("=" * 78)
for dirpath, dirnames, filenames in os.walk(ROOT):
    dirnames[:] = [d for d in dirnames
                   if d not in (".git", ".venv", "__pycache__")]
    depth = dirpath.replace(ROOT, "").count(os.sep)
    if depth > 2:
        continue
    rel = dirpath.replace(ROOT, ".") or "."
    print(f"  {rel}/")
    for f in sorted(filenames):
        if f.endswith(".pyc"):
            continue
        fp = os.path.join(dirpath, f)
        b = os.path.getsize(fp)
        size = f"{b/1024/1024:.1f}MB" if b > 1024 * 1024 else f"{b/1024:.0f}KB"
        print(f"      {f:46} {size:>8}")
