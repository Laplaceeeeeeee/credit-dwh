# credit-dwh —— 金融信贷离线数仓

> **项目定位**：金融/信贷离线 + 实时数仓，作为**研究生实习简历**的项目经历
> **投递方向**：银行/券商 数据开发（数仓方向）
> **最后更新**：2026-09-15

---

## 🎯 项目一句话

> 把一份 226 万行的原始信贷放款流水，加工成一套能回答
> "**这笔贷款会不会逾期 / 哪批客户质量在恶化 / 风控策略改完有没有效果**"的数据底座，
> 并在同一套口径上完成**单机与大数据引擎的对照实验**，得出选型依据。

**做的是数据工程/数仓，不是机器学习建模。**

---

## 📊 数据事实（全部本地实测，可复现）

| 项 | 值 | 说明 |
|---|---|---|
| 原始文件 | `accepted_2007_to_2018Q4.csv.gz` | 374.4 MB |
| 物理行数 | 2,260,702 | 逐行统计 |
| **非数据行（页脚）** | **33** | ⚠️ 散布全文，非仅末尾，详见下文 |
| 表头 | 1 | |
| **真实数据行** | **2,260,668** | = 2,260,702 − 1 − 33 |
| 字段数 | 151 → **保留 137** | 剔除 14 个整列为空的字段 |
| 放款时间跨度 | **Apr-2008 ~ Sep-2018** | |
| 主键 `id` | 2,260,668 行**完全唯一** | 存为字符串，避免精度隐患 |
| 终态不良率 | **19.9807%** | 269,360 / 1,348,099 |
| 列存体积 | 337.6 MB | Parquet + snappy，压缩比约 5× |

### 🚨 三个必须知道的数据陷阱

**① 页脚行有 33 行，而且散布在文件中部**

这个文件**不是一份单纯导出**，而是多个年度分段拼接而成，段间夹着章节标题与汇总行：

```
行  421096   Total amount funded in policy code 1: 6417608175,,,,...
行  421097   Total amount funded in policy code 2: 1944088810,,,,...
...          共 16 对，间隔 10~23 万行出现一次
行 1651666   Loans that do not meet the credit policy,,,,...     ← 章节标题
行 2260700   Total amount funded in policy code 1: 1465324575,,,,...
行 2260701   Total amount funded in policy code 2: 521953170,,,,...
```

**只跳过最后 1 行是错的**，另外 32 行会混进 `id` 列。
本项目用 `src/prepare_raw.py` 全文件扫描剔除，页脚原文留证于 `data/clean/footer_line.txt`。

**② `member_id` 整列为空**

实测 2,260,668 行中非空值为 **0**，LendingClub 已移除该字段。
因此**无法建客户维**，"一人多贷"分析不可做。
本项目**不虚构客户主键**，改用 `dim_applicant`（申请人画像维，粒度 = 一笔贷款）。

**③ `skipfooter` 与 `chunksize`/`nrows` 互斥**

pandas 2.2.2 与 3.0.5 上均直接抛 `ValueError`：

```
ValueError: 'skipfooter' not supported for iteration     # 与 chunksize
ValueError: 'skipfooter' not supported with 'nrows'      # 与 nrows
```

因此本项目**不在每次读取时跳页脚**，而是先用 `prepare_raw.py` 剔除一次，
产出干净 Parquet，之后所有脚本用 C 引擎读取（顺带把读取从 20–40 分钟压到 **80 秒**）。

---

## 🏗️ 架构

```
原始 gz (374MB)
   │  src/prepare_raw.py   ← 剔 33 行页脚 + 剔 14 个全空列 + 固定 dtype
   ▼
data/clean/loans_full.parquet (337MB, 226万行 × 137列)
   │
   ├─► ODS  ods_loan_raw            原样落地（39 关键列）
   ├─► DWD  dwd_loan_fact           申请时点事实表（22 列，⭐无表现期字段）
   │        dwd_loan_perf_fact      表现期事实表（14 列，与上表物理隔离）
   ├─► DIM  dim_applicant           申请人画像维（粒度=一笔贷款）
   │        dim_product / dim_date
   ├─► DWS  dws_loan_snapshot_m     月末快照事实表（周期快照）
   └─► ADS  ads_risk_segment / ads_vintage / ads_roll_rate / ads_delinq_monthly
                                     │
                          08 dq_check│8 项质量校验（含 Q8 防泄漏）
```

**13 张表**，DDL 见 `sql/ddl/01_schema.sql`。

---

## ⭐ 三个核心设计决策

### 1. 申请时点 / 表现期**物理分表**，数据泄漏靠机制不靠自觉

信贷数据的经典陷阱：`loan_status`、`out_prncp`、`total_rec_prncp` 等
都是**放款后才知道**的字段。若与申请时点字段同表使用，指标就全错了。

三道防线：

| 防线 | 手段 |
|---|---|
| 一 | **DDL 物理分表** —— 表现期字段只进 `dwd_loan_perf_fact` |
| 二 | **代码白名单** —— `config.APPLY_COLS` |
| 三 | **自动校验** —— `dq_check.py` Q8 用 `config.LEAKAGE_BLACKLIST` 扫表结构，发现即 ERROR |

### 2. 用**月末快照事实表**替代 SCD2 拉链表

本数据集是**放款时点的静态快照**，不存在真实的客户属性变更序列，
硬做 SCD2 只能造假数据。月末快照是标准数仓模式，**数据真实可校验**，
且天然支撑 Vintage 与迁徙率。

### 3. 明确声明数据边界，而不是编造因果

见 `03_metrics/数据边界与偏差说明.md`。核心边界：

- 仅含放款样本 → **幸存者偏差**，批准率不可计算（已删除该指标）
- `loan_status` 为最终快照 → 迁徙率的 DPD 档位为**规则推导**，需标注
- 无策略与宏观数据 → 分析结论**只陈述数据内可验证的事实**

---

## 📁 目录结构

```
credit-dwh/
├── README.md                  本文件
├── docker-compose.yml         MySQL(3307) + Ubuntu
├── requirements.txt
├── data/
│   ├── raw/                   原始 gz（不入 git）
│   └── clean/                 🆕 干净 Parquet + 页脚留证 + 元信息
├── docs/                      规划与指导文档
├── 01_eda/                    字段画像 / 关键检查
├── 02_design/                 数仓设计文档
├── 03_metrics/                指标口径字典 / 数据边界说明
├── 04_analysis/               图表与分析报告
├── 05_quality/                质量校验报告
├── 06_ops/                    调度脚本 / 运维手册 / 日志
├── sql/
│   ├── ddl/01_schema.sql      13 张表
│   └── dq/                    质量校验 SQL（含防泄漏）
└── src/
    ├── config.py              路径 / DB / 口径常量（口径字典代码化）
    ├── utils.py               日志 / 连接 / 读干净数据（含控制台编码修正）
    ├── prepare_raw.py         ⭐ 预处理：剔页脚，产出干净 Parquet
    ├── metrics.py             口径纯函数（可单测）
    ├── ods_load.py            ODS 落地
    ├── dwd_clean.py           DWD 清洗
    ├── dwd_derived.py         DWD 表现期派生
    ├── dim_build.py           维表构建
    ├── dws_snapshot.py        DWS 月末快照
    ├── ads_metrics.py         ADS 指标
    ├── watermark.py           增量水位
    └── dq_check.py            8 项质量校验
```

---

## 🚀 如何运行

```powershell
# 1. 启动数据库（注意端口 3307）
docker start credit-dwh-mysql

# 2. 一键跑全流程（8 步）
.\06_ops\shell\run_pipeline.ps1
```

或分步执行：

```powershell
$PY = ".\.venv\Scripts\python.exe"

& $PY -m src.prepare_raw      # 01 预处理（剔 33 行页脚）
& $PY -m src.ods_load         # 02 ODS 落地
& $PY -m src.dwd_clean        # 03 DWD 清洗
& $PY -m src.dwd_derived      # 04 DWD 表现期
& $PY -m src.dim_build        # 05 维表
& $PY -m src.dws_snapshot     # 06 月末快照
& $PY -m src.ads_metrics      # 07 ADS 指标
& $PY -m src.dq_check         # 08 质量校验
```

建表：

```powershell
Get-Content sql\ddl\01_schema.sql -Raw -Encoding UTF8 |
  docker exec -i credit-dwh-mysql mysql -uroot -proot123456
```

---

## ⚙️ 数据规模开关

`src/config.py`：

```python
MAX_ROWS = 200_000   # 子集，调流程用（前 20 万行，偏早期放款）
MAX_ROWS = None      # 全量 2,260,668 行，出最终数字用
```

> ⚠️ **比例型指标（不良率、Vintage）必须用全量算**。
> 子集里 `Current` 只占 11.3%，全量是 38.9% —— 分布完全不同。
> 切换规模后从第 02 步起重跑即可（预处理已同时生成全量与子集两份）。

---

## 📚 文档清单

| 文档 | 作用 |
|---|---|
| `docs/项目指导书v1.md` | **阶段二执行手册**（第 0–10 步） |
| `docs/项目指导书v2(阶段三).md` | **阶段三执行手册**（第 11–17 步，含 5 个对照实验） |
| `docs/v1.2_Project.md` | 总纲：做什么、为什么、时间表、面试话术 |
| `docs/配置进度与问题记录.md` | **本次配置过程的问题与修正记录**（推荐先读） |
| `docs/v1.1_Project.md` / `v1.0_Project.md` | 历史版本，归档 |
| `06_ops/runbook.md` | 运维手册：故障排查与数据事实速查 |

---

## 📌 数据说明

原始数据来自 Kaggle 公开数据集 LendingClub（`accepted_2007_to_2018Q4`），
**未随仓库分发**，请自行下载至 `data/raw/`。

本项目为个人学习项目，数据为公开脱敏数据，**不代表任何机构的真实业务数据**。
