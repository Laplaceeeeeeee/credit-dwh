# credit-dwh —— 金融信贷离线数仓

> **项目定位**：金融/信贷离线数仓，作为**研究生实习简历**的项目经历
> **投递方向**：银行/券商 数据开发（数仓方向）
> **当前进度**：阶段二 ✅ 已完成 · 阶段三 **7/9 步**（第 12–18 步：环境/迁移/五实验/工程化；
> 第 19 步实时链路是**可砍项、未做**）
> **最后更新**：2026-09-22

---

## 📚 文档导航（只需要看两份 + 一本词典）

| 文档 | 作用 | 什么时候看 |
|---|---|---|
| **`docs/信贷数仓实施手册(阶段2).md`** | ⭐ **阶段二主文档**：原理 + 11 步实践 + 排错 + 面试话术 | **从头到尾读一遍**，之后当手册查 |
| **`docs/项目指导书v3(阶段三).md`** | ⭐ **阶段三主文档**：Hive/Spark 环境、5 个对照实验、工程化、附录 B 排错 **51 条** | 看阶段三怎么做、踩过哪些坑 |
| **`docs/名词讲解.md`** | ⭐ **名词词典**：70 个技术/金融词，每个给"一句话 + 类比 + 本项目实例" | **遇到不懂的词就查**；面试前过一遍速记卡 |
| `docs/技术栈讲解.md` | 技术栈逐个讲清（为什么用、怎么用、边界在哪） | 面试前扫一遍 |
| `docs/SLA与监控.md`、`docs/数据血缘.md` | 四类 SLA 判据 ｜ 23 节点血缘图（自动生成） | 讲"工程化"时用 |
| `docs/archive/` | 历史规划文档与旧版指导书 | 一般不用看 |

> **归一说明**：原先 6 份重叠文档（共 9500+ 行）已合并为一份实施手册 + 一份名词词典，
> 历史版本移入 `docs/archive/`。

---

## 🎯 项目一句话

> 把一份 **226 万行**的原始信贷放款流水，加工成一套能回答
> "**这笔贷款会不会逾期 / 哪批客户质量在恶化 / 风控策略改完有没有效果**"
> 的数据底座。

**做的是数据工程/数仓，不是机器学习建模。**

---

## 📊 数据事实（全部本地实测，可复现）

| 项 | 值 |
|---|---|
| 原始文件 | `accepted_2007_to_2018Q4.csv.gz`（374.4 MB） |
| 物理行数 | 2,260,702 |
| 表头 | 1 |
| **非数据行（页脚）** | **33** ⚠️ 散布全文，非仅末尾 |
| **真实数据行** | **2,260,668** |
| 字段数 | 151 → **保留 137**（剔除 14 个整列为空） |
| 放款时间跨度 | **2007-06 ~ 2018-12** |
| 主键 `id` | 完全唯一，存为 **string** |
| **终态不良率** | **19.9807%**（269,360 / 1,348,099） |
| 不限定终态的错误口径 | 11.9155%（**低估 8 个百分点**） |
| 列存体积 | 337.6 MB（Parquet + snappy） |

### 🚨 三个必须知道的数据陷阱

1. **页脚有 33 行且散布在文件中部** —— 文件是多个年度分段拼接而成，
   段间夹着章节标题与汇总行。`skipfooter=1` 只删最后一行，另 32 行会混进 `id` 列。
2. **`member_id` 整列为空**（+13 个全空列）—— 无法建客户维，改用 `dim_applicant`。
3. **`skipfooter` 与 `chunksize`/`nrows` 互斥** —— 原设计的分块读取一行都跑不起来，
   改为"预处理一次 + 读 Parquet"。

---

## 🏗️ 架构

```
原始 gz (374MB)
   │  src/prepare_raw.py   ← 剔 33 行页脚 + 剔 14 个全空列 + 固定 dtype
   ▼
data/clean/loans_full.parquet (338MB, 2,260,668 行 × 137 列)
   │
   ├─► ODS  ods_loan_raw            原样落地（39 关键列）
   ├─► DWD  dwd_loan_fact           申请时点事实表 ⭐无表现期字段
   │        dwd_loan_perf_fact      表现期事实表（物理隔离）
   ├─► DIM  dim_applicant           申请人画像维（非客户维）
   │        dim_product / dim_date
   ├─► DWS  dws_loan_snapshot_m     月末快照事实表（周期快照）
   └─► ADS  ads_risk_segment / ads_vintage / ads_roll_rate / ads_delinq_monthly
                                     │
                          08 dq_check│8 项质量校验（含 Q8 防泄漏）
```

**13 张表**，DDL 见 `sql/ddl/01_schema.sql`。

---

## ⭐ 四个核心设计决策

| # | 决策 | 一句话理由 |
|---|---|---|
| 1 | **申请时点 / 表现期物理分表** + 字段黑名单自动校验 | 数据泄漏靠机制，不靠自觉 |
| 2 | **月末快照事实表**替代 SCD2 拉链表 | 本数据无真实属性变更序列，硬做 SCD2 只能造假 |
| 3 | **申请人画像维**替代客户维 | `member_id` 整列为空，不拼伪客户 ID |
| 4 | 快照表主键用 `(snapshot_month, loan_id)` | 与逐月插入同序 → 吞吐 5k→38k 行/秒 |

---

## 🧪 阶段三：大数据层（Hive / Spark）与五个对照实验

> **一句话**：把**同一套口径**从 MySQL 迁到 Hive 分区表（Spark 执行 + 外部 Hive Metastore），
> 再用 **5 个对照实验**量化"什么时候才真的需要分布式"，并给出**带边界的选型结论**。

### 引擎与选型（E5：同一查询语义、两侧各跑 3 次取中位数）

| 引擎 | 配置 | 226 万行跑批（中位） | 固定开销 | 结论 |
|---|---|---|---|---|
| MySQL（单机容器） | 未限核 ｜ 1 容器 ≈ 378 MB | **3.94 s**（端到端） | ~0 | ⭐ **本量级推荐** |
| Spark SQL（AQE 关） | worker 4 核 / 2 GB | 1.52 s（应用内） | — | 纯计算更快，但端到端被固定开销吃掉 |
| Spark SQL（AQE 开） | 同上 | 1.13 s（应用内） | — | |
| Spark + `REBALANCE` 提示 | 同上 | **1.08 s**（应用内） | **3.94 s** | 端到端 **≈ 5.07 s**，仍慢于 MySQL |

- **结论**：226 万行 / 约 6.8 GB 下**推荐 MySQL 单机** —— 端到端 MySQL 3.94 s vs Spark 5.07 s，
  且资源占用与运维成本低一个量级。完整论证见 `08_benchmark/引擎选型结论.md`。
- **阈值**：端到端口径的交叉点**外推**在**约 270 万行（8–9 GB）**附近。
  ⚠️ 这是**外推不是实测**，且只对"单 JOIN + 分组聚合"这一种查询形态成立。

### 五个对照实验（全部本地实测，复现方式在各报告里）

| 实验 | 对照组 | 实测结果 |
|---|---|---|
| **E1 分区裁剪** | 全表 vs 单分区 vs **非分区表同谓词** | 扫描文件 **139 → 1**、扫描字节 **48.54 MB → 0.88 MB**；**非分区对照仍读 47.87 MB** → 证明省的是 **IO 而不是计算** |
| **E2 小文件合并** | 2400 个文件 vs 12 个 | 查询 **2.22 s → 0.28 s（7.9×）**，任务数 75 → 4；小文件总体积是合并后的 **2 倍** |
| **E3 列存格式** | TextFile / Parquet / ORC | 体积 **151.68 / 28.85 / 34.37 MB**；少列查询 **0.95 / 0.33 / 0.32 s（列存快 3.0×）**，全列查询优势收窄到 **2.4×**。⚠️ 本次 **Parquet 比 ORC 小 16%**（列里有唯一字符串 `loan_id`）→ **推翻了"ORC 一定更小"的成见** |
| **E4 数据倾斜** | 无盐 vs 加盐 5/10/20 桶 + **AQE 三态** | `loan_status` 最大 key **47.63%**、倾斜比 **4.29**；基线最长÷中位 task **4.48**；加盐 **5 桶无效（7.58 s）**、**10 桶 7.25 → 2.39 s**；20 桶 2.00 s（⚠️ 与 10 桶的 0.39 s 差异**落在单次测量噪声内，未下定论**）；AQE **管不了聚合倾斜**（7.31 s 无改善），`REBALANCE` 提示 **1.75 s 最快** |
| **E5 三引擎基准** | MySQL vs Spark（3 组配置）+ 数据量阶梯 | 见上表；阶梯 10% / 25% / 100% 两侧用**同一个确定性谓词**、行数核对相等 |

### 口径与工程化（阶段三补的是"能被 CI 拦住"的能力）

- **双引擎逐行比对**：关键指标 **7/7 一致、最大差异 0.0**；4 张 ADS 表**逐行一致**
  （18 / 24,768 / 108 / 3,105 行，键未匹配 0）。比率列曾有 **5e-5** 差异，查明是
  MySQL `div_precision_increment=4` 的**量化语义**，用"差异 ≤ 5e-5 **+ 平局判据**"
  两条可验证断言覆盖 —— **没有放宽容差糊过去**。
- **指标单测**：`pytest tests/ -v` → **34 条全绿**，**不依赖任何数据库**。
  其中 3 条是把踩过的坑固化的**回归防线**（`Current` 不进不良率分母、倾斜比公式的
  恒等式错写法、行数守恒应为 **0** 而非历史误判的 32）。
- **CI**：`.github/workflows/ci.yml` 三步 —— ruff 静态检查 → 指标单测 → 血缘一致性校验，
  **全程不连数据库**（这正是把口径抽成纯函数的意义）。
- **数据血缘**：`docs/lineage.yaml` → `docs/gen_lineage.py` → `docs/数据血缘.md`
  （23 个节点、Mermaid）；CI 跑 `--check` 卡住"悬空引用 / 单向边"。
- **四类 SLA**：新鲜度 / 数据量波动 / 指标越界 / ⭐ **跨链路对账**（双引擎 + 批流），见 `docs/SLA与监控.md`。

### 实时链路（**未实施** —— 诚实声明）

第 19 步（CDC → Kafka → 实时指标 + 批流对账）是**可砍项，本项目没有做**。
`docs/SLA与监控.md` 里只**定义了批流对账的判据**，血缘图里 `ads_loan_daily_realtime`
以 `status: planned` 标注（画成虚线）。
所以本项目的正确说法是"**离线**双引擎一致性已验证"，**不是"做过实时"**。

---

## 📁 目录结构

```
credit-dwh/
├── README.md                  本文件
├── docker-compose.yml         MySQL(3307) + Ubuntu
├── requirements.txt           运行时依赖 ｜ requirements-dev.txt 开发/CI 依赖
├── pytest.ini / ruff.toml     指标单测与静态检查配置
├── .github/workflows/ci.yml   ⭐ CI：ruff + 指标单测 + 血缘校验（不连数据库）
├── data/                      ❌ 体积型数据不入 git
│   ├── raw/                   原始 gz
│   └── clean/                 干净 Parquet + 页脚留证
├── docs/
│   ├── 信贷数仓实施手册(阶段2).md  ⭐ 阶段二主文档
│   ├── 项目指导书v3(阶段三).md     ⭐ 阶段三主文档
│   ├── 数据血缘.md / lineage.yaml / gen_lineage.py   ⭐ 血缘（生成物不要手改）
│   ├── SLA与监控.md            ⭐ 四类 SLA
│   └── archive/               历史版本
├── 01_eda/                    字段画像 / 关键检查 / 数据探查报告
├── 03_metrics/                指标口径字典 / 数据边界说明
├── 04_analysis/               图表与分析报告
├── 06_ops/                    一键流水线 / 运维手册 / verify_stage3.ps1
├── 07_bigdata/                🆕 阶段三：docker-compose、Hive SQL、PySpark、交换脚本
├── 08_benchmark/              🆕 阶段三：测量框架（harness/事件日志/数文件）+ E1–E5 报告
├── tests/                     ⭐ 指标口径回归单测（34 条）
├── sql/ddl/                   13 张表
├── sql/dq/                    质量校验 SQL（含防泄漏）
└── src/                       12 个核心模块（metrics.py 是口径的纯函数层）
```

---

## 🚀 如何运行

```powershell
# 1. 启动数据库（注意端口 3307）
docker start credit-dwh-mysql

# 2. 建表（仅首次）
Get-Content sql\ddl\01_schema.sql -Raw -Encoding UTF8 |
  docker exec -i credit-dwh-mysql mysql -uroot -proot123456

# 3. 一键跑全流程（8 步）
.\06_ops\shell\run_pipeline.ps1
```

> ⚠️ **必须用 venv 的 python**（`.venv\Scripts\python.exe`），
> 系统环境缺 `sqlalchemy` / `pymysql` / `pyarrow`。

### 阶段三（Hive / Spark）

```powershell
# 1. 起大数据环境（Hive Metastore + Spark 集群；MySQL 要先起来）
cd 07_bigdata; docker compose -f docker-compose-bigdata.yml up -d; cd ..

# 2. Hive 建表 + 装载 + 校验（TSV 交换，见 07_bigdata/export_mysql_tsv.py）
docker exec -i bd-spark /opt/spark/bin/spark-sql -f /workspace/07_bigdata/hive/01_ddl.sql
docker exec -i bd-spark /opt/spark/bin/spark-sql -f /workspace/07_bigdata/hive/02_load.sql

# 3. 双引擎一致性 + 4 张 ADS 逐行比对
& .\.venv\Scripts\python.exe 07_bigdata\check_consistency.py
& .\.venv\Scripts\python.exe 07_bigdata\compare_ads.py

# 4. 工程化三连（与 CI 完全一致，不依赖数据库/集群）
& .\.venv\Scripts\ruff.exe check src/ tests/ 07_bigdata/ 08_benchmark/ docs/gen_lineage.py
& .\.venv\Scripts\python.exe -m pytest tests\ -v -m "not integration"   # 期望 34 passed
& .\.venv\Scripts\python.exe docs\gen_lineage.py --check

# 5. 阶段三机器验收（一条命令出结论；-Full 会额外查 Docker 与跑 Spark 一致性）
.\06_ops\shell\verify_stage3.ps1          # 快速模式
.\06_ops\shell\verify_stage3.ps1 -Full    # 完整模式
```

> ⚠️ **Docker VM 只有 7.64 GB 内存**：跑 Spark 重任务前先 `docker stop credit-dwh-mysql` 腾内存
> （Spark 侧已缩容到 worker 2 GB / 4 核）。

---

## ⚙️ 数据规模开关

`src/config.py`：

```python
MAX_ROWS = None       # 全量 2,260,668 行（出最终数字用）
MAX_ROWS = 200_000    # 子集（快速调流程，但比例型指标不可用）
```

> ⚠️ **比例型指标（不良率、Vintage）必须用全量算**。
> 子集里 `Current` 只占 11.3%，全量是 38.9% —— 分布完全不同。

---

## 📌 数据说明

原始数据来自 Kaggle 公开数据集 LendingClub（`accepted_2007_to_2018Q4`），
**未随仓库分发**，请自行下载至 `data/raw/`。

本项目为个人学习项目，数据为公开脱敏数据，**不代表任何机构的真实业务数据**。
