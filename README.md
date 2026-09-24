# credit-dwh —— 金融信贷离线数仓

[![CI](https://github.com/Laplaceeeeeeee/credit-dwh/actions/workflows/ci.yml/badge.svg)](https://github.com/Laplaceeeeeeee/credit-dwh/actions/workflows/ci.yml)

> **项目定位**：金融/信贷离线数仓，作为**研究生实习简历**的项目经历
> **投递方向**：银行/券商 数据开发（数仓方向）
> **当前进度**：阶段二 ✅ · 阶段三 ✅（8/9 步）· **阶段四 ✅ 实时链路 + 跨链路对账（M1–M4 全部完成）** · CI ✅ **passing**
> **最后更新**：2026-09-24（阶段四收口：README 补上实时链路实测章节，撤掉"实时未实施"的旧声明）

---

## 📚 文档导航

| 文档 | 作用 |
|---|---|
| **`docs/SLA与监控.md`** | 四类 SLA 判据（新鲜度 / 数据量波动 / 指标越界 / ⭐ **跨链路对账**），每条附可执行 SQL 与实测基线 |
| **`docs/数据血缘.md`** | **23 节点**血缘图（Mermaid，由 `lineage.yaml` 自动生成，**不要手改**） |
| `docs/lineage.yaml`、`docs/gen_lineage.py` | 血缘的**单一事实来源** + 生成/校验脚本（CI 跑 `--check`，卡住悬空引用与单向边） |
| `docs/第18步-工程化收口验收记录.md` | 口径单测 / CI / 血缘 / SLA 的验收记录，含 6 处"照抄模板就会踩坑"的实测修正 |
| `docs/第20步-面试资产验收记录.md` | 项目文档与机器验收表的验收记录 |
| `08_benchmark/*.md` | E1–E5 五个对照实验报告 + 引擎选型结论 + 双引擎口径差异记录 |
| **`09_realtime/`** | ⭐ **阶段四实时链路**：`技术决策.md` + **M1–M4 四份验收记录** + 对账脚本 + CDC/湖仓 SQL；排错手册 **B-1~B-23** |
| `06_ops/runbook.md` | 运维手册（含 PowerShell 编码坑的一手教训） |
| `06_ops/docs_claims_guard.txt` | **文档过期说法守卫**：防止 README 与实测数字脱节，由验收脚本读取 |

> **本仓库只收可复现的工程交付物** —— 代码、SQL、单测、CI、血缘、SLA、
> 实验报告与验收记录。学习手册（逐步指导书、名词讲解、技术栈讲解、环境配置详解）
> 与求职材料**不入库**：它们对读者没有复现价值，且会让"文档重于工程"。
> 那些材料保留在仓库外的同级目录 `credit-dwh-private/`，规则见 `.gitignore` 的"私有材料"一节。

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
  **全程不连数据库**（这正是把口径抽成纯函数的意义）。**已跑绿**：
  [Actions run `35698960325`](https://github.com/Laplaceeeeeeee/credit-dwh/actions/runs/35698960325)，
  首页 badge 显示 `passing`。
- **数据血缘**：`docs/lineage.yaml` → `docs/gen_lineage.py` → `docs/数据血缘.md`
  （**25 个节点**、Mermaid，含阶段四的实时层；CI 跑 `--check` 卡住"悬空引用 / 单向边"）。
- **四类 SLA**：新鲜度 / 数据量波动 / 指标越界 / ⭐ **跨链路对账**（双引擎 + 批流），见 `docs/SLA与监控.md`。

## 🌊 阶段四：实时链路（CDC → Kafka → Flink → Paimon）与跨链路对账

> **一句话**：把同一份 `dwd_loan_fact` 用 CDC 接进流式链路，在 Paimon 上维护三张实时表，
> 并**把阶段三只写了判据、始终没实现的"跨链路对账"真正跑通**。

### 链路与版本矩阵

```
MySQL dwd_loan_fact --(Flink CDC 抓 binlog)--> Kafka（debezium-json 变更日志）
    --> 一个 Flink 作业（EXECUTE STATEMENT SET，三个 sink 共享同一个源）
          ├─ rt_loan_fact_latest   主键表：镜像最新状态（含 sink_ts 供延迟测量）
          ├─ rt_loan_change_1min   1 分钟滚动窗口（事件时间 + Watermark）
          └─ rt_loan_month_agg     按放款月维护的 upsert 指标表  ← 对账的实时侧
```

版本矩阵（2026-09-24 查制品锁定，全部实拉/实跑）：
Flink **1.20.5** · Paimon **1.4.2** · Flink CDC **3.6.0-1.20** · Kafka **4.3.1** · Paimon-Spark **3.5_2.12:1.4.2**
⚠️ 刻意**不用**最新的 `flink:2.3.0` —— Paimon 只发布到 `paimon-flink-2.2`，照抄"最新 tag"必然撞墙。

### 实测结果

| 里程碑 | 关键数字 |
|---|---|
| **M1 链路通** | 端到端延迟 **≈310 ms**（MySQL `NOW(3)` → Paimon `sink_ts`）；226 万行全量快照 **34 秒** |
| **M2 正确性** | 故障演练：`pause` TaskManager → JM 在 **50.4 秒**判定失效（= `heartbeat.timeout`）→ 恢复后 **5.1 秒** RUNNING、**10 秒**内补发停机期间的变更；`restored=1` |
| **M3 批流对账** | **139 个放款月，笔数差 0、金额差 0.00**；总笔数 2,260,668、总额 34,004,208,600.00 两侧完全一致 |
| **M4 湖仓查询** | Spark 直读**同一份** Paimon 存储；`VERSION AS OF` 回读历史快照，与最新快照并存 |

### ⭐ M2 抓出的真实缺陷（本阶段最有价值的产出）

故障演练发现：**`scan.startup.mode = latest-offset` 会在重启时静默丢数据**。
它的语义是"从启动那一刻的 binlog 末尾开始读"，于是重启时取新位点，
把停机期间的变更**整段跳过，且不报任何错**（Debezium 日志实测打印了 `restartBinlogPosition`）。
更危险的是**不确定**：同一份配置两次演练，一次丢、一次没丢。

修复：改用 `initial`（有存储位点就续读，没有才快照；万一位点丢失则退回全量快照，
重复由 Paimon 主键去重 —— **最坏是慢，不会丢**）。
验证方式是**只改这一个参数的对照实验**：修复后同一演练 Kafka 偏移
2,260,668 → 2,260,671，停机期间插入的 3 笔全部补上。

### 事件时间语义（实测边界，不是"配了个 Watermark"）

| 用例 | 记录级（主键表） | 窗口级 |
|---|---|---|
| `op_ts` 落在**已关闭**窗口（迟到 3 分钟） | **收到了** | **未计入** —— 窗口定稿后不被迟到数据改写 |
| 按墙上时钟"晚 90 秒"、但 Watermark 尚未越过 | 收到了 | **正常计入** |

> **记录不会丢，但窗口会丢迟到数据；而"迟到"是相对 Watermark 判定，不是相对墙上时钟。**

### 跨链路对账（`docs/SLA与监控.md` §5.3，从"只有判据"变成"有结果"）

```powershell
& .\.venv\Scripts\python.exe 09_realtime\check_batch_stream.py   # 退出码 0 = 通过
```

- 离线侧：MySQL `dwd_loan_fact`（阶段二/三那套 T+1 批处理的结果）
- 实时侧：Paimon `rt_loan_month_agg` —— **流上**维护的聚合，不是"镜像表的副本聚合"
- 容差：计数要求**严格相等**；金额容差 **0.00**（两侧都是精确十进制求和，差异本身就该是问题信号）
- ⚠️ 粒度是**放款月**不是日：本数据集的 `issue_date` 一律为"当月 1 日"，没有真正的日粒度

### 湖仓查询（M4：第二个引擎直读同一份存储）

Spark 通过 Paimon catalog 直读（`warehouse` 以 **`:ro` 只读**挂载 —— 读者不写湖）。
Flink 全程持续流写，同一次 Spark 查询里读两种视图：

| 视图 | `issue_month='2007-06'` | 总笔数 |
|---|---|---|
| 最新快照 | **25 / 93,084.56** | **2,260,669** |
| `VERSION AS OF 5` | **24 / 91,850.00** | **2,260,668** |

⭐ **可见性延迟 ≈ 9.8 秒**：Paimon 的快照提交**绑定在 Flink checkpoint 上**（配置 10 秒间隔）。
注意它和 M1 的 310 ms 是两件事 —— **310 ms 是"入队"，9.8 秒是"可被查询引擎看到"**。
要压可见性延迟就得调小 checkpoint 间隔，代价是更频繁的提交与更多小文件。

> 实现与验收细节见 `09_realtime/` 下的 `技术决策.md` 与 M1–M4 四份验收记录
> （含 **B-1 ~ B-23 共 23 条排错手册**：`docker manifest inspect` 不走 registry-mirrors、
> 权限主体不一致导致的"伪 Hadoop bug"、Windows bind mount 对 JVM 有状态服务不可靠等）。

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
├── docs/                      ⭐ 工程交付物（学习手册不入库，见 .gitignore"私有材料"）
│   ├── 数据血缘.md / lineage.yaml / gen_lineage.py   血缘（生成物不要手改）
│   ├── SLA与监控.md           四类 SLA 判据
│   └── 第18步/第20步-*验收记录.md
├── 01_eda/                    字段画像 / 关键检查 / 数据探查报告
├── 03_metrics/                指标口径字典 / 数据边界说明
├── 04_analysis/               图表与分析报告
├── 06_ops/                    一键流水线 / 运维手册 / verify_stage3.ps1 / docs_claims_guard.txt
├── 07_bigdata/                阶段三：docker-compose、Hive SQL、PySpark、交换脚本
├── 08_benchmark/              阶段三：测量框架（harness/事件日志/数文件）+ E1–E5 报告
├── 09_realtime/               🆕 阶段四：实时链路（CDC/Kafka/Flink/Paimon）+ 跨链路对账
│   ├── docker-compose-streaming.yml   Kafka(KRaft) + Flink JM/TM [+ Spark 按需 profile]
│   ├── sql/01-04              CDC→Kafka、Kafka→Paimon(3 sink)、对账导出、Spark 直读
│   ├── check_batch_stream.py  ⭐ 批流对账脚本（退出码 0/1）
│   ├── init/                  共享网络 / 最小权限 CDC 账号 / 卷归属
│   └── M1..M4-*验收记录.md     四份验收记录（含 B-1~B-23 排错手册）
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
& .\.venv\Scripts\ruff.exe check src/ tests/ 07_bigdata/ 08_benchmark/ 09_realtime/ docs/gen_lineage.py
& .\.venv\Scripts\python.exe -m pytest tests\ -v -m "not integration"   # 期望 34 passed
& .\.venv\Scripts\python.exe docs\gen_lineage.py --check                # 期望 25 个节点

# 5. 阶段三机器验收（一条命令出结论；-Full 会额外查 Docker 与跑 Spark 一致性）
.\06_ops\shell\verify_stage3.ps1          # 快速模式
.\06_ops\shell\verify_stage3.ps1 -Full    # 完整模式
```

### 阶段四（实时链路 / 湖仓对账）

```powershell
# 1. 初始化（幂等）：共享网络 + 最小权限 CDC 账号 + 卷归属
cd 09_realtime
powershell -NoProfile -ExecutionPolicy Bypass -File .\init_network.ps1
Get-Content .\init\01_create_cdc_user.sql -Raw |
  docker exec -i credit-dwh-mysql mysql -uroot -proot123456

# 2. 起实时栈（Kafka KRaft + Flink JM/TM）
docker compose -f docker-compose-streaming.yml up -d
powershell -NoProfile -ExecutionPolicy Bypass -File .\init\02_prepare_volumes.ps1

# 3. 建主题（M1a 还没产出时，主题不会被自动创建）
docker exec rt-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 `
  --create --if-not-exists --topic credit_dwd_loan_fact --partitions 1 --replication-factor 1

# 4. 提交两个作业
#    ⚠️ 必须 -u flink：docker exec 默认 root，而 Flink 守护进程是 uid 9999，
#       DDL 以 root 建出的 warehouse 目录会让 TaskManager 写不进去（B-12）
docker exec -u flink rt-jobmanager /opt/flink/bin/sql-client.sh -f /sql/01_cdc_to_kafka.sql
docker exec -u flink rt-jobmanager /opt/flink/bin/sql-client.sh -f /sql/02_kafka_to_paimon.sql

# 5. ⭐ 跨链路对账（退出码 0 = 通过）
cd ..
& .\.venv\Scripts\python.exe 09_realtime\check_batch_stream.py

# 6. 湖仓查询：按需启动第二个引擎，用完停掉省内存
cd 09_realtime
docker compose -f docker-compose-streaming.yml --profile query up -d spark-query
docker exec rt-spark-query /opt/spark/bin/spark-sql `
  --jars /opt/paimon/paimon-spark-3.5_2.12-1.4.2.jar `
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog `
  --conf spark.sql.catalog.paimon.warehouse=file:///warehouse `
  --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions `
  --conf spark.ui.enabled=false -f /sql/04_spark_read_paimon.sql
docker compose -f docker-compose-streaming.yml --profile query stop spark-query
```

> ⚠️ **两条内存规则方向相反，不要照抄**：
> 阶段三是"跑 Spark 重任务前停 MySQL"；
> **阶段四是 CDC 需要 MySQL 活着，该停的是 Hive/Spark 那四个容器**。

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
