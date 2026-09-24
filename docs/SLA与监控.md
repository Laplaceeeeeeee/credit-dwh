# 数据 SLA 与监控设计

> **定位**：数仓岗每天做的事，本质上就是"保住 SLA"。
> 本文定义**四类业务级 SLA 检查**、每类的可执行 SQL/命令、以及实测基线。
>
> **诚实边界（先写在最前面）**：
> 本项目是单机学习项目，**没有真的调度器（Airflow/DolphinScheduler）也没有告警通道（钉钉/邮件）**。
> 触发方式是**阶段二的一键流水线 + 人工**：`06_ops/shell/run_pipeline.ps1` 任一非可选步骤失败即中止，
> 阶段三的跑批后追加 `07_bigdata/check_consistency.py` 与 `07_bigdata/compare_ads.py`。
> 本文的价值在于**把"什么叫出了事故"写成可执行的判据和阈值**，而不是"我搭了个监控平台"。

---

## 1. 四类 SLA 总表

| # | SLA 类型 | 回答的问题 | 阈值 | 违反的典型原因 | 状态 |
|---|---|---|---|---|---|
| 1 | **新鲜度** | 数据有没有按时到 | MySQL 表 T+1 08:00 前产出；Hive 最新分区 = 本批应产出分区 | 上游断流、任务排队 | ✅ 规则+命令已定义 |
| 2 | **数据量波动** | 数据量对不对 | 放款月笔数 / 近 7 月均值 ∈ [0.8, 1.2]；全表行数 == 2,260,668 | 重复写入 / 上游断流 | ✅ 规则+命令已定义 |
| 3 | **指标越界** | 指标有没有突变 | 不良率环比波动 ≤ 30%；口径自检 4 条全 0 | 口径被改错、数据异常 | ✅ 规则+命令已定义 |
| 4 | **跨链路对账** ⭐ | 两条链路都"成功"，数字一样吗 | 双引擎：计数 0、比率 ≤ 5e-5 ｜ 批流：计数 0、金额 0.00（**月粒度**，见 §5.3） | 引擎语义差异、分区漏挂、重复消费 | ✅ **双引擎已实现 ｜ 批流已实现（2026-09-24，见 §5.3）** |

> **为什么第 4 类是阶段三的重点**：前 3 类是"**单条链路内部**"的健康检查；
> 第 4 类是"**跨链路一致性**"检查 —— 这才是最难查的一类事故：
> **两条链路各自都返回成功，数字却对不上。**

---

## 2. 第一类：新鲜度

### 2.1 MySQL 侧（阶段二，T+1 08:00）

```sql
-- 水位表：增量链路是否推进到本批
SELECT table_name, last_value AS watermark, updated_at,
       CASE WHEN updated_at >= CURDATE() THEN 'OK' ELSE 'ALERT' END AS status
FROM etl_watermark
ORDER BY updated_at DESC;
```

流水线侧的强校验：`run_pipeline.ps1` 先 `mysqladmin ping` 探活，
再按 `prepare_raw → ods_load → dwd_clean → dwd_derived → dim_build → dws_snapshot → ads_metrics → dq_check`
顺序执行，**任一步非 0 退出码即中止**（"任务没跑完"本身就是新鲜度事故）。

### 2.2 Hive 侧（阶段三）：分区新鲜度

```sql
-- 最新分区是否等于本批应产出的月份
SHOW PARTITIONS dwd_loan_fact_lc;
-- 期望：139 个 issue_month 分区，最新 = 2018-12（实测放款跨度 2007-06 ~ 2018-12）
SHOW PARTITIONS dws_loan_snapshot_lc;
```

```bash
# 分区目录的文件数（Hive-serde 写路径的文件没有 .orc 扩展名，叫 part-*.c000）
docker exec bd-spark bash /workspace/08_benchmark/measure_files.sh dwd_loan_fact_lc 2018-06
```

> ⚠️ 实测坑：`measure_files.sh` 早期版本按 `*.orc` 统计，把分区表数成 **0 个文件**（假阴性）。
> 正确做法是按 `part-*` 统计并排除 `.crc`。

---

## 3. 第二类：数据量波动

### 3.1 全表行数守恒（最强的单条断言）

```sql
SELECT 'ods_loan_raw'       AS tbl, COUNT(*) AS cnt FROM ods_loan_raw
UNION ALL SELECT 'dwd_loan_fact',      COUNT(*) FROM dwd_loan_fact
UNION ALL SELECT 'dwd_loan_perf_fact', COUNT(*) FROM dwd_loan_perf_fact;
-- 期望三者都等于 2,260,668（实测差 0，见 05_quality/数据质量校验报告.md Q1）
```

### 3.2 放款月笔数的波动检测

```sql
-- 每个月笔数 vs 近 7 个月（含自身）均值，超出 ±20% 告警
WITH m AS (
  SELECT issue_month, COUNT(*) AS cnt
  FROM dwd_loan_fact
  GROUP BY issue_month
)
SELECT issue_month, cnt,
       ROUND(AVG(cnt) OVER (ORDER BY issue_month
                            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1) AS ref_avg,
       ROUND(cnt / AVG(cnt) OVER (ORDER BY issue_month
                            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 3) AS ratio,
       CASE WHEN cnt / AVG(cnt) OVER (ORDER BY issue_month
                            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
                 NOT BETWEEN 0.8 AND 1.2
            THEN 'ALERT' ELSE 'OK' END AS status
FROM m
ORDER BY issue_month;
```

> ⚠️ **对指导书 18.5 示例 SQL 的一处修正**：原文把"当日行数 / 近 7 日均值"
> 写成了两个都读 `dwd_loan_fact` 全表的子查询（`COUNT(*)` / `COUNT(*)`），
> 那个比值**恒等于 1.0**，永远返回 OK —— 是个**假监控**（和 E4 里
> `COUNT(DISTINCT key)` 恒等于 1 是同一类错误）。上面改成了按放款月的真实参照窗口。
>
> 另外诚实说明：本项目是**一次性全量装载**，没有"每日增量"，所以真正能监控的是
> **分区/分月行数的相对波动**与**全表行数守恒**，而不是"今天比昨天多了几行"。
> 生产里应该按 `etl_date` 落一张装载日志表再比，本项目没有这张表 —— 这是缺失，不是设计。

### 3.3 实测基线（用作阈值锚点）

| 指标 | 实测值 |
|---|---|
| 全表行数 | **2,260,668**（2,260,668 = ODS = DWD = DWS 来源行） |
| 放款月数 | **139**（2007-06 ~ 2018-12） |
| Hive `dwd_loan_fact_lc` 分区数 | **139**（= 放款月数；且**每分区恰好 1 个文件**，48.54 MB） |
| Hive `dws_loan_snapshot_lc` 分区数 | **141**（= 快照观测月数，560.31 MB） |
| 终态笔数 | **1,348,099** |
| 不良笔数 | **269,360** |

---

## 4. 第三类：指标越界

### 4.1 不良率环比突变（口径被改错的强信号）

```sql
-- mob=6 的累计不良率，环比波动 > 30% 告警（MySQL 8 窗口函数）
SELECT issue_month, mob, bad_rate,
       LAG(bad_rate) OVER (ORDER BY issue_month) AS prev_bad_rate,
       ROUND((bad_rate - LAG(bad_rate) OVER (ORDER BY issue_month))
             / LAG(bad_rate) OVER (ORDER BY issue_month), 4)          AS chg,
       CASE WHEN ABS((bad_rate - LAG(bad_rate) OVER (ORDER BY issue_month))
                     / LAG(bad_rate) OVER (ORDER BY issue_month)) > 0.3
            THEN 'ALERT' ELSE 'OK' END                                AS status
FROM ads_delinq_monthly
WHERE mob = 6
ORDER BY issue_month;
```

> ⚠️ 阈值 30% 要配合"**变化是否有业务解释**"一起看：Vintage 曲线本身在
> 早/晚批次会有系统性差异（早年批次观察期长、2008 金融危机批次质量差），
> 单纯按 30% 卡会产生误报。**告警指标要配注释，否则会被忽略。**

### 4.2 口径自检 4 条（直接返回 0 才算通过）

阶段三把它固化成了 SQL：`07_bigdata/hive/08_ads_selfcheck.sql`

| 自检 | 判据 | 实测 |
|---|---|---|
| Vintage 单调性 | 同批次内 mob 越大不良率不下降 | 0 行违反 |
| ALL 行守恒 | `grade='ALL'` 的暴露笔数 = 各评级之和 | 0 行违反 |
| 迁徙率归一 | 每个 `from_month × from_bucket` 的迁出占比之和 = 1 | 0 行违反 |
| 分层汇总一致 | 各分层笔数之和 = 全量笔数 | 0 行违反 |

### 4.3 关键指标基线（越界即 ALERT）

| 指标 | 实测基线 | 说明 |
|---|---|---|
| 终态不良率 | **19.9807%** | 269,360 / 1,348,099 |
| 总放款金额 | **340.04 亿** | `SUM(funded_amnt)` |
| 加权平均利率 | **13.3824%** | 必须按金额加权（简单平均会失真） |
| Current 占比 | 38.85% | 非终态，**不得进不良率分母** |

> 这四条里最容易出事的是分母口径：把 `Current` 算进分母会把不良率从
> 19.98% 压到约 11.9%。这条已经写成单测
> （`tests/test_metric_definitions.py::Test终态口径::test_不良率分母必须排除非终态`），
> **改错代码会让 CI 变红，而不是等报表出数才发现。**

---

## 5. ⭐ 第四类：跨链路对账（阶段三新增）

### 5.1 双引擎对账（MySQL vs Hive/Spark）

| 检查 | 命令 | 规则 | 实测结果 |
|---|---|---|---|
| 关键指标 | `python 07_bigdata/check_consistency.py` | 计数差 0；比率差 ≤ 5e-5 | **7/7 一致，最大差异 0.0**（含总放款金额 340.04 亿、加权平均利率 13.3824） |
| 4 张 ADS 逐行 | `python 07_bigdata/compare_ads.py` | 行数与逐行键值全等 | **18 / 24,768 / 108 / 3,105 行，键未匹配 0** |
| Hive 侧口径自检 | `docker exec -i bd-spark /opt/spark/bin/spark-sql -f /workspace/07_bigdata/hive/08_ads_selfcheck.sql` | 4 条全返回 0 行 | 通过 |

> ⚠️ **容差为什么是 5e-5 而不是 1e-6**（指导书 18.5 表里写的是 1e-6）：
> MySQL 的 `div_precision_increment=4` 让 `DECIMAL` 除法**只有 4 位小数**，
> 而 Spark 给 6 位 —— 两侧最多差 5e-5，这是**引擎语义差异，不是数据错误**，
> 1e-6 在当前配置下**永远无法通过**。
> **没有放宽容差糊过去**，而是补了第二条可验证断言："平局判据"
> （差异恰好落在 5e-5 边界时，检查两侧的分母是否一致，一致则判为等价）。
> 面试被追问"你为什么不直接把容差调大"时，这就是答案。

### 5.2 分区覆盖对账

```sql
-- Hive：目标表最新分区 == 上游应产出分区（漏分区是最隐蔽的"对账不平"来源）
SHOW PARTITIONS dwd_loan_fact_lc;
SHOW PARTITIONS dws_loan_snapshot_lc;
```

```sql
-- MySQL 侧重算分区键集合，与上面人工/SQL 逐月比对
SELECT DISTINCT issue_month FROM dwd_loan_fact ORDER BY issue_month;
```

### 5.3 批流对账（✅ **已实现**，2026-09-24）

**实现的规则**

| 检查 | 规则 | 容忍度 | 状态 |
|---|---|---|---|
| 离线 vs 实时**月粒度**笔数 | `count(offline) == count(realtime)`，逐月 | **严格相等（diff = 0）** | ✅ 已跑通 |
| 离线 vs 实时**月粒度**金额 | `abs(offline - realtime) == 0.00`，逐月 | 阈值 0.00（见下） | ✅ 已跑通 |
| 放款月集合 | 两侧 `issue_month` 集合**完全相同** | 多/少一个月即失败 | ✅ 无差异 |

**实测结果**（`09_realtime/check_batch_stream.py`，退出码 0）

| 项 | 离线侧（MySQL `dwd_loan_fact`） | 实时侧（Paimon `rt.rt_loan_month_agg`） |
|---|---|---|
| 放款月数 | **139** | **139** |
| 总笔数 | **2,260,668** | **2,260,668** |
| 总金额 | **34,004,208,600.00** | **34,004,208,600.00** |
| 最大逐月计数差 | — | **0** |
| 最大逐月金额差 | — | **0.00** |

逐月明细：`09_realtime/_results/batch_stream_diff.csv`

**两条路径是什么**

- **离线侧**：阶段二/三那套 T+1 批处理的结果，落在 MySQL `dwd_loan_fact`；
- **实时侧**：`rt.rt_loan_month_agg` —— **流上**按 `issue_month` 维护的 upsert 聚合
  （输入是含 `+I / -U / +U / -D` 的 changelog，不是"镜像表的副本聚合"）。

二者算的是同一个指标，**路径完全独立**，一致才说明跨链路对得上。

**⚠️ 与原文的偏差：粒度是"放款月"，不是"日" —— 必须如实说明**

本节原先写的判据是"**日粒度**笔数/金额"。那是为"有日事件"的系统写的。
本项目的数据集**没有真正的日粒度**：`dwd_loan_fact.issue_date` 一律是"当月 1 日"
（DDL 注释原文即 `放款日期(当月1日)`），业务事实的最小粒度就是放款月。
所以对账按 `issue_month` 做，**不能声称"做过日粒度对账"**。

生产环境若事件本身带日粒度，本节的两条规则可直接照用（把 `issue_month` 换成日期分区键即可）；
届时还要额外考虑**时区与口径窗口边界**（"哪一天"在两侧按谁的时区算）。

**容差为什么是 0.00 而不是 0.01%**

金额两侧都是精确十进制求和（MySQL `DECIMAL(14,2)` vs Flink `DECIMAL` 求和），
**理论上就该精确相等**，实测也是 0.00。所以容差设 0.00：
**一旦出现差异，差异本身就是问题信号**，不该被容差吞掉。
（对比 §5.1 的比率列：那里的 5e-5 是 MySQL `div_precision_increment=4`
带来的**引擎量化语义差异**，有明确根因，才给了容差 —— 两者性质不同。）

**怎么跑**

```powershell
cd F:\BUPT\Internship_Preparation\credit-dwh
& .\.venv\Scripts\python.exe 09_realtime\check_batch_stream.py
# 退出码 0 = 通过；1 = 有超容差项或月份集合不一致
```

**实现方式**：实时侧不能靠 Python 直接读 Paimon（主键表的 LSM 语义无法靠直读 parquet 复现），
所以用 Flink 自己的 **batch 读 + filesystem sink 落 CSV**，再由脚本从容器里取出比对
（`09_realtime/sql/03_export_realtime_agg.sql`）—— 读取用的是**引擎的正确语义**。

> **本文其余部分仍然成立**：下面 §5.3 之外的批流相关限制没有变化
> —— 本项目**没有真实业务实时源**（源是真实 binlog，但业务场景是历史库的回放式写入），
> 规模是单机单 broker，**不能声称高可用或生产级吞吐**。

---

## 6. 告警接入（现状与差距）

**已实现**：

1. `06_ops/shell/run_pipeline.ps1`：任一步失败 → 非 0 退出码 → 中止后续步骤；
2. 跑批后串行执行 `07_bigdata/check_consistency.py` 与 `07_bigdata/compare_ads.py`，任一非 0 即视为告警；
3. CI（`.github/workflows/ci.yml`）：口径单测 + ruff + 血缘一致性校验，**把"口径被改错"拦在提交阶段**。

**未实现（诚实列出）**：

- 没有调度器（靠人工/一条命令跑批）；
- 没有告警通道（没有钉钉/邮件/短信）；
- 没有把本文的 SQL 包成一个 `sla_check.py` 并定时执行 —— 规则是"可执行的 SQL"，但**不是自动化任务**。

> 面试话术："我实现的是**判据**，不是**平台**。四类检查里前两类和第四类的双引擎部分
> 都有真实命令和实测结果；批流对账和定时告警还没落地，我不会说自己做过。"

---

## 7. 我为什么不做全链路监控平台

Atlas、Great Expectations、DolphinScheduler、Prometheus + Grafana 都能做这件事，
但**单机学习项目引入重型平台是过度设计**：
装起来容易（几十分钟），讲清楚"它到底在监控什么"才是难的。

我用 **SQL + 纯函数单测 + 一个 CI** 实现了四类核心检查，覆盖了实际场景的绝大部分：

- 口径类判据 → 抽成**纯函数**并由 CI 断言（不需要数据库就能跑）；
- 数据类判据 → **可执行 SQL**（跑批后手动/串行执行）；
- 跨链路判据 → **已有的两个对账脚本**（`check_consistency.py` / `compare_ads.py`）。

**知道什么不做，和知道什么要做一样重要。**
如果这是生产项目，我的下一步**不是**买平台，而是：
① 把跑批挂到调度器上；② 给 `sla_check.py` 接一个通知通道；③ 落一张装载日志表让"每日波动"真正可算。
