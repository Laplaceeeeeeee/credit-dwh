# 运维手册（Runbook）

> 出问题时先看这里，再动手排查。

## 1. 一键跑全流程

```powershell
# 启动数据库
docker start credit-dwh-mysql

# 跑全流程（8 步，含质量校验）
.\06_ops\shell\run_pipeline.ps1
```

## 2. 环境前置检查

| 检查项 | 命令 | 期望 |
|---|---|---|
| Docker 在跑 | `docker info` | 有 ServerVersion |
| MySQL 在跑 | `docker ps` | credit-dwh-mysql 为 Up |
| MySQL 端口 | 3307（**不是 3306**） | 与 docker-compose.yml 一致 |
| venv 存在 | `.venv\Scripts\python.exe --version` | Python 3.11 |
| 干净数据 | `data\clean\loans_full.parquet` | 存在 |

## 3. 常见故障与处理

| 现象 | 原因 | 处理 |
|---|---|---|
| `skipfooter not supported for iteration` | pandas 的 skipfooter 与 chunksize 互斥 | **本项目已不依赖 skipfooter**，改走 `prepare_raw` + Parquet。若仍报错说明用了旧脚本 |
| `could not convert string to float: 'Total amount funded...'` | 页脚行混入数据 | 重跑 `python -m src.prepare_raw`（会剔除 33 行页脚） |
| `Table schema does not match schema used to create file` | 分块读取 dtype 漂移 | `prepare_raw` 已固定 dtype；重跑即可 |
| `UnicodeEncodeError: 'gbk' codec` | Windows 控制台 GBK | `src/utils.py` 导入时自动修；若内联脚本报错，先 `$env:PYTHONIOENCODING='utf-8'` |
| `ERROR 1064 ... near 'year_month'` | MySQL 保留字 | DDL 已全部加反引号；不要手写不带反引号的列名 |
| 连接被拒绝 / 端口不通 | 端口写成了 3306 | 改 `config.DB['port']` 或设环境变量 `DB_PORT=3307` |
| `id` 校验失败"不是纯数字" | 干净数据被污染或 Parquet 里存成了 float | 确认 `prepare_raw` 把 id 存为 string；重跑预处理 |
| 快照表为空 | 未跑 `dws_snapshot` | ADS 里依赖快照的指标会被跳过，先跑第 06 步 |
| 内存不足 | 全量 137 列一次读入 | 用 `MAX_ROWS` 限制规模，或按列读取 |

## 4. 数据规模切换

`src/config.py`：

```python
MAX_ROWS = 200_000   # 子集，调流程用（前 20 万行，偏早期放款）
MAX_ROWS = None      # 全量 2,260,668 行，出最终数字用
```

> ⚠️ **比例型指标（不良率、Vintage）必须用全量算**。
> 子集里 Current 只占 11.3%，全量是 38.9% —— 分布完全不同。

切换后需要**从第 02 步起重跑**（预处理产物已同时生成全量与子集两份，不用重跑第 01 步）。

## 5. 数据事实速查（实测）

| 项 | 值 |
|---|---|
| 物理行数 | 2,260,702 |
| 页脚行（已剔除） | 33 |
| 真实数据行 | **2,260,668** |
| 保留列数 | 137（原 151，剔除 14 个全空列） |
| issue_d 范围 | 2007-06 ~ 2018-12 |
| 终态不良率 | **19.9807%** |
| 不良笔数 | 269,360 |
| 终态笔数 | 1,348,099 |

---

## 6. ⚠️ 磁盘规划（重要，本项目踩过大坑）

### 6.1 背景

Windows 上的 Docker Desktop 把整个 Linux 环境装在**一个虚拟磁盘文件**里：

```
%LOCALAPPDATA%\Docker\wsl\disk\docker_data.vhdx
```

**这个文件只增不减。** 实测数据库真实数据只有 10.73 GB，
但 vhdx 涨到了 **74 GB**（碎片 + 已删除块 + 临时文件）。
一旦它把宿主盘撑满，**Docker daemon 会启动失败**，
所有容器和数据都取不出来。

### 6.2 各步磁盘开销（全量 226 万行，实测）

| 项 | 占用 |
|---|---|
| 预处理 Parquet（在项目盘，不进 Docker） | 338 MB |
| ODS + DWD + 维表 | 约 2.7 GB |
| 快照表 MOB≤24（4471 万行） | 约 8 GB |
| 快照表 MOB≤12（2521 万行） | 约 4.5 GB |
| **索引重建 / 大表 JOIN 的临时空间** | **可能翻倍，最危险** |

### 6.3 跑重任务前的三条检查

```powershell
# ① 看各盘剩余
Get-PSDrive C,D,E,F | Select-Object Name, @{N='可用GB';E={[math]::Round($_.Free/1GB,1)}}

# ② 看 Docker 虚拟盘大小与位置
Get-Item "$env:LOCALAPPDATA\Docker\wsl\disk\docker_data.vhdx" |
  Select-Object @{N='GB';E={[math]::Round($_.Length/1GB,2)}}, LastWriteTime

# ③ 看数据库真实占用
docker exec credit-dwh-mysql mysql -uroot -proot123456 -N -e @"
SELECT ROUND(SUM(DATA_LENGTH+INDEX_LENGTH)/1024/1024/1024,2) AS GB
FROM information_schema.TABLES WHERE TABLE_SCHEMA='credit_dwh';"
```

**规则**：任何盘剩余 **< 20 GB** 时，不要跑快照重建或大表 JOIN。

### 6.4 爆盘后的恢复顺序

```powershell
# 1. 关闭 Docker Desktop（这一步常能还回几十 GB）
& "C:\Program Files\Docker\Docker\DockerCli.exe" -Shutdown
Start-Sleep 20

# 2. 确认系统盘已回收到 ≥ 20 GB
Get-PSDrive C | Select-Object @{N='可用GB';E={[math]::Round($_.Free/1GB,1)}}

# 3. 启动 Docker Desktop
Start-Process "C:\Program Files\Docker\Docker\Docker Desktop.exe"
# 等 daemon 就绪（docker info 能返回版本号）

# 4. 立刻验证数据完好
docker start credit-dwh-mysql
docker exec credit-dwh-mysql mysql -uroot -proot123456 -N -e @"
USE credit_dwh;
SELECT 'snapshot' t, COUNT(*) c FROM dws_loan_snapshot_m
UNION ALL SELECT 'dwd', COUNT(*) FROM dwd_loan_fact;"

# 5. 【关键】把大表规模降下来，避免再次爆盘
#    改 src/dws_snapshot.py 的 MOB_MAX，然后重跑 src.dws_snapshot
```

### 6.5 根治：把 Docker 数据目录迁到空闲盘

见学习手册 `信贷数仓实施手册(阶段2).md` §5.4（不入库）—— 两种做法（GUI 设置 / 目录联接）都有步骤。

### 6.6 规模与磁盘的对照表（选规模时参考）

| 规模 | 快照行数 | 数据库占用 | 建议磁盘余量 |
|---|---|---|---|
| 子集 20 万行，MOB≤24 | 480 万 | 约 1.2 GB | ≥ 10 GB |
| **全量，MOB≤12** | **2521 万** | **约 4.5 GB** | **≥ 30 GB** |
| 全量，MOB≤24 | 4471 万 | 约 8 GB | ≥ 40 GB |
| 全量，MOB≤36 | 5916 万 | 约 11 GB | ≥ 50 GB |

> 💡 **推荐**：全量 + `MOB_MAX = 12`。它覆盖首年表现窗口（年化不良率的关键），
> 磁盘占用减半，Vintage 与迁徙率分析都够用。
