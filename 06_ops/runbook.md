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
| issue_d 范围 | Apr-2008 ~ Sep-2018 |
| 终态不良率 | **19.9807%** |
| 不良笔数 | 269,360 |
| 终态笔数 | 1,348,099 |
