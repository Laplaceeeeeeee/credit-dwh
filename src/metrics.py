"""指标口径的纯函数实现 —— 口径字典在代码层的唯一来源。

⭐ 为什么单独抽出来：只有无副作用的纯函数才能被单元测试覆盖。
   这是一次有价值的重构，也是"我为了让口径可测试而重构"的面试素材。
   阶段三会用它跑 pytest 指标单测。
"""
from src import config


def is_bad_status(status) -> bool:
    """是否不良。不良 = Charged Off + Default（含政策外核销）。"""
    if status is None:
        return False
    try:
        return str(status).strip() in config.BAD_STATUSES
    except Exception:
        return False


def is_terminal_status(status) -> bool:
    """是否为终态（已结清或已核销）。

    ⭐ 算不良率必须限定终态，否则 Current 会稀释分母
       —— 本数据集 Current 占 38.85%，会把不良率从 19.98% 压到约 11.9%。
    """
    if status is None:
        return False
    try:
        return str(status).strip() in config.TERMINAL_STATUSES
    except Exception:
        return False


def dpd_bucket_of(status) -> str:
    """loan_status -> DPD 档位。未知状态返回 UNKNOWN，不抛异常。"""
    if status is None:
        return "UNKNOWN"
    return config.DPD_MAP.get(str(status).strip(), "UNKNOWN")


def weighted_avg_rate(pairs) -> float | None:
    """加权平均利率。pairs = [(金额, 利率), ...]

    ⭐ 必须按金额加权 —— 简单平均会让小额与大额贷款同权。
    """
    valid = [(a, r) for a, r in pairs if a is not None and r is not None]
    total = sum(a for a, _ in valid)
    if not total:
        return None
    return sum(a * r for a, r in valid) / total


def skew_ratio_of(share: float, n_keys: int) -> float:
    """倾斜比 = 该 key 占比 ÷ 平均占比 = share × n_keys。

    ⭐ 为什么要把它抽成函数并单测：
       这个公式在数据倾斜分析里极其常用，而它有一个非常隐蔽的写错方式 ——
       在按 key 分组的查询里写 `COUNT(DISTINCT key)` 来自算"平均占比"。
       那个表达式**恒等于 1**，于是算出来的"倾斜比"就等于占比本身
       （47.63% → 0.4763），你会得出"这里没有倾斜"的错误结论。
       写成纯函数 + 单测，就很难再错第二次。

    例：9 个 key、某 key 占 47.63% → 0.4763 × 9 ≈ 4.29（严重倾斜）
    """
    if share is None or not n_keys:
        return 0.0
    return share * n_keys
