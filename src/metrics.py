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
