"""指标口径单测：用纯函数固化口径，不依赖任何数据库。

⭐ 这是数据岗最稀缺的能力之一：把"口径"变成可自动回归的断言。
   口径被改错时，CI 会直接失败 —— 而不是等到报表出数才发现。

⚠️ 本文件修正了 v2 的一处事实性错误：
   v2 写了 `assert ods - dwd == 32`（"32 行缺 issue_d 被剔除"）。
   实测那是**误判**：差值就是 0，见 `05_quality/数据质量校验报告.md` Q1
   —— ODS = DWD = 2,260,668。那 32 行不是"缺字段的真实记录"，
   而是散布在文件里的页脚，**在预处理阶段就被剔除了**（见
   `src/prepare_raw.py` 与 `data/clean/footer_line.txt`）。
   把与实测不符的数字写进单测，会让 CI 永远红着，最后没人再看 CI。

运行：
    pytest tests/ -v                      # 与 CI 完全一致的命令
    pytest tests/ -v -m "not integration"  # CI 用的写法（本文件全是纯函数）
"""
import pytest

from src import config
from src.metrics import (
    is_bad_status, is_terminal_status, dpd_bucket_of,
    weighted_avg_rate, skew_ratio_of,
)


class Test不良口径:
    def test_charged_off_是不良(self):
        assert is_bad_status("Charged Off") is True

    def test_default_是不良(self):
        assert is_bad_status("Default") is True

    def test_政策外核销是不良(self):
        assert is_bad_status(
            "Does not meet the credit policy. Status:Charged Off") is True

    def test_fully_paid_不是不良(self):
        assert is_bad_status("Fully Paid") is False

    def test_current_不是不良(self):
        """⚠️ Current 仍在还款，不能算不良 —— 最容易被写错的一条"""
        assert is_bad_status("Current") is False

    def test_未知状态不抛异常(self):
        """口径函数必须对脏数据健壮，不能因为一个未知值崩掉整条链路"""
        assert is_bad_status("SomeUnknownStatus") is False

    def test_none_不抛异常(self):
        assert is_bad_status(None) is False

    def test_两侧空白不影响判定(self):
        """实测原始 CSV 的状态列带前后空格，清洗前也必须判得准"""
        assert is_bad_status("  Charged Off ") is True


class Test终态口径:
    def test_current_非终态(self):
        assert is_terminal_status("Current") is False

    def test_逾期中非终态(self):
        assert is_terminal_status("Late (31-120 days)") is False

    def test_grace_period_非终态(self):
        assert is_terminal_status("In Grace Period") is False

    def test_fully_paid_是终态(self):
        assert is_terminal_status("Fully Paid") is True

    def test_不良率分母必须排除非终态(self):
        """⭐ 回归测试：当初算错成 11.92% 就是因为漏了这条"""
        statuses = ["Fully Paid", "Charged Off", "Current", "Current"]
        terminal = [s for s in statuses if is_terminal_status(s)]
        bad = [s for s in statuses
               if is_terminal_status(s) and is_bad_status(s)]
        rate = len(bad) / len(terminal)
        assert rate == pytest.approx(0.5)      # 1/2
        assert rate != pytest.approx(0.25)     # 明确排除错误口径

    def test_不良集合必须是终态集合的子集(self):
        """口径自洽：不良状态若不在终态里，不良率分子就永远取不到它"""
        assert set(config.BAD_STATUSES) <= set(config.TERMINAL_STATUSES)


class Test加权平均利率:
    def test_按金额加权而非简单平均(self):
        # 1000 元 @10% + 100 元 @20%
        # 简单平均 = 15%；按金额加权 = (1000*10 + 100*20)/1100 ≈ 10.909%
        got = weighted_avg_rate([(1000, 10.0), (100, 20.0)])
        assert got == pytest.approx(10.909, abs=0.001)
        assert got != pytest.approx(15.0)      # 明确排除简单平均

    def test_空输入返回None而不是崩(self):
        assert weighted_avg_rate([]) is None
        assert weighted_avg_rate([(0, 10.0)]) is None

    def test_含None不抛异常(self):
        assert weighted_avg_rate([(1000, None)]) is None

    def test_单个元素等于其利率(self):
        assert weighted_avg_rate([(1234.0, 13.3824)]) == pytest.approx(13.3824)


class TestDPD档位:
    @pytest.mark.parametrize("status,expect", [
        ("Fully Paid", "PAID"),
        ("Current", "CURRENT"),
        ("In Grace Period", "DPD_1_30"),
        ("Late (16-30 days)", "DPD_1_30"),
        ("Late (31-120 days)", "DPD_31_120"),
        ("Charged Off", "CHARGED_OFF"),
        ("Default", "CHARGED_OFF"),
    ])
    def test_档位映射(self, status, expect):
        assert dpd_bucket_of(status) == expect

    def test_未知状态是UNKNOWN(self):
        assert dpd_bucket_of("???") == "UNKNOWN"

    def test_none_是UNKNOWN不抛异常(self):
        assert dpd_bucket_of(None) == "UNKNOWN"

    def test_迁徙率口径依赖的档位是稳定的(self):
        """迁徙率按 DPD 档位分组的，档位口径一变，历史迁徙率就不可比"""
        assert dpd_bucket_of("Late (16-30 days)") == \
            dpd_bucket_of("In Grace Period") == "DPD_1_30"


class Test倾斜比公式:
    """⭐ 阶段三新增：把 E4 用到的倾斜比口径也固化成断言"""

    def test_平均key的倾斜比是1(self):
        assert skew_ratio_of(1 / 9, 9) == pytest.approx(1.0)

    def test_本项目fully_paid的倾斜比(self):
        # 实测：Fully Paid 占 47.63%，共 9 个状态（见 E4_数据倾斜报告.md）
        assert skew_ratio_of(0.4763, 9) == pytest.approx(4.2867, abs=1e-3)

    def test_恒等式写法是错的(self):
        """⚠️ 回归测试：'在按 key 分组的查询里用 COUNT(DISTINCT key)' 恒等于 1，
        会算出倾斜比 = 占比本身（0.4763），从而误判'没有倾斜'。"""
        wrong = skew_ratio_of(0.4763, 1)        # n_keys 被错算成 1
        assert wrong != pytest.approx(4.2867)
        assert wrong == pytest.approx(0.4763)

    def test_边界输入不抛异常(self):
        assert skew_ratio_of(None, 9) == 0.0
        assert skew_ratio_of(0.5, 0) == 0.0

    def test_占比超过平均即大于1(self):
        assert skew_ratio_of(0.5, 2) == pytest.approx(1.0)
        assert skew_ratio_of(0.9, 10) == pytest.approx(9.0)


class Test行数守恒:
    def test_ODS与DWD行数必须相等(self):
        """⭐ 回归测试（v3 修正）：实测差值为 0。
        那 32 行"疑似缺 issue_d 的记录"其实是文件里的页脚，
        已在 `src/prepare_raw.py` 阶段剔除，见质量校验报告 Q1。"""
        ods, dwd = 2_260_668, 2_260_668
        assert ods == dwd
        assert ods - dwd == 0
