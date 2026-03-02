"""
业务逻辑层单元测试
覆盖环比/同比计算核心算法 + 6 个 format 数据组装函数。
"""

from unittest.mock import patch, MagicMock
import datetime

from services.dashboard_service import (
    calculate_qoq_yoy,
    format_trend,
    format_funnel,
    format_rankings,
    format_dimensions,
    format_rfm,
    format_retention,
)


# ═══════════════════════════════════════
#  环比/同比核心算法测试
# ═══════════════════════════════════════

class TestCalculateQoqYoy:
    """环比/同比增长率计算逻辑"""

    # 固定的测试日期区间
    CURR_START = datetime.datetime(2017, 11, 10)
    CURR_END = datetime.datetime(2017, 11, 20)

    def _make_backend(self, side_effect):
        """构建 mock backend，控制 fetch_comparison_sales 返回值"""
        backend = MagicMock()
        backend.fetch_comparison_sales = MagicMock(side_effect=side_effect)
        return backend

    def test_normal_growth(self):
        """正常增长场景：上期 1000 → 本期 1500 = +50%"""
        backend = self._make_backend([1000.0, 800.0])
        result = calculate_qoq_yoy(1500.0, "day", self.CURR_START, self.CURR_END, backend)
        assert result["qoq_rate"] == 50.0
        assert result["yoy_rate"] is not None

    def test_negative_growth(self):
        """负增长场景：上期 2000 → 本期 1000 = -50%"""
        backend = self._make_backend([2000.0, 2000.0])
        result = calculate_qoq_yoy(1000.0, "day", self.CURR_START, self.CURR_END, backend)
        assert result["qoq_rate"] == -50.0

    def test_no_baseline_returns_none(self):
        """无基线数据场景：上期为 None → 环比应为 None"""
        backend = self._make_backend([None, None])
        result = calculate_qoq_yoy(1500.0, "day", self.CURR_START, self.CURR_END, backend)
        assert result["qoq_rate"] is None
        assert result["yoy_rate"] is None

    def test_zero_baseline_returns_none(self):
        """基线为 0 场景：不能做除法，应返回 None"""
        backend = self._make_backend([0, 0])
        result = calculate_qoq_yoy(1500.0, "day", self.CURR_START, self.CURR_END, backend)
        assert result["qoq_rate"] is None
        assert result["yoy_rate"] is None

    def test_qoq_has_value_but_yoy_is_none(self):
        """混合场景：环比有数据，同比无数据"""
        backend = self._make_backend([1000.0, None])
        result = calculate_qoq_yoy(1500.0, "day", self.CURR_START, self.CURR_END, backend)
        assert result["qoq_rate"] == 50.0
        assert result["yoy_rate"] is None

    def test_week_period_offset(self):
        """周期为 week 时，环比偏移 7 天"""
        backend = self._make_backend([1000.0, 800.0])
        result = calculate_qoq_yoy(1500.0, "week", self.CURR_START, self.CURR_END, backend)
        # 验证 fetch_comparison_sales 的第一次调用参数（环比）
        call_args = backend.fetch_comparison_sales.call_args_list[0]
        prev_start = call_args[0][0]
        prev_end = call_args[0][1]
        assert prev_start == "2017-11-03"  # 11-10 减 7 天
        assert prev_end == "2017-11-13"    # 11-20 减 7 天
        assert result["qoq_rate"] == 50.0

    def test_month_period_offset(self):
        """周期为 month 时，环比偏移 1 个月"""
        backend = self._make_backend([1000.0, 800.0])
        result = calculate_qoq_yoy(1500.0, "month", self.CURR_START, self.CURR_END, backend)
        call_args = backend.fetch_comparison_sales.call_args_list[0]
        prev_start = call_args[0][0]
        prev_end = call_args[0][1]
        assert prev_start == "2017-10-10"  # 11-10 减 1 个月
        assert prev_end == "2017-10-20"    # 11-20 减 1 个月
        assert result["qoq_rate"] == 50.0


# ═══════════════════════════════════════
#  format 系列函数测试
# ═══════════════════════════════════════

class TestFormatTrend:
    """趋势数据格式化"""

    def test_normal_input(self):
        raw = {"dates": ["2017-11-10", "2017-11-11"], "sales": [100.0, 200.0], "orders": [10, 20]}
        result = format_trend(raw)
        assert result["dates"] == ["2017-11-10", "2017-11-11"]
        assert len(result["sales"]) == 2
        assert len(result["orders"]) == 2

    def test_empty_input(self):
        result = format_trend({})
        assert result["dates"] == []
        assert result["sales"] == []
        assert result["orders"] == []


class TestFormatFunnel:
    """漏斗数据格式化"""

    def test_normal_funnel(self):
        raw = {"pv": 1000, "cart": 100, "buy": 20}
        result = format_funnel(raw)
        assert len(result) == 3
        assert result[0]["name"] == "浏览 (PV)"
        assert result[0]["value"] == 1000
        assert result[2]["name"] == "购买 (成交)"
        assert result[2]["value"] == 20

    def test_missing_keys(self):
        """缺失字段时用 0 兜底"""
        result = format_funnel({})
        assert all(item["value"] == 0 for item in result)


class TestFormatRankings:
    """排名数据格式化"""

    def test_normal_rankings(self):
        rows = [
            {"item_id": 101, "sales": 5000.0},
            {"item_id": 102, "sales": 3000.555},
        ]
        result = format_rankings(rows)
        assert result["items"] == ["商品101", "商品102"]
        assert result["sales"] == [5000.0, 3000.56] or result["sales"] == [5000.0, 3000.55]

    def test_empty_rankings(self):
        result = format_rankings([])
        assert result["items"] == []
        assert result["sales"] == []


class TestFormatDimensions:
    """维度分布数据格式化"""

    def test_normal_dimensions(self):
        cat = [{"category_id": 10, "sales": 1000.0}]
        chan = [{"channel": "App Store", "sales": 2000.0}]
        age = [{"age_group": "25-34", "sales": 3000.0}]
        result = format_dimensions(cat, chan, age)
        assert result["category"][0]["name"] == "品类10"
        assert result["channel"][0]["name"] == "App Store"
        assert result["age_group"][0]["name"] == "25-34"

    def test_empty_dimensions(self):
        result = format_dimensions([], [], [])
        assert result["category"] == []
        assert result["channel"] == []
        assert result["age_group"] == []


class TestFormatRfm:
    """RFM 标签数据格式化"""

    def test_normal_rfm(self):
        rows = [{"rfm_label": "核心高价值客户", "cnt": 50}]
        result = format_rfm(rows)
        assert result[0]["name"] == "核心高价值客户"
        assert result[0]["value"] == 50


class TestFormatRetention:
    """留存率数据格式化"""

    def test_normal_retention(self):
        rows = [
            {"day_diff": 0, "cohort_date": "2017-11-15", "cohort_users": 100, "active_users": 100},
            {"day_diff": 1, "cohort_date": "2017-11-15", "cohort_users": 100, "active_users": 72},
        ]
        result = format_retention(rows)
        assert result[0] == [0, "2017-11-15", 100.0]
        assert result[1] == [1, "2017-11-15", 72.0]

    def test_zero_cohort_users(self):
        """分母为 0 时不应崩溃（除以 max(0,1) 保护）"""
        rows = [{"day_diff": 0, "cohort_date": "2017-11-15", "cohort_users": 0, "active_users": 0}]
        result = format_retention(rows)
        assert result[0][2] == 0.0  # 0/1 = 0.0，不会除零
