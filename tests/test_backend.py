"""
SqliteBackend 集成测试
使用 conftest.py 中的内存 SQLite 数据库验证所有查询方法的正确性。
"""

import pytest


# ═══════════════════════════════════════
#  核心指标查询
# ═══════════════════════════════════════

class TestSqliteBackendCoreMetrics:
    """fetch_core_metrics 测试"""

    def test_normal_range(self, sqlite_backend):
        """正常日期范围 → 返回正确的总销售额和订单数"""
        result = sqlite_backend.fetch_core_metrics("2017-11-15", "2017-11-16")
        # 3 条数据: 99.9 + 199.0 + 50.0 = 348.9
        assert abs(result["total_sales"] - 348.9) < 0.01
        assert result["total_orders"] == 3

    def test_single_day(self, sqlite_backend):
        """单日查询 → 只包含该日数据"""
        result = sqlite_backend.fetch_core_metrics("2017-11-16", "2017-11-16")
        assert abs(result["total_sales"] - 50.0) < 0.01
        assert result["total_orders"] == 1

    def test_no_data_range(self, sqlite_backend):
        """无数据日期范围 → 返回 0"""
        result = sqlite_backend.fetch_core_metrics("2099-01-01", "2099-12-31")
        assert result["total_sales"] == 0.0
        assert result["total_orders"] == 0


# ═══════════════════════════════════════
#  趋势查询
# ═══════════════════════════════════════

class TestSqliteBackendTrend:
    """fetch_trend 测试"""

    def test_daily_trend(self, sqlite_backend):
        """按天聚合趋势"""
        period_cfg = {"sqlite": "%Y-%m-%d", "ch": "%Y-%m-%d", "label": "日"}
        result = sqlite_backend.fetch_trend("2017-11-15", "2017-11-16", period_cfg)
        assert len(result["dates"]) == 2
        assert result["dates"][0] == "2017-11-15"
        assert len(result["sales"]) == 2


# ═══════════════════════════════════════
#  比较销售额
# ═══════════════════════════════════════

class TestSqliteBackendComparison:
    """fetch_comparison_sales 测试"""

    def test_normal_range(self, sqlite_backend):
        """正常范围 → 返回销售额"""
        result = sqlite_backend.fetch_comparison_sales("2017-11-15", "2017-11-16")
        assert result is not None
        assert abs(result - 348.9) < 0.01

    def test_no_data(self, sqlite_backend):
        """无数据 → 返回 None"""
        result = sqlite_backend.fetch_comparison_sales("2099-01-01", "2099-12-31")
        assert result is None


# ═══════════════════════════════════════
#  排行榜查询
# ═══════════════════════════════════════

class TestSqliteBackendTop10:
    """fetch_top10 测试"""

    def test_returns_list(self, sqlite_backend):
        """返回列表类型"""
        result = sqlite_backend.fetch_top10("2017-11-15", "2017-11-16")
        assert isinstance(result, list)
        assert len(result) <= 10

    def test_sorted_by_sales_desc(self, sqlite_backend):
        """按销售额降序"""
        result = sqlite_backend.fetch_top10("2017-11-15", "2017-11-16")
        if len(result) > 1:
            assert result[0]["sales"] >= result[1]["sales"]


# ═══════════════════════════════════════
#  维度查询
# ═══════════════════════════════════════

class TestSqliteBackendDimensions:
    """品类/渠道/年龄段查询"""

    def test_category_returns_list(self, sqlite_backend):
        result = sqlite_backend.fetch_category("2017-11-15", "2017-11-16")
        assert isinstance(result, list)
        assert all("category_id" in r for r in result)

    def test_channel_returns_list(self, sqlite_backend):
        result = sqlite_backend.fetch_channel("2017-11-15", "2017-11-16")
        assert isinstance(result, list)
        assert all("channel" in r for r in result)

    def test_age_group_returns_list(self, sqlite_backend):
        result = sqlite_backend.fetch_age_group("2017-11-15", "2017-11-16")
        assert isinstance(result, list)
        assert all("age_group" in r for r in result)


# ═══════════════════════════════════════
#  日期范围
# ═══════════════════════════════════════

class TestSqliteBackendDateRange:
    """fetch_date_range_impl 测试"""

    def test_returns_min_max(self, sqlite_backend):
        """返回数据集的最小和最大日期"""
        base_start, base_end = sqlite_backend.fetch_date_range_impl()
        assert base_start == "2017-11-15"
        assert base_end == "2017-11-16"


# ═══════════════════════════════════════
#  漏斗查询
# ═══════════════════════════════════════

class TestSqliteBackendFunnel:
    """fetch_funnel 测试"""

    def test_normal_funnel(self, sqlite_backend):
        """漏斗数据 → pv/cart/buy"""
        result = sqlite_backend.fetch_funnel("2017-11-15", "2017-11-16")
        assert result["pv"] == 1000
        assert result["cart"] == 100
        assert result["buy"] == 20

    def test_no_data(self, sqlite_backend):
        """无数据 → 全部为 0"""
        result = sqlite_backend.fetch_funnel("2099-01-01", "2099-12-31")
        assert result["pv"] == 0
        assert result["cart"] == 0
        assert result["buy"] == 0


# ═══════════════════════════════════════
#  RFM 查询
# ═══════════════════════════════════════

class TestSqliteBackendRfm:
    """fetch_rfm 测试"""

    def test_returns_labels(self, sqlite_backend):
        result = sqlite_backend.fetch_rfm("2017-11-15", "2017-11-16")
        assert isinstance(result, list)
        labels = [r["rfm_label"] for r in result]
        assert "核心高价值客户" in labels


# ═══════════════════════════════════════
#  留存矩阵
# ═══════════════════════════════════════

class TestSqliteBackendCohort:
    """fetch_cohort 测试"""

    def test_returns_days(self, sqlite_backend):
        result = sqlite_backend.fetch_cohort("2017-11-15", "2017-11-16")
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["day_diff"] == 0
        assert result[1]["day_diff"] == 1
        assert result[1]["active_users"] == 72
