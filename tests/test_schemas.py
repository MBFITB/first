"""
Pydantic 模型序列化测试
验证 ApiResponse 泛型结构和 Optional[float] 字段的 null 保留行为。
"""

import json

from api.schemas import ApiResponse, CoreMetricsData, DashboardAllData


# ═══════════════════════════════════════
#  ApiResponse 泛型测试
# ═══════════════════════════════════════

class TestApiResponse:
    """标准 API 响应结构序列化"""

    def test_default_values(self):
        """默认 code=200, message=ok"""
        resp = ApiResponse(data={"key": "value"})
        dumped = resp.model_dump()
        assert dumped["code"] == 200
        assert dumped["message"] == "ok"
        assert dumped["data"]["key"] == "value"

    def test_with_none_data(self):
        """data 为 None 时正确序列化"""
        resp = ApiResponse(data=None)
        dumped = resp.model_dump()
        assert dumped["data"] is None

    def test_json_serialization(self):
        """model_dump_json 序列化后可被正确反序列化"""
        resp = ApiResponse(data={"hello": "world"})
        json_str = resp.model_dump_json()
        parsed = json.loads(json_str)
        assert parsed["code"] == 200
        assert parsed["data"]["hello"] == "world"


# ═══════════════════════════════════════
#  CoreMetricsData null 保留测试（核心 Bug 回归）
# ═══════════════════════════════════════

class TestCoreMetricsNullPreservation:
    """验证 qoq_rate / yoy_rate 为 None 时 JSON 输出保留 null"""

    def test_null_rates_preserved_in_json(self):
        """核心验证：Optional[float] = None 序列化后必须是 null 而非 0.0"""
        metrics = CoreMetricsData(
            total_sales=100000.0,
            total_orders=500,
            paying_users=200,
            avg_order_value=200.0,
            qoq_rate=None,
            yoy_rate=None,
            period_label="日",
        )
        json_str = metrics.model_dump_json()
        parsed = json.loads(json_str)

        # 核心断言：None 不能被转换为 0.0
        assert parsed["qoq_rate"] is None, f"qoq_rate 应为 null，实际为 {parsed['qoq_rate']}"
        assert parsed["yoy_rate"] is None, f"yoy_rate 应为 null，实际为 {parsed['yoy_rate']}"

    def test_rates_with_actual_values(self):
        """有值时正常序列化"""
        metrics = CoreMetricsData(
            total_sales=100000.0,
            total_orders=500,
            paying_users=200,
            avg_order_value=200.0,
            qoq_rate=50.0,
            yoy_rate=-10.5,
            period_label="日",
        )
        dumped = metrics.model_dump()
        assert dumped["qoq_rate"] == 50.0
        assert dumped["yoy_rate"] == -10.5

    def test_wrapped_in_api_response(self):
        """包裹在 ApiResponse 中时 null 仍然保留"""
        metrics_dict = {
            "total_sales": 100000.0,
            "total_orders": 500,
            "paying_users": 200,
            "avg_order_value": 200.0,
            "qoq_rate": None,
            "yoy_rate": None,
            "period_label": "日",
        }
        resp = ApiResponse[CoreMetricsData](data=metrics_dict)
        json_str = resp.model_dump_json()
        parsed = json.loads(json_str)
        assert parsed["data"]["qoq_rate"] is None
        assert parsed["data"]["yoy_rate"] is None
