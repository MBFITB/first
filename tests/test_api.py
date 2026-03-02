"""
API 路由端到端测试
使用 FastAPI TestClient 验证登录、鉴权、看板聚合端点。
"""

import pytest


# ═══════════════════════════════════════
#  认证 API 测试
# ═══════════════════════════════════════

class TestAuthApi:
    """POST /api/auth/login 端到端测试"""

    def test_login_success(self, test_client):
        """正确账密 → 200 + token"""
        resp = test_client.post("/api/auth/login", json={"username": "admin", "password": "123456"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["code"] == 200
        assert "token" in data["data"]
        assert data["data"]["role"] == "admin"

    def test_login_wrong_password(self, test_client):
        """错误密码 → 401"""
        resp = test_client.post("/api/auth/login", json={"username": "admin", "password": "wrong"})
        assert resp.status_code == 401

    def test_login_missing_fields(self, test_client):
        """缺少字段 → 422 (Pydantic 校验失败)"""
        resp = test_client.post("/api/auth/login", json={"username": "admin"})
        assert resp.status_code == 422


# ═══════════════════════════════════════
#  鉴权中间件测试
# ═══════════════════════════════════════

class TestAuthMiddleware:
    """JWT 中间件拦截测试"""

    def test_dashboard_without_token(self, test_client):
        """无 Token 访问看板 → 401"""
        resp = test_client.get("/api/dashboard/all")
        assert resp.status_code == 401

    def test_dashboard_with_invalid_token(self, test_client):
        """伪造 Token → 401"""
        resp = test_client.get(
            "/api/dashboard/all",
            headers={"Authorization": "Bearer fake.token.here"}
        )
        assert resp.status_code == 401


# ═══════════════════════════════════════
#  看板聚合端点测试
# ═══════════════════════════════════════

class TestDashboardApi:
    """GET /api/dashboard/all 端到端测试"""

    def _get_token(self, client):
        """辅助：获取合法 Token"""
        resp = client.post("/api/auth/login", json={"username": "admin", "password": "123456"})
        return resp.json()["data"]["token"]

    def test_dashboard_with_valid_token(self, test_client):
        """合法 Token → 200 + 完整数据结构"""
        token = self._get_token(test_client)
        resp = test_client.get(
            "/api/dashboard/all",
            headers={"Authorization": f"Bearer {token}"}
        )
        assert resp.status_code == 200
        data = resp.json()["data"]

        # 验证顶层 key 完整性
        expected_keys = {"date_range", "core", "trend", "funnel", "rankings", "dimensions", "rfm", "retention"}
        assert expected_keys.issubset(set(data.keys())), f"缺少字段: {expected_keys - set(data.keys())}"

        # 验证 core 指标结构
        core = data["core"]
        assert "total_sales" in core
        assert "qoq_rate" in core
        assert "yoy_rate" in core

    def test_dashboard_invalid_period(self, test_client):
        """非法 period 参数 → 400"""
        token = self._get_token(test_client)
        resp = test_client.get(
            "/api/dashboard/all?period=invalid",
            headers={"Authorization": f"Bearer {token}"}
        )
        assert resp.status_code == 400

    def test_date_range_config_api(self, test_client):
        """GET /api/config/date_range → 200 + min/max"""
        token = self._get_token(test_client)
        resp = test_client.get(
            "/api/config/date_range",
            headers={"Authorization": f"Bearer {token}"}
        )
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert "min" in data
        assert "max" in data
