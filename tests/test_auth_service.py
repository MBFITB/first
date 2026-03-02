"""
认证服务单元测试
覆盖登录验证、JWT 签发/解码、白名单路径判断。
"""

import jwt as pyjwt

from services.auth_service import authenticate_user
from core.security import create_jwt_token, decode_jwt_token, is_jwt_whitelisted
from core.config import JWT_SECRET, JWT_ALGORITHM


# ═══════════════════════════════════════
#  用户认证测试
# ═══════════════════════════════════════

class TestAuthenticateUser:
    """用户登录认证逻辑"""

    def test_login_success_admin(self):
        """管理员登录成功"""
        result = authenticate_user("admin", "123456")
        assert result is not None
        assert result["role"] == "admin"
        assert result["username"] == "admin"
        assert "token" in result and len(result["token"]) > 0

    def test_login_success_viewer(self):
        """只读用户登录成功"""
        result = authenticate_user("viewer", "123456")
        assert result is not None
        assert result["role"] == "viewer"

    def test_login_wrong_password(self):
        """密码错误返回 None"""
        result = authenticate_user("admin", "wrong_password")
        assert result is None

    def test_login_nonexistent_user(self):
        """不存在的用户返回 None"""
        result = authenticate_user("nobody", "123456")
        assert result is None


# ═══════════════════════════════════════
#  JWT 工具函数测试
# ═══════════════════════════════════════

class TestJwtSecurity:
    """JWT 签发与解码"""

    def test_encode_and_decode(self):
        """签发 Token 后能正确解码出 payload"""
        token = create_jwt_token("admin", "admin")
        payload = decode_jwt_token(token)
        assert payload["sub"] == "admin"
        assert payload["role"] == "admin"
        assert "exp" in payload

    def test_invalid_token_raises(self):
        """伪造的 Token 解码应抛出异常"""
        import pytest
        with pytest.raises(pyjwt.InvalidTokenError):
            decode_jwt_token("this.is.fake")

    def test_tampered_token_raises(self):
        """使用错误密钥签发的 Token 解码应失败"""
        import pytest
        bad_token = pyjwt.encode(
            {"sub": "hacker", "role": "admin"},
            "wrong_secret",
            algorithm=JWT_ALGORITHM
        )
        with pytest.raises(pyjwt.InvalidSignatureError):
            decode_jwt_token(bad_token)


# ═══════════════════════════════════════
#  JWT 白名单路径测试
# ═══════════════════════════════════════

class TestJwtWhitelist:
    """JWT 白名单路径匹配"""

    def test_login_path_whitelisted(self):
        """登录接口在白名单中"""
        assert is_jwt_whitelisted("/api/auth/login") is True

    def test_docs_path_whitelisted(self):
        """Swagger 文档在白名单中"""
        assert is_jwt_whitelisted("/docs") is True
        assert is_jwt_whitelisted("/docs/oauth2-redirect") is True

    def test_dashboard_path_not_whitelisted(self):
        """看板接口不在白名单中"""
        assert is_jwt_whitelisted("/api/dashboard/all") is False

    def test_random_path_not_whitelisted(self):
        """随机路径不在白名单中"""
        assert is_jwt_whitelisted("/random/path") is False
