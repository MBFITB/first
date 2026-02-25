"""
认证业务逻辑层
负责用户校验和 Token 签发，不包含 HTTP 相关逻辑。
"""

from core.security import create_jwt_token


# 模拟用户数据（毕业设计演示：admin/123456 或 viewer/123456）
VALID_USERS = {
    "admin": {"password": "123456", "role": "admin"},
    "viewer": {"password": "123456", "role": "viewer"},
}


def authenticate_user(username: str, password: str) -> dict | None:
    """
    校验用户名和密码。
    返回: 包含 token/role/username 的字典，校验失败返回 None。
    """
    user_info = VALID_USERS.get(username)
    if not user_info or user_info["password"] != password:
        return None

    token = create_jwt_token(username, user_info["role"])
    return {
        "token": token,
        "role": user_info["role"],
        "username": username,
    }
