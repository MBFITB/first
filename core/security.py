"""
JWT 安全工具模块
提供 Token 签发、解码和白名单检查功能。
"""

import datetime
import jwt

from core.config import JWT_SECRET, JWT_ALGORITHM, JWT_EXPIRATION_HOURS, JWT_WHITELIST_PATTERNS


def create_jwt_token(username: str, role: str) -> str:
    """签发 JWT Token"""
    expiration = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
        hours=JWT_EXPIRATION_HOURS
    )
    payload = {
        "sub": username,
        "role": role,
        "exp": expiration
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def decode_jwt_token(token: str) -> dict:
    """解码并校验 JWT Token（会自动校验 exp 过期时间）"""
    return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])


def is_jwt_whitelisted(path: str) -> bool:
    """检查请求路径是否匹配 JWT 白名单"""
    return any(pattern.match(path) for pattern in JWT_WHITELIST_PATTERNS)
