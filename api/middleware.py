"""
JWT 认证中间件
拦截所有请求，校验 JWT Token，白名单路径和 CORS 预检请求放行。
"""

import jwt
from fastapi import Request
from fastapi.responses import JSONResponse
from starlette import status

from core.config import JWT_SECRET, JWT_ALGORITHM
from core.security import is_jwt_whitelisted


async def verify_jwt_token(request: Request, call_next):
    """JWT Token 校验中间件"""
    # 放行 CORS 预检请求（浏览器在发送带自定义 Header 的跨域请求前会先发 OPTIONS）
    if request.method == "OPTIONS":
        return await call_next(request)

    # 使用正则白名单匹配，支持文档子路径
    if is_jwt_whitelisted(request.url.path):
        return await call_next(request)

    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={"code": 401, "message": "缺失或无效的认证 Token"}
        )

    token = auth_header.split(" ")[1]
    try:
        # 解析并校验 Token（会自动校验 exp 过期时间）
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        # 将用户信息挂载到 request.state 以供后续路由使用
        request.state.user = payload
    except jwt.ExpiredSignatureError:
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={"code": 401, "message": "Token 已过期，请重新登录"}
        )
    except jwt.InvalidTokenError:
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={"code": 401, "message": "无效的 Token"}
        )

    return await call_next(request)
