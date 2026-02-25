"""
认证路由
POST /api/auth/login — 用户登录，返回 JWT Token。
"""

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from starlette import status

from services.auth_service import authenticate_user

router = APIRouter(prefix="/api/auth", tags=["认证"])


class LoginRequest(BaseModel):
    username: str
    password: str


@router.post("/login")
def login(request: LoginRequest):
    """用户登录接口"""
    result = authenticate_user(request.username, request.password)
    if result is None:
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={"code": 401, "message": "用户名或密码错误"}
        )
    return {"code": 200, "data": result}
