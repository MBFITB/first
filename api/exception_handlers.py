"""
全局异常处理器
统一处理 HTTPException 和未捕获的异常，提供标准化的 JSON 错误响应。
"""

import traceback

from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse
from starlette import status

from core.logging import logger


async def http_exception_handler(request: Request, exc: HTTPException):
    """
    业务级 HTTPException 原样返回（如 400 参数校验失败），
    保留具体业务错误信息以便前端展示。
    """
    return JSONResponse(
        status_code=exc.status_code,
        content={"code": exc.status_code, "message": exc.detail}
    )


async def global_exception_handler(request: Request, exc: Exception):
    """
    全局兜底异常处理器：
    - 将详细的错误 traceback 记录到 stderr（含请求路径和方法）
    - 返回给客户端泛化的错误信息，不泄露内部实现细节
    """
    logger.error(
        "未处理异常 [%s %s]: %s\n%s",
        request.method,
        request.url.path,
        str(exc),
        traceback.format_exc()
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"code": 500, "message": "Internal Server Error"}
    )
