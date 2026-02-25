"""
日志配置模块
统一日志格式和输出方式，全项目共用 logger 实例。
"""

import sys
import logging

logger = logging.getLogger("ecommerce_backend")
logger.setLevel(logging.INFO)

_stderr_handler = logging.StreamHandler(sys.stderr)
_stderr_handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
))
logger.addHandler(_stderr_handler)
