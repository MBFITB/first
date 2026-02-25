"""
DAO 层通用工具函数
提供安全的类型转换，避免 None 或异常类型导致路由层崩溃。
"""

from typing import Any


def safe_float(val: Any, default: float = 0.0) -> float:
    """安全转换为 float，None 或异常时返回默认值"""
    try:
        return float(val) if val is not None else default
    except (TypeError, ValueError):
        return default


def safe_int(val: Any, default: int = 0) -> int:
    """安全转换为 int，None 或异常时返回默认值"""
    try:
        return int(val) if val is not None else default
    except (TypeError, ValueError):
        return default
