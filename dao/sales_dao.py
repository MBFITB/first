"""
销售数据访问层
通过 DatabaseBackend 策略接口，直接调用后端实现。
"""

from typing import Optional


def fetch_core_metrics(backend, start_date: str, end_date: str) -> dict:
    """查询核心指标（总销售额、总订单数）"""
    return backend.fetch_core_metrics(start_date, end_date)


def fetch_trend(backend, start_date: str, end_date: str, period_cfg: dict) -> dict:
    """查询销售趋势（按日/周/月聚合）"""
    return backend.fetch_trend(start_date, end_date, period_cfg)


def fetch_comparison_sales(backend, start_date: str, end_date: str) -> Optional[float]:
    """查询对比周期的销售额（用于环比/同比）"""
    return backend.fetch_comparison_sales(start_date, end_date)


def fetch_top10(backend, start_date: str, end_date: str) -> list:
    """查询 Top10 商品"""
    return backend.fetch_top10(start_date, end_date)


def fetch_category(backend, start_date: str, end_date: str) -> list:
    """查询品类维度销售分布"""
    return backend.fetch_category(start_date, end_date)


def fetch_channel(backend, start_date: str, end_date: str) -> list:
    """查询渠道维度销售分布"""
    return backend.fetch_channel(start_date, end_date)


def fetch_age_group(backend, start_date: str, end_date: str) -> list:
    """查询年龄段维度销售分布"""
    return backend.fetch_age_group(start_date, end_date)
