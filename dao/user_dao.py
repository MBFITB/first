"""
用户数据访问层
通过 DatabaseBackend 策略接口，直接调用后端实现。
"""

from db.manager import db_manager


def fetch_date_range(backend) -> tuple:
    """获取日期范围（带缓存，通过 DatabaseManager 的缓存机制）"""
    return db_manager.get_date_range_cached(backend)


def fetch_funnel(backend, start_date: str, end_date: str) -> dict:
    """查询转化漏斗数据"""
    return backend.fetch_funnel(start_date, end_date)


def fetch_rfm(backend, start_date: str, end_date: str) -> list:
    """查询 RFM 用户标签分布"""
    return backend.fetch_rfm(start_date, end_date)


def fetch_cohort(backend, start_date: str, end_date: str) -> list:
    """查询同期群留存矩阵"""
    return backend.fetch_cohort(start_date, end_date)
