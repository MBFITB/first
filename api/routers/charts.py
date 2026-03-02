"""
图表路由
GET /api/metrics/core     — 核心指标
GET /api/charts/trend     — 销售趋势
GET /api/charts/funnel    — 转化漏斗
GET /api/charts/rankings  — Top10 商品
GET /api/charts/dimensions — 多维分析（品类/渠道/年龄）
GET /api/charts/rfm       — RFM 分布
GET /api/charts/retention — 留存矩阵
"""

from typing import Optional, List

from fastapi import APIRouter, HTTPException

from core.config import PERIOD_MAP
from db.manager import db_manager
from services.dashboard_service import (
    resolve_dates, get_core_metrics_data, format_trend, format_funnel,
    format_rankings, format_dimensions, format_rfm, format_retention
)
from api.schemas import (
    ApiResponse, CoreMetricsData, TrendData, NameValueItem,
    RankingsData, DimensionsData
)

router = APIRouter(prefix="/api", tags=["图表"])


@router.get("/metrics/core", response_model=ApiResponse[CoreMetricsData])
def get_core_metrics(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    period: str = 'day'
):
    """核心指标端点。period 参数校验为业务级错误，使用 HTTPException 抛出。"""
    if period not in PERIOD_MAP:
        raise HTTPException(status_code=400, detail="period 参数非法，仅支持 day/week/month")

    backend = db_manager.get_backend()
    sd, ed = resolve_dates(start_date, end_date, backend)
    data = get_core_metrics_data(sd, ed, period, backend)
    return ApiResponse(data=data)


@router.get("/charts/trend", response_model=ApiResponse[TrendData])
def get_trend_chart(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    period: str = 'day'
):
    """销售趋势图表"""
    if period not in PERIOD_MAP:
        raise HTTPException(status_code=400, detail="period 参数非法，仅支持 day/week/month")

    backend = db_manager.get_backend()
    sd, ed = resolve_dates(start_date, end_date, backend)
    trend = backend.fetch_trend(sd, ed, PERIOD_MAP[period])
    return ApiResponse(data=format_trend(trend))


@router.get("/charts/funnel", response_model=ApiResponse[List[NameValueItem]])
def get_funnel_chart(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """转化漏斗图表"""
    backend = db_manager.get_backend()
    sd, ed = resolve_dates(start_date, end_date, backend)
    funnel_data = backend.fetch_funnel(sd, ed)
    return ApiResponse(data=format_funnel(funnel_data))


@router.get("/charts/rankings", response_model=ApiResponse[RankingsData])
def get_rankings_chart(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """Top10 商品排行"""
    backend = db_manager.get_backend()
    sd, ed = resolve_dates(start_date, end_date, backend)
    top10_rows = backend.fetch_top10(sd, ed)
    return ApiResponse(data=format_rankings(top10_rows))


@router.get("/charts/dimensions", response_model=ApiResponse[DimensionsData])
def get_dimensions_chart(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """多维分析图表（品类/渠道/年龄）"""
    backend = db_manager.get_backend()
    sd, ed = resolve_dates(start_date, end_date, backend)
    cat_rows = backend.fetch_category(sd, ed)
    chan_rows = backend.fetch_channel(sd, ed)
    age_rows = backend.fetch_age_group(sd, ed)
    return ApiResponse(data=format_dimensions(cat_rows, chan_rows, age_rows))


@router.get("/charts/rfm", response_model=ApiResponse[List[NameValueItem]])
def get_rfm_chart(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """RFM 分布图表"""
    backend = db_manager.get_backend()
    sd, ed = resolve_dates(start_date, end_date, backend)
    rfm_rows = backend.fetch_rfm(sd, ed)
    return ApiResponse(data=format_rfm(rfm_rows))


@router.get("/charts/retention", response_model=ApiResponse[List[list]])
def get_retention_chart(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """留存矩阵图表"""
    backend = db_manager.get_backend()
    sd, ed = resolve_dates(start_date, end_date, backend)
    cohort_rows = backend.fetch_cohort(sd, ed)
    return ApiResponse(data=format_retention(cohort_rows))
