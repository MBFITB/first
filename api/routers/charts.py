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

from typing import Optional

from fastapi import APIRouter, HTTPException

from core.config import PERIOD_MAP
from db.manager import db_manager
from dao.base import safe_float, safe_int
from dao.sales_dao import fetch_trend, fetch_top10, fetch_category, fetch_channel, fetch_age_group
from dao.user_dao import fetch_funnel, fetch_rfm, fetch_cohort
from services.dashboard_service import resolve_dates, get_core_metrics_data

router = APIRouter(prefix="/api", tags=["图表"])


@router.get("/metrics/core")
def get_core_metrics(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    period: str = 'day'
):
    """核心指标端点。period 参数校验为业务级错误，使用 HTTPException 抛出。"""
    if period not in PERIOD_MAP:
        raise HTTPException(status_code=400, detail="period 参数非法，仅支持 day/week/month")

    db, is_sqlite = db_manager.get_connection()
    sd, ed = resolve_dates(start_date, end_date, db, is_sqlite)
    data = get_core_metrics_data(sd, ed, period, db, is_sqlite)
    return {"code": 200, "data": data}


@router.get("/charts/trend")
def get_trend_chart(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    period: str = 'day'
):
    """销售趋势图表"""
    if period not in PERIOD_MAP:
        raise HTTPException(status_code=400, detail="period 参数非法，仅支持 day/week/month")

    db, is_sqlite = db_manager.get_connection()
    sd, ed = resolve_dates(start_date, end_date, db, is_sqlite)
    trend = fetch_trend(db, is_sqlite, sd, ed, PERIOD_MAP[period])
    return {"code": 200, "data": {
        "dates": trend.get("dates", []),
        "sales": trend.get("sales", []),
        "orders": trend.get("orders", [])
    }}


@router.get("/charts/funnel")
def get_funnel_chart(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """转化漏斗图表"""
    db, is_sqlite = db_manager.get_connection()
    sd, ed = resolve_dates(start_date, end_date, db, is_sqlite)
    funnel_data = fetch_funnel(db, is_sqlite, sd, ed)
    return {"code": 200, "data": [
        {"name": "浏览 (PV)", "value": funnel_data.get('pv', 0)},
        {"name": "意向 (加购)", "value": funnel_data.get('cart', 0)},
        {"name": "购买 (成交)", "value": funnel_data.get('buy', 0)}
    ]}


@router.get("/charts/rankings")
def get_rankings_chart(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """Top10 商品排行"""
    db, is_sqlite = db_manager.get_connection()
    sd, ed = resolve_dates(start_date, end_date, db, is_sqlite)
    top10_rows = fetch_top10(db, is_sqlite, sd, ed)
    return {"code": 200, "data": {
        "items": [f"商品{r.get('item_id', '未知')}" for r in top10_rows],
        "sales": [round(safe_float(r.get('sales')), 2) for r in top10_rows]
    }}


@router.get("/charts/dimensions")
def get_dimensions_chart(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """多维分析图表（品类/渠道/年龄）"""
    db, is_sqlite = db_manager.get_connection()
    sd, ed = resolve_dates(start_date, end_date, db, is_sqlite)
    cat_rows = fetch_category(db, is_sqlite, sd, ed)
    chan_rows = fetch_channel(db, is_sqlite, sd, ed)
    age_rows = fetch_age_group(db, is_sqlite, sd, ed)
    return {"code": 200, "data": {
        "category": [
            {"name": f"品类{r.get('category_id', '未知')}", "value": round(safe_float(r.get('sales')), 2)}
            for r in cat_rows
        ],
        "channel": [
            {"name": r.get('channel', '未知'), "value": round(safe_float(r.get('sales')), 2)}
            for r in chan_rows
        ],
        "age_group": [
            {"name": r.get('age_group', '未知'), "value": round(safe_float(r.get('sales')), 2)}
            for r in age_rows
        ]
    }}


@router.get("/charts/rfm")
def get_rfm_chart(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """RFM 分布图表"""
    db, is_sqlite = db_manager.get_connection()
    sd, ed = resolve_dates(start_date, end_date, db, is_sqlite)
    rfm_rows = fetch_rfm(db, is_sqlite, sd, ed)
    return {"code": 200, "data": [
        {"name": r.get('rfm_label', '未知'), "value": safe_int(r.get('cnt'))}
        for r in rfm_rows
    ]}


@router.get("/charts/retention")
def get_retention_chart(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """留存矩阵图表"""
    db, is_sqlite = db_manager.get_connection()
    sd, ed = resolve_dates(start_date, end_date, db, is_sqlite)
    cohort_rows = fetch_cohort(db, is_sqlite, sd, ed)
    return {"code": 200, "data": [
        [
            safe_int(r.get('day_diff')),
            str(r.get('cohort_date', '')),
            round(safe_int(r.get('active_users')) / max(safe_int(r.get('cohort_users')), 1) * 100, 2)
        ] for r in cohort_rows
    ]}
