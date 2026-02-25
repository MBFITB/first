"""
看板路由
GET /api/dashboard/all  — 聚合端点，一次返回所有看板数据
GET /api/config/date_range — 获取数据日期范围配置
"""

from typing import Optional

from fastapi import APIRouter, HTTPException

from core.config import PERIOD_MAP
from db.manager import db_manager
from dao.user_dao import fetch_date_range
from services.dashboard_service import resolve_dates, get_dashboard_all_data

router = APIRouter(prefix="/api", tags=["看板"])


@router.get("/dashboard/all")
def get_dashboard_all(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    period: str = 'day'
):
    """
    聚合端点：一次请求返回所有看板数据。
    将 8 次 HTTP 请求合并为 1 次，内部只调用一次 resolve_dates。
    """
    if period not in PERIOD_MAP:
        raise HTTPException(status_code=400, detail="period 参数非法，仅支持 day/week/month")

    db, is_sqlite = db_manager.get_connection()
    sd, ed = resolve_dates(start_date, end_date, db, is_sqlite)
    data = get_dashboard_all_data(sd, ed, period, db, is_sqlite)
    return {"code": 200, "data": data}


@router.get("/config/date_range")
def get_date_range_config():
    """获取数据日期范围配置"""
    db, is_sqlite = db_manager.get_connection()
    base_start, base_end = fetch_date_range(db, is_sqlite)
    return {"code": 200, "data": {"min": base_start, "max": base_end}}
