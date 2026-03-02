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
from api.schemas import ApiResponse, DashboardAllData, DateRangeConfigData

router = APIRouter(prefix="/api", tags=["看板"])


@router.get("/dashboard/all", response_model=ApiResponse[DashboardAllData])
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

    backend = db_manager.get_backend()
    sd, ed = resolve_dates(start_date, end_date, backend)
    data = get_dashboard_all_data(sd, ed, period, backend)
    resp = ApiResponse(data=data)
    
    from core.logging import logger
    logger.info(f"API RESPONSE CORE DUMP: {resp.model_dump().get('data', {}).get('core', {})}")
    
    return resp


@router.get("/config/date_range", response_model=ApiResponse[DateRangeConfigData])
def get_date_range_config():
    """获取数据日期范围配置"""
    backend = db_manager.get_backend()
    base_start, base_end = fetch_date_range(backend)
    return ApiResponse(data={"min": base_start, "max": base_end})
