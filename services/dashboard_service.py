"""
看板业务逻辑层
负责日期解析、环比同比计算、数据组装等业务编排逻辑。
不包含 SQL 查询和 HTTP 相关逻辑。
"""

import datetime
from typing import Optional

from fastapi import HTTPException
from dateutil.relativedelta import relativedelta

from core.config import PERIOD_MAP, DATE_PATTERN
from db.manager import db_manager
from dao.base import safe_float, safe_int
from dao.user_dao import fetch_date_range


def resolve_dates(
    start_date: Optional[str],
    end_date: Optional[str],
    backend
) -> tuple:
    """
    解析和校验日期参数。
    若未传入，则默认取数据库中最大日期往前回溯 30 天。
    """
    base_start, base_end = fetch_date_range(backend)
    if not start_date or not end_date:
        end_date = base_end
        # 默认回溯 30 天（不钳制下界，前端 disabledDate 会负责标灰无数据日期）
        start_date = (
            datetime.datetime.strptime(base_end, '%Y-%m-%d')
            - datetime.timedelta(days=30)
        ).strftime('%Y-%m-%d')
    if not DATE_PATTERN.match(start_date) or not DATE_PATTERN.match(end_date):
        raise HTTPException(status_code=400, detail="日期格式非法，必须为 YYYY-MM-DD")
    return start_date, end_date


def calculate_qoq_yoy(
    total_sales: float,
    period: str,
    curr_start: datetime.datetime,
    curr_end: datetime.datetime,
    backend
) -> dict:
    """
    计算环比（QoQ）和同比（YoY）增长率。
    基于 period 类型动态判断对比周期。
    """
    period_days = (curr_end - curr_start).days + 1

    # 环比周期计算
    if period == 'month':
        prev_start = curr_start - relativedelta(months=1)
        prev_end = curr_end - relativedelta(months=1)
    elif period == 'week':
        prev_start = curr_start - datetime.timedelta(weeks=1)
        prev_end = curr_end - datetime.timedelta(weeks=1)
    else:
        prev_end = curr_start - datetime.timedelta(days=1)
        prev_start = prev_end - datetime.timedelta(days=period_days - 1)

    # 同比：去年同期
    ly_start = curr_start - relativedelta(years=1)
    ly_end = curr_end - relativedelta(years=1)

    prev_sales = backend.fetch_comparison_sales(
        prev_start.strftime('%Y-%m-%d'),
        prev_end.strftime('%Y-%m-%d')
    )
    ly_sales = backend.fetch_comparison_sales(
        ly_start.strftime('%Y-%m-%d'),
        ly_end.strftime('%Y-%m-%d')
    )

    qoq_rate = round((total_sales - prev_sales) / prev_sales * 100, 2) if prev_sales else None
    yoy_rate = round((total_sales - ly_sales) / ly_sales * 100, 2) if ly_sales else None
    
    from core.logging import logger
    logger.info(f"DEBUG: prev_sales={prev_sales}, ly_sales={ly_sales} -> qoq={qoq_rate}, yoy={yoy_rate}")

    return {"qoq_rate": qoq_rate, "yoy_rate": yoy_rate}


def get_core_metrics_data(sd: str, ed: str, period: str, backend) -> dict:
    """组装核心指标数据（供路由层直接使用）"""
    curr_start = datetime.datetime.strptime(sd, '%Y-%m-%d')
    curr_end = datetime.datetime.strptime(ed, '%Y-%m-%d')

    core = backend.fetch_core_metrics(sd, ed)
    funnel_data = backend.fetch_funnel(sd, ed)
    total_sales = core.get("total_sales", 0.0)
    total_orders = core.get("total_orders", 0)
    comparison = calculate_qoq_yoy(total_sales, period, curr_start, curr_end, backend)

    return {
        "total_sales": total_sales,
        "total_orders": total_orders,
        "paying_users": funnel_data.get('buy', 0),
        "avg_order_value": round(total_sales / total_orders, 2) if total_orders > 0 else 0.0,
        "qoq_rate": comparison.get("qoq_rate"),
        "yoy_rate": comparison.get("yoy_rate"),
        "period_label": PERIOD_MAP[period]["label"],
    }


def format_trend(trend_raw: dict) -> dict:
    return {
        "dates": trend_raw.get("dates", []),
        "sales": trend_raw.get("sales", []),
        "orders": trend_raw.get("orders", []),
    }


def format_funnel(funnel_raw: dict) -> list:
    return [
        {"name": "浏览 (PV)", "value": funnel_raw.get('pv', 0)},
        {"name": "意向 (加购)", "value": funnel_raw.get('cart', 0)},
        {"name": "购买 (成交)", "value": funnel_raw.get('buy', 0)},
    ]


def format_rankings(top10_rows: list) -> dict:
    return {
        "items": [f"商品{r.get('item_id', '未知')}" for r in top10_rows],
        "sales": [round(safe_float(r.get('sales')), 2) for r in top10_rows],
    }


def format_dimensions(cat_rows: list, chan_rows: list, age_rows: list) -> dict:
    return {
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
        ],
    }


def format_rfm(rfm_rows: list) -> list:
    return [
        {"name": r.get('rfm_label', '未知'), "value": safe_int(r.get('cnt'))}
        for r in rfm_rows
    ]


def format_retention(cohort_rows: list) -> list:
    return [
        [
            safe_int(r.get('day_diff')),
            str(r.get('cohort_date', '')),
            round(safe_int(r.get('active_users')) / max(safe_int(r.get('cohort_users')), 1) * 100, 2),
        ] for r in cohort_rows
    ]


def get_dashboard_all_data(sd: str, ed: str, period: str, backend) -> dict:
    """
    组装全看板聚合数据（一次请求返回所有看板数据）。
    将 8 次 HTTP 请求合并为 1 次，内部只调用一次 resolve_dates。
    """
    curr_start = datetime.datetime.strptime(sd, '%Y-%m-%d')
    curr_end = datetime.datetime.strptime(ed, '%Y-%m-%d')

    # 一次性获取所有原生数据（直接通过 backend 策略接口调用）
    core = backend.fetch_core_metrics(sd, ed)
    total_sales = core.get("total_sales", 0.0)
    total_orders = core.get("total_orders", 0)
    comparison = calculate_qoq_yoy(total_sales, period, curr_start, curr_end, backend)
    trend = backend.fetch_trend(sd, ed, PERIOD_MAP[period])
    funnel_data = backend.fetch_funnel(sd, ed)
    top10_rows = backend.fetch_top10(sd, ed)
    cat_rows = backend.fetch_category(sd, ed)
    chan_rows = backend.fetch_channel(sd, ed)
    age_rows = backend.fetch_age_group(sd, ed)
    rfm_rows = backend.fetch_rfm(sd, ed)
    cohort_rows = backend.fetch_cohort(sd, ed)
    base_start, base_end = fetch_date_range(backend)

    # 交给 format 方法组装出安全结构
    return {
        "date_range": {"min": base_start, "max": base_end},
        "core": {
            "total_sales": total_sales,
            "total_orders": total_orders,
            "paying_users": funnel_data.get('buy', 0),
            "avg_order_value": round(total_sales / total_orders, 2) if total_orders > 0 else 0.0,
            "qoq_rate": comparison.get("qoq_rate"),
            "yoy_rate": comparison.get("yoy_rate"),
            "period_label": PERIOD_MAP[period]["label"],
        },
        "trend": format_trend(trend),
        "funnel": format_funnel(funnel_data),
        "rankings": format_rankings(top10_rows),
        "dimensions": format_dimensions(cat_rows, chan_rows, age_rows),
        "rfm": format_rfm(rfm_rows),
        "retention": format_retention(cohort_rows),
    }
