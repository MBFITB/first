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
from dao.sales_dao import (
    fetch_core_metrics,
    fetch_trend,
    fetch_comparison_sales,
    fetch_top10,
    fetch_category,
    fetch_channel,
    fetch_age_group,
)
from dao.user_dao import (
    fetch_date_range,
    fetch_funnel,
    fetch_rfm,
    fetch_cohort,
)


def resolve_dates(
    start_date: Optional[str],
    end_date: Optional[str],
    db,
    is_sqlite: bool
) -> tuple:
    """
    解析和校验日期参数。
    若未传入，则默认取数据库中最大日期往前回溯 30 天。
    """
    base_start, base_end = fetch_date_range(db, is_sqlite)
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
    db,
    is_sqlite: bool
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

    prev_sales = fetch_comparison_sales(
        db, is_sqlite,
        prev_start.strftime('%Y-%m-%d'),
        prev_end.strftime('%Y-%m-%d')
    )
    ly_sales = fetch_comparison_sales(
        db, is_sqlite,
        ly_start.strftime('%Y-%m-%d'),
        ly_end.strftime('%Y-%m-%d')
    )

    qoq_rate = round((total_sales - prev_sales) / prev_sales * 100, 2) if prev_sales != 0 else 0.0
    yoy_rate = round((total_sales - ly_sales) / ly_sales * 100, 2) if ly_sales != 0 else 0.0

    return {"qoq_rate": qoq_rate, "yoy_rate": yoy_rate}


def get_core_metrics_data(sd: str, ed: str, period: str, db, is_sqlite: bool) -> dict:
    """组装核心指标数据（供路由层直接使用）"""
    curr_start = datetime.datetime.strptime(sd, '%Y-%m-%d')
    curr_end = datetime.datetime.strptime(ed, '%Y-%m-%d')

    core = fetch_core_metrics(db, is_sqlite, sd, ed)
    funnel_data = fetch_funnel(db, is_sqlite, sd, ed)
    total_sales = core.get("total_sales", 0.0)
    total_orders = core.get("total_orders", 0)
    comparison = calculate_qoq_yoy(total_sales, period, curr_start, curr_end, db, is_sqlite)

    return {
        "total_sales": total_sales,
        "total_orders": total_orders,
        "paying_users": funnel_data.get('buy', 0),
        "avg_order_value": round(total_sales / total_orders, 2) if total_orders > 0 else 0.0,
        "qoq_rate": comparison.get("qoq_rate", 0.0),
        "yoy_rate": comparison.get("yoy_rate", 0.0),
        "period_label": PERIOD_MAP[period]["label"],
    }


def get_dashboard_all_data(sd: str, ed: str, period: str, db, is_sqlite: bool) -> dict:
    """
    组装全看板聚合数据（一次请求返回所有看板数据）。
    将 8 次 HTTP 请求合并为 1 次，内部只调用一次 resolve_dates。
    """
    curr_start = datetime.datetime.strptime(sd, '%Y-%m-%d')
    curr_end = datetime.datetime.strptime(ed, '%Y-%m-%d')

    # 一次性获取所有数据
    core = fetch_core_metrics(db, is_sqlite, sd, ed)
    total_sales = core.get("total_sales", 0.0)
    total_orders = core.get("total_orders", 0)
    comparison = calculate_qoq_yoy(total_sales, period, curr_start, curr_end, db, is_sqlite)
    trend = fetch_trend(db, is_sqlite, sd, ed, PERIOD_MAP[period])
    funnel_data = fetch_funnel(db, is_sqlite, sd, ed)
    top10_rows = fetch_top10(db, is_sqlite, sd, ed)
    cat_rows = fetch_category(db, is_sqlite, sd, ed)
    chan_rows = fetch_channel(db, is_sqlite, sd, ed)
    age_rows = fetch_age_group(db, is_sqlite, sd, ed)
    rfm_rows = fetch_rfm(db, is_sqlite, sd, ed)
    cohort_rows = fetch_cohort(db, is_sqlite, sd, ed)
    base_start, base_end = fetch_date_range(db, is_sqlite)

    return {
        "date_range": {"min": base_start, "max": base_end},
        "core": {
            "total_sales": total_sales,
            "total_orders": total_orders,
            "paying_users": funnel_data.get('buy', 0),
            "avg_order_value": round(total_sales / total_orders, 2) if total_orders > 0 else 0.0,
            "qoq_rate": comparison.get("qoq_rate", 0.0),
            "yoy_rate": comparison.get("yoy_rate", 0.0),
            "period_label": PERIOD_MAP[period]["label"],
        },
        "trend": {
            "dates": trend.get("dates", []),
            "sales": trend.get("sales", []),
            "orders": trend.get("orders", []),
        },
        "funnel": [
            {"name": "浏览 (PV)", "value": funnel_data.get('pv', 0)},
            {"name": "意向 (加购)", "value": funnel_data.get('cart', 0)},
            {"name": "购买 (成交)", "value": funnel_data.get('buy', 0)},
        ],
        "rankings": {
            "items": [f"商品{r.get('item_id', '未知')}" for r in top10_rows],
            "sales": [round(safe_float(r.get('sales')), 2) for r in top10_rows],
        },
        "dimensions": {
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
        },
        "rfm": [
            {"name": r.get('rfm_label', '未知'), "value": safe_int(r.get('cnt'))}
            for r in rfm_rows
        ],
        "retention": [
            [
                safe_int(r.get('day_diff')),
                str(r.get('cohort_date', '')),
                round(safe_int(r.get('active_users')) / max(safe_int(r.get('cohort_users')), 1) * 100, 2),
            ]
            for r in cohort_rows
        ],
    }
