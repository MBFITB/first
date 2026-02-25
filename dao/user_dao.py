"""
用户数据访问层
封装所有与用户行为分析相关的 SQL 查询（ClickHouse / SQLite 双路径）。
包括：日期范围、转化漏斗、RFM 分布、留存矩阵。
"""

import datetime

from db.manager import db_manager
from dao.base import safe_int


def fetch_date_range_impl(db, is_sqlite: bool) -> tuple:
    """
    获取数据库中日期的最小值和最大值（合并为单条 SQL）。
    返回: (base_start, base_end)
    """
    today_str = str(datetime.date.today())
    if is_sqlite:
        with db_manager.get_sqlite_cursor() as cursor:
            row = cursor.execute(
                "SELECT MIN(date) as min_d, MAX(date) as max_d FROM buy_fact"
            ).fetchone()
            if row:
                base_start = row['min_d'] if row['min_d'] else today_str
                base_end = row['max_d'] if row['max_d'] else today_str
            else:
                base_start = base_end = today_str
    else:
        rows = db.query(
            "SELECT MIN(date) as min_d, MAX(date) as max_d FROM buy_fact"
        ).result_rows
        if rows and rows[0]:
            base_start = str(rows[0][0]) if rows[0][0] else today_str
            base_end = str(rows[0][1]) if rows[0][1] else today_str
        else:
            base_start = base_end = today_str
    return base_start, base_end


def fetch_date_range(db, is_sqlite: bool) -> tuple:
    """获取日期范围（带缓存，通过 DatabaseManager 的缓存机制）"""
    return db_manager.get_date_range_cached(fetch_date_range_impl, db, is_sqlite)


def fetch_funnel(db, is_sqlite: bool, start_date: str, end_date: str) -> dict:
    """获取用户转化漏斗数据（参数化查询）"""
    if is_sqlite:
        with db_manager.get_sqlite_cursor() as cursor:
            row = cursor.execute(
                "SELECT SUM(has_pv) as pv, SUM(has_cart) as cart, SUM(has_buy) as buy "
                "FROM user_funnel_mart WHERE date BETWEEN ? AND ?",
                (start_date, end_date)
            ).fetchone()
            if row:
                return {
                    'pv': safe_int(row['pv']),
                    'cart': safe_int(row['cart']),
                    'buy': safe_int(row['buy'])
                }
    else:
        rows = list(db.query(
            "SELECT SUM(has_pv) as pv, SUM(has_cart) as cart, SUM(has_buy) as buy "
            "FROM user_funnel_mart WHERE date BETWEEN {sd:String} AND {ed:String}",
            parameters={'sd': start_date, 'ed': end_date}
        ).named_results())
        if rows:
            return {
                'pv': safe_int(rows[0]['pv']),
                'cart': safe_int(rows[0]['cart']),
                'buy': safe_int(rows[0]['buy'])
            }

    return {'pv': 0, 'cart': 0, 'buy': 0}


def fetch_rfm(db, is_sqlite: bool, start_date: str, end_date: str) -> list:
    """获取 RFM 分布（参数化查询）"""
    if is_sqlite:
        with db_manager.get_sqlite_cursor() as cursor:
            return [dict(r) for r in cursor.execute(
                "SELECT r.rfm_label, COUNT(DISTINCT b.user_id) as cnt "
                "FROM buy_fact b JOIN user_rfm r ON b.user_id = r.user_id "
                "WHERE b.date BETWEEN ? AND ? GROUP BY r.rfm_label",
                (start_date, end_date)
            ).fetchall()]
    else:
        return list(db.query(
            "SELECT r.rfm_label as rfm_label, uniqExact(b.user_id) as cnt "
            "FROM buy_fact b JOIN user_rfm r ON b.user_id = r.user_id "
            "WHERE b.date BETWEEN {sd:String} AND {ed:String} GROUP BY r.rfm_label",
            parameters={'sd': start_date, 'ed': end_date}
        ).named_results())


def fetch_cohort(db, is_sqlite: bool, start_date: str, end_date: str) -> list:
    """获取留存矩阵数据（参数化查询）"""
    if is_sqlite:
        with db_manager.get_sqlite_cursor() as cursor:
            return [dict(r) for r in cursor.execute(
                "SELECT cohort_date, day_diff, active_users, cohort_users "
                "FROM cohort_matrix WHERE cohort_date BETWEEN ? AND ? "
                "ORDER BY cohort_date, day_diff",
                (start_date, end_date)
            ).fetchall()]
    else:
        return list(db.query(
            "SELECT cohort_date, day_diff, active_users, cohort_users "
            "FROM cohort_matrix WHERE cohort_date BETWEEN {sd:String} AND {ed:String} "
            "ORDER BY cohort_date, day_diff",
            parameters={'sd': start_date, 'ed': end_date}
        ).named_results())
