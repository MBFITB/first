"""
销售数据访问层
封装所有与销售数据相关的 SQL 查询（ClickHouse / SQLite 双路径）。
"""

from db.manager import db_manager
from dao.base import safe_float, safe_int


def fetch_core_metrics(db, is_sqlite: bool, start_date: str, end_date: str) -> dict:
    """
    获取核心销售指标（总销售额、总订单数）。
    使用参数化查询防止 SQL 注入。
    """
    if is_sqlite:
        with db_manager.get_sqlite_cursor() as cursor:
            row = cursor.execute(
                "SELECT SUM(price) as total_sales, COUNT(DISTINCT order_id) as total_orders "
                "FROM buy_fact WHERE date BETWEEN ? AND ?",
                (start_date, end_date)
            ).fetchone()
            total_sales = safe_float(row['total_sales']) if row else 0.0
            total_orders = safe_int(row['total_orders']) if row else 0
    else:
        rows = list(db.query(
            "SELECT SUM(price) as total_sales, uniqExact(order_id) as total_orders "
            "FROM buy_fact WHERE date BETWEEN {sd:String} AND {ed:String}",
            parameters={'sd': start_date, 'ed': end_date}
        ).named_results())
        total_sales = safe_float(rows[0]['total_sales']) if rows else 0.0
        total_orders = safe_int(rows[0]['total_orders']) if rows else 0

    return {
        "total_sales": round(total_sales, 2),
        "total_orders": total_orders,
    }


def fetch_trend(db, is_sqlite: bool, start_date: str, end_date: str, period_cfg: dict) -> dict:
    """
    获取销售趋势数据（按 period 分组聚合）。
    使用参数化查询。
    """
    if is_sqlite:
        sqlite_fmt = period_cfg["sqlite"]
        with db_manager.get_sqlite_cursor() as cursor:
            trend_rows = [dict(r) for r in cursor.execute(
                "SELECT strftime(?, date) as dt, SUM(price) as sales, "
                "COUNT(DISTINCT order_id) as orders "
                "FROM buy_fact WHERE date BETWEEN ? AND ? "
                "GROUP BY dt ORDER BY dt",
                (sqlite_fmt, start_date, end_date)
            ).fetchall()]
    else:
        ch_fmt = period_cfg["ch"]
        trend_rows = list(db.query(
            "SELECT formatDateTime(date, {fmt:String}) as dt, "
            "SUM(price) as sales, uniqExact(order_id) as orders "
            "FROM buy_fact WHERE date BETWEEN {sd:String} AND {ed:String} "
            "GROUP BY dt ORDER BY dt",
            parameters={'fmt': ch_fmt, 'sd': start_date, 'ed': end_date}
        ).named_results())

    return {
        "dates": [r['dt'] for r in trend_rows],
        "sales": [round(safe_float(r['sales']), 2) for r in trend_rows],
        "orders": [safe_int(r['orders']) for r in trend_rows],
    }


def fetch_comparison_sales(db, is_sqlite: bool, start_date: str, end_date: str) -> float:
    """
    获取某一时段的销售总额（用于环比/同比计算）。
    使用参数化查询。
    """
    if is_sqlite:
        with db_manager.get_sqlite_cursor() as cursor:
            row = cursor.execute(
                "SELECT SUM(price) as sales FROM buy_fact WHERE date BETWEEN ? AND ?",
                (start_date, end_date)
            ).fetchone()
            return safe_float(row['sales']) if row else 0.0
    else:
        rows = list(db.query(
            "SELECT SUM(price) as sales FROM buy_fact "
            "WHERE date BETWEEN {sd:String} AND {ed:String}",
            parameters={'sd': start_date, 'ed': end_date}
        ).named_results())
        return safe_float(rows[0]['sales']) if rows else 0.0


def fetch_top10(db, is_sqlite: bool, start_date: str, end_date: str) -> list:
    """获取 Top10 商品（参数化查询）"""
    if is_sqlite:
        with db_manager.get_sqlite_cursor() as cursor:
            return [dict(r) for r in cursor.execute(
                "SELECT item_id, SUM(price) as sales FROM buy_fact "
                "WHERE date BETWEEN ? AND ? GROUP BY item_id ORDER BY sales DESC LIMIT 10",
                (start_date, end_date)
            ).fetchall()]
    else:
        return list(db.query(
            "SELECT item_id, SUM(price) as sales FROM buy_fact "
            "WHERE date BETWEEN {sd:String} AND {ed:String} "
            "GROUP BY item_id ORDER BY sales DESC LIMIT 10",
            parameters={'sd': start_date, 'ed': end_date}
        ).named_results())


def fetch_category(db, is_sqlite: bool, start_date: str, end_date: str) -> list:
    """获取品类分布（参数化查询）"""
    if is_sqlite:
        with db_manager.get_sqlite_cursor() as cursor:
            return [dict(r) for r in cursor.execute(
                "SELECT category_id, SUM(price) as sales FROM buy_fact "
                "WHERE date BETWEEN ? AND ? GROUP BY category_id ORDER BY sales DESC LIMIT 10",
                (start_date, end_date)
            ).fetchall()]
    else:
        return list(db.query(
            "SELECT category_id, SUM(price) as sales FROM buy_fact "
            "WHERE date BETWEEN {sd:String} AND {ed:String} "
            "GROUP BY category_id ORDER BY sales DESC LIMIT 10",
            parameters={'sd': start_date, 'ed': end_date}
        ).named_results())


def fetch_channel(db, is_sqlite: bool, start_date: str, end_date: str) -> list:
    """获取渠道分布（参数化查询）"""
    if is_sqlite:
        with db_manager.get_sqlite_cursor() as cursor:
            return [dict(r) for r in cursor.execute(
                "SELECT channel, SUM(price) as sales FROM buy_fact "
                "WHERE date BETWEEN ? AND ? GROUP BY channel ORDER BY sales DESC",
                (start_date, end_date)
            ).fetchall()]
    else:
        return list(db.query(
            "SELECT channel, SUM(price) as sales FROM buy_fact "
            "WHERE date BETWEEN {sd:String} AND {ed:String} "
            "GROUP BY channel ORDER BY sales DESC",
            parameters={'sd': start_date, 'ed': end_date}
        ).named_results())


def fetch_age_group(db, is_sqlite: bool, start_date: str, end_date: str) -> list:
    """获取年龄分布（参数化查询）"""
    if is_sqlite:
        with db_manager.get_sqlite_cursor() as cursor:
            return [dict(r) for r in cursor.execute(
                "SELECT age_group, SUM(price) as sales FROM buy_fact "
                "WHERE date BETWEEN ? AND ? GROUP BY age_group ORDER BY sales DESC",
                (start_date, end_date)
            ).fetchall()]
    else:
        return list(db.query(
            "SELECT age_group, SUM(price) as sales FROM buy_fact "
            "WHERE date BETWEEN {sd:String} AND {ed:String} "
            "GROUP BY age_group ORDER BY sales DESC",
            parameters={'sd': start_date, 'ed': end_date}
        ).named_results())
