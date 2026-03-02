"""
数据库抽象后端（Strategy Pattern）
消除 DAO 层的 if is_sqlite 分支，提供统一的查询接口。
"""

from abc import ABC, abstractmethod
from typing import Optional


class DatabaseBackend(ABC):
    """
    数据库抽象基类，定义所有业务层所需的查询接口。
    """
    
    @abstractmethod
    def fetch_core_metrics(self, start_date: str, end_date: str) -> dict:
        pass

    @abstractmethod
    def fetch_trend(self, start_date: str, end_date: str, period_cfg: dict) -> dict:
        pass

    @abstractmethod
    def fetch_comparison_sales(self, start_date: str, end_date: str) -> float:
        pass

    @abstractmethod
    def fetch_top10(self, start_date: str, end_date: str) -> list:
        pass

    @abstractmethod
    def fetch_category(self, start_date: str, end_date: str) -> list:
        pass

    @abstractmethod
    def fetch_channel(self, start_date: str, end_date: str) -> list:
        pass

    @abstractmethod
    def fetch_age_group(self, start_date: str, end_date: str) -> list:
        pass

    @abstractmethod
    def fetch_date_range_impl(self) -> tuple:
        pass

    @abstractmethod
    def fetch_funnel(self, start_date: str, end_date: str) -> dict:
        pass

    @abstractmethod
    def fetch_rfm(self, start_date: str, end_date: str) -> list:
        pass

    @abstractmethod
    def fetch_cohort(self, start_date: str, end_date: str) -> list:
        pass


class SqliteBackend(DatabaseBackend):
    """SQLite 具体实现"""
    
    def __init__(self, db_manager):
        self.db_manager = db_manager

    from dao.base import safe_float, safe_int
    
    def fetch_core_metrics(self, start_date: str, end_date: str) -> dict:
        with self.db_manager.get_sqlite_cursor() as cursor:
            row = cursor.execute(
                "SELECT SUM(price) as total_sales, COUNT(DISTINCT order_id) as total_orders "
                "FROM buy_fact WHERE date BETWEEN ? AND ?",
                (start_date, end_date)
            ).fetchone()
            from dao.base import safe_float, safe_int
            return {
                "total_sales": round(safe_float(row['total_sales']), 2) if row else 0.0,
                "total_orders": safe_int(row['total_orders']) if row else 0,
            }

    def fetch_trend(self, start_date: str, end_date: str, period_cfg: dict) -> dict:
        sqlite_fmt = period_cfg["sqlite"]
        with self.db_manager.get_sqlite_cursor() as cursor:
            trend_rows = [dict(r) for r in cursor.execute(
                "SELECT strftime(?, date) as dt, SUM(price) as sales, "
                "COUNT(DISTINCT order_id) as orders "
                "FROM buy_fact WHERE date BETWEEN ? AND ? "
                "GROUP BY dt ORDER BY dt",
                (sqlite_fmt, start_date, end_date)
            ).fetchall()]
            from dao.base import safe_float, safe_int
            return {
                "dates": [r['dt'] for r in trend_rows],
                "sales": [round(safe_float(r['sales']), 2) for r in trend_rows],
                "orders": [safe_int(r['orders']) for r in trend_rows],
            }

    def fetch_comparison_sales(self, start_date: str, end_date: str) -> Optional[float]:
        with self.db_manager.get_sqlite_cursor() as cursor:
            row = cursor.execute(
                "SELECT SUM(price) as sales FROM buy_fact WHERE date BETWEEN ? AND ?",
                (start_date, end_date)
            ).fetchone()
            from dao.base import safe_float
            return safe_float(row['sales'], default=None) if row else None

    def fetch_top10(self, start_date: str, end_date: str) -> list:
        with self.db_manager.get_sqlite_cursor() as cursor:
            return [dict(r) for r in cursor.execute(
                "SELECT item_id, SUM(price) as sales FROM buy_fact "
                "WHERE date BETWEEN ? AND ? GROUP BY item_id ORDER BY sales DESC LIMIT 10",
                (start_date, end_date)
            ).fetchall()]

    def fetch_category(self, start_date: str, end_date: str) -> list:
        with self.db_manager.get_sqlite_cursor() as cursor:
            return [dict(r) for r in cursor.execute(
                "SELECT category_id, SUM(price) as sales FROM buy_fact "
                "WHERE date BETWEEN ? AND ? GROUP BY category_id ORDER BY sales DESC LIMIT 10",
                (start_date, end_date)
            ).fetchall()]

    def fetch_channel(self, start_date: str, end_date: str) -> list:
        with self.db_manager.get_sqlite_cursor() as cursor:
            return [dict(r) for r in cursor.execute(
                "SELECT channel, SUM(price) as sales FROM buy_fact "
                "WHERE date BETWEEN ? AND ? GROUP BY channel ORDER BY sales DESC",
                (start_date, end_date)
            ).fetchall()]

    def fetch_age_group(self, start_date: str, end_date: str) -> list:
        with self.db_manager.get_sqlite_cursor() as cursor:
            return [dict(r) for r in cursor.execute(
                "SELECT age_group, SUM(price) as sales FROM buy_fact "
                "WHERE date BETWEEN ? AND ? GROUP BY age_group ORDER BY sales DESC",
                (start_date, end_date)
            ).fetchall()]

    def fetch_date_range_impl(self) -> tuple:
        import datetime
        today_str = str(datetime.date.today())
        with self.db_manager.get_sqlite_cursor() as cursor:
            row = cursor.execute("SELECT MIN(date) as min_d, MAX(date) as max_d FROM buy_fact").fetchone()
            if row:
                base_start = row['min_d'] if row['min_d'] else today_str
                base_end = row['max_d'] if row['max_d'] else today_str
            else:
                base_start = base_end = today_str
        return base_start, base_end

    def fetch_funnel(self, start_date: str, end_date: str) -> dict:
        with self.db_manager.get_sqlite_cursor() as cursor:
            row = cursor.execute(
                "SELECT SUM(has_pv) as pv, SUM(has_cart) as cart, SUM(has_buy) as buy "
                "FROM user_funnel_mart WHERE date BETWEEN ? AND ?",
                (start_date, end_date)
            ).fetchone()
            from dao.base import safe_int
            if row:
                return {'pv': safe_int(row['pv']), 'cart': safe_int(row['cart']), 'buy': safe_int(row['buy'])}
        return {'pv': 0, 'cart': 0, 'buy': 0}

    def fetch_rfm(self, start_date: str, end_date: str) -> list:
        with self.db_manager.get_sqlite_cursor() as cursor:
            return [dict(r) for r in cursor.execute(
                "SELECT r.rfm_label, COUNT(DISTINCT b.user_id) as cnt "
                "FROM buy_fact b JOIN user_rfm r ON b.user_id = r.user_id "
                "WHERE b.date BETWEEN ? AND ? GROUP BY r.rfm_label",
                (start_date, end_date)
            ).fetchall()]

    def fetch_cohort(self, start_date: str, end_date: str) -> list:
        with self.db_manager.get_sqlite_cursor() as cursor:
            return [dict(r) for r in cursor.execute(
                "SELECT cohort_date, day_diff, active_users, cohort_users "
                "FROM cohort_matrix WHERE cohort_date BETWEEN ? AND ? "
                "ORDER BY cohort_date, day_diff",
                (start_date, end_date)
            ).fetchall()]


class ClickHouseBackend(DatabaseBackend):
    """ClickHouse 具体实现"""
    
    def __init__(self, db):
        self.db = db

    def fetch_core_metrics(self, start_date: str, end_date: str) -> dict:
        rows = list(self.db.query(
            "SELECT SUM(price) as total_sales, uniqExact(order_id) as total_orders "
            "FROM buy_fact WHERE date BETWEEN {sd:String} AND {ed:String}",
            parameters={'sd': start_date, 'ed': end_date}
        ).named_results())
        from dao.base import safe_float, safe_int
        return {
            "total_sales": round(safe_float(rows[0]['total_sales']), 2) if rows else 0.0,
            "total_orders": safe_int(rows[0]['total_orders']) if rows else 0,
        }

    def fetch_trend(self, start_date: str, end_date: str, period_cfg: dict) -> dict:
        ch_fmt = period_cfg["ch"]
        trend_rows = list(self.db.query(
            "SELECT formatDateTime(date, {fmt:String}) as dt, "
            "SUM(price) as sales, uniqExact(order_id) as orders "
            "FROM buy_fact WHERE date BETWEEN {sd:String} AND {ed:String} "
            "GROUP BY dt ORDER BY dt",
            parameters={'fmt': ch_fmt, 'sd': start_date, 'ed': end_date}
        ).named_results())
        from dao.base import safe_float, safe_int
        return {
            "dates": [r['dt'] for r in trend_rows],
            "sales": [round(safe_float(r['sales']), 2) for r in trend_rows],
            "orders": [safe_int(r['orders']) for r in trend_rows],
        }

    def fetch_comparison_sales(self, start_date: str, end_date: str) -> Optional[float]:
        rows = list(self.db.query(
            "SELECT SUM(price) as sales FROM buy_fact "
            "WHERE date BETWEEN {sd:String} AND {ed:String}",
            parameters={'sd': start_date, 'ed': end_date}
        ).named_results())
        from dao.base import safe_float
        return safe_float(rows[0]['sales'], default=None) if rows else None

    def fetch_top10(self, start_date: str, end_date: str) -> list:
        return list(self.db.query(
            "SELECT item_id, SUM(price) as sales FROM buy_fact "
            "WHERE date BETWEEN {sd:String} AND {ed:String} "
            "GROUP BY item_id ORDER BY sales DESC LIMIT 10",
            parameters={'sd': start_date, 'ed': end_date}
        ).named_results())

    def fetch_category(self, start_date: str, end_date: str) -> list:
        return list(self.db.query(
            "SELECT category_id, SUM(price) as sales FROM buy_fact "
            "WHERE date BETWEEN {sd:String} AND {ed:String} "
            "GROUP BY category_id ORDER BY sales DESC LIMIT 10",
            parameters={'sd': start_date, 'ed': end_date}
        ).named_results())

    def fetch_channel(self, start_date: str, end_date: str) -> list:
        return list(self.db.query(
            "SELECT channel, SUM(price) as sales FROM buy_fact "
            "WHERE date BETWEEN {sd:String} AND {ed:String} "
            "GROUP BY channel ORDER BY sales DESC",
            parameters={'sd': start_date, 'ed': end_date}
        ).named_results())

    def fetch_age_group(self, start_date: str, end_date: str) -> list:
        return list(self.db.query(
            "SELECT age_group, SUM(price) as sales FROM buy_fact "
            "WHERE date BETWEEN {sd:String} AND {ed:String} "
            "GROUP BY age_group ORDER BY sales DESC",
            parameters={'sd': start_date, 'ed': end_date}
        ).named_results())

    def fetch_date_range_impl(self) -> tuple:
        import datetime
        today_str = str(datetime.date.today())
        rows = self.db.query("SELECT MIN(date) as min_d, MAX(date) as max_d FROM buy_fact").result_rows
        if rows and rows[0]:
            base_start = str(rows[0][0]) if rows[0][0] else today_str
            base_end = str(rows[0][1]) if rows[0][1] else today_str
        else:
            base_start = base_end = today_str
        return base_start, base_end

    def fetch_funnel(self, start_date: str, end_date: str) -> dict:
        rows = list(self.db.query(
            "SELECT SUM(has_pv) as pv, SUM(has_cart) as cart, SUM(has_buy) as buy "
            "FROM user_funnel_mart WHERE date BETWEEN {sd:String} AND {ed:String}",
            parameters={'sd': start_date, 'ed': end_date}
        ).named_results())
        from dao.base import safe_int
        if rows:
            return {'pv': safe_int(rows[0]['pv']), 'cart': safe_int(rows[0]['cart']), 'buy': safe_int(rows[0]['buy'])}
        return {'pv': 0, 'cart': 0, 'buy': 0}

    def fetch_rfm(self, start_date: str, end_date: str) -> list:
        return list(self.db.query(
            "SELECT r.rfm_label as rfm_label, uniqExact(b.user_id) as cnt "
            "FROM buy_fact b JOIN user_rfm r ON b.user_id = r.user_id "
            "WHERE b.date BETWEEN {sd:String} AND {ed:String} GROUP BY r.rfm_label",
            parameters={'sd': start_date, 'ed': end_date}
        ).named_results())

    def fetch_cohort(self, start_date: str, end_date: str) -> list:
        return list(self.db.query(
            "SELECT cohort_date, day_diff, active_users, cohort_users "
            "FROM cohort_matrix WHERE cohort_date BETWEEN {sd:String} AND {ed:String} "
            "ORDER BY cohort_date, day_diff",
            parameters={'sd': start_date, 'ed': end_date}
        ).named_results())


def get_backend(db, is_sqlite: bool) -> DatabaseBackend:
    """策略工厂：根据当前连接状态返回对应后端的实例"""
    if is_sqlite:
        from db.manager import db_manager
        return SqliteBackend(db_manager)
    return ClickHouseBackend(db)
