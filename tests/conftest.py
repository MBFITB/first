"""
pytest 共享 fixture
提供内存 SQLite 测试数据库、SqliteBackend fixture 和 FastAPI TestClient。
"""

import sqlite3
import pytest
from unittest.mock import MagicMock
from contextlib import contextmanager
from fastapi.testclient import TestClient

from main import app


@pytest.fixture
def sqlite_db():
    """创建内存 SQLite 数据库并插入模拟数据"""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # 创建测试表并插入基础数据
    cursor.executescript("""
        CREATE TABLE buy_fact (
            user_id INTEGER, item_id INTEGER, category_id INTEGER,
            price REAL, date TEXT, channel TEXT, age_group TEXT, order_id TEXT
        );
        CREATE TABLE user_funnel_mart (date TEXT, has_pv INTEGER, has_cart INTEGER, has_buy INTEGER);
        CREATE TABLE cohort_matrix (cohort_date TEXT, day_diff INTEGER, cohort_users INTEGER, active_users INTEGER);
        CREATE TABLE user_rfm (user_id INTEGER, rfm_label TEXT);

        INSERT INTO buy_fact VALUES (1, 101, 10, 99.9, '2017-11-15', 'App Store', '25-34', 'ORD001');
        INSERT INTO buy_fact VALUES (2, 102, 10, 199.0, '2017-11-15', '官网', '18-24', 'ORD002');
        INSERT INTO buy_fact VALUES (3, 103, 20, 50.0, '2017-11-16', '小程序', '35-45', 'ORD003');

        INSERT INTO user_funnel_mart VALUES ('2017-11-15', 1000, 100, 20);
        INSERT INTO cohort_matrix VALUES ('2017-11-15', 0, 100, 100);
        INSERT INTO cohort_matrix VALUES ('2017-11-15', 1, 100, 72);
        INSERT INTO user_rfm VALUES (1, '核心高价值客户');
        INSERT INTO user_rfm VALUES (2, '一般维持客户');
    """)
    conn.commit()
    yield conn
    conn.close()


@pytest.fixture
def sqlite_backend(sqlite_db):
    """
    构建基于内存 SQLite 的 SqliteBackend 测试实例。
    通过 mock db_manager 的 get_sqlite_cursor 方法来使用内存数据库。
    """
    from dao.backend import SqliteBackend

    mock_manager = MagicMock()

    @contextmanager
    def mock_cursor():
        cursor = sqlite_db.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    mock_manager.get_sqlite_cursor = mock_cursor
    return SqliteBackend(mock_manager)


@pytest.fixture
def test_client():
    """创建 FastAPI TestClient（不启动真实服务器）"""
    return TestClient(app)
