"""
数据库连接管理器（单例模式）

连接策略：
- SQLite: 使用 threading.local() 实现每线程独立连接缓存，
  消除 Lock 竞争瓶颈，天然支持 WAL 模式下的并发读取。
- ClickHouse: 缓存单个 clickhouse_connect Client 实例，
  内部基于 urllib3.PoolManager 已实现 HTTP 连接池。
  增加指数退避的重连重试逻辑，提升面对网络抖动时的稳定性。
"""

import os
import time
import sqlite3
import threading
from typing import Optional, List
from contextlib import contextmanager

from core.config import (
    SQLITE_DB,
    CH_MAX_RETRIES,
    CH_RETRY_BASE_DELAY,
    CH_RETRY_BACKOFF,
)
from core.logging import logger


class DatabaseManager:
    """
    数据库连接管理器（单例模式）。

    连接策略：
    - SQLite: 使用 threading.local() 实现每线程独立连接缓存，
      消除 Lock 竞争瓶颈，天然支持 WAL 模式下的并发读取。
      每线程首次使用时自动创建连接，应用退出时统一关闭。
    - ClickHouse: 缓存单个 clickhouse_connect Client 实例，
      内部基于 urllib3.PoolManager 已实现 HTTP 连接池。
      增加指数退避的重连重试逻辑（最多 CH_MAX_RETRIES 次），
      提升面对网络抖动时的稳定性。
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        # SQLite: 每线程独立连接缓存
        self._thread_local = threading.local()
        self._sqlite_conns: List[sqlite3.Connection] = []  # 追踪所有线程连接，用于统一关闭
        self._sqlite_conns_lock = threading.Lock()
        # ClickHouse
        self._ch_client = None
        self._ch_lock = threading.Lock()  # 保护 ClickHouse 重连操作
        self._ch_available: Optional[bool] = None  # None=未检测, True/False=缓存结果
        self._backend_type: Optional[str] = None   # "clickhouse" 或 "sqlite"
        # 日期范围缓存（TTL 60 秒，避免每个请求都查 MIN/MAX）
        self._date_range_cache: Optional[tuple] = None
        self._date_range_ts: float = 0.0
        self._date_range_ttl: float = 60.0
        # ClickHouse 心跳与断路器配置
        self._ch_heartbeat_ts: float = 0.0
        self._ch_heartbeat_ttl: float = 10.0  # 心跳缓存 10 秒
        self._ch_cb_open_until: float = 0.0   # 断路器打开到什么时间（默认闭合）

    def get_date_range_cached(self, backend) -> tuple:
        """
        获取日期范围，60 秒内复用缓存。
        backend: DatabaseBackend 实例，直接调用其 fetch_date_range_impl() 方法。
        """
        now = time.time()
        if self._date_range_cache and (now - self._date_range_ts) < self._date_range_ttl:
            return self._date_range_cache
        result = backend.fetch_date_range_impl()
        self._date_range_cache = result
        self._date_range_ts = now
        return result

    def get_backend(self):
        """获取当前活跃的 DatabaseBackend 实例（策略模式入口）"""
        db, is_sqlite = self.get_connection()
        from dao.backend import get_backend as _factory
        return _factory(db, is_sqlite)

    def _try_clickhouse_with_retry(self) -> bool:
        """
        尝试连接 ClickHouse，带指数退避重试逻辑。
        成功则缓存 client 并返回 True，全部失败返回 False。
        """
        delay = CH_RETRY_BASE_DELAY
        for attempt in range(1, CH_MAX_RETRIES + 1):
            try:
                import clickhouse_connect
                client = clickhouse_connect.get_client(
                    host='localhost', port=8123,
                    username='default', password='password123'
                )
                client.query("SELECT 1")  # 心跳验证
                self._ch_client = client
                self._ch_available = True
                self._backend_type = "clickhouse"
                self._ch_heartbeat_ts = time.time()
                logger.info("ClickHouse 连接成功（第 %d 次尝试），使用 ClickHouse 后端", attempt)
                return True
            except Exception as e:
                logger.warning(
                    "ClickHouse 连接失败（第 %d/%d 次）: %s",
                    attempt, CH_MAX_RETRIES, str(e)
                )
                if attempt < CH_MAX_RETRIES:
                    time.sleep(delay)
                    delay *= CH_RETRY_BACKOFF

        self._ch_available = False
        self._ch_cb_open_until = time.time() + 60.0  # 连不上时打开断路器 60 秒
        logger.warning("ClickHouse 全部 %d 次重试失败，回退到 SQLite (断路器打开 60s)", CH_MAX_RETRIES)
        return False

    def _get_sqlite_conn(self) -> sqlite3.Connection:
        """
        获取当前线程专属的 SQLite 连接。
        利用 threading.local() 实现每线程独立连接缓存，无需加锁即可并发读取。
        """
        conn = getattr(self._thread_local, 'sqlite_conn', None)
        if conn is not None:
            return conn

        if not os.path.exists(SQLITE_DB):
            raise Exception(f"SQLite 数据库文件不存在: {SQLITE_DB}")

        conn = sqlite3.connect(SQLITE_DB)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        self._thread_local.sqlite_conn = conn

        # 追踪连接以便统一关闭
        with self._sqlite_conns_lock:
            self._sqlite_conns.append(conn)

        if self._backend_type != "sqlite":
            self._backend_type = "sqlite"
            logger.info("使用 SQLite 后端: %s", SQLITE_DB)
        return conn

    def get_connection(self):
        """
        获取数据库连接。
        返回: (connection_or_client, is_sqlite: bool)
        """
        now = time.time()

        # 断路器开启中，直接回退 SQLite 保护系统
        if now < self._ch_cb_open_until:
            return self._get_sqlite_conn(), True

        # 如果已确认 ClickHouse 可用，检查心跳
        if self._ch_available is True and self._ch_client is not None:
            # 采用 TTL 缓存：距离上次成功心跳在 10s 内，则直接认定可用，减少网络往返
            if (now - self._ch_heartbeat_ts) < self._ch_heartbeat_ttl:
                return self._ch_client, False

            try:
                # 心跳过期，真实去检测一次
                self._ch_client.query("SELECT 1")
                self._ch_heartbeat_ts = now
                return self._ch_client, False
            except Exception:
                logger.warning("ClickHouse 心跳失败，尝试重新连接...")
                with self._ch_lock:
                    if self._ch_available is True:
                        self._ch_client = None
                        self._ch_available = None

        # 首次检测或 ClickHouse 失效后重试
        if self._ch_available is None:
            with self._ch_lock:
                if self._ch_available is None:
                    if self._try_clickhouse_with_retry():
                        return self._ch_client, False

        # 回退到 SQLite
        return self._get_sqlite_conn(), True

    @contextmanager
    def get_sqlite_cursor(self):
        """
        获取当前线程 SQLite 连接的 cursor（上下文管理器）。
        由于每线程独立连接，cursor 无需加锁。
        """
        conn = self._get_sqlite_conn()
        cursor = conn.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def close_all(self):
        """应用关闭时释放所有线程的连接"""
        # 关闭所有追踪到的 SQLite 连接
        with self._sqlite_conns_lock:
            for conn in self._sqlite_conns:
                try:
                    conn.close()
                except Exception:
                    pass
            self._sqlite_conns.clear()

        if self._ch_client is not None:
            try:
                if hasattr(self._ch_client, 'close'):
                    self._ch_client.close()
                elif hasattr(self._ch_client, 'disconnect'):
                    self._ch_client.disconnect()
            except Exception:
                pass
            self._ch_client = None

        self._ch_available = None
        self._initialized = False


# 全局单例
db_manager = DatabaseManager()
