"""
配置常量模块
集中管理所有配置参数，便于统一维护和环境切换。
"""

import os
import re

# --- 数据库配置 ---

SQLITE_DB = "ecommerce.db"

# ClickHouse 重连重试：指数退避策略，避免网络抖动导致服务不可用
CH_MAX_RETRIES = 3          # 最大重试次数
CH_RETRY_BASE_DELAY = 1.0   # 初始退避延迟（秒）
CH_RETRY_BACKOFF = 2.0      # 退避倍数

# --- CORS 配置 ---

# 从环境变量读取允许的前端域名，支持部署时动态配置
CORS_ORIGINS = os.environ.get(
    "CORS_ORIGINS",
    "http://localhost:5173,http://127.0.0.1:5173"
).split(",")

# --- 业务参数 ---

# period 参数白名单映射：通过枚举限制合法值，防止 SQL 注入
PERIOD_MAP = {
    "day":   {"sqlite": "%Y-%m-%d", "ch": "%Y-%m-%d", "label": "日"},
    "week":  {"sqlite": "%Y-%W",    "ch": "%Y-%W",    "label": "周"},
    "month": {"sqlite": "%Y-%m",    "ch": "%Y-%m",    "label": "月"},
}

# 日期格式正则（编译一次复用，避免重复编译开销）
DATE_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}$')

# --- JWT 安全配置 ---

JWT_SECRET = os.environ.get("JWT_SECRET", "graduation_project_super_secret_key_2026")
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION_HOURS = 24

# JWT 白名单路径：匹配的请求跳过 Token 校验（Swagger 文档和登录接口）
JWT_WHITELIST_PATTERNS = [
    re.compile(r"^/docs(/.*)?$"),         # Swagger UI 及其静态资源
    re.compile(r"^/redoc(/.*)?$"),         # ReDoc 及其静态资源
    re.compile(r"^/openapi\.json$"),       # OpenAPI 规范
    re.compile(r"^/api/auth/login$"),       # 登录接口
]
