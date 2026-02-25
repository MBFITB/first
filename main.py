"""
电商数据看板 FastAPI 后端
入口文件：仅负责创建 FastAPI 实例、挂载中间件、注册路由和生命周期事件。
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from core.config import CORS_ORIGINS
from core.logging import logger
from db.manager import db_manager
from api.exception_handlers import http_exception_handler, global_exception_handler
from api.middleware import verify_jwt_token
from api.routers import auth, dashboard, charts


# ════════════════════════════════════════════════════════════════════
# 创建 FastAPI 应用
# ════════════════════════════════════════════════════════════════════

app = FastAPI(title="电商数据看板 API", version="3.0.0")

# ── CORS 中间件 ──
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── JWT 认证中间件 ──
app.middleware("http")(verify_jwt_token)

# ── 全局异常处理器 ──
app.add_exception_handler(HTTPException, http_exception_handler)
app.add_exception_handler(Exception, global_exception_handler)

# ── 注册路由 ──
app.include_router(auth.router)
app.include_router(dashboard.router)
app.include_router(charts.router)


# ════════════════════════════════════════════════════════════════════
# 生命周期事件
# ════════════════════════════════════════════════════════════════════

@app.on_event("startup")
def startup_event():
    """启动时自动创建 SQLite 索引，加速大数据量下的按日期查询"""
    try:
        db, is_sqlite = db_manager.get_connection()
        if is_sqlite:
            with db_manager.get_sqlite_cursor() as cursor:
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_buy_fact_date ON buy_fact(date)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_funnel_date ON user_funnel_mart(date)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_cohort_date ON cohort_matrix(cohort_date)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_buy_fact_user ON buy_fact(user_id)")
                db.commit()
            logger.info("✅ SQLite 索引已建立/确认存在")
    except Exception as e:
        logger.warning("索引创建跳过（表可能不存在）: %s", e)


@app.on_event("shutdown")
def shutdown_event():
    """应用关闭时释放数据库连接"""
    db_manager.close_all()
    logger.info("🔒 数据库连接已全部释放")


# ════════════════════════════════════════════════════════════════════
# 启动入口
# ════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)