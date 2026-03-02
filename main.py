"""
电商数据看板 FastAPI 后端
入口文件：创建 FastAPI 实例、挂载中间件、注册路由和生命周期事件。
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from core.config import CORS_ORIGINS
from core.logging import logger
from db.manager import db_manager
from api.exception_handlers import http_exception_handler, global_exception_handler
from api.middleware import verify_jwt_token
from api.routers import auth, dashboard, charts, ai_chat
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os


# --- 生命周期事件 ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理器"""
    # Startup 阶段
    try:
        db, is_sqlite = db_manager.get_connection()
        if is_sqlite:
            with db_manager.get_sqlite_cursor() as cursor:
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_buy_fact_date ON buy_fact(date)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_funnel_date ON user_funnel_mart(date)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_cohort_date ON cohort_matrix(cohort_date)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_buy_fact_user ON buy_fact(user_id)")
                db.commit()
            logger.info("SQLite 索引已建立/确认存在")
    except Exception as e:
        logger.warning("索引创建跳过（表可能不存在）: %s", e)

    yield  # 运行应用

    # Shutdown 阶段
    db_manager.close_all()
    logger.info("数据库连接已全部释放")


# --- 创建 FastAPI 应用 ---

app = FastAPI(title="电商数据看板 API", version="3.0.0", lifespan=lifespan)

# CORS: 允许前端跨域访问（白名单来自环境变量或默认本地开发地址）
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# JWT 认证中间件：拦截所有请求校验 Token（白名单路径除外）
app.middleware("http")(verify_jwt_token)

# 全局异常处理：HTTPException 保留业务错误信息，其余 500 兜底
app.add_exception_handler(HTTPException, http_exception_handler)
app.add_exception_handler(Exception, global_exception_handler)

# 注册路由模块
app.include_router(auth.router)
app.include_router(dashboard.router)
app.include_router(charts.router)
app.include_router(ai_chat.router)


# --- 前端静态页面托管 ---

frontend_dist = os.path.join(os.path.dirname(__file__), "frontend", "dist")
if os.path.isdir(frontend_dist):
    app.mount("/assets", StaticFiles(directory=os.path.join(frontend_dist, "assets")), name="assets")
    
    @app.get("/")
    async def serve_index():
        return FileResponse(os.path.join(frontend_dist, "index.html"))
        
    @app.get("/{catchall:path}")
    async def serve_catchall(catchall: str):
        # API 路由 404
        if catchall.startswith("api/"):
            raise HTTPException(status_code=404, detail="API route not found")
        # 其他路由全部 fallback 到 index.html 由 vue-router 处理
        return FileResponse(os.path.join(frontend_dist, "index.html"))
else:
    logger.warning("未找到 frontend/dist 目录，前端静态页面托管不可用。请先运行 npm run build")
# --- 启动入口 ---

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)