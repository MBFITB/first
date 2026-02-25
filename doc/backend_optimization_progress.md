# 后端深度优化进展文档

> 更新时间: 2026-02-25 13:25

## 优化目标

对 `main.py` (FastAPI 后端) 进行多轮深度优化，解决架构耦合、安全隐患和工程健壮性问题。

---

## 改造清单与完成状态

### ✅ 1. 参数化查询（防 SQL 注入）

**问题**: `where_clause` 通过 f-string 直接拼接日期字符串，存在 SQL 注入风险。

**方案**: 
- SQLite: 全部改用 `?` 占位符参数化
- ClickHouse: 使用 `{name:Type}` 格式的参数绑定

**影响范围**: 所有 12+ 个 SQL 查询全部改为参数化。

**状态**: ✅ 已完成

---

### ✅ 2. 数据库连接池 / 单例模式

**问题**: `get_db()` 每次请求都新建连接，高并发下会耗尽资源。

**方案**: 引入 `DatabaseManager` 单例类。

**状态**: ✅ 已完成

---

### ✅ 3. 代码层级解耦（Data Service）

**问题**: 所有 SQL 逻辑堆在 `get_dashboard` 路由函数，难以维护和测试。

**方案**: 提取 12 个独立的 Data Service 函数，路由函数现在只负责：参数校验 → 调用 Service → 组装响应。

**状态**: ✅ 已完成

---

### ✅ 4. 增强异常处理与健壮性

**问题**: 数据库查询结果为空时，前端可能因解构赋值失败而崩溃。

**方案**:
- 引入 `_safe_float()` 和 `_safe_int()` 工具函数
- 响应组装使用 `.get()` 防止 KeyError
- 连接生命周期由 `DatabaseManager` 统一管理

**状态**: ✅ 已完成

---

### ✅ 5. CORS 安全限制

**问题**: `allow_origins=["*"]` 允许任何来源访问 API。

**方案**: 从环境变量 `CORS_ORIGINS` 读取，默认限制为前端开发服务器地址。

**状态**: ✅ 已完成

---

### ✅ 6. SQLite 线程级连接池（threading.local）

**问题**: 之前使用单连接 + `threading.Lock` 互斥访问，高并发时 Lock 竞争成为瓶颈。

**方案**:
- 将 `self._sqlite_conn` + `self._sqlite_lock` 替换为 `threading.local()`
- 每个工作线程首次访问时自动创建独立 SQLite 连接
- 使用 `self._sqlite_conns` 列表追踪所有线程连接，在 `close_all()` 关闭时统一释放
- `get_sqlite_cursor()` 不再需要加锁，cursor 操作完全免锁

**关键优势**:
- WAL 模式下多线程真并发读取（不再串行化）
- 每线程独立连接无锁竞争
- 仍然在 `on_shutdown` 统一释放所有连接

**状态**: ✅ 已完成

---

### ✅ 7. ClickHouse 健壮重连重试逻辑

**问题**: 原有 ClickHouse 连接逻辑仅尝试一次，网络瞬断即回退到 SQLite。

**方案**:
- 引入 `_try_clickhouse_with_retry()` 替代原 `_try_clickhouse()`
- **指数退避重试**：初始延迟 1s，倍数 2x，最多 3 次 (1s → 2s → 4s)
- 配置可通过 `CH_MAX_RETRIES`, `CH_RETRY_BASE_DELAY`, `CH_RETRY_BACKOFF` 常量调节
- 心跳失败时使用 `self._ch_lock` 双重检查锁避免多线程同时重连
- 所有连接日志使用 `logger` 输出到 stderr

**状态**: ✅ 已完成

---

### ✅ 8. 全局异常处理器（global_exception_handler）

**问题**: 
- 每个路由都有 `try-except + traceback.print_exc()` 的重复模板代码
- 500 错误响应中 `str(e)` 泄露了内部实现细节（SQL 结构、数据库路径等）

**方案**:
- 新增 `@app.exception_handler(HTTPException)`: 
  - 业务级错误（400 参数校验、401 认证失败等）原样返回具体错误信息
- 新增 `@app.exception_handler(Exception)`:
  - 将详细 traceback 记录到 stderr（含 `request.method` 和 `request.url.path`）
  - 返回给客户端泛化 `{"code": 500, "message": "Internal Server Error"}`
- **路由层全部移除 try-catch**，代码量显著减少约 80 行
- 业务级参数校验改为 `raise HTTPException(status_code=400, detail="...")`

**安全性提升**:
- 客户端不再能看到 Python traceback、数据库路径、SQL 语句等内部信息
- 运维人员可通过 stderr 日志排查详细错误

**状态**: ✅ 已完成

---

### ✅ 9. JWT 白名单正则扩充

**问题**: 原白名单使用 `in ["/docs", "/redoc", "/openapi.json"]` 精确匹配，无法覆盖 Swagger UI 的子路径（如 `/docs/oauth2-redirect`、静态资源等）。

**方案**:
- 定义 `JWT_WHITELIST_PATTERNS` 正则列表：
  ```python
  JWT_WHITELIST_PATTERNS = [
      re.compile(r"^/docs(/.*)?$"),         # Swagger UI 及子路径
      re.compile(r"^/redoc(/.*)?$"),         # ReDoc 及子路径
      re.compile(r"^/openapi\.json$"),       # OpenAPI 规范
      re.compile(r"^/api/auth/login$"),      # 登录接口
  ]
  ```
- 新增 `_is_jwt_whitelisted(path)` 辅助函数
- 中间件中改用 `_is_jwt_whitelisted(request.url.path)` 替代 `in` 精确匹配
- 可轻松扩展新路径（如 `/health`、`/api/public/...` 等）

**状态**: ✅ 已完成

---

## 验证结果

- ✅ Python 语法检查通过
- ✅ Uvicorn 服务启动成功
- ✅ 所有 API 端点正常返回
- ✅ ClickHouse 不可用时自动回退到 SQLite

## 文件变更

| 文件 | 操作 | 说明 |
|------|------|------|
| `main.py` | 重写 | 九项深度优化 |
| `doc/backend_optimization_progress.md` | 更新 | 本文档 |
