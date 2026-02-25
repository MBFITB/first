# 前端加载性能优化进展文档

> 更新时间: 2026-02-25 14:05

## 优化目标

前端看板首次加载耗时过长，需优化前后端数据获取链路。

---

## 瓶颈分析

| 瓶颈 | 位置 | 影响 |
|------|------|------|
| `resolve_dates` 重复调用 | 后端 | 8个路由 × 2条SQL = 16次冗余查询 |
| `fetch_date_range` 两条SQL | 后端 | MIN和MAX分开查询 |
| `get_core_metrics` 内部串行 | 后端 | 额外5次SQL |
| `date_range` 串行阻塞 | 前端 | 浪费一个RTT |

**优化前总计**: ~28次SQL + 8次HTTP请求

---

## 优化措施

### ✅ 1. 日期范围缓存 (TTL 60s)
- `DatabaseManager` 新增 `get_date_range_cached()` 方法
- 60秒内复用，8个并发请求只触发1次SQL

### ✅ 2. `fetch_date_range` 合并为单条SQL
- `SELECT MIN(date), MAX(date) FROM buy_fact` 替代两条独立查询

### ✅ 3. 聚合端点 `/api/dashboard/all`
- 一次请求返回全部看板数据（core/trend/funnel/rankings/dimensions/rfm/retention/date_range）
- 内部只调用一次 `resolve_dates`

### ✅ 4. 前端优先使用聚合端点
- `fetchData` 先尝试 `/api/dashboard/all`，成功则直接渲染
- 失败则自动回退到原有多端点模式（兼容性保障）

---

## 优化效果

| 指标 | 优化前 | 优化后 |
|------|--------|--------|
| HTTP 请求数 | 8 | **1** |
| SQL 查询数 | ~28 | **~10** |
| 网络 RTT | 8× 并发 + 1× 串行 | **1×** |

## 验证结果

- ✅ 后端语法检查通过
- ✅ `/api/dashboard/all` 返回 200 OK
- ✅ 浏览器确认登录后仅 1 次数据请求
- ✅ 看板数据完整展示，无控制台错误
- ✅ 回退逻辑保留，原有分散端点仍可用

## 文件变更

| 文件 | 说明 |
|------|------|
| `main.py` | 日期缓存 + 合并SQL + 聚合端点 |
| `frontend/src/App.vue` | fetchData 优先聚合端点 |
