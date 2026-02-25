# 多文件缺陷修复进展文档

> 更新时间: 2026-02-25 01:30

## 修复概览

针对用户提出的 8 项潜在缺陷，经逐一核查后执行修复。

---

## 修复清单

### ✅ 前端 App.vue (2 项)

**1. 静默失败防护**
- 在安全解构后新增数据结构完整性校验
- 当 `res.data.code === 200` 但 `core` 字段缺失时，同时触发 `console.warn` 和 `errorMsg`
- 保留 `safeNumber` 防崩溃兜底能力的同时，主动告警帮助快速定位问题

**2. ResizeObserver 防泄漏**
- `observeResize` 中使用 `try-catch` 包裹 `observer.observe()` 调用
- 注册失败时仅 `console.warn`，不影响其他图表的正常工作

---

### ✅ 后端 main.py (2 项)

**3. 除零风险专业处理**
- `max(total_orders, 1)` → `if total_orders > 0 else 0.0`
- 语义更清晰，零订单情况下显式返回 0.0

**4. 连接池架构说明**
- `DatabaseManager` 文档字符串补充当前实现的适用范围和局限性
- 说明 SQLite 单连接适用于读多写少场景
- 说明 clickhouse_connect 内部已有 HTTP 连接池

---

### ✅ Spark spark_final.py (2 项)

**5. 漏斗业务假设声明**
- 在 `[4/6]` 段落前添加 Strict Subset Assumption 说明
- 明确非标准路径被排除、可能略微低估转化率
- 给出全口径统计的替代建议

**6. 订单 ID 局限性声明**
- 在 `order_id` 生成处添加模拟口径限制说明
- 指出真实系统应使用分布式唯一 ID
- 建议毕业设计文档中明确此局限性

---

### ℹ️ 无需修改的项 (2 项)

**7. SQL 拼接残余风险** — 经核查，ClickHouse 使用的 `{sd:String}` 是原生服务端参数绑定，非字符串拼接

**8. write_to_clickhouse 参数 Bug** — 第 550-554 行已在先前重构中修复，函数内部使用传入参数

---

## 验证结果

- ✅ Python 语法检查通过 (main.py + spark_final.py)
- ✅ 浏览器截图确认：核心指标、所有图表正常渲染，无错误提示

## 文件变更

| 文件 | 操作 | 说明 |
|------|------|------|
| `frontend/src/App.vue` | 修改 | 静默失败防护 + ResizeObserver 防泄漏 |
| `main.py` | 修改 | 除零处理 + 连接池文档 |
| `spark_final.py` | 修改 | 漏斗假设声明 + 订单 ID 局限性声明 |
| `doc/defect_fix_progress.md` | 新建 | 本文档 |
