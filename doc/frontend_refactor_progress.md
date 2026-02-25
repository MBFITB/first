# 前端 App.vue 重构进展文档

> 更新时间: 2026-02-25 12:45

## 重构目标

对 `frontend/src/App.vue` 进行多轮重构，提升防御性编程能力、图表自适应性和安全性。

---

## 改造清单与完成状态

### ✅ 1. 数据安全解构与兜底

**问题**: 直接访问深层嵌套属性，如果后端返回为空则页面崩溃。

**方案**:
- 在 `fetchData` 中使用完整的**解构赋值 + 默认值**：
  ```js
  const { core = {}, trend = {}, funnel = [], ... } = res?.data?.data || {}
  ```
- 引入 `safeNumber()` 工具函数，所有数值模板绑定都经过安全转换
- `metrics.value` 使用完整的字段枚举

**状态**: ✅ 已完成

---

### ✅ 2. ECharts 配置项解耦

**问题**: `setOption` 的配置代码全部内联在 `fetchData` 中，难以维护。

**方案**: 提取 8 个独立的 Option 生成函数：

| 函数 | 用途 |
|------|------|
| `getTrendOption(data)` | 趋势折线图 |
| `getFunnelOption(data)` | 漏斗图 |
| `getTop10Option(data)` | Top10 横向柱状图 |
| `getCategoryOption(data)` | 品类环形图 |
| `getChannelOption(data)` | 渠道饼图 |
| `getAgeGroupOption(data)` | 年龄玫瑰图 |
| `getRfmOption(data)` | RFM 画像环形图 |
| `getRetentionOption(data)` | 留存热力图 |

另外引入统一的 `renderChart()` 渲染调度器，负责判空 + 初始化 + setOption。

**状态**: ✅ 已完成

---

### ✅ 3. 暂无数据 UI 状态（el-empty）

**问题**: 后端返回空数据时，显示空坐标轴。

**方案**:
- 引入 `hasData` reactive 对象，追踪 8 个图表的数据状态
- 模板中使用 `v-if="!hasData.xxx"` 显示 `<el-empty />` 组件
- 图表容器使用 `v-show="hasData.xxx"` 而非 `v-if`，避免 DOM 频繁重建

**状态**: ✅ 已完成

---

### ✅ 4. 深度优化自适应逻辑（ResizeObserver）

**问题**: 仅监听 `.main-body`，Flex 布局下有时失效。

**方案**:
- 为**每个图表容器**（8 个 ref）独立注册 ResizeObserver
- 每个 observer 仅触发对应容器内的 ECharts 实例 resize
- 保留全局 `window.resize` 作为兜底
- 统一在 `onUnmounted` 中断开所有 observer

**状态**: ✅ 已完成

---

### ✅ 5. 性能优化

**问题**: dispose 不完善可能导致内存泄漏。

**方案**:
- 统一渲染器为 `canvas`（`RENDERER` 常量），适合大数据量场景
- `onUnmounted` 中遍历所有实例执行 `dispose()`
- 断开所有 ResizeObserver
- 清空 `instances` 和 `resizeObservers` 引用

**状态**: ✅ 已完成

---

### ✅ 6. 增强错误提示 UI

**问题**: 请求失败只在 console 输出。

**方案**:
- 新增 `errorMsg` 状态
- 请求失败时显示 `<el-alert>` 组件，红色错误提示
- 所有图表标记为无数据

**状态**: ✅ 已完成

---

### ✅ 7. safeNumber 函数优化（区分 null 和 0）

**问题**: 原 `safeNumber` 将 `null` 和 `0` 同等对待，后端未返回数据时仍显示 `0`，而非 `--`。

**方案**:
- `safeNumber(val)`: `null/undefined` 返回 `null`（而非 0），`0` 正常返回 `0`
- 新增 `formatMetric(val)`: 将 `null` 显示为 `'--'`，数字调用 `toLocaleString()`
- 新增 `formatRate(val)`: 将 `null` 显示为 `'--'`，数字显示 `±xx%`
- 模板层使用 `formatMetric` / `formatRate` 替代直接调用 `safeNumber().toLocaleString()`
- `metrics.value` 赋值时保留原始值（不再包裹 `safeNumber`），由模板层决定显示方式

**状态**: ✅ 已完成

---

### ✅ 8. renderChart 健壮性增强（数据格式校验 + try-catch）

**问题**: 若后端返回的数据结构发生变化（例如字段缺失或类型不对），`optionFn` 内部可能抛异常导致整个页面崩溃。

**方案**:
- **所有 8 个 optionFn 内部增加数据格式校验**：
  - `getTrendOption`: 校验 `dates/sales/orders` 是否为数组
  - `getFunnelOption`: 校验每项是否包含 `name` (string) 和 `value` (number)
  - `getTop10Option`: 校验 `items/sales` 是否为数组
  - `getCategoryOption / getChannelOption / getAgeGroupOption / getRfmOption`: 校验 `name/value` 字段
  - `getRetentionOption`: 校验是否为二维数组且每项 ≥ 3 元素
- **renderChart 渲染调度器两层 try-catch**：
  - 第一层：包裹 `hasDataCheck(data)` 调用
  - 第二层：包裹 `optionFn(data)` + `setOption()` 调用
  - 渲染失败时标记 `hasData[name] = false`，降级显示 `el-empty`

**状态**: ✅ 已完成

---

### ✅ 9. 趋势图双 Y 轴优化（单位 + splitNumber）

**问题**: 左右 Y 轴无单位标识，且刻度线数量不同导致视觉不平衡。

**方案**:
- 左 Y 轴 `name: '交易额 (￥)'`，`splitNumber: 5`
- 右 Y 轴 `name: '订单数 (单)'`，`splitNumber: 5`，`alignTicks: true`
- 左轴 `axisLabel.formatter`: 超过1万的数值自动转为"x.x万"格式
- 右轴 `grid.right` 从 4% 调整为 5%，为单位标签留出空间

**状态**: ✅ 已完成

---

### ✅ 10. 登录拦截器增加 401 统一自动跳转

**问题**: 原有 401 检测逻辑仅在 `fetchData` 的 `Promise.allSettled` 结果中手动检查，其他请求（如登录）未覆盖。

**方案**:
- 新增 **axios 全局响应拦截器** (`axios.interceptors.response.use`)
  - 拦截所有 HTTP 响应的错误分支
  - 如果 `error.response?.status === 401`，自动调用 `handleLogout()` 并使用 `ElMessage.error` 弹出提示
- 同时修复 `fetchData` 中 `Promise.allSettled()` 的 401 检测条件，增加 `r.status === 'rejected'` 前置判断，避免访问 fulfilled 结果的 `reason` 属性

**状态**: ✅ 已完成

---

## 验证结果

- ✅ 前端成功启动 (Vite v7.3.1, port 5173)
- ✅ 所有 8 个图表正确渲染
- ✅ 核心指标数据正确显示
- ✅ 浏览器截图确认无异常

## 文件变更

| 文件 | 操作 | 说明 |
|------|------|------|
| `frontend/src/App.vue` | 重写 | 十项重构优化 |
| `doc/frontend_refactor_progress.md` | 更新 | 本文档 |
