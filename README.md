# 电商数据全景分析系统 (E-commerce Analytics Fullstack)

这是一个经过深度工程化重构的电商数据分析大屏系统，涵盖了从底层数据采集（Spark ETL）、高性能存储（ClickHouse）、稳健后端（FastAPI）到现代化前端（Vue 3）的全栈架构。

---

## 🏗️ 核心架构

### 1. 数据处理层 (Spark ETL)
- **架构**：采用模块化 Pipeline 架构（`etl/`）。
- **流程**：`DataLoader` (清洗) -> `FeatureEngineer` (RFM/KMeans) -> `BusinessTransformer` (同期群/漏斗) -> `DataWriter` (原子写入)。
- **特性**：
    - 基于 KMeans 聚类中心的**智能用户打标**。
    - **双口径转化漏斗**：支持严格时序约束与宽松非时序约束。
    - **容灾机制**：首选 ClickHouse 原子写入，失败自动回退至 SQLite。
    - **DQ 监控**：全链路数据质量报告收集与审计。

### 2. 后端服务 (FastAPI)
- **架构**：三层架构 (Controller-Service-DAO/Repository)。
- **安全**：集成 JWT 认证与正则白名单机制。
- **性能**：支持数据库连接池，针对 ClickHouse 提供连接重试补偿。
- **业务**：内聚的日期偏移策略（QoQ/YoY 计算）与多源数据汇聚逻辑。

### 3. 前端展示 (Vue 3 + ECharts)
- **架构**：Vue 3 Composition API + 组件化拆分。
- **特性**：
    - **响应式大屏**：通过 `ResizeObserver` 确保在各种分辨率下完美适配。
    - **8大核心图表**：包含交易趋势、用户漏斗、Top 10 商品、品类/渠道/年龄画像、RFM 价值矩阵、同期群留存热力图。
    - **容灾加载**：主看板端点异常时自动降级为多端点并行拉取，确保大屏永不黑屏。

---

## 📂 项目目录

```text
├── api/                # 后端路由与中间件
├── core/               # 基础设施 (配置、日志、安全)
├── dao/                # 数据访问层 (SQL 逻辑)
├── services/           # 业务逻辑服务层
├── db/                 # 数据库连接管理器
├── etl/                # Spark ETL 模块化 Pipeline
├── frontend/           # Vue 3 前端代码
│   ├── src/api/        # 封装请求逻辑
│   ├── src/components/ # 复用 UI 组件
│   ├── src/composables/# 响应式逻辑复用
│   └── src/views/      # 看板主页面
├── doc/                # 重构进展与设计文档
├── main.py             # 后端入口
└── spark_final.py      # ETL 入口
```

---

## 🚀 快速开始

### 后端与数据
1. **环境准备**：`pip install fastapi uvicorn pyspark clickhouse-connect python-jwt`
2. **启动后端**：`uvicorn main:app --reload`
3. **运行 ETL**：`python spark_final.py`

### 前端
1. **安装依赖**：`cd frontend && npm install`
2. **配置环境**：修改 `.env` 中的 `VITE_API_BASE_URL`
3. **启动开发环境**：`npm run dev`

---

## 🛠️ 主要修复记录
- [x] 后端 main.py 巨型单文件解耦。
- [x] 前端 App.vue 布局重构及组件化。
- [x] 修复了 ECharts 在侧边栏收缩时的 resize 错位问题。
- [x] 修复了跨域 (CORS) 与 JWT 鉴权的集成问题。
- [x] 统一了 ETL 的 SQL 逻辑与后端 Data Service 的查询口径。
