# 电商数据全景分析系统 — 技术架构说明文档

> **版本**：v3.0.0  
> **技术栈**：PySpark ETL · FastAPI · Vue 3 + ECharts · SQLite / ClickHouse  
> **最后更新**：2026-02-28

---

## 一、系统总览

本系统是一个全链路电商用户行为分析平台，覆盖：  
**数据生成 → ETL 清洗/建模 → 后端 API 服务 → 前端可视化大屏**

```
┌──────────────────────────────────────────────────────────────────┐
│                    前端 (Vue 3 + ECharts)                        │
│  LoginBox → HeaderBar → Dashboard → MetricCards / BaseChart     │
│        DailyDetailDrawer (深度交互：图表下钻)                     │
└───────────────────────────┬──────────────────────────────────────┘
                            │ HTTP/JSON (JWT 鉴权)
┌───────────────────────────▼──────────────────────────────────────┐
│                   后端 (FastAPI Python)                           │
│  路由层 (api/routers) → 服务层 (services) → DAO 层 (dao)         │
│  中间件：JWT 认证 · CORS 跨域 · 全局异常处理                      │
└───────────────────────────┬──────────────────────────────────────┘
                            │ SQL 查询
┌───────────────────────────▼──────────────────────────────────────┐
│              数据库 (ClickHouse / SQLite 自动切换)                 │
│  buy_fact · user_rfm · cohort_matrix · user_funnel_mart          │
└──────────────────────────────────────────────────────────────────┘
        ▲                                                          
        │ ETL 写入                                                 
┌───────┴──────────────────────────────────────────────────────────┐
│               ETL 流水线 (PySpark)                                │
│  ConfigManager → DataLoader → FeatureEngineer →                  │
│  BusinessTransformer → DataWriter (+ DataQualityReport)          │
└──────────────────────────────────────────────────────────────────┘
```

---

## 二、项目目录结构

```
项目根目录/
├── main.py                  # 后端启动入口（FastAPI 应用创建）
├── spark_final.py           # ETL 启动入口（调用 etl/pipeline.py）
├── generate_data.py         # 模拟数据生成器（统计分布驱动）
├── config.json              # ETL 配置文件（ClickHouse/Spark/RFM 参数）
├── requirements.txt         # Python 依赖清单
├── ecommerce.db             # SQLite 数据库文件（ETL 产出）
│
├── api/                     # 🔹 路由层 + 中间件
│   ├── routers/             #    路由模块
│   │   ├── auth.py          #    认证路由 (POST /api/auth/login)
│   │   ├── dashboard.py     #    看板聚合路由 (GET /api/dashboard/all)
│   │   └── charts.py        #    图表路由 (7 个 GET 端点)
│   ├── middleware.py         #    JWT 认证中间件
│   ├── schemas.py           #    Pydantic 响应模型 (ApiResponse<T>)
│   └── exception_handlers.py #   全局异常处理器
│
├── services/                # 🔹 业务逻辑层
│   ├── dashboard_service.py #    看板业务：日期解析、环比同比、数据组装
│   └── auth_service.py      #    认证业务：用户校验、Token 签发
│
├── dao/                     # 🔹 数据访问层 (DAO)
│   ├── backend.py           #    策略模式：DatabaseBackend 抽象基类
│   │                        #    + SqliteBackend / ClickHouseBackend 实现
│   ├── base.py              #    通用工具函数 (safe_float, safe_int)
│   ├── user_dao.py          #    用户相关 DAO
│   └── sales_dao.py         #    销售相关 DAO
│
├── core/                    # 🔹 基础设施
│   ├── config.py            #    配置常量（CORS、JWT、period 白名单等）
│   ├── security.py          #    JWT 工具（签发/解码/白名单判定）
│   └── logging.py           #    日志配置（统一格式的 logger 实例）
│
├── db/                      # 🔹 数据库连接管理
│   └── manager.py           #    单例 DatabaseManager
│                             #    (断路器 + 心跳 TTL + 指数退避重连)
│
├── etl/                     # 🔹 Spark ETL 模块化管线
│   ├── pipeline.py          #    管线主入口（6 步流水线编排）
│   ├── config_manager.py    #    配置加载 + 预检 + Hadoop 环境
│   ├── data_loader.py       #    Spark 初始化 + CSV 读取 + 清洗
│   ├── feature_engineer.py  #    RFM 特征工程 + KMeans 聚类打标
│   ├── business_transformer.py # 同期群留存 + 双口径漏斗 + 事实表
│   ├── data_writer.py       #    ClickHouse 原子写入 + SQLite 回退
│   └── data_quality.py      #    全链路数据质量报告收集器
│
├── frontend/                # 🔹 Vue 3 前端
│   ├── src/
│   │   ├── App.vue          #    根组件（登录/看板状态切换）
│   │   ├── main.js          #    入口 (Vue + ElementPlus 注册)
│   │   ├── style.css        #    全局样式
│   │   ├── views/
│   │   │   └── Dashboard.vue #   看板主页面（8 大图表布局）
│   │   ├── components/
│   │   │   ├── LoginBox.vue  #   登录表单组件
│   │   │   ├── HeaderBar.vue #   顶部栏（日期选择、粒度切换）
│   │   │   ├── MetricCards.vue # 核心指标卡片（4 个 KPI）
│   │   │   ├── BaseChart.vue #   图表容器基础组件
│   │   │   └── DailyDetailDrawer.vue # 图表下钻侧边抽屉
│   │   ├── composables/
│   │   │   ├── useAuth.js   #   认证状态管理 (Composition API)
│   │   │   └── useEcharts.js #  ECharts 生命周期管理
│   │   ├── api/
│   │   │   ├── request.js   #   Axios 实例 (Token 注入 + 401 拦截)
│   │   │   └── dashboard.js #   看板 API 封装 (聚合 + 回退)
│   │   └── config/          #   前端配置
│   ├── package.json         #   Node.js 依赖清单
│   └── vite.config.js       #   Vite 构建配置
│
├── tests/                   # 🔹 单元测试 (pytest)
│   ├── conftest.py          #   测试夹具 (fixtures)
│   ├── test_api.py          #   API 端点测试
│   ├── test_backend.py      #   数据库后端策略测试
│   ├── test_dashboard_service.py # 看板服务层测试
│   ├── test_auth_service.py #   认证服务测试
│   ├── test_dao_base.py     #   DAO 工具函数测试
│   ├── test_schemas.py      #   Pydantic Schema 测试
│   ├── test_etl_config.py   #   ETL 配置管理器测试
│   ├── test_etl_data_quality.py # ETL 数据质量报告测试
│   └── test_etl_feature_engineer.py # ETL 特征工程测试
│
├── doc/                     # 📄 文档目录
├── hadoop_home/             # Windows Hadoop 占位目录 (winutils.exe)
├── UserBehavior.csv         # 原始用户行为数据 (~200 万行)
├── items_simulated.csv      # 商品维表 (~5000 条)
└── users_simulated.csv      # 用户维表 (~50000 条)
```

---

## 三、后端架构详解

### 3.1 三层架构

| 层级 | 目录 | 职责 |
|------|------|------|
| **路由层 (Controller)** | `api/routers/` | 接收 HTTP 请求、参数校验、调用 Service、返回 `ApiResponse<T>` |
| **服务层 (Service)** | `services/` | 业务逻辑编排：日期解析、环比同比计算、数据格式化 |
| **数据访问层 (DAO)** | `dao/` | 封装 SQL 查询，通过策略模式屏蔽 SQLite/ClickHouse 差异 |

### 3.2 策略模式 — 数据库后端抽象

```
DatabaseBackend (ABC 抽象基类)
├── fetch_core_metrics()   ── 核心指标查询
├── fetch_trend()          ── 销售趋势查询
├── fetch_comparison_sales() ── 对比销售额（环比/同比）
├── fetch_top10()          ── Top10 商品
├── fetch_category()       ── 品类分布
├── fetch_channel()        ── 渠道分布
├── fetch_age_group()      ── 年龄分组
├── fetch_funnel()         ── 转化漏斗
├── fetch_rfm()            ── RFM 分布
├── fetch_cohort()         ── 同期群留存
└── fetch_date_range_impl() ── 日期范围

SqliteBackend (具体实现)     ← 使用 sqlite3 cursor + Row
ClickHouseBackend (具体实现)  ← 使用 clickhouse-connect Client
```

### 3.3 数据库连接管理（`db/manager.py`）

- **单例模式**：双重检查锁定 (`__new__` + `threading.Lock`)
- **SQLite**：`threading.local()` 每线程独立连接缓存，支持 WAL 并发读取
- **ClickHouse**：`urllib3.PoolManager` 内置连接池 + 指数退避重连（最多 3 次）
- **断路器机制**：ClickHouse 连接全部失败后打开断路器 60 秒，期间直接回退 SQLite
- **心跳 TTL**：每 10 秒探测一次 ClickHouse 可用性，减少不必要的网络往返
- **日期范围缓存**：60 秒 TTL，避免每个请求重复查询 MIN/MAX 日期

### 3.4 安全机制

- **JWT 认证**：登录后签发 HS256 Token（有效期 24 小时）
- **中间件拦截**：所有请求默认校验 Token，白名单路径放行（登录、Swagger 文档）
- **CORS 白名单**：从环境变量 `CORS_ORIGINS` 读取，默认允许 `localhost:5173`
- **period 参数白名单**：枚举限制合法值为 `day/week/month`，防止 SQL 注入
- **参数化查询**：所有日期参数使用参数化绑定，不拼接 SQL

### 3.5 API 端点清单

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/auth/login` | 用户登录，返回 JWT Token |
| GET | `/api/dashboard/all` | **聚合端点**：一次返回全部看板数据 |
| GET | `/api/config/date_range` | 获取数据可用日期范围 |
| GET | `/api/metrics/core` | 核心指标（销售额/订单数/客单价/环比/同比） |
| GET | `/api/charts/trend` | 销售趋势（按日/周/月粒度） |
| GET | `/api/charts/funnel` | 用户转化漏斗（浏览→加购→购买） |
| GET | `/api/charts/rankings` | Top10 热销商品 |
| GET | `/api/charts/dimensions` | 多维分析（品类/渠道/年龄分布） |
| GET | `/api/charts/rfm` | RFM 用户价值分布 |
| GET | `/api/charts/retention` | 同期群留存热力图 |

### 3.6 演示账号

| 账号 | 密码 | 角色 | 说明 |
|------|------|------|------|
| `admin` | `123456` | admin | 管理员，完整权限 |
| `viewer` | `123456` | viewer | 观察者，只读权限 |

---

## 四、ETL 管线详解

### 4.1 执行流程（6 步流水线）

```
Step 0: 初始化 DataQualityReport（贯穿全链路收集指标）
Step 1: ConfigManager — 配置加载 + 预检校验 + Hadoop 环境配置 (Windows)
Step 2: DataLoader    — Spark 初始化 + CSV 读取 + 清洗 + 大宽表 JOIN
Step 3: FeatureEngineer — RFM 指标计算 + StandardScaler + KMeans 聚类
Step 4: BusinessTransformer — 同期群留存矩阵 + 双口径漏斗 + buy_fact
Step 5: DataWriter    — ClickHouse 原子写入（EXCHANGE TABLES）/ SQLite 回退
Step 6: 输出 DQ 报告 + 释放 Spark 资源
```

### 4.2 核心算法

#### RFM 特征工程 + KMeans 聚类
1. 从购买行为计算 R(最近购买距今天数)、F(购买频次)、M(消费金额)
2. `VectorAssembler` 组装特征向量 → `StandardScaler` 标准化（消除量纲）
3. KMeans 自动寻优：遍历 K=3~5，选择**轮廓系数**最高的 K
4. 基于聚类中心的**智能标签判定**（优先级：流失检测 > 高价值 > 高频 > 潜力 > 一般）

#### 双口径转化漏斗
- **严格口径**：要求 `cart_ts >= pv_ts` 且 `buy_ts >= cart_ts`（时序约束）
- **宽松口径**：只看行为是否发生过，不约束时序顺序

#### 同期群留存矩阵
- 按用户首次行为日期分群（cohort_date）
- 计算 Day 0 ~ Day 7 的活跃用户数和留存率

### 4.3 数据库写入策略

- **ClickHouse 原子写入**：先写入临时表 `{table}_tmp_new`，再 `EXCHANGE TABLES` 原子替换
- **SQLite 回退**：ClickHouse 不可用时自动回退，使用 `pandas.to_sql()` 写入
- **DQ 日志持久化**：将 DataQualityReport 序列化后写入 `etl_dq_log` 表

### 4.4 产出数据表

| 表名 | 说明 | 主要字段 |
|------|------|---------|
| `buy_fact` | 购买事实表 | date, user_id, order_id, item_id, category_id, price, channel, age_group |
| `user_rfm` | 用户 RFM 标签 | user_id, rfm_label |
| `cohort_matrix` | 同期群留存矩阵 | cohort_date, day_diff, active_users, cohort_users |
| `user_funnel_mart` | 严格口径漏斗 | user_id, date, has_pv, has_cart, has_buy |
| `user_funnel_loose_mart` | 宽松口径漏斗 | user_id, date, has_pv, has_cart, has_buy |
| `etl_dq_log` | ETL 质量审计日志 | run_time, elapsed_seconds, metrics, warnings, cluster_profiles |

---

## 五、前端架构详解

### 5.1 技术选型

| 技术 | 版本 | 用途 |
|------|------|------|
| Vue 3 | ^3.5 | 前端框架 (Composition API + `<script setup>`) |
| ECharts | ^6.0 | 图表渲染引擎 |
| Element Plus | ^2.13 | UI 组件库（表单/卡片/抽屉/加载等） |
| Axios | ^1.13 | HTTP 请求 |
| Vite | ^7.3 | 构建工具 + 开发服务器 |

### 5.2 组件结构

```
App.vue                        ← 根组件：登录/看板状态切换
├── LoginBox.vue               ← 登录表单（用户名+密码）
└── Dashboard.vue              ← 看板主页面
    ├── HeaderBar.vue           ← 顶部栏：日期范围选择器 + 粒度切换 + 用户信息
    ├── MetricCards.vue         ← 4 个核心指标卡片 (销售额/订单数/付费用户/客单价)
    ├── BaseChart.vue × 8       ← 图表容器 (趋势/漏斗/Top10/品类/渠道/年龄/RFM/留存)
    └── DailyDetailDrawer.vue   ← 图表下钻：点击趋势图某天 → 抽屉展示当日详情
```

### 5.3 8 大核心图表

| 图表 | 类型 | 数据来源 |
|------|------|---------|
| 交易趋势 | 折线图 (双Y轴) | `/api/charts/trend` |
| 用户转化漏斗 | 漏斗图 | `/api/charts/funnel` |
| Top 10 热销商品 | 水平柱状图 | `/api/charts/rankings` |
| 品类分布 | 饼图 | `/api/charts/dimensions` (category) |
| 渠道效能 | 饼图 | `/api/charts/dimensions` (channel) |
| 年龄画像 | 玫瑰图 (nightingale) | `/api/charts/dimensions` (age_group) |
| RFM 价值画像 | 环形图 | `/api/charts/rfm` |
| 同期群留存 | 热力图 (heatmap) | `/api/charts/retention` |

### 5.4 容灾加载策略

前端采用**两级数据获取策略**确保大屏永不黑屏：

1. **优先调用聚合端点** `GET /api/dashboard/all`（1 次请求获取全部数据）
2. **聚合失败时**降级为**多端点并行拉取**（`Promise.allSettled` 7 个端点并行）
3. 单个端点失败不影响其他图表渲染

---

## 六、数据生成器（`generate_data.py`）

模拟 200 万行真实感电商行为数据，核心统计分布：

| 维度 | 分布模型 | 说明 |
|------|---------|------|
| 用户活跃度 | Pareto 分布 | 多数低频、少数高频（符合幂律） |
| 商品热度 | Zipf 分布 | 头部商品吃掉大部分流量 |
| 日流量 | 双11 高斯叠加 | 11 月 11 日 5 倍峰值，高斯衰减 |
| 小时流量 | 潮汐分布 | 晚高峰 19-23 点权重 1.5~2.0 |
| 转化漏斗 | 分层概率 | PV→Cart 10%, Cart→Buy 20% |
| 价格摩擦 | Sigmoid 衰减 | 价格越高转化越难 |
| 用户留存 | 指数衰减 | 模拟用户活跃天数 |

---

## 七、测试体系

共 **10 个测试文件**，覆盖后端各层：

| 文件 | 测试对象 | 测试内容 |
|------|---------|---------|
| `test_api.py` | API 端点 | 路由返回值、状态码、参数校验 |
| `test_backend.py` | 数据库后端 | 策略模式两种实现的查询行为 |
| `test_dashboard_service.py` | 看板服务 | 日期解析、环比同比计算、数据格式化 |
| `test_auth_service.py` | 认证服务 | 登录校验、Token 签发 |
| `test_dao_base.py` | DAO 工具函数 | safe_float / safe_int 边界值 |
| `test_schemas.py` | Pydantic 模型 | ApiResponse 序列化/字段校验 |
| `test_etl_config.py` | ETL 配置 | 配置加载、预检、缺失字段处理 |
| `test_etl_data_quality.py` | DQ 报告 | 指标收集、告警、序列化 |
| `test_etl_feature_engineer.py` | 特征工程 | KMeans 标签判定逻辑 |

运行测试：
```bash
pytest tests/ -v --cov=. --cov-report=term-missing
```

---

## 八、配置文件说明

### `config.json`（ETL 配置）

```jsonc
{
    "ch_host": "localhost",        // ClickHouse 主机地址
    "ch_port": 8123,               // ClickHouse HTTP 端口
    "ch_user": "default",          // ClickHouse 用户名
    "ch_password": "password123",  // ClickHouse 密码
    "ch_database": "default",      // ClickHouse 数据库名
    "behavior_csv": "UserBehavior.csv",   // 用户行为 CSV
    "items_csv": "items_simulated.csv",   // 商品维表 CSV
    "users_csv": "users_simulated.csv",   // 用户维表 CSV
    "data_limit": 1000000,         // 数据量限制 (null 表示不限制)
    "driver_memory": "4g",         // Spark Driver 内存
    "default_parallelism": "8",    // Spark 并行度
    "locality_wait": "3s",         // Spark 数据本地性等待时间
    "rfm_weights": {               // RFM 加权分数权重
        "R": -0.2,                 //   R 权重为负（越近越好）
        "F": 0.3,                  //   F 权重
        "M": 0.5                   //   M 权重（消费金额最重要）
    },
    "rfm_thresholds": {            // 聚类标签判定阈值
        "high_r": 0.5,             //   R 高于此值判定为流失
        "high_m": 0.3,             //   M 高于此值判定为高价值
        "high_f": 0.3              //   F 高于此值判定为高频
    }
}
```

### `core/config.py`（后端运行时配置）

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `SQLITE_DB` | `ecommerce.db` | SQLite 文件路径 |
| `CH_MAX_RETRIES` | 3 | ClickHouse 最大重试次数 |
| `CH_RETRY_BASE_DELAY` | 1.0s | 初始退避延迟 |
| `CH_RETRY_BACKOFF` | 2.0 | 退避倍数 |
| `CORS_ORIGINS` | `localhost:5173` | CORS 允许的前端域名 (环境变量) |
| `JWT_SECRET` | (默认值) | JWT 签名密钥 (建议通过 `JWT_SECRET` 环境变量覆盖) |
| `JWT_ALGORITHM` | HS256 | JWT 签名算法 |
| `JWT_EXPIRATION_HOURS` | 24 | Token 有效期(小时) |

---

## 九、涉及的设计模式

| 模式 | 应用位置 | 说明 |
|------|---------|------|
| **策略模式** | `dao/backend.py` | 抽象基类 + SQLite/ClickHouse 两种具体实现 |
| **单例模式** | `db/manager.py` | 双重检查锁定保证全局唯一 DatabaseManager |
| **流水线模式** | `etl/pipeline.py` | 6 步 ETL 串行编排 |
| **工厂方法** | `db/manager.py#get_backend()` | 根据连接类型自动创建对应 Backend |
| **断路器模式** | `db/manager.py` | ClickHouse 失败后 60s 内直接回退 SQLite |
| **上下文管理器** | `db/manager.py#get_sqlite_cursor()` | 自动管理 cursor 生命周期 |
| **观察者(回调)** | `frontend/api/request.js` | `setUnauthorizedHandler` 注入 401 登出回调 |
