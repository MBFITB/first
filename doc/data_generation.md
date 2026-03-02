# 数据生成与 ETL 技术文档

> 本文档完整描述了项目中数据从「无到有」的全过程，包括模拟数据的生成原理、统计分布选型、Spark ETL 管道的处理逻辑，以及最终入库的数据结构。

---

## 一、总体架构概览

整个数据流分为 **两大阶段**：

```
┌─────────────────────────────────────────────────────────────┐
│                    阶段一：数据模拟生成                        │
│                   (generate_data.py)                        │
│                                                             │
│   NumPy 统计分布引擎                                         │
│   ├── Zipf 分布 → 商品热度 / 品类分布                         │
│   ├── Pareto 分布 → 用户活跃度                               │
│   ├── Log-Normal 分布 → 商品价格                             │
│   ├── 高斯叠加模型 → 双11 流量爆发                            │
│   ├── Sigmoid 衰减 → 价格摩擦因子                            │
│   └── 指数衰减模型 → 用户留存                                │
│                                                             │
│   输出 ─→ UserBehavior.csv + items_simulated.csv             │
│          + users_simulated.csv                              │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                   阶段二：Spark ETL 管道                      │
│                 (etl/pipeline.py)                            │
│                                                             │
│   PySpark 流水线                                             │
│   ├── DataLoader:          CSV 加载 → 清洗 → 大宽表 JOIN      │
│   ├── FeatureEngineer:     RFM 计算 → KMeans 聚类打标         │
│   ├── BusinessTransformer: 同期群留存 + 双口径漏斗              │
│   └── DataWriter:          ClickHouse 原子写入 / SQLite 回退   │
│                                                             │
│   输出 ─→ 5 张业务表 + 1 张 DQ 日志表                          │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
              FastAPI 后端 → Vue 前端仪表盘
```

---

## 二、阶段一：模拟数据生成（`generate_data.py`）

### 2.1 全局配置

| 参数 | 值 | 说明 |
|------|-----|------|
| `SEED` | 42 | 随机种子，确保每次生成结果可复现 |
| `NUM_USERS` | 50,000 | 模拟用户数量 |
| `NUM_ITEMS` | 10,000 | 模拟商品 SKU 数量 |
| `NUM_CATEGORIES` | 500 | 商品品类数量 |
| `TARGET_ROWS` | 2,000,000 | 目标行为流水行数 |
| 时间范围 | 2017-11-01 ~ 2017-12-10 | 40 天，覆盖双11大促 |

### 2.2 输出文件

| 文件名 | 字段 | 规模 | 用途 |
|--------|------|------|------|
| `UserBehavior.csv` | user_id, item_id, category_id, type, ts | ~200 万行 (~57MB) | 用户行为流水（无 header） |
| `items_simulated.csv` | item_id, category_id, price | 10,000 行 | 商品维表（含 header） |
| `users_simulated.csv` | user_id, age_group, channel | 50,000 行 | 用户维表（含 header） |

### 2.3 核心统计模型详解

#### 2.3.1 商品维表生成 — Zipf 分布 + Log-Normal 分布

**品类-商品映射**：使用 **Zipf 分布**（α=1.5）模拟品类热度的长尾效应。

```python
# 少数热门品类拥有大量商品，符合电商品类集中度的真实规律
category_weights = np.random.zipf(a=1.5, size=NUM_CATEGORIES)
```

**价格分布**：使用 **Log-Normal 分布**（μ=4.0, σ=1.2）生成自然连续的价格曲线，范围裁剪到 [5, 9999] 元。

```python
# Log-Normal 分布天然适合价格建模：中位数 ~80 元，长尾延伸到数千元
raw_prices = np.random.lognormal(mean=4.0, sigma=1.2, size=NUM_ITEMS)
prices = np.clip(raw_prices, 5, 9999)
```

> **为什么选 Log-Normal？** 在真实电商中，大多数商品定价集中在中低档（9.9~200），少量高端/奢侈品定价极高。Log-Normal 分布的右偏长尾特性完美匹配这种分布规律。

**最终价格分布预期：**
- 低价 (9.9~50)：约 40%，浏览量大，转化率高
- 中低 (50~200)：约 30%
- 中高 (200~800)：约 20%
- 高价 (800~5000)：约 10%，浏览适中，转化率极低

#### 2.3.2 用户维表生成 — 离散概率采样

用户画像基于市场调研的经验比例进行人工设定：

| 维度 | 分类 | 比例 |
|------|------|------|
| **年龄段** | 18-24 | 25% |
| | 25-34 | 35% |
| | 35-45 | 22% |
| | 46+ | 15% |
| | 未知 | 3% |
| **渠道** | App Store | 34% |
| | 官网 | 33% |
| | 小程序 | 33% |

#### 2.3.3 时间分布引擎 — 高斯叠加 + 潮汐模型

时间维度由 **日级权重** 和 **小时级权重** 两层组合而成。

**日级权重 — 双11 高斯爆发模型**：

```python
# 双11 当天流量 5 倍，前后按高斯衰减，σ=1.5 天
spike = 4.0 * np.exp(-0.5 * (delta_days / 1.5) ** 2)
```

- 基础权重：工作日 = 1.0
- 周末效应：周六/周日 × 1.20
- 双11 爆发：11月11日当天叠加 4.0 的高斯峰值（总计 5.0 倍），前后 3 天（11-08~11-14）按 σ=1.5 天的高斯曲线衰减

**小时级权重 — 日内潮汐分布**：

```
时段          | 权重  | 说明
─────────────|────── |──────────
00:00~01:00  | 0.8   | 深夜中等
02:00~06:00  | 0.2~0.5 | 凌晨低谷
07:00~09:00  | 1.0~1.2 | 早高峰
10:00~12:00  | 1.0   | 上午平稳
12:00~14:00  | 0.8~0.9 | 午休低谷
14:00~18:00  | 1.0~1.2 | 下午平稳
19:00~23:00  | 1.5~2.0 | 晚间高峰（全天最高）
```

#### 2.3.4 行为流水生成 — 多模型联合驱动

这是最核心的模块，融合了 5 个子模型：

**(1) 用户活跃度 — Pareto 分布**

```python
# α=1.2，大量「沉默用户」只产生几条记录，少数 VIP 产生数百条
user_activity = np.random.pareto(a=1.2, size=NUM_USERS) + 1
events_per_user = user_activity / user_activity.sum() * TARGET_ROWS
```

> **Pareto 分布**（帕累托分布）：经典的「二八定律」数学表达。α=1.2 产生比标准 80/20 更极端的长尾，与电商「少数超级用户贡献大量行为」的规律一致。

**(2) 商品热度 — Zipf 分布**

```python
# a=1.3，头部 20% 商品吞掉约 80% 的浏览量
item_popularity = np.random.zipf(a=1.3, size=NUM_ITEMS)
```

**(3) 用户留存 — 指数衰减模型**

```python
# 衰减时间常数 τ=3.5 天
# P(第d天仍活跃) = exp(-d / τ)
retention_prob = np.exp(-days_since / tau)
```

预期留存率：
| 天数 | 留存率 |
|------|--------|
| 次日 (Day 1) | ~75% |
| 3日 | ~42% |
| 7日 | ~13% |
| 14日 | ~2% |
| 30日 | ~0.02% |

> 每个用户有一个随机的「首次活跃日」（按日级权重采样），后续每天独立按指数衰减概率决定是否活跃。

**(4) 漏斗转化 — 分层概率 + 价格摩擦**

基础转化率：
- PV → Cart (加购)：**10%**
- Cart → Buy (购买)：**20%**
- 总体 PV → Buy：约 **2%**

**价格摩擦因子**使用 Sigmoid 衰减函数：

```python
# 价格越高，转化越难
friction = 1.0 / (1.0 + (price / 200.0) ** 1.5)
```

| 价格区间 | 摩擦因子 | 对转化率的影响 |
|----------|----------|---------------|
| ≤50 元 | ~1.0 | 几乎不影响 |
| 50~200 元 | 0.7~1.0 | 轻微降低 |
| 200~800 元 | 0.3~0.7 | 显著降低 |
| >800 元 | 0.1~0.3 | 大幅降低 |
| >2000 元 | 0.05~0.1 | 极低转化 |

实际转化率 = 基础转化率 × 价格摩擦因子

**(5) 双11 转化率加成**

```python
# 双11 周期内 (11-08 ~ 11-14)，转化率提升 50%
cart_boost = 1.5 if is_double11 else 1.0
```

#### 2.3.5 数据质量报告

生成完成后，脚本自动输出以下校验信息：

- **日期分布柱状图**：验证双11 峰值是否形成
- **二八法则验证**：Top 20% 商品的流量占比是否接近 80%
- **日内时段分布**：验证晚高峰是否出现
- **漏斗转化率**：验证 PV→Cart→Buy 的实际比率

---

## 三、阶段二：Spark ETL 管道（`etl/` 目录）

### 3.1 技术栈

| 技术 | 版本/用途 |
|------|----------|
| **PySpark** | 分布式数据处理引擎 |
| **PySpark ML** | VectorAssembler + StandardScaler + KMeans |
| **ClickHouse** | 列式 OLAP 数据库（主存储） |
| **SQLite** | 轻量级回退存储 |
| **clickhouse-connect** | ClickHouse Python 客户端 |

### 3.2 模块化架构

ETL 管道采用**模块化流水线**设计，每个模块职责单一：

```
etl/
├── pipeline.py            # 流水线编排入口
├── config_manager.py      # 配置加载 + 环境预检 + Hadoop 路径
├── data_loader.py         # Spark 初始化 + CSV 加载 + 清洗
├── feature_engineer.py    # RFM 特征工程 + KMeans 聚类
├── business_transformer.py # 同期群留存 + 双口径漏斗
├── data_writer.py         # ClickHouse 原子写入 / SQLite 回退
└── data_quality.py        # 全链路数据质量报告
```

### 3.3 详细处理流程

#### Step 1：配置加载（`ConfigManager`）

读取 `config.json`，包含：
- ClickHouse 连接信息（host, port, user, password）
- CSV 文件路径
- Spark 参数（driver_memory, parallelism）
- RFM 权重和阈值
- Hadoop 环境变量设置

#### Step 2：数据加载与清洗（`DataLoader`）

```
3个 CSV 文件
    │
    ▼
Spark 读取 (schema 指定)
    │
    ├── UserBehavior.csv  → 无 header, 手动 schema
    ├── items_simulated.csv → 有 header, inferSchema
    └── users_simulated.csv → 有 header, inferSchema
    │
    ▼
日期解析与过滤
    │ ├── Unix 时间戳 → Date 类型
    │ ├── 过滤范围: 2017-11-01 ~ 2017-12-10
    │ └── 生成 order_id = user_id_ts_item_id
    │
    ▼
大宽表 JOIN (LEFT JOIN)
    │ ├── JOIN items (关联价格)
    │ └── JOIN users (关联用户画像)
    │
    ▼
数据质量校验
    │ ├── 检测缺失价格 → 丢弃
    │ ├── 检测缺失渠道 → 填充 '未知渠道'
    │ └── 检测缺失年龄 → 填充 '未知'
    │
    ▼
缓存 (MEMORY_AND_DISK 策略)
```

#### Step 3：RFM 特征工程 + KMeans 聚类（`FeatureEngineer`）

**RFM 指标计算**：

| 指标 | 含义 | 计算方式 |
|------|------|---------|
| **R** (Recency) | 最近一次购买距今天数 | `datediff(截止日期, max(购买日期))` |
| **F** (Frequency) | 购买频次 | `countDistinct(order_id)` |
| **M** (Monetary) | 累计消费金额 | `sum(price)` |

**处理流程**：

```
购买行为过滤 (type == 'buy')
    │
    ▼
按 user_id 聚合 RFM
    │
    ▼
VectorAssembler 组装特征向量
    │
    ▼
StandardScaler 标准化 (消除 R/F/M 量纲差异)
    │
    ▼
KMeans 轮廓系数自动选 K (K=3~5)
    │  ├── 尝试 K=3,4,5
    │  ├── 计算 Silhouette 轮廓系数
    │  └── 选择得分最高的 K
    │
    ▼
智能标签映射 (基于聚类中心特征判定)
    │  ├── 流失/沉睡客户      (R 值极高)
    │  ├── 核心高价值客户      (M 高 + R 低)
    │  ├── 高频忠诚客户        (F 高)
    │  ├── 潜力发展客户        (R 低，F/M 中等)
    │  └── 一般维持客户        (默认)
    │
    ▼
输出: user_rfm (user_id, rfm_label)
```

**标签判定优先级规则**：

```python
# 判定优先级：流失检测 > 高价值识别 > 高频识别 > 潜力识别 > 一般
if r_val > threshold_high_r:  return "流失/沉睡客户"
if m_val > threshold_high_m and r_val < 0:  return "核心高价值客户"
if f_val > threshold_high_f:  return "高频忠诚客户"
if r_val <= 0 and (f_val > 0 or m_val > 0):  return "潜力发展客户"
return "一般维持客户"
```

#### Step 4：业务转换（`BusinessTransformer`）

**(1) 同期群留存矩阵**

```
所有用户按「首次出现日期」分群
    │
    ▼
计算每个 cohort 在 Day 0~7 的活跃用户数
    │
    ▼
输出: cohort_matrix (cohort_date, day_diff, active_users, cohort_users)
```

**(2) 双口径转化漏斗**

| 口径 | 约束 | 适用场景 |
|------|------|---------|
| **严格口径** | 要求时序：cart_ts ≥ pv_ts 且 buy_ts ≥ cart_ts | 真实转化路径分析 |
| **宽松口径** | 不要求时序：只看是否发生过 PV/Cart/Buy | 整体行为覆盖分析 |

> **为什么要双口径？** 严格口径反映真实的购物路径（先浏览→再加购→再购买），但会遗漏「直接购买」等跳步行为。宽松口径统计全量行为，提供更高的数值基准。两者对比能揭示用户行为路径的复杂度。

**(3) 购买事实表（`buy_fact`）**

从大宽表中提取 `type == 'buy'` 的记录，保留关键字段：
`date, user_id, order_id, item_id, category_id, price, channel, age_group`

#### Step 5：数据库写入（`DataWriter`）

**双路径容灾设计**：

```
检测 ClickHouse 连接
    │
    ├── ✅ 可用 → ClickHouse 原子写入
    │       │
    │       ├── 确保目标表存在 (CREATE TABLE IF NOT EXISTS)
    │       ├── 建立临时表 (table_tmp_new)
    │       ├── 写入数据到临时表
    │       ├── EXCHANGE TABLES (原子交换，零停机)
    │       └── 删除旧临时表
    │
    └── ❌ 不可用 → SQLite 回退写入
            │
            ├── 生成 ecommerce.db
            ├── DROP + REPLACE 写入
            └── 创建索引加速查询
```

> **EXCHANGE TABLES 策略**：ClickHouse 提供的原子表交换命令，在交换瞬间完成新旧数据切换，保证在 ETL 重刷数据期间前端查询不会读到空表或脏数据。

### 3.4 最终入库表结构

| 表名 | 字段 | 引擎/排序键 |
|------|------|------------|
| `buy_fact` | date, user_id, order_id, item_id, category_id, price, channel, age_group | MergeTree / (date, user_id) |
| `user_rfm` | user_id, rfm_label | MergeTree / (user_id) |
| `cohort_matrix` | cohort_date, day_diff, active_users, cohort_users | MergeTree / (cohort_date, day_diff) |
| `user_funnel_mart` | user_id, has_pv, has_cart, has_buy, date | MergeTree / (user_id) |
| `user_funnel_loose_mart` | user_id, has_pv, has_cart, has_buy, date | MergeTree / (user_id) |
| `etl_dq_log` | run_time, elapsed_seconds, metrics, warnings, cluster_profiles | MergeTree / (run_time) |

### 3.5 全链路数据质量报告（`DataQualityReport`）

DQ 报告贯穿整个 ETL 管道，收集以下信息：

- **原始数据指标**：行数、缺失值统计、数据丢弃率
- **KMeans 聚类画像**：各簇 RFM 均值、用户数、标签
- **告警信息**：缺失价格/渠道/年龄等字段的处理记录
- **各表行数统计**：写入数据库的实际条数

最终 DQ 报告会被写入 `etl_dq_log` 表，支持每次 ETL 运行的可追溯审计。

---

## 四、关键技术亮点总结

| 技术点 | 实现方式 | 业务价值 |
|--------|---------|---------|
| **统计分布驱动的数据合成** | Zipf + Pareto + Log-Normal + 高斯 + 指数衰减 | 模拟数据具备真实电商的统计特征 |
| **可复现的随机生成** | 固定 SEED=42 | 任何人在任何环境运行都能得到完全一致的数据 |
| **KMeans 自动寻优** | 轮廓系数遍历 K=3~5 | 无需人工指定聚类数，自动选择最优分群 |
| **RFM 智能标签映射** | 基于聚类中心坐标的优先级规则 | 自动化客户分群，无需人工标注 |
| **双口径漏斗分析** | 严格时序约束 vs 宽松非时序约束 | 多维度审视转化效率，避免单一口径偏差 |
| **ClickHouse EXCHANGE TABLES** | 先写临时表再原子交换 | 零停机数据刷新，前端无感知 |
| **SQLite 容灾回退** | 自动探测 ClickHouse 可用性 | 无 ClickHouse 环境也能正常运行 |
| **全链路 DQ 报告** | DataQualityReport 类贯穿管道 | 每次 ETL 运行可追溯、可审计 |

---

## 五、如何运行

### 5.1 生成模拟数据

```bash
python generate_data.py
```

输出 3 个 CSV 文件到项目根目录，耗时约 1~3 分钟。

### 5.2 运行 ETL 管道

```bash
python spark_final.py
```

自动完成数据加载 → 特征工程 → 业务转换 → 数据库写入，并在控制台输出 DQ 报告。
