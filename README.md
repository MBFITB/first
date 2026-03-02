# 电商数据全景分析系统 (E-commerce Analytics Fullstack)

![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)
![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-green.svg)
![Vue3](https://img.shields.io/badge/Vue.js-3.5-4FC08D.svg)
![Spark](https://img.shields.io/badge/PySpark-3.5-orange.svg)
![ClickHouse](https://img.shields.io/badge/ClickHouse-24.8-yellow.svg)

这是一个经过深度工程化重构的电商数据分析大屏系统。系统涵盖了从底层**数据采集与处理**（PySpark ETL）、**高性能数据存储**（ClickHouse/SQLite）、**提供稳定的业务接口**（FastAPI）到**现代化响应式前端展示**（Vue 3 + ECharts）的全栈架构。

---

## ✨ 核心特性

- **🚀 高性能大数据处理**：基于 PySpark 的模块化 ETL 流水线，支持海量行为数据的极速清洗和特征提取。
- **🧠 智能特征工程**：内置 KMeans 算法，基于用户行为从 R、F、M 维度进行价值分层和智能打标。
- **📊 动态双口径分析**：支持“严格时序约束”与“宽松时序约束”下的转化漏斗计算，精准描绘用户流失环节。
- **🛡️ 极致的高可用与容灾机制**：
  - **存储层**：优先使用 ClickHouse 获取极致查询性能，异常时断路器触发，自动降级至 SQLite 无缝衔接。
  - **API 层**：内嵌连接池与退避重试补偿机制。
  - **前端层**：主聚合看板端点异常时，自动平滑降级为多端点并行拉取。
- **🎨 现代化交互大屏**：通过 Vue 3 Composition API 构建的响应式大屏，搭载 8 大分析图表并支持深度下钻。

---

## 🏗️ 核心架构

全链路处理架构如下：

1. **数据处理层 (Spark ETL)**：提供 `DataLoader`、`FeatureEngineer`、`BusinessTransformer` 等模块化处理。
2. **后端服务层 (FastAPI)**：经典 Controller-Service-DAO 三层架构，支持 JWT 认证鉴权和策略模式自动适配底层数据库。
3. **前端展示层 (Vue 3)**：封装 ECharts 图表引擎，自带 `ResizeObserver` 保证在不同长宽比和分辨率下平滑适配。

详细架构设计请查阅 [技术架构说明文档](./doc/technical_architecture.md)。

---

## 📂 项目结构

```text
├── api/                # 后端路由与统一中间件拦截
├── core/               # 基础设施 (配置、日志、白名单安全机制)
├── dao/                # 数据访问层 (SQL 统一逻辑抽象与策略实现)
├── services/           # 业务逻辑编排 (日期预处理、重计算等)
├── db/                 # 数据库连接池与故障断路器管理
├── etl/                # PySpark ETL 模块化处理与数据质量(DQ)检测
├── frontend/           # Vue 3 交互式大屏代码
├── doc/                # 项目架构、特性提取与实施部署文档合集
├── main.py             # FastAPI 服务启动核心入口
└── spark_final.py      # ETL 流水线全链路触发脚本
```

---

## 🚀 快速开始

> 提示：如果希望查看详细、从零开始的环境配置与部署流程，建议直接阅读：**[详细部署指南](./doc/deployment_guide.md)**。此处仅提供核心操作命令，详细也可参见 **[极简运行指南](./doc/quick_start.md)**。

### 1. 数据生成与处理 (ETL)

确保已安装所需依赖：
```cmd
pip install -r requirements.txt
```

生成数据并开始全链路的清洗与导入：
```cmd
# 生成模拟的电商系统数据集
python generate_data.py

# 启动 ETL 作业 (支持 ClickHouse 写入，异常自动回退兼容 SQLite)
python spark_final.py
```

### 2. 启动核心服务

```cmd
# 启动 FastAPI 数据网关
python main.py
```
> 服务将在 `http://127.0.0.1:8000` 端口开启监听，启动完成后可通过浏览器访问 `/docs` 查阅 Swagger 交互式接口文档。

### 3. 启动大屏前端

```cmd
cd frontend

# 安装项目前端依赖
npm install

# 启动本地开发热更新服务器
npm run dev
```
> 地址默认将在 `http://localhost:5173` 暴露。登录名可用：`admin`，密码：`123456`。

---

## 📚 项目官方文档目录

我们为本项目配置了详尽的核心架构与功能解读，均位于项目的 `doc/` 目录下。强烈建议在开发和调试时参考：

- 📐 **[技术架构说明 (technical_architecture.md)](./doc/technical_architecture.md)**：覆盖四层解耦、设计模式实现、接口统一返回约定及组件联动说明。
- ⚙️ **[完整部署指南 (deployment_guide.md)](./doc/deployment_guide.md)**：包含了运行项目所需的一切步骤说明（尤其针对 ClickHouse 安装及 Windows 环境特殊适配）。
- 🏃 **[极简运行指南 (quick_start.md)](./doc/quick_start.md)**：一份给新开发者的 3 分钟跑通核心命令指引。
- 🖥️ **[前端功能特性 (frontend_features.md)](./doc/frontend_features.md)**：包含组件状态化隔离、基于 ECharts 的联动细节与双路容灾数据拉取机制。

---

*电商数据全景分析系统 / 追求全栈工程化的极致实践*
