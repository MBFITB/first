# 电商数据全景分析系统 — 零基础环境配置与运行指南

> **适用环境**：Windows 10 / 11  
> **预计耗时**：约 30~60 分钟  
> **最后更新**：2026-02-28

---

## 目录

1. [环境准备总览](#一环境准备总览)
2. [安装 Python](#二安装-python)
3. [安装 Java（Spark ETL 运行必需）](#三安装-java)
4. [安装 Node.js（前端运行必需）](#四安装-nodejs)
5. [安装 ClickHouse（可选，推荐）](#五安装-clickhouse可选推荐)
6. [配置项目后端](#六配置项目后端)
7. [运行 ETL 管线（数据入库）](#七运行-etl-管线数据入库)
8. [启动后端服务](#八启动后端服务)
9. [启动前端服务](#九启动前端服务)
10. [访问系统](#十访问系统)
11. [常见问题排查](#十一常见问题排查)

---

## 一、环境准备总览

本项目涉及以下环境，请按顺序安装：

| 序号 | 工具 | 用途 | 是否必须 |
|:---:|------|------|:---:|
| 1 | Python 3.10+ | 后端 API + ETL 脚本 | ✅ 必须 |
| 2 | Java 8 或 11 | PySpark 运行依赖 | ✅ 必须 |
| 3 | Node.js 18+ | 前端开发服务器 | ✅ 必须 |
| 4 | ClickHouse | 高性能列式数据库 | ⭐ 可选（不装则自动使用 SQLite） |

> **重要提示**：即使不安装 ClickHouse，系统也能完整运行，ETL 会自动回退到 SQLite 写入，后端会自动使用 SQLite 查询。ClickHouse 主要用于体验大数据量下的高性能查询。

---

## 二、安装 Python

### 2.1 下载

访问 Python 官网下载页面：  
👉 https://www.python.org/downloads/

选择 **Python 3.10** 或更高版本（推荐 3.11 或 3.12），下载 **Windows installer (64-bit)**。

### 2.2 安装

1. 双击运行下载的安装程序
2. **⚠️ 关键步骤**：在安装界面底部勾选 **「Add Python to PATH」**（非常重要！）
3. 点击 **「Install Now」** 完成安装

### 2.3 验证

打开 **命令提示符**（按 `Win + R` 输入 `cmd` 回车），执行：

```cmd
python --version
pip --version
```

应看到类似输出：
```
Python 3.12.x
pip 24.x.x from ...
```

---

## 三、安装 Java

PySpark 运行需要 Java 环境。推荐安装 **Java 8** 或 **Java 11**。

### 3.1 下载

推荐使用 Adoptium (Eclipse Temurin) 发行版：  
👉 https://adoptium.net/zh-CN/temurin/releases/?os=windows&arch=x64

选择 **JDK 11 (LTS)** → **Windows x64** → 下载 `.msi` 安装包。

### 3.2 安装

1. 双击 `.msi` 安装包
2. 安装过程中，确保勾选以下选项：
   - ✅ **Set JAVA_HOME variable**
   - ✅ **Add to PATH**
3. 完成安装

### 3.3 验证

重新打开一个命令提示符窗口，执行：

```cmd
java -version
```

应看到类似输出：
```
openjdk version "11.0.x" ...
```

> 如果提示 `java 不是内部或外部命令`，请手动配置环境变量：  
> 右键「此电脑」→ 属性 → 高级系统设置 → 环境变量 → 新建系统变量：  
> - 变量名：`JAVA_HOME`  
> - 变量值：`C:\Program Files\Eclipse Adoptium\jdk-11.x.x.x-hotspot`（你的实际安装路径）  
> 然后在 `Path` 变量中添加：`%JAVA_HOME%\bin`

---

## 四、安装 Node.js

### 4.1 下载

访问 Node.js 官网：  
👉 https://nodejs.org/

下载 **LTS 版本**（推荐 18.x 或 20.x），选择 **Windows Installer (.msi)**。

### 4.2 安装

1. 双击运行安装程序
2. 一路 **Next** 即可，默认选项已包含 `Add to PATH`
3. 完成安装

### 4.3 验证

重新打开命令提示符，执行：

```cmd
node --version
npm --version
```

应看到：
```
v20.x.x
10.x.x
```

---

## 五、安装 ClickHouse（可选，推荐）

> **再次提醒**：不安装 ClickHouse 也能运行项目，系统会自动使用 SQLite。  
> 如果你只是想快速看到效果，可以**跳过本节**。

### 5.1 下载

ClickHouse 提供 Windows 原生版本：  
👉 https://github.com/ClickHouse/ClickHouse/releases

在 Assets 列表中找到类似 `clickhouse-*-amd64.zip` 的 Windows 压缩包下载。

或者使用 PowerShell 一键下载（推荐）：

```powershell
# 在你想安装 ClickHouse 的目录下执行
curl.exe -L -o clickhouse.zip https://github.com/ClickHouse/ClickHouse/releases/download/v24.8-lts/clickhouse-v24.8-lts-windows-amd64.zip
Expand-Archive clickhouse.zip -DestinationPath clickhouse
```

### 5.2 启动 ClickHouse 服务

进入解压后的目录，找到 `clickhouse.exe`（或 `clickhouse-server.exe`），执行：

```cmd
cd clickhouse
clickhouse.exe server --config-file=config.xml
```

> 如果没有 `config.xml`，ClickHouse 会使用默认配置：
> - HTTP 端口：**8123**
> - TCP 端口：**9000**

看到 `Ready for connections` 字样即表示启动成功。

**⚠️ 请保持此窗口不要关闭**，ClickHouse 需要持续运行。

### 5.3 设置密码

本项目默认配置密码为 `password123`，你需要设置 ClickHouse 的 `default` 账户密码与之匹配。

**方式一：通过配置文件**

在 ClickHouse 解压目录下创建 `users.xml`（如果不存在），添加以下内容：

```xml
<?xml version="1.0"?>
<clickhouse>
    <users>
        <default>
            <password>password123</password>
            <networks>
                <ip>::/0</ip>
            </networks>
            <profile>default</profile>
            <quota>default</quota>
        </default>
    </users>
</clickhouse>
```

然后重启 ClickHouse。

**方式二：通过客户端设置**

打开另一个命令提示符窗口：

```cmd
cd clickhouse
clickhouse.exe client
```

在 ClickHouse 客户端中执行：

```sql
ALTER USER default IDENTIFIED BY 'password123';
```

### 5.4 验证 ClickHouse

在浏览器中访问：  
👉 http://localhost:8123/?query=SELECT%201

如果看到返回 `1`，说明 ClickHouse 运行正常。

或者用命令行验证：

```cmd
curl http://localhost:8123 -d "SELECT 1"
```

### 5.5 修改项目密码配置（如果你用了其他密码）

如果你的 ClickHouse 密码不是 `password123`，需要修改两处：

1. **ETL 配置** — 编辑项目根目录的 `config.json`：
   ```json
   "ch_password": "你的实际密码"
   ```

2. **后端连接** — 编辑 `db/manager.py` 第 105 行：
   ```python
   password='你的实际密码'
   ```

---

## 六、配置项目后端

### 6.1 打开项目目录

假设项目解压/拷贝到了 `C:\Users\你的用户名\Desktop\new`：

```cmd
cd C:\Users\你的用户名\Desktop\new
```

### 6.2 创建 Python 虚拟环境（推荐）

```cmd
python -m venv venv
venv\Scripts\activate
```

激活成功后，命令行前面会出现 `(venv)` 标识。

> **之后每次打开新的命令提示符窗口操作项目，都需要先执行**：
> ```cmd
> cd C:\Users\你的用户名\Desktop\new
> venv\Scripts\activate
> ```

### 6.3 安装 Python 依赖

```cmd
pip install -r requirements.txt
```

这会安装所有后端所需的 Python 包，包括：
- `fastapi` — Web 框架
- `uvicorn` — ASGI 服务器
- `pydantic` — 数据校验
- `PyJWT` — JWT Token
- `pyspark` — Spark ETL
- `clickhouse-connect` — ClickHouse 连接
- `numpy` — 数值计算
- `pytest` — 测试框架
- 等等

安装过程中可能需要几分钟，请耐心等待。

---

## 七、运行 ETL 管线（数据入库）

### 7.1 前提条件

确认以下文件存在于项目根目录：
- ✅ `UserBehavior.csv`（约 60MB，用户行为数据）
- ✅ `items_simulated.csv`（商品维表）
- ✅ `users_simulated.csv`（用户维表）

> 如果缺少这些数据文件，先运行数据生成器：
> ```cmd
> python generate_data.py
> ```
> 生成过程约需 2~5 分钟。

### 7.2 运行 ETL

```cmd
python spark_final.py
```

ETL 执行过程中会看到 6 个步骤的进度输出：

```
[1/6] 加载数据与预处理...
[2/6] 特征工程与 KMeans 聚类打标...
[3/6] 计算同期群留存矩阵...
[4/6] 计算转化漏斗...
[5/6] 抽取底层业务事实表...
[6/6] 写入数据库...
```

- 如果你安装了 ClickHouse 且正在运行 → 数据会写入 ClickHouse
- 如果未安装 ClickHouse → 自动回退写入 `ecommerce.db`（SQLite）

**整个过程约需 3~10 分钟**（取决于电脑性能和数据量）。

> **注意**：如果项目中已经存在 `ecommerce.db` 文件（约 140MB），说明数据已经入库，可以**跳过此步骤**直接启动后端。

---

## 八、启动后端服务

确保在项目根目录且已激活虚拟环境：

```cmd
cd C:\Users\你的用户名\Desktop\new
venv\Scripts\activate
python main.py
```

看到以下输出即表示启动成功：

```
INFO:     Uvicorn running on http://127.0.0.1:8000
INFO:     SQLite 索引已建立/确认存在
```

**⚠️ 请保持此窗口不要关闭**，后端需要持续运行。

> API 文档自动可用：打开浏览器访问 http://127.0.0.1:8000/docs 即可查看 Swagger 文档。

---

## 九、启动前端服务

**打开一个新的命令提示符窗口**（不要关闭后端窗口）：

### 9.1 进入前端目录

```cmd
cd C:\Users\你的用户名\Desktop\new\frontend
```

### 9.2 安装前端依赖

```cmd
npm install
```

首次运行需要下载依赖，约需 1~3 分钟。

### 9.3 启动开发服务器

```cmd
npm run dev
```

看到以下输出即表示启动成功：

```
  VITE v7.x.x  ready in xxx ms

  ➜  Local:   http://localhost:5173/
```

---

## 十、访问系统

### 10.1 打开浏览器

访问：**http://localhost:5173**

### 10.2 登录

在登录页面输入：

| 字段 | 值 |
|------|-----|
| 用户名 | `admin` |
| 密码 | `123456` |

点击登录后即可进入数据看板。

### 10.3 使用看板

- **顶部栏**可以选择日期范围和数据粒度（日/周/月）
- 看板包含 **8 大图表**：交易趋势、用户漏斗、Top10 商品、品类/渠道/年龄分布、RFM 价值画像、同期群留存热力图
- **点击趋势图上的数据点**可以展开当日详情侧边栏（下钻功能）

---

## 十一、常见问题排查

### Q1：`pip install` 报错 / 下载太慢

使用国内镜像加速：

```cmd
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
```

### Q2：`npm install` 报错 / 下载太慢

设置 npm 国内镜像：

```cmd
npm config set registry https://registry.npmmirror.com
npm install
```

### Q3：运行 `spark_final.py` 报 `JAVA_HOME is not set`

说明 Java 环境变量未配置成功，请：
1. 确认 Java 已安装（`java -version`）
2. 手动设置 `JAVA_HOME` 环境变量（参考第三节）
3. **重新打开**命令提示符窗口再运行

### Q4：运行 `spark_final.py` 报 `winutils.exe` 相关错误

项目已自动处理这个问题（`etl/config_manager.py` 会创建占位文件）。如果仍报错，可以手动下载 `winutils.exe` 放到 `hadoop_home/bin/` 目录下：

👉 https://github.com/steveloughran/winutils/tree/master/hadoop-3.0.0/bin

### Q5：后端启动时报 `ClickHouse 连接失败`

这是**正常现象**。如果你没有安装 ClickHouse，系统会自动回退到 SQLite：

```
WARNING: ClickHouse 全部 3 次重试失败，回退到 SQLite
INFO: 使用 SQLite 后端: ecommerce.db
```

不影响系统正常使用。

### Q6：前端页面空白 / 图表不显示

1. 确认后端正在运行（http://127.0.0.1:8000/docs 能打开）
2. 打开浏览器开发者工具（F12）→ Console 查看錯误信息
3. 确认 `ecommerce.db` 存在且非空（若不存在，需要运行 ETL 或数据生成器）

### Q7：ClickHouse 启动报端口被占用

ClickHouse 默认使用端口 8123（HTTP）和 9000（TCP）。检查是否被占用：

```cmd
netstat -ano | findstr :8123
netstat -ano | findstr :9000
```

如果被占用，可以在 ClickHouse 配置中修改端口，同时更新 `config.json` 中的 `ch_port`。

### Q8：运行测试

```cmd
cd C:\Users\你的用户名\Desktop\new
venv\Scripts\activate
pytest tests/ -v
```

---

## 附录：完整启动顺序速查

```
# ── 第 1 步：启动 ClickHouse（可选）──
cd clickhouse目录
clickhouse.exe server

# ── 第 2 步：运行 ETL（仅首次需要）──
cd C:\Users\你的用户名\Desktop\new
venv\Scripts\activate
python spark_final.py

# ── 第 3 步：启动后端 ──
python main.py

# ── 第 4 步：启动前端（新窗口）──
cd C:\Users\你的用户名\Desktop\new\frontend
npm run dev

# ── 第 5 步：浏览器打开 ──
http://localhost:5173
# 账号: admin  密码: 123456
```
