# Windows 极简部署运行指南

> **本指南提供最核心的运行命令。如果遇到报错或环境配置问题，请参考完整版的 `deployment_guide.md`。**

## 1. 核心环境要求
- **Python 3.10+** (安装时必选 `Add to PATH`)
- **Java 8/11** (安装并配置 `JAVA_HOME` 环境变量)
- **Node.js 18+**

---

## 2. 安装与启动 ClickHouse (⭐ 可选，推荐)

> **说明**：如果不安装 ClickHouse，项目运行时会自动降级使用内置的 SQLite，完全不影响功能完整性。这里提供一键安装是为了体验大数据量下的极致查询性能。如果你只想看跑起来的效果，可以**直接跳过本节**。

打开 **PowerShell** 窗口，一键下载并启动：
```powershell
# 下载并解压
curl.exe -L -o clickhouse.zip https://github.com/ClickHouse/ClickHouse/releases/download/v24.8-lts/clickhouse-v24.8-lts-windows-amd64.zip
Expand-Archive clickhouse.zip -DestinationPath clickhouse
cd clickhouse

# 启动服务端 (请保持此窗口不关！)
.\clickhouse.exe server
```

**新开一个 cmd 窗口**，设置 ClickHouse 密码：
```cmd
cd clickhouse
# 启动客户端连接
clickhouse.exe client
# 在客户端内执行以下 SQL 语句来修改密码
ALTER USER default IDENTIFIED BY 'password123';
# 执行后输入 exit 退出客户端
```

---

## 3. 生成数据与入库 (ETL)

**新开一个 cmd 窗口**，进入项目根目录：
```cmd
cd 你的项目路径

# 创建并激活虚拟环境 (成功后会有 venv 前缀)
python -m venv venv
venv\Scripts\activate

# 1. 安装依赖模块 (使用清华源加速)
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

# 2. 生成模拟数据 (约耗时 2-5 分钟)
python generate_data.py

# 3. 运行 ETL 清洗并写入 ClickHouse (约耗时 3-10 分钟)
python spark_final.py
```

---

## 4. 启动后端 API 服务

数据入库完成后，继续在**刚刚跑完 ETL 的 cmd 窗口**执行：
```cmd
python main.py
```
> **提示**：看到 `Uvicorn running on http://127.0.0.1:8000` 表示成功，请保持此开启状态。

---

## 5. 启动前端页面

**新开一个 cmd 窗口**，进入前端目录：
```cmd
cd 你的项目路径\frontend

# 安装依赖模块 (使用淘宝镜像加速)
npm install --registry=https://registry.npmmirror.com

# 启动前端页面
npm run dev
```

---

## 6. 访问系统大屏

- **打开浏览器访问**：`http://localhost:5173`
- **登录账号**：`admin`
- **登录密码**：`123456`
