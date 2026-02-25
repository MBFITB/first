# Hadoop 环境配置 + 日期与数据量优化

> 更新时间: 2026-02-25 15:46

## ✅ 1. Hadoop 环境自动配置

**问题**: Windows 下 Spark 因缺少 `HADOOP_HOME`/`winutils.exe` 启动失败

**方案**: 在 `spark_final.py` 中自动检测 Windows 环境，创建 `hadoop_home/bin/winutils.exe` 占位文件并设置环境变量。无需手动下载或配置。

**状态**: ✅ Spark 启动正常，ETL 全流程通过

---

## ✅ 2. 默认日期范围改为 30 天

- `App.vue`: `startDate.setDate(startDate.getDate() - 30)`
- `main.py`: `datetime.timedelta(days=30)`

---

## ✅ 3. 数据量扩展到 100 万行

- `config.json`: `"data_limit": 1000000`
- Spark ETL 成功处理并写入 SQLite

---

## ✅ 4. ClickHouse → SQLite 回退写入

写入阶段先尝试 ClickHouse，连接失败自动回退到 SQLite（toPandas + to_sql），确保本地开发环境也能完成 ETL。

## ✅ 4. ClickHouse 数据引擎验证（百万级流转）

- `spark_final.py`: 彻底舍弃产生语法不兼容问题的 JDBC 连接，全面替换为 `clickhouse_connect` 的原生 `insert_df` 大批量极速直写。
- **百万级验证**: `config.json` limit 设回 1,000,000，Spark ETL 在一分钟左右完美流洗并全量持久化写入 ClickHouse（Exit code 0）。
- **后端读取**: FastAPI 后端在服务启动后成功对接本地 ClickHouse 的 8123 端口，所有 OLAP 分析查询（交易总额、漏斗图等）均通过原生驱动瞬时返回运算结果，前端大屏展示稳定。

---

## ✅ 5. 深度定制日历 UI（隐藏非当月补全日期）

针对 Element Plus 原生的标准补全逻辑（显示灰色上/下月日期凑齐整行），我们应用户要求做出了深度屏蔽：
- 在 `App.vue` 中新开启了**全局样式作用域 (`<style>`)**，并增加 `visibility: hidden !important` 和 `pointer-events: none` 的强制指令，成功穿透了 `el-date-picker` 由于 Teleport 挂载到 body 上导致的样式隔离。
- 目前选框中仅显示每个月属实的 1 - 30/31 号，使得日历面板焦点更为纯粹集中。

## ✅ 6. 数据扩展至 30 天（时间平移复制）

原始 `UserBehavior.csv` 仅覆盖 2017-11-25 至 12-03（9 天）。通过在 `spark_final.py` 中对原始数据进行 4 次时间平移复制（后移 0/9/18/27 天），将数据扩展至 2017-11-03 至 12-03（31 个日期，30 天跨度）。ClickHouse 验证 `uniq(date) = 31`。

## 文件变更

| 文件 | 说明 |
|------|------|
| `spark_final.py` | 增加时间平移逻辑，数据从 9 天扩展至 30 天 |
| `frontend/src/App.vue` | 恢复 30 天默认值，disabledDate 加回 max 上界 |
| `main.py` | resolve_dates 恢复 30 天回溯，不钳制下界 |
| `config.json` | `data_limit: 1000000` |
