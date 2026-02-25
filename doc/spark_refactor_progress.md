# Spark ETL Pipeline 重构进展文档

> 更新时间: 2026-02-25 13:30

## 重构目标

对 `spark_final.py` 进行多轮工程化重构，提升配置安全性、业务灵活度和数据写入可靠性。

---

## 改造清单与完成状态

### ✅ 1~5（前期已完成项）

- ✅ 外部配置解耦 (config.json + 环境变量)
- ✅ 条件聚合优化漏斗（单次扫描替代三次 filter+union）
- ✅ 数据质量报告收集器 (DataQualityReport)
- ✅ KMeans 标准化 + 轮廓系数自动选K
- ✅ 同期群留存矩阵

---

### ✅ 6. CONFIG 预检函数 (validate_config)

**问题**: Spark 启动耗时数十秒，配置错误（如 CSV 文件路径不存在、端口类型错误）要等到实际执行时才报错。

**方案**:
- 新增 `validate_config(cfg)` 函数，在 Spark 初始化**前**执行
- 检查项：
  - 必填字符串字段存在性和非空
  - `ch_port` 必须为正整数
  - CSV 文件路径可达性（`os.path.exists`）
  - `rfm_weights` 必须包含 R/F/M 三个数值
  - `rfm_thresholds` 必须包含 high_r/high_m/high_f 三个数值
- 所有错误收集后一次性输出，调用 `raise SystemExit(1)` 快速失败

**状态**: ✅ 已完成

---

### ✅ 7. 双口径转化漏斗

**问题**: 原漏斗仅提供严格时序约束口径，无法了解非标准购物路径（如直接购买）的转化情况。

**方案**:
- **严格口径** (`user_funnel_mart`): 保持原逻辑，cart_ts >= pv_ts 且 buy_ts >= cart_ts
- **宽松口径** (`user_funnel_loose_mart`): 只看是否发生过 pv/cart/buy 行为，不要求时序
- 提取公共逻辑为 `build_funnel_mart(funnel_df)` 函数，两套口径复用同一套 explode+array 展开逻辑
- 两张表独立写入 ClickHouse，前端可按需选择展示哪种口径

**状态**: ✅ 已完成

---

### ✅ 8. ClickHouse 原子写入策略

**问题**: 原策略 `DROP TABLE` + `CREATE TABLE` + `APPEND`，在 DROP 和写入完成之间存在数据不可用窗口，对在线看板查询有影响。

**方案**:
- `CREATE TABLE IF NOT EXISTS` 替代 `DROP TABLE` + `CREATE TABLE`
- 新增 `write_to_clickhouse_atomic()` 函数：
  1. `CREATE TABLE {name}_tmp_new AS {name}` — 创建同结构临时表
  2. Spark JDBC `append` 写入临时表
  3. `EXCHANGE TABLES {name} AND {name}_tmp_new` — **原子交换**，对在线查询无感
  4. `DROP TABLE {name}_tmp_new` — 清理旧数据
- 交换操作是 ClickHouse 的原子 DDL，保证看板在 ETL 过程中始终可查到完整数据

**状态**: ✅ 已完成

---

### ✅ 9. DataQualityReport 写入 etl_dq_log 监控表

**问题**: 数据质量报告仅在控制台输出，无法历史追溯。

**方案**:
- 新建 `etl_dq_log` 表（MergeTree）：
  - `run_time DateTime` — ETL 执行时间
  - `elapsed_seconds Float64` — 执行耗时
  - `metrics String` — 指标 JSON（如行数、丢弃率等）
  - `warnings String` — 告警 JSON
  - `cluster_profiles String` — 聚类画像 JSON
- ETL 完成后使用 `clickhouse_connect` 参数化插入
- 写入失败不影响主流程（try-catch 兜底），避免监控逻辑拖垮 ETL

**状态**: ✅ 已完成

---

## 验证结果

- ✅ Python 语法检查通过
- ✅ CONFIG 预检功能正常

## 文件变更

| 文件 | 操作 | 说明 |
|------|------|------|
| `spark_final.py` | 重写 | 四项新增重构 |
| `doc/spark_refactor_progress.md` | 更新 | 本文档 |
