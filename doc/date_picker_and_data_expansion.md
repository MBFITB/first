# 日期选择器修复 + 数据量扩展

> 更新时间: 2026-02-25 14:50

## 修复内容

### ✅ 1. 日期选择器跳转修复

**问题**: 日期选择器打开后默认显示 2026 年 2 月，而数据在 2017 年 11-12 月

**修复**:
- `fetchData` 获取到 `date_range` 后自动初始化 `dateRange = [max-7天, max]`
- el-date-picker 添加 `:default-value` 绑定到数据最大日期所在月份
- 新增 `datePickerDefault` computed 属性
- 添加 `computed` 到 vue 导入

### ✅ 2. 数据量扩展配置

- `config.json` 新增 `"data_limit": 1000000`
- 重跑 Spark ETL 时将处理 100 万行数据

### ✅ 3. SQLite 查询加速

- `main.py` 启动时自动建立 4 个索引：
  - `idx_buy_fact_date ON buy_fact(date)`
  - `idx_funnel_date ON user_funnel_mart(date)`
  - `idx_cohort_date ON cohort_matrix(cohort_date)`
  - `idx_buy_fact_user ON buy_fact(user_id)`

## 验证结果

- ✅ 日期选择器显示 2017-11-25 至 2017-12-02
- ✅ 日历面板打开时显示 2017 年 11/12 月
- ✅ SQLite 索引启动时自动建立
- ✅ 所有图表数据正常展示

## 文件变更

| 文件 | 说明 |
|------|------|
| `frontend/src/App.vue` | 日期选择器修复 |
| `main.py` | 启动时建索引 |
| `config.json` | data_limit: 1000000 |
