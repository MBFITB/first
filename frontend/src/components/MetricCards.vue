<template>
  <el-row :gutter="20" class="m-b-20">
    <el-col :span="6">
      <el-card shadow="hover" class="metric-card">
        <div class="m-label">交易总额 (￥)</div>
        <div class="m-value">{{ formatMetric(metrics.total_sales) }}</div>
        <div class="m-sub">
          环比: <span :class="safeNumber(metrics.qoq_rate) >= 0 ? 'up' : 'down'">{{ formatRate(metrics.qoq_rate) }}</span>
          <span style="margin: 0 8px; color: #e8e8e8;">|</span>
          同比: <span :class="safeNumber(metrics.yoy_rate) >= 0 ? 'up' : 'down'">{{ formatRate(metrics.yoy_rate) }}</span>
        </div>
      </el-card>
    </el-col>
    <el-col :span="6">
      <el-card shadow="hover" class="metric-card">
        <div class="m-label">有效订单规模</div>
        <div class="m-value">{{ formatMetric(metrics.total_orders) }}</div>
        <div class="m-sub">基于时间戳去重口径</div>
      </el-card>
    </el-col>
    <el-col :span="6">
      <el-card shadow="hover" class="metric-card">
        <div class="m-label">支付买家基数</div>
        <div class="m-value">{{ formatMetric(metrics.paying_users) }}</div>
        <div class="m-sub">完成漏斗转化链路用户数</div>
      </el-card>
    </el-col>
    <el-col :span="6">
      <el-card shadow="hover" class="metric-card">
        <div class="m-label">平均客单价 (￥)</div>
        <div class="m-value">{{ formatMetric(metrics.avg_order_value) }}</div>
        <div class="m-sub">基于订单级聚合计算</div>
      </el-card>
    </el-col>
  </el-row>
</template>

<script setup>
defineProps({
  metrics: { type: Object, default: () => ({}) },
})

/**
 * 安全数字转换
 */
const safeNumber = (val, fallback = 0) => {
  if (val === null || val === undefined) return null
  const n = Number(val)
  return Number.isFinite(n) ? n : fallback
}

const formatMetric = (val) => {
  const n = safeNumber(val)
  return n === null ? '--' : n.toLocaleString()
}

const formatRate = (val) => {
  const n = safeNumber(val)
  if (n === null) return '--'
  return (n >= 0 ? '+' : '') + n + '%'
}
</script>

<style scoped>
.metric-card { text-align: center; border: none; border-top: 5px solid #409EFF; }
.m-label { font-size: 14px; color: #8c8c8c; margin-bottom: 8px; }
.m-value { font-size: 30px; font-weight: bold; color: #262626; font-family: monospace; }
.m-sub { font-size: 12px; color: #bfbfbf; margin-top: 5px; }
.m-sub .up { color: #f5222d; font-weight: bold; }
.m-sub .down { color: #52c41a; font-weight: bold; }
.m-b-20 { margin-bottom: 20px; }
</style>
