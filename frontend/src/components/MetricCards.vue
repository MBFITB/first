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
const safeNumber = (val, fallback = null) => {
  if (val === null || val === undefined) return fallback
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
.metric-card { 
  text-align: center; 
  border: none; 
  border-radius: 16px;
  background: #ffffff;
  transition: all 0.3s cubic-bezier(0.25, 0.8, 0.25, 1);
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.03);
}
.metric-card:hover {
  transform: translateY(-6px);
  box-shadow: 0 12px 24px rgba(0, 0, 0, 0.08);
}
.m-label { 
  font-size: 14px; 
  font-weight: 500;
  color: #6b7280; 
  margin-bottom: 12px; 
  letter-spacing: 0.5px;
}
.m-value { 
  font-size: 32px; 
  font-weight: 700; 
  color: #111827; 
  font-family: 'Inter', monospace; 
  line-height: 1.2;
}
.m-sub { 
  font-size: 13px; 
  color: #9ca3af; 
  margin-top: 10px; 
}
.m-sub .up { color: #ef4444; font-weight: 600; }
.m-sub .down { color: #10b981; font-weight: 600; }
.m-b-20 { margin-bottom: 24px; }
</style>
