<template>
  <el-drawer
    v-model="drawerVisible"
    :title="`${selectedDate} 单日数据详情`"
    size="55%"
    direction="rtl"
    @closed="handleClosed"
    custom-class="daily-drawer"
  >
    <div v-loading="loading" element-loading-text="正在加载数据..." class="drawer-content">
      <el-alert v-if="errorMsg" :title="errorMsg" type="error" show-icon closable @close="errorMsg = ''" class="m-b-20" />
      
      <!-- 核心指标 -->
      <MetricCards v-if="metrics" :metrics="metrics" class="m-b-20" />
      <el-empty v-else-if="!loading" description="当日无核心指标数据" class="m-b-20"/>

      <!-- 漏斗与商品排行 -->
      <el-row :gutter="20" class="m-b-20">
        <el-col :span="12">
          <BaseChart ref="chartFunnelComp" title="核心交易转化漏斗 (Funnel)" :hasData="hasData.funnel" emptyText="暂无漏斗数据" />
        </el-col>
        <el-col :span="12">
          <BaseChart ref="chartTopComp" title="单日畅销商品黑马榜 Top10" :hasData="hasData.top10" emptyText="暂无排行数据" />
        </el-col>
      </el-row>

      <!-- 品类与渠道 -->
      <el-row :gutter="20" class="m-b-20">
        <el-col :span="12">
          <BaseChart ref="chartCategoryComp" title="热销品类结构 (Category)" :hasData="hasData.category" emptyText="暂无品类数据" />
        </el-col>
        <el-col :span="12">
          <BaseChart ref="chartChannelComp" title="交易渠道分布大盘 (Channel)" :hasData="hasData.channel" emptyText="暂无渠道数据" />
        </el-col>
      </el-row>
    </div>
  </el-drawer>
</template>

<script setup>
import { ref, watch, nextTick, computed } from 'vue'
import MetricCards from '@/components/MetricCards.vue'
import BaseChart from '@/components/BaseChart.vue'
import { fetchDashboardAll } from '@/api/dashboard'
import { useEcharts } from '@/composables/useEcharts'
import {
  getFunnelOption,
  getTop10Option,
  getCategoryOption,
  getChannelOption
} from '@/config/chartOptions'

const props = defineProps({
  visible: { type: Boolean, default: false },
  selectedDate: { type: String, default: '' },
})

const emit = defineEmits(['update:visible'])

const drawerVisible = ref(props.visible)
const loading = ref(false)
const errorMsg = ref('')
const metrics = ref(null)

// ECharts 组件 Ref
const chartFunnelComp = ref(null)
const chartTopComp = ref(null)
const chartCategoryComp = ref(null)
const chartChannelComp = ref(null)

const getChartEl = (comp) => computed(() => comp.value?.chartEl ?? null)

const { hasData, renderChart, clearAllHasData } = useEcharts([
  getChartEl(chartFunnelComp),
  getChartEl(chartTopComp),
  getChartEl(chartCategoryComp),
  getChartEl(chartChannelComp)
])

// 监听 props
watch(() => props.visible, (newVal) => {
  drawerVisible.value = newVal
  if (newVal && props.selectedDate) {
    fetchDailyData(props.selectedDate)
  }
})

watch(() => props.selectedDate, (newVal) => {
  if (drawerVisible.value && newVal) {
    fetchDailyData(newVal)
  }
})

const handleClosed = () => {
  emit('update:visible', false)
  errorMsg.value = ''
  clearAllHasData()
  metrics.value = null
}

const fetchDailyData = async (dateStr) => {
  loading.value = true
  errorMsg.value = ''
  clearAllHasData()
  try {
    const data = await fetchDashboardAll({
      start_date: dateStr,
      end_date: dateStr,
      period: 'day'
    })
    if (!data) throw new Error('服务器无响应或未返回有效数据')

    metrics.value = data.core

    // 等待弹窗和卡片内的 DOM 完全出来再渲染 ECharts
    await nextTick()

    // 渲染各个图表
    renderChart(getChartEl(chartFunnelComp), 'funnel', data.funnel, getFunnelOption, d => d && d.length > 0)
    renderChart(getChartEl(chartTopComp), 'top10', data.rankings, getTop10Option, d => d && d.items?.length > 0)
    renderChart(getChartEl(chartCategoryComp), 'category', data.dimensions?.category, getCategoryOption, d => d && d.length > 0)
    renderChart(getChartEl(chartChannelComp), 'channel', data.dimensions?.channel, getChannelOption, d => d && d.length > 0)

  } catch (err) {
    errorMsg.value = err.message || '获取单日数据失败'
  } finally {
    loading.value = false
  }
}
</script>

<style scoped>
.m-b-20 { margin-bottom: 20px; }
.drawer-content {
  padding: 0 20px 20px 20px;
  height: 100%;
  overflow-y: auto;
  background-color: #f3f4f6;
}
:deep(.el-drawer__body) {
  padding: 0;
}
:deep(.el-drawer__header) {
  margin-bottom: 0px;
  padding: 16px 20px;
  border-bottom: 1px solid #e5e7eb;
  background: white;
  color: #1f2937;
  font-weight: bold;
}
</style>
