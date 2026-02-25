<template>
  <el-container class="dashboard-wrapper">

    <el-aside width="220px" class="aside-menu">
      <div class="logo-area">
        <el-icon :size="18"><DataAnalysis /></el-icon>
        <span style="margin-left: 10px;">全景电商分析系统</span>
      </div>
      <el-menu default-active="1" background-color="#001529" text-color="#a6adb4" active-text-color="#fff">
        <el-menu-item index="1">
          <el-icon><Monitor /></el-icon><span>多源数据融合大屏</span>
        </el-menu-item>
      </el-menu>
    </el-aside>

    <el-container direction="vertical">
      <HeaderBar
        v-model:dateRange="dateRange"
        v-model:period="period"
        :currentRole="currentRole"
        :dataRangeLimit="dataRangeLimit"
        @change="fetchData"
        @logout="handleLogout"
      />

      <el-main class="main-body" v-loading="loading" element-loading-text="正在加载数据...">

        <!-- 错误提示 -->
        <el-alert v-if="errorMsg" :title="errorMsg" type="error" show-icon closable @close="errorMsg = ''" class="m-b-20" />

        <!-- 核心指标卡片 -->
        <MetricCards :metrics="metrics" />

        <!-- 趋势图 + 漏斗图 -->
        <el-row :gutter="20" class="m-b-20">
          <el-col :span="14">
            <BaseChart ref="chartTrendComp" :title="`观测周期内交易额演化趋势 (${periodLabel})`" :hasData="hasData.trend" emptyText="暂无趋势数据" />
          </el-col>
          <el-col :span="10">
            <BaseChart ref="chartFunnelComp" title="用户转化漏斗 (时序约束)" :hasData="hasData.funnel" emptyText="暂无漏斗数据" />
          </el-col>
        </el-row>

        <!-- Top10 + 品类 + 渠道 -->
        <el-row :gutter="20" class="m-b-20">
          <el-col :span="8">
            <BaseChart ref="chartTopComp" title="成交贡献度 TOP 10 商品" :hasData="hasData.top10" emptyText="暂无商品数据" />
          </el-col>
          <el-col :span="8">
            <BaseChart ref="chartCategoryComp" title="品类销售额分布 (Category)" :hasData="hasData.category" emptyText="暂无品类数据" />
          </el-col>
          <el-col :span="8">
            <BaseChart ref="chartChannelComp" title="各交易渠道效能分布 (Channel)" :hasData="hasData.channel" emptyText="暂无渠道数据" />
          </el-col>
        </el-row>

        <!-- RFM + 年龄 -->
        <el-row :gutter="20" class="m-b-20">
          <el-col :span="12">
            <BaseChart ref="chartRFMComp" title="所选时段活跃用户: 价值画像 (RFM)" :hasData="hasData.rfm" emptyText="暂无 RFM 数据" />
          </el-col>
          <el-col :span="12">
            <BaseChart ref="chartAgeComp" title="所选时段活跃用户: 年龄段分布 (Age Group)" :hasData="hasData.age_group" emptyText="暂无年龄数据" />
          </el-col>
        </el-row>

        <!-- 留存热力图 -->
        <el-row :gutter="20">
          <el-col :span="24">
            <BaseChart ref="chartRetentionComp" title="同期群分析：N-Day 留存率衰减热力图矩阵 (Cohort Heatmap)" :hasData="hasData.retention" emptyText="暂无留存数据" :wide="true" />
          </el-col>
        </el-row>
      </el-main>
    </el-container>
  </el-container>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { Monitor, DataAnalysis } from '@element-plus/icons-vue'

import HeaderBar from '@/components/HeaderBar.vue'
import MetricCards from '@/components/MetricCards.vue'
import BaseChart from '@/components/BaseChart.vue'

import { useEcharts } from '@/composables/useEcharts'
import { fetchDashboardAll, fetchDateRange, fetchDashboardFallback } from '@/api/dashboard'
import {
  getTrendOption, getFunnelOption, getTop10Option,
  getCategoryOption, getChannelOption, getAgeGroupOption,
  getRfmOption, getRetentionOption,
} from '@/config/chartOptions'

// ── Props ──
const props = defineProps({
  currentRole: { type: String, default: '' },
})
const emit = defineEmits(['logout'])

// ── 响应式状态 ──
const loading = ref(false)
const errorMsg = ref('')
const dateRange = ref([])
const period = ref('day')
const periodLabel = ref('日')
const metrics = ref({})
const dataRangeLimit = ref({ min: null, max: null })

// ── BaseChart 组件引用 ──
const chartTrendComp = ref(null)
const chartFunnelComp = ref(null)
const chartTopComp = ref(null)
const chartCategoryComp = ref(null)
const chartChannelComp = ref(null)
const chartAgeComp = ref(null)
const chartRFMComp = ref(null)
const chartRetentionComp = ref(null)

// 获取 BaseChart 内部的 DOM ref
const getChartEl = (comp) => computed(() => comp.value?.chartEl ?? null)

const chartTrend = getChartEl(chartTrendComp)
const chartFunnel = getChartEl(chartFunnelComp)
const chartTop = getChartEl(chartTopComp)
const chartCategory = getChartEl(chartCategoryComp)
const chartChannel = getChartEl(chartChannelComp)
const chartAge = getChartEl(chartAgeComp)
const chartRFM = getChartEl(chartRFMComp)
const chartRetention = getChartEl(chartRetentionComp)

// ── ECharts 生命周期管理 ──
const { hasData, renderChart, clearAllHasData } = useEcharts([
  chartTrend, chartFunnel, chartTop, chartCategory,
  chartChannel, chartAge, chartRFM, chartRetention,
])

// ── 供父组件调用的登出处理 ──
const handleLogout = () => {
  clearAllHasData()
  emit('logout')
}

// ── 渲染所有图表 ──
const renderAllCharts = (data) => {
  renderChart(chartTrend, 'trend', data.trend || {}, getTrendOption, d => Array.isArray(d.dates) && d.dates.length > 0)
  renderChart(chartFunnel, 'funnel', data.funnel || [], getFunnelOption, d => Array.isArray(d) && d.length > 0)
  renderChart(chartTop, 'top10', data.rankings || {}, getTop10Option, d => Array.isArray(d.items) && d.items.length > 0)

  const dims = data.dimensions || {}
  renderChart(chartCategory, 'category', dims.category, getCategoryOption, d => Array.isArray(d) && d.length > 0)
  renderChart(chartChannel, 'channel', dims.channel, getChannelOption, d => Array.isArray(d) && d.length > 0)
  renderChart(chartAge, 'age_group', dims.age_group, getAgeGroupOption, d => Array.isArray(d) && d.length > 0)

  renderChart(chartRFM, 'rfm', data.rfm || [], getRfmOption, d => Array.isArray(d) && d.length > 0)
  renderChart(chartRetention, 'retention', data.retention || [], getRetentionOption, d => Array.isArray(d) && d.length > 0)
}

// ── 数据获取核心函数 ──
const fetchData = async () => {
  loading.value = true
  errorMsg.value = ''

  const params = {}
  if (dateRange.value?.length === 2) {
    params.start_date = dateRange.value[0]
    params.end_date = dateRange.value[1]
  }

  try {
    // ── 优先使用聚合端点 ──
    const allData = await fetchDashboardAll({ ...params, period: period.value })

    if (allData) {
      // 日期范围配置
      if (allData.date_range) {
        dataRangeLimit.value = allData.date_range
        if (!dateRange.value || dateRange.value.length === 0) {
          const maxDate = new Date(allData.date_range.max.replace(/-/g, '/'))
          const startDate = new Date(maxDate)
          startDate.setDate(startDate.getDate() - 30)
          const fmt = (dt) => {
            const y = dt.getFullYear()
            const m = String(dt.getMonth() + 1).padStart(2, '0')
            const dd = String(dt.getDate()).padStart(2, '0')
            return `${y}-${m}-${dd}`
          }
          dateRange.value = [fmt(startDate), fmt(maxDate)]
        }
      }

      // 核心指标
      const core = allData.core || {}
      metrics.value = {
        total_sales: core.total_sales,
        total_orders: core.total_orders,
        paying_users: core.paying_users,
        avg_order_value: core.avg_order_value,
        qoq_rate: core.qoq_rate,
        yoy_rate: core.yoy_rate,
      }
      periodLabel.value = core.period_label || '日'

      renderAllCharts(allData)
      loading.value = false
      return
    }

    // ── 回退：多端点模式 ──
    console.warn('[fetchData] 聚合端点不可用，回退到多端点模式')

    const dateRangeConfig = await fetchDateRange()
    if (dateRangeConfig) {
      dataRangeLimit.value = dateRangeConfig
    }

    const fallback = await fetchDashboardFallback(params, period.value)

    if (fallback.unauthorized) {
      handleLogout()
      throw new Error('登录已过期或无权限访问')
    }

    const core = fallback.core
    metrics.value = {
      total_sales: core.total_sales,
      total_orders: core.total_orders,
      paying_users: core.paying_users,
      avg_order_value: core.avg_order_value,
      qoq_rate: core.qoq_rate,
      yoy_rate: core.yoy_rate,
    }
    periodLabel.value = core.period_label || '日'

    renderAllCharts(fallback)

    if (fallback.failedCount > 0) {
      errorMsg.value = `部分组件加载失败 (${fallback.failedCount}/${fallback.totalCount})，大屏已降级显示。`
    }

  } catch (error) {
    console.error('全景大屏数据汇聚失败:', error)
    errorMsg.value = `数据加载失败: ${error.message || '网络或接口异常'}`
    clearAllHasData()
  } finally {
    loading.value = false
  }
}

// 暴露 fetchData 供父组件调用
defineExpose({ fetchData })

onMounted(() => {
  fetchData()
})
</script>

<style scoped>
.dashboard-wrapper { height: 100vh; background: #f0f2f5; overflow-y: auto; }
.aside-menu { background: #001529 !important; box-shadow: 2px 0 10px rgba(0,0,0,0.2); }
.logo-area { height: 60px; display: flex; align-items: center; justify-content: center; color: #fff; font-weight: bold; background: #002140; font-size: 16px; }
.main-body { padding: 30px; }
.m-b-20 { margin-bottom: 20px; }
:deep(.el-card) { border-radius: 10px; border: none; }
:deep(.el-card__header) { background: #fafafa; font-weight: bold; font-size: 15px; }
</style>
