<template>
  <el-card shadow="never" :header="title">
    <el-empty v-if="!hasData" :description="emptyText" />
    <div v-show="hasData" ref="chartEl" :class="wide ? 'chart-box-wide' : 'chart-box'"></div>
  </el-card>
</template>

<script setup>
import { ref } from 'vue'

const props = defineProps({
  title: { type: String, default: '' },
  hasData: { type: Boolean, default: false },
  emptyText: { type: String, default: '暂无数据' },
  wide: { type: Boolean, default: false },
})

const chartEl = ref(null)

// 暴露 DOM ref 给父组件使用
defineExpose({ chartEl })
</script>

<style scoped>
.chart-box { height: 300px; width: 100%; }
.chart-box-wide { height: 350px; width: 100%; }
:deep(.el-card) { border-radius: 10px; border: none; }
:deep(.el-card__header) { background: #fafafa; font-weight: bold; font-size: 15px; }
</style>
