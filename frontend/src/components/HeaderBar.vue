<template>
  <el-header class="main-header">
    <div class="header-left">
      <span class="title">观测视窗 (动态OLAP)</span>
      <el-date-picker
        v-model="dateRangeModel"
        type="daterange"
        range-separator="至"
        start-placeholder="起始日期"
        end-placeholder="截止日期"
        value-format="YYYY-MM-DD"
        :disabled-date="disabledDate"
        :default-value="datePickerDefault"
        @change="$emit('change')"
        size="small"
        style="margin-left: 20px; width: 260px;"
      />
      <el-radio-group v-model="periodModel" size="small" style="margin-left: 15px;" @change="$emit('change')">
        <el-radio-button label="day">日视图</el-radio-button>
        <el-radio-button label="week">周视图</el-radio-button>
        <el-radio-button label="month">月视图</el-radio-button>
      </el-radio-group>
    </div>
    <div class="header-right" style="display: flex; align-items: center;">
      <el-tag type="info" effect="plain" class="engine-tag" style="margin-right: 20px;">Spark + ClickHouse OLAP 分布式架构</el-tag>
      <el-dropdown @command="handleCommand">
        <span class="el-dropdown-link" style="cursor: pointer; display: flex; align-items: center; color: #606266; font-weight: 500;">
          <el-avatar :size="32" icon="UserFilled" style="margin-right: 8px; background: #c0c4cc;" />
          {{ currentRole === 'admin' ? '系统管理员' : '访客' }}
          <el-icon class="el-icon--right"><ArrowDown /></el-icon>
        </span>
        <template #dropdown>
          <el-dropdown-menu>
            <el-dropdown-item command="logout">退出登录</el-dropdown-item>
          </el-dropdown-menu>
        </template>
      </el-dropdown>
    </div>
  </el-header>
</template>

<script setup>
import { computed } from 'vue'

const props = defineProps({
  dateRange: { type: Array, default: () => [] },
  period: { type: String, default: 'day' },
  currentRole: { type: String, default: '' },
  dataRangeLimit: { type: Object, default: () => ({ min: null, max: null }) },
})

const emit = defineEmits(['update:dateRange', 'update:period', 'change', 'logout'])

// v-model 代理
const dateRangeModel = computed({
  get: () => props.dateRange,
  set: (val) => emit('update:dateRange', val),
})

const periodModel = computed({
  get: () => props.period,
  set: (val) => emit('update:period', val),
})

/**
 * 判断日期是否禁用（超出数据范围）
 */
const disabledDate = (time) => {
  if (!props.dataRangeLimit.min || !props.dataRangeLimit.max) return false

  const target = new Date(time)
  target.setHours(0, 0, 0, 0)

  const start = new Date(props.dataRangeLimit.min.replace(/-/g, '/'))
  start.setHours(0, 0, 0, 0)

  const end = new Date(props.dataRangeLimit.max.replace(/-/g, '/'))
  end.setHours(0, 0, 0, 0)

  return target.getTime() < start.getTime() || target.getTime() > end.getTime()
}

/**
 * 日期选择器面板默认显示月份
 */
const datePickerDefault = computed(() => {
  if (props.dataRangeLimit.max) {
    const end = new Date(props.dataRangeLimit.max.replace(/-/g, '/'))
    const start = new Date(end)
    start.setDate(start.getDate() - 30)
    return [start, end]
  }
  return [new Date(), new Date()]
})

const handleCommand = (command) => {
  if (command === 'logout') emit('logout')
}
</script>

<style scoped>
.main-header { 
  background: rgba(255, 255, 255, 0.85); 
  backdrop-filter: blur(12px);
  -webkit-backdrop-filter: blur(12px);
  display: flex; 
  align-items: center; 
  justify-content: space-between; 
  padding: 0 30px; 
  border-bottom: none;
  box-shadow: 0 1px 4px rgba(0, 0, 0, 0.05);
  height: 64px !important; 
  position: sticky;
  top: 0;
  z-index: 100;
}
.header-left { display: flex; align-items: center; }
.title { font-size: 18px; font-weight: 700; color: #111827; letter-spacing: -0.5px; }
.engine-tag { font-family: 'Inter', monospace; font-weight: 600; font-size: 12px; }
</style>
