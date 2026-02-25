<template>
  <!-- 登录拦截遮罩层 -->
  <LoginBox
    v-if="!isLoggedIn"
    :loginForm="loginForm"
    :loginLoading="loginLoading"
    :loginError="loginError"
    @login="handleLogin"
  />

  <!-- 已登录：看板主页 -->
  <Dashboard
    v-else
    ref="dashboardRef"
    :currentRole="currentRole"
    @logout="handleLogout"
  />
</template>

<script setup>
import { ref } from 'vue'
import LoginBox from '@/components/LoginBox.vue'
import Dashboard from '@/views/Dashboard.vue'
import { useAuth } from '@/composables/useAuth'

const dashboardRef = ref(null)

const {
  isLoggedIn,
  currentRole,
  loginForm,
  loginLoading,
  loginError,
  handleLogin,
  handleLogout,
} = useAuth({
  onLoginSuccess: () => {
    // 登录成功后，Dashboard 组件通过 onMounted 自动拉取数据
  },
  onLogout: () => {
    // 登出后 Dashboard 会被 v-if 卸载，无需额外清理
  },
})
</script>

<style>
/* 隐藏日历面板中非当前月（上/下个月）的灰色填补占位日期（全局样式作用于 Teleport 弹窗） */
.el-date-table td.prev-month,
.el-date-table td.next-month {
  visibility: hidden !important;
  pointer-events: none !important;
}
</style>