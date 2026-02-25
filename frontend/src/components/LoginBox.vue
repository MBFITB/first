<template>
  <div class="login-overlay">
    <div class="login-box">
      <div class="login-logo">
        <el-icon :size="28" color="#409eff"><DataAnalysis /></el-icon>
      </div>
      <h2>系统安全登录</h2>
      <p class="subtitle">全景电商分析系统 Authentication</p>
      <el-input v-model="loginForm.username" placeholder="请输入账号 (如 admin)" prefix-icon="User" class="m-b-20 custom-input" size="large" />
      <el-input v-model="loginForm.password" type="password" placeholder="请输入密码 (如 123456)" prefix-icon="Lock" show-password class="m-b-20 custom-input" size="large" @keyup.enter="handleLogin" />
      <el-button type="primary" class="login-btn" size="large" :loading="loginLoading" @click="handleLogin">登录系统</el-button>
      <div v-if="loginError" class="login-error">{{ loginError }}</div>
    </div>
  </div>
</template>

<script setup>
defineProps({
  loginForm: { type: Object, required: true },
  loginLoading: { type: Boolean, default: false },
  loginError: { type: String, default: '' },
})

const emit = defineEmits(['login'])

const handleLogin = () => emit('login')
</script>

<style scoped>
.login-overlay { position: fixed; top: 0; left: 0; width: 100vw; height: 100vh; background: linear-gradient(135deg, #1890ff 0%, #3b5998 100%); display: flex; align-items: center; justify-content: center; z-index: 9999; }
.login-box { background: rgba(255, 255, 255, 0.95); padding: 50px 40px; border-radius: 16px; width: 400px; text-align: center; box-shadow: 0 10px 30px rgba(0,0,0,0.2); backdrop-filter: blur(10px); }
.login-logo { margin-bottom: 20px; background: #e6f7ff; width: 60px; height: 60px; border-radius: 50%; display: flex; align-items: center; justify-content: center; margin: 0 auto 20px; }
.login-box h2 { margin: 0; color: #262626; font-size: 24px; font-weight: 600; }
.login-box .subtitle { color: #8c8c8c; font-size: 14px; margin-top: 8px; margin-bottom: 30px; }
.login-btn { width: 100%; height: 44px; font-size: 16px; border-radius: 8px; letter-spacing: 1px; }
.login-error { color: #f5222d; font-size: 13px; margin-top: 15px; }
.m-b-20 { margin-bottom: 20px; }
:deep(.custom-input .el-input__wrapper) { box-shadow: 0 0 0 1px #d9d9d9 inset; border-radius: 8px; }
:deep(.custom-input.is-focus .el-input__wrapper) { box-shadow: 0 0 0 1px #409EFF inset !important; }
</style>
