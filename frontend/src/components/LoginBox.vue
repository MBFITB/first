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
.login-overlay { 
  position: fixed; top: 0; left: 0; width: 100vw; height: 100vh; 
  background: linear-gradient(135deg, #0ea5e9 0%, #3b82f6 100%); 
  display: flex; align-items: center; justify-content: center; z-index: 9999; 
}
.login-box { 
  background: rgba(255, 255, 255, 0.95); 
  padding: 50px 48px; 
  border-radius: 20px; 
  width: 420px; 
  text-align: center; 
  box-shadow: 0 20px 40px rgba(0,0,0,0.15); 
  backdrop-filter: blur(20px); 
}
.login-logo { 
  margin-bottom: 24px; 
  background: #f0f9ff; 
  width: 64px; height: 64px; 
  border-radius: 50%; 
  display: flex; align-items: center; justify-content: center; 
  margin: 0 auto 24px; 
  box-shadow: 0 4px 12px rgba(14, 165, 233, 0.15);
}
.login-box h2 { margin: 0; color: #111827; font-size: 26px; font-weight: 700; letter-spacing: -0.5px; }
.login-box .subtitle { color: #6b7280; font-size: 14px; margin-top: 8px; margin-bottom: 32px; font-weight: 500;}
.login-btn { 
  width: 100%; height: 48px; font-size: 16px; 
  border-radius: 10px; letter-spacing: 1px; font-weight: 600;
  transition: all 0.2s cubic-bezier(0.25, 0.8, 0.25, 1);
}
.login-btn:hover {
  transform: translateY(-2px);
  box-shadow: 0 8px 16px rgba(59, 130, 246, 0.3);
}
.login-btn:active {
  transform: translateY(0) scale(0.98);
}
.login-error { color: #ef4444; font-size: 13px; margin-top: 16px; font-weight: 500; }
.m-b-20 { margin-bottom: 20px; }
:deep(.custom-input .el-input__wrapper) { 
  box-shadow: 0 0 0 1px #e5e7eb inset; 
  border-radius: 10px; 
  padding: 4px 12px;
  background: #f9fafb;
}
:deep(.custom-input.is-focus .el-input__wrapper) { 
  box-shadow: 0 0 0 2px #3b82f6 inset !important; 
  background: #ffffff;
}
</style>
