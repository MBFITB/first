/**
 * 登录状态管理 Composable
 * 封装认证相关的响应式状态和方法。
 */

import { ref, reactive } from 'vue'
import request from '@/api/request'
import { setUnauthorizedHandler } from '@/api/request'

// ── 单例状态（跨组件共享） ──
const isLoggedIn = ref(!!localStorage.getItem('token'))
const currentRole = ref(localStorage.getItem('role') || '')
const loginForm = reactive({ username: '', password: '' })
const loginLoading = ref(false)
const loginError = ref('')

/**
 * 登录状态管理
 * @param {Object} options
 * @param {Function} options.onLoginSuccess - 登录成功回调
 * @param {Function} options.onLogout - 登出回调（清理看板数据等）
 */
export function useAuth({ onLoginSuccess, onLogout } = {}) {
    const handleLogin = async () => {
        if (!loginForm.username || !loginForm.password) {
            loginError.value = '账号和密码不能为空'
            return
        }
        loginLoading.value = true
        loginError.value = ''
        try {
            const res = await request.post('/api/auth/login', loginForm)
            if (res.data.code === 200) {
                const { token, role } = res.data.data
                localStorage.setItem('token', token)
                localStorage.setItem('role', role)
                isLoggedIn.value = true
                currentRole.value = role
                onLoginSuccess?.()
            } else {
                loginError.value = res.data.message || '登录失败'
            }
        } catch (err) {
            if (err.response?.data?.message) {
                loginError.value = err.response.data.message
            } else {
                loginError.value = '网络异常，请尝试重新刷新页面或联系管理员'
            }
        } finally {
            loginLoading.value = false
        }
    }

    const handleLogout = () => {
        localStorage.removeItem('token')
        localStorage.removeItem('role')
        isLoggedIn.value = false
        currentRole.value = ''
        loginForm.password = ''
        onLogout?.()
    }

    // 注册 401 自动登出回调
    setUnauthorizedHandler(handleLogout)

    return {
        isLoggedIn,
        currentRole,
        loginForm,
        loginLoading,
        loginError,
        handleLogin,
        handleLogout,
    }
}
