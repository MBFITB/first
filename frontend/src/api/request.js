/**
 * Axios 实例配置
 * 统一管理 baseURL、请求头注入和 401 拦截器。
 */

import axios from 'axios'
import { ElMessage } from 'element-plus'

// 创建 axios 实例
const request = axios.create({
    baseURL: import.meta.env.VITE_API_BASE_URL || 'http://127.0.0.1:8000',
    timeout: 30000,
})

// 401 登出回调（由 useAuth 注入，避免循环依赖）
let onUnauthorized = null

/**
 * 注册 401 未授权回调函数
 * @param {Function} callback - 当收到 401 响应时执行的回调
 */
export function setUnauthorizedHandler(callback) {
    onUnauthorized = callback
}

// ── 请求拦截器：自动注入 Token ──
request.interceptors.request.use(
    (config) => {
        const token = localStorage.getItem('token')
        if (token) {
            config.headers.Authorization = `Bearer ${token}`
        }
        return config
    },
    (error) => Promise.reject(error)
)

// ── 响应拦截器：统一 401 处理 ──
request.interceptors.response.use(
    (response) => response,
    (error) => {
        if (error.response?.status === 401) {
            if (onUnauthorized) {
                onUnauthorized()
            }
            ElMessage.error('登录已过期，请重新登录')
        }
        return Promise.reject(error)
    }
)

export default request
