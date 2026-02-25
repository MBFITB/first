/**
 * 看板 API 封装
 * 提供聚合端点和多端点回退两种数据获取方式。
 */

import request from './request'

/**
 * 获取全看板聚合数据（优先使用 /api/dashboard/all）
 * @param {Object} params - { start_date?, end_date?, period }
 * @returns {Promise<Object|null>} 成功返回 data 对象，失败返回 null
 */
export async function fetchDashboardAll(params) {
    try {
        const res = await request.get('/api/dashboard/all', { params })
        if (res.data?.code === 200) {
            return res.data.data
        }
    } catch (e) {
        console.warn('[API] 聚合端点请求失败:', e.message)
    }
    return null
}

/**
 * 获取日期范围配置
 */
export async function fetchDateRange() {
    try {
        const res = await request.get('/api/config/date_range')
        if (res.data?.code === 200) {
            return res.data.data
        }
    } catch (e) {
        console.warn('[API] 日期范围获取失败:', e.message)
    }
    return null
}

/**
 * 多端点回退模式：并行请求所有单独端点
 * @param {Object} params - { start_date?, end_date? }
 * @param {string} period - 'day' | 'week' | 'month'
 * @returns {Promise<Object>} { results, failedCount, unauthorized }
 */
export async function fetchDashboardFallback(params, period) {
    const coreParams = { ...params, period }
    const noPeriodParams = { ...params }

    const endpoints = [
        request.get('/api/metrics/core', { params: coreParams }),
        request.get('/api/charts/trend', { params: coreParams }),
        request.get('/api/charts/funnel', { params: noPeriodParams }),
        request.get('/api/charts/rankings', { params: noPeriodParams }),
        request.get('/api/charts/dimensions', { params: noPeriodParams }),
        request.get('/api/charts/rfm', { params: noPeriodParams }),
        request.get('/api/charts/retention', { params: noPeriodParams }),
    ]

    const results = await Promise.allSettled(endpoints)

    // 检测是否有 401 未授权
    const unauthorized = results.find(
        (r) => r.status === 'rejected' && r.reason?.response?.status === 401
    )

    // 安全提取数据
    const safeData = (index, fallback = {}) => {
        const res = results[index]
        if (res.status === 'fulfilled' && res.value.data.code === 200) {
            return res.value.data.data || fallback
        }
        return fallback
    }

    const failedCount = results.filter(
        (r) => r.status === 'rejected' || (r.status === 'fulfilled' && r.value.data.code !== 200)
    ).length

    return {
        core: safeData(0),
        trend: safeData(1),
        funnel: safeData(2, []),
        rankings: safeData(3),
        dimensions: safeData(4),
        rfm: safeData(5, []),
        retention: safeData(6, []),
        failedCount,
        totalCount: endpoints.length,
        unauthorized: !!unauthorized,
    }
}
