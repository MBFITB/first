/**
 * ECharts 实例生命周期管理 Composable
 * 封装图表初始化、ResizeObserver、渲染调度和销毁逻辑。
 */

import { reactive, nextTick, onMounted, onUnmounted } from 'vue'
import * as echarts from 'echarts'

// 统一渲染器配置（数据量大用 canvas 更佳）
const RENDERER = 'canvas'

/**
 * @param {Array<import('vue').Ref>} chartRefs - 图表 DOM ref 数组，用于注册 ResizeObserver
 */
export function useEcharts(chartRefs = []) {
    // ECharts 实例注册表
    const instances = {}

    // ResizeObserver 注册表
    let resizeObservers = []

    // 各图表是否有数据的标志（控制 el-empty 显示）
    const hasData = reactive({
        trend: false,
        funnel: false,
        top10: false,
        category: false,
        channel: false,
        age_group: false,
        rfm: false,
        retention: false,
    })

    /**
     * 初始化或复用 ECharts 实例
     */
    const initChart = (elRef, name) => {
        if (!elRef || !elRef.value) return null

        let instance = echarts.getInstanceByDom(elRef.value)
        if (!instance) {
            // 销毁可能游离的旧实例
            if (instances[name] && !instances[name].isDisposed()) {
                instances[name].dispose()
            }
            instance = echarts.init(elRef.value, null, { renderer: RENDERER })
        }
        instances[name] = instance
        return instance
    }

    /**
     * 为指定 DOM 元素注册 ResizeObserver（深度自适应）
     */
    const observeResize = (elRef) => {
        if (!elRef || !elRef.value) return
        try {
            const observer = new ResizeObserver(() => {
                const instance = echarts.getInstanceByDom(elRef.value)
                if (instance && !instance.isDisposed()) {
                    instance.resize()
                }
            })
            observer.observe(elRef.value)
            resizeObservers.push(observer)
        } catch (e) {
            console.warn('[ResizeObserver] 注册失败:', e)
        }
    }

    /**
     * 安全地渲染指定图表
     * - 如果没有数据，标记 hasData 为 false（触发 el-empty 显示）
     * - optionFn 内部做了数据格式校验，此处再包裹 try-catch 提供最终兜底
     */
    const renderChart = (elRef, name, data, optionFn, hasDataCheck) => {
        let dataAvailable = false
        try {
            dataAvailable = hasDataCheck(data)
        } catch (e) {
            console.warn(`[renderChart] ${name} 数据校验异常:`, e)
        }
        hasData[name] = dataAvailable

        if (!dataAvailable) return

        // 需要使用 nextTick 确保 v-show 切换后 DOM 已更新
        nextTick(() => {
            try {
                const chart = initChart(elRef, name)
                if (chart) {
                    const option = optionFn(data)
                    chart.setOption(option, true)
                    chart.resize()
                }
            } catch (err) {
                console.error(`[renderChart] ${name} 渲染失败:`, err)
                hasData[name] = false
            }
        })
    }

    /**
     * 全局 resize 处理
     */
    const handleResize = () => {
        Object.values(instances).forEach((i) => {
            if (i && !i.isDisposed()) i.resize()
        })
    }

    /**
     * 清空所有图表的 hasData 标志
     */
    const clearAllHasData = () => {
        Object.keys(hasData).forEach((k) => (hasData[k] = false))
    }

    // ── 生命周期管理 ──
    onMounted(() => {
        window.addEventListener('resize', handleResize)

        // 为每个图表容器独立注册 ResizeObserver
        nextTick(() => {
            chartRefs.forEach((elRef) => observeResize(elRef))
        })
    })

    onUnmounted(() => {
        window.removeEventListener('resize', handleResize)

        // 断开所有 ResizeObserver
        resizeObservers.forEach((obs) => {
            try { obs.disconnect() } catch (_) { /* 忽略 */ }
        })
        resizeObservers = []

        // 销毁所有 ECharts 实例，释放 GPU/Canvas 内存
        Object.entries(instances).forEach(([name, inst]) => {
            if (inst && !inst.isDisposed()) {
                inst.dispose()
            }
        })
        Object.keys(instances).forEach((k) => delete instances[k])
    })

    return {
        hasData,
        renderChart,
        clearAllHasData,
    }
}
