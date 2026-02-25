/**
 * ECharts 图表配置函数
 * 纯配置生成，不包含任何 DOM 操作或副作用。
 * 每个函数内置数据格式校验，确保异常数据不会导致渲染崩溃。
 */

/**
 * 趋势图配置（折线 + 柱状混合图）
 */
export const getTrendOption = (trend) => {
    const dates = Array.isArray(trend?.dates) ? trend.dates : []
    const sales = Array.isArray(trend?.sales) ? trend.sales : []
    const orders = Array.isArray(trend?.orders) ? trend.orders : []

    return {
        tooltip: { trigger: 'axis' },
        legend: { data: ['交易额(￥)', '订单数'], bottom: 0 },
        grid: { left: '3%', right: '5%', bottom: '10%', containLabel: true },
        xAxis: { type: 'category', data: dates },
        yAxis: [
            {
                type: 'value',
                name: '交易额 (￥)',
                splitNumber: 5,
                axisLabel: {
                    formatter: (v) => v >= 10000 ? (v / 10000).toFixed(1) + '万' : v,
                },
            },
            {
                type: 'value',
                name: '订单数 (单)',
                splitNumber: 5,
                alignTicks: true,
                splitLine: { show: false },
            },
        ],
        series: [
            {
                name: '交易额(￥)',
                data: sales,
                type: 'line',
                smooth: true,
                areaStyle: { opacity: 0.15 },
                itemStyle: { color: '#409EFF' },
            },
            {
                name: '订单数',
                yAxisIndex: 1,
                data: orders,
                type: 'bar',
                barWidth: '30%',
                itemStyle: { color: '#67C23A', opacity: 0.5 },
            },
        ],
    }
}

/**
 * 转化漏斗图配置
 */
export const getFunnelOption = (funnel) => {
    const data = Array.isArray(funnel)
        ? funnel.filter(item => item && typeof item.name === 'string' && typeof item.value === 'number')
        : []
    return {
        tooltip: { trigger: 'item' },
        series: [{
            type: 'funnel',
            left: '10%',
            top: 20,
            bottom: 20,
            width: '80%',
            data,
            label: { show: true, position: 'inside', formatter: '{b}: {c}人' },
            itemStyle: { borderColor: '#fff', borderWidth: 2 },
        }],
    }
}

/**
 * Top10 商品横向柱状图配置
 */
export const getTop10Option = (top10) => {
    const items = Array.isArray(top10?.items) ? [...top10.items].reverse() : []
    const sales = Array.isArray(top10?.sales) ? [...top10.sales].reverse() : []
    return {
        grid: { left: '2%', right: '15%', bottom: '2%', top: '2%', containLabel: true },
        xAxis: { type: 'value', splitLine: { show: false } },
        yAxis: {
            type: 'category',
            data: items,
            axisTick: { show: false },
        },
        series: [{
            type: 'bar',
            data: sales,
            label: { show: true, position: 'right', formatter: '￥{c}' },
            itemStyle: { color: '#5470c6', borderRadius: [0, 4, 4, 0] },
        }],
    }
}

/**
 * 品类分布饼图配置
 */
export const getCategoryOption = (category) => {
    const data = Array.isArray(category)
        ? category.filter(item => item && item.name !== undefined && item.value !== undefined)
        : []
    return {
        tooltip: { trigger: 'item' },
        series: [{
            type: 'pie',
            radius: ['40%', '70%'],
            minAngle: 15,
            avoidLabelOverlap: true,
            itemStyle: { borderRadius: 10, borderColor: '#fff', borderWidth: 2 },
            data,
            label: {
                show: true,
                formatter: '{b}\n{d}%',
                fontSize: 12,
            },
            labelLine: {
                show: true,
                length: 15,
                length2: 25,
                smooth: true,
            },
            labelLayout: {
                moveOverlap: 'shiftY',
            },
        }],
    }
}

/**
 * 渠道分布饼图配置
 */
export const getChannelOption = (channel) => {
    const data = Array.isArray(channel)
        ? channel.filter(item => item && item.name !== undefined && item.value !== undefined)
        : []
    return {
        tooltip: { trigger: 'item' },
        series: [{
            type: 'pie',
            radius: '65%',
            center: ['50%', '50%'],
            data,
            label: { show: true, formatter: '{b}: {d}%' },
        }],
    }
}

/**
 * 年龄分布饼图配置
 */
export const getAgeGroupOption = (ageGroup) => {
    const data = Array.isArray(ageGroup)
        ? ageGroup.filter(item => item && item.name !== undefined && item.value !== undefined)
        : []
    return {
        tooltip: { trigger: 'item' },
        series: [{
            type: 'pie',
            radius: ['35%', '65%'],
            center: ['50%', '50%'],
            itemStyle: { borderRadius: 8, borderColor: '#fff', borderWidth: 2 },
            data,
            label: { show: true, formatter: '{b}\n{d}%' },
        }],
    }
}

/**
 * RFM 分布饼图配置
 */
export const getRfmOption = (rfm) => {
    const data = Array.isArray(rfm)
        ? rfm.filter(item => item && item.name !== undefined && item.value !== undefined)
        : []
    return {
        tooltip: { trigger: 'item' },
        series: [{
            type: 'pie',
            radius: ['45%', '70%'],
            center: ['50%', '50%'],
            data,
            itemStyle: { borderRadius: 4, borderColor: '#fff', borderWidth: 2 },
            label: { show: true, formatter: '{b}\n{c}人' },
        }],
    }
}

/**
 * 留存热力图配置
 */
export const getRetentionOption = (retention) => {
    if (!Array.isArray(retention) || retention.length === 0) {
        return { series: [] }
    }
    const validData = retention.filter(item => Array.isArray(item) && item.length >= 3)
    if (validData.length === 0) return { series: [] }

    // 按时间排序去重生成 Y 轴
    const yAxisData = Array.from(new Set(validData.map(item => item[1])))
        .sort((a, b) => new Date(a) - new Date(b))
    const xAxisData = ['Day0', 'Day1', 'Day2', 'Day3', 'Day4', 'Day5', 'Day6', 'Day7']

    const heatmapData = validData.map(item => [
        item[0],                        // x: day_diff (0~7)
        yAxisData.indexOf(item[1]),     // y: cohort_date index
        item[2],                        // value: 留存率
    ])

    return {
        tooltip: {
            position: 'top',
            formatter: (p) =>
                `获取批次: ${yAxisData[p.value[1]]}<br/>留存周期: ${xAxisData[p.value[0]]}<br/>留存率: ${p.value[2]}%`,
        },
        grid: { height: '70%', top: '10%', left: '10%', right: '15%' },
        xAxis: { type: 'category', data: xAxisData, splitArea: { show: true } },
        yAxis: { type: 'category', data: yAxisData, splitArea: { show: true } },
        dataZoom: [
            {
                type: 'slider',
                yAxisIndex: 0,
                startValue: Math.max(0, yAxisData.length - 10),
                endValue: yAxisData.length - 1,
                right: '2%',
                width: 15,
            },
            {
                type: 'inside',
                yAxisIndex: 0,
            },
        ],
        visualMap: {
            min: 0,
            max: 100,
            calculable: true,
            orient: 'horizontal',
            left: 'center',
            bottom: '0%',
            inRange: { color: ['#fafafa', '#ffe58f', '#ff4d4f', '#a8071a'] },
        },
        series: [{
            name: '留存衰减',
            type: 'heatmap',
            data: heatmapData,
            label: { show: true, formatter: (p) => p.value[2] + '%' },
            emphasis: { itemStyle: { shadowBlur: 10, shadowColor: 'rgba(0, 0, 0, 0.5)' } },
        }],
    }
}
