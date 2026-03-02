"""
Pydantic API 响应模型
统一定义所有的接口响应格式，供给 FastAPI 生成 OpenAPI 文档。
"""

from typing import Generic, TypeVar, Optional, List, Union

from pydantic import BaseModel, Field, RootModel

T = TypeVar("T")


class ApiResponse(BaseModel, Generic[T]):
    """标准统一的 API 响应结构"""
    code: int = Field(default=200, description="业务状态码，200表示成功")
    message: str = Field(default="ok", description="提示信息")
    data: Optional[T] = Field(default=None, description="响应负载数据")


# --- 组件子模型 ---

class CoreMetricsData(BaseModel):
    total_sales: float
    total_orders: int
    paying_users: int
    avg_order_value: float
    qoq_rate: Optional[float] = Field(description="环比增长率（%），无上期基线时为 null")
    yoy_rate: Optional[float] = Field(description="同比增长率（%），无上期基线时为 null")
    period_label: str = Field(description="对应请求中 period 的人类可读标签（如：日/周/月）")


class TrendData(BaseModel):
    dates: List[str]
    sales: List[float]
    orders: List[int]


class NameValueItem(BaseModel):
    """通用的键值对字典组件（适用于漏斗、类别、分布、玫瑰图等）"""
    name: str
    value: Union[int, float]


class RankingsData(BaseModel):
    items: List[str]
    sales: List[float]


class DimensionsData(BaseModel):
    category: List[NameValueItem]
    channel: List[NameValueItem]
    age_group: List[NameValueItem]


class RetentionItem(RootModel):
    """留存热力图单点数据结构，由前端 ECharts 读取：[天数差距, 日期, 留存百分比]"""
    # FastAPI 将 Pydantic List (内含异构) 转为 Array
    root: List[Union[int, str, float]]


class DateRangeConfigData(BaseModel):
    min: str
    max: str


# --- 聚合大宽表 ---

class DashboardAllData(BaseModel):
    date_range: DateRangeConfigData
    core: CoreMetricsData
    trend: TrendData
    funnel: List[NameValueItem]
    rankings: RankingsData
    dimensions: DimensionsData
    rfm: List[NameValueItem]
    retention: List[list]  # 规避 ECharts tuple 问题，直接暴露原生 list
