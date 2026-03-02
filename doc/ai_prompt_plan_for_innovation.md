# 毕设创新点落地指南：AI 执行提示词（Prompt）清单

本文档为你规划了实现“第一梯队”两大创新点（**智能异常检测+自动预警** 和 **自然语言大屏问答**）的实施路径。
你可以直接复制以下的提示词（Prompt）发给 AI（比如通义千问、DeepSeek、ChatGPT 或我就在这里直接执行），AI 就能精准地给出你需要修改和添加的代码。

---

## 创新点一：基于统计学的智能异常检测与预警 (预计工时：1天)

**目标**：在现有的每日销售额/订单趋势数据上，检测异常突增或骤降的点，并在前端 ECharts 上用特殊标记（如红点或警告图标）高亮显示。

### 阶段 1：后端算法实现
**给 AI 的 Prompt：**
> “我的毕设是一个基于 FastAPI 的电商数据看板。目前我已经有一个接口可以返回时间序列趋势数据（包含 dates, sales, orders 三个列表）。
> 我希望在后端增加一个轻量级的基于统计学的「异常检测算法（Anomaly Detection）」。
> 请帮我写一个 Python 函数：
> 1. 输入参数是时间序列的数值列表（如 sales 列表）。
> 2. 使用加权移动平均（WMA）结合 3-Sigma 法则，或者 IQR 法则，检测出异常突增或暴跌的数据点。
> 3. 返回一个布尔值列表（is_anomaly），长度与输入一致。
> 4. 函数要具有鲁棒性，能够处理数据量较少的情况（比如只有7天数据）。
> 5. 给出集成到 FastAPI 服务层（service）的示例思路。”

### 阶段 2：数据结构与 API 改造
**给 AI 的 Prompt：**
> “之前的异常检测函数已经写好。现在，我的前端通过 `/api/dashboard/all` 接口获取趋势数据 `trend: {dates: [...], sales: [...], orders: [...]}`。
> 我希望在这个返回结构中，新增两个数组：`sales_anomalies: [...]` 和 `orders_anomalies: [...]`，里面存放布尔值。
> 请帮我修改 FastAPI 的 Schema 定义（Pydantic models）以及 Service 层的数据组装逻辑代码。
> 要求：保持原有接口兼容性，如果检测为空则全返回 false。”

### 阶段 3：前端 ECharts 视觉呈现
**给 AI 的 Prompt：**
> “我的前端使用的是 Vue3 + ECharts。现在后端的趋势数据返回中包含了 `sales_anomalies` 的布尔数组。
> 在我的趋势折线图（Line Chart）的 ECharts option 配置中：
> 1. 请教我如何利用 ECharts 的 `markPoint` 或者是 `itemStyle` 等特效，把 `sales_anomalies` 为 true 的数据点标记为显著的红色（或者带波纹动画的散点 `effectScatter`）。
> 2. 鼠标悬浮到这个异常点时，Tooltip 中能额外提示一段高亮的文字：‘系统检测到该日数据出现异常波动’。
> 请给出 ECharts option 的核心修改代码。”

---

## 创新点二：基于大语言模型（LLM）的自然语言查询 (预计工时：2~3天)

**目标**：在侧边栏或顶部加一个“AI 智能助理”聊天框，用户输入自然语言（例如：“这周四各个渠道的销售额对比如何？”），系统通过调用免费的 LLM API 生成 SQL，查询 SQLite 并以文字或微型图表形式返回。

### 阶段 1：LLM API 接入与 Prompt 工程
**给 AI 的 Prompt：**
> “我希望在我的 Python (FastAPI) 后端接入一个大语言模型（准备使用 DeepSeek-Chat 或通义千问 API），实现 Text-to-SQL 的功能。
> 我的 SQLite 有一张主要的事实表名为 `buy_fact`，包含字段：date, user_id, order_id, item_id, category_id, price, channel, age_group。
> 请帮我写一段 Python 代码：
> 1. 构建一个针对此表结构的 System Prompt，告诉大模型表结构和业务含义。
> 2. 写一个调用 OpenAI 兼容 API 格式的代码框架（使用 httpx 异步调用）。
> 3. 大模型只需返回纯净的 SQL 语句，如果用户的提问无关此电商数据，则返回特定的错误标记。
> 要求：代码高内聚，封装在一个独立的 `ai_service.py` 文件中。”

### 阶段 2：SQL 安全校验与执行
**给 AI 的 Prompt：**
> “我已经拿到了 LLM 生成的 SQL 语句。为了安全起见，要在 FastAPI 后端用 SQLite 库执行这段 SQL 前做一层保护。
> 请帮我写一个安全的 SQL 执行验证模块：
> 1. 正则或逻辑校验：该 SQL 必须只能是 SELECT 查询语句，严禁出现 DROP, DELETE, UPDATE, INSERT 等修改语句。
> 2. 限制查询返回行数：自动在 SQL 末尾拼装或检查 `LIMIT 50`，防止查出千万级别数据撑爆内存。
> 3. 写出执行查询并把结果转为字典列表（List[dict]）返回的代码，务必使用 SQLite/ClickHouse 的只读模式或者进行适当的错误捕获。”

### 阶段 3：前后端交互联调与 UI 组件
**给 AI 的 Prompt：**
> “后端接口 `/api/ai_chat` 目前设计接收参数 `question: str`，返回 `answer_text: str` 和 `data: List[dict]`。
> 请帮我用 Vue3 和 Element Plus 写一个「AI 数据助手」侧边抽屉组件（el-drawer）。
> 要求：
> 1. 类似 ChatGPT 的对话气泡流样式。用户在底部输入框提问。
> 2. 发送后，会有一个正在思考的打字机或者 loading 动画效果。
> 3. 拿到数据后，如果在 `data` 数组里有结果，请用一个简单的 `el-table` 将数据渲染在对话气泡下面。
> 4. 组件要美观，具有现代化科技感。”

---

## 总结：如何与 AI 高效配合？

1. **按阶段进行，切勿贪多**：每次只发**一个阶段**的 Prompt，拿到代码测试成功编译没报错后，再发下一个。
2. **给出上下文**：如果 AI 给的代码跑不通，直接把你相关的原文件代码全都复制给它说：“这是我现在的 `dashboard_service.py`，请基于它直接给出修改后的完整代码，不要只给片段。”
3. **保持容错**：如果你觉得代码太长改乱了，用 Git 随时 commit 或者备份文件。
