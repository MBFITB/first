# -*- coding: utf-8 -*-
"""
AI 自然语言查询服务 (Text-to-SQL + 数据分析)
基于大语言模型将用户自然语言问题转换为 SQL 查询，或直接进行数据分析。

支持 OpenAI 兼容 API（DeepSeek、通义千问、Moonshot 等）。
"""

import re
import json
import sqlite3
import httpx

from core.config import LLM_API_KEY, LLM_API_URL, LLM_MODEL, SQLITE_DB
from core.logging import logger


# ── System Prompt（核心上下文）──

SYSTEM_PROMPT = (
    "你是一个专业的电商数据分析师AI助手，精通SQL和业务数据解读。\n"
    "\n"
    "## 你的数据库表结构（SQLite）\n"
    "\n"
    "### buy_fact（购买事实表）\n"
    "字段：date(TEXT,YYYY-MM-DD), user_id(INT), order_id(TEXT), item_id(INT), "
    "category_id(INT), price(REAL,元), channel(TEXT: App Store/官网/小程序), "
    "age_group(TEXT: 18-24/25-34/35-45/46+/未知)\n"
    "\n"
    "### user_rfm（用户RFM分群表）\n"
    "字段：user_id(INT), rfm_label(TEXT)\n"
    "\n"
    "### user_funnel_mart（转化漏斗表）\n"
    "字段：user_id(INT), date(TEXT), has_pv(INT 0/1), has_cart(INT 0/1), has_buy(INT 0/1)\n"
    "\n"
    "### cohort_matrix（同期群留存矩阵）\n"
    "字段：cohort_date(TEXT), day_diff(INT), active_users(INT), cohort_users(INT)\n"
    "\n"
    "数据范围：2017-11-01 至 2017-12-10。\n"
    "\n"
    "## 行为规则（严格遵守）\n"
    "\n"
    "你有两种回复模式，根据用户意图自动选择：\n"
    "\n"
    "### 模式A：生成SQL\n"
    "当用户想获取具体数据时（如查询、统计、排名等），直接返回纯SQL语句。\n"
    "要求：SELECT only，SQLite方言，带LIMIT 50。\n"
    "不要加任何解释文字，不要用markdown代码块包裹。\n"
    "\n"
    "### 模式B：自然语言分析\n"
    "当用户要求你分析、解读、总结、解释数据含义时（如'分析一下'、'这代表什么'、"
    "'给我洞察'、'什么含义'等），你必须在回复最前面加上 [TEXT_REPLY] 标记，"
    "然后像资深数据分析师一样直接给出专业的业务分析。\n"
    "你的分析必须包含：\n"
    "1. 数据特征总结（引用对话中出现过的具体数字）\n"
    "2. 业务含义解读\n"
    "3. 可能的原因分析\n"
    "4. 可落地的运营建议\n"
    "绝对不要让用户'再去查别的'或'提出更具体的问题'，而是直接基于已有信息深度分析。\n"
    "\n"
    "### 无关问题\n"
    "与电商数据完全无关的问题（如天气、编程语法等）返回：IRRELEVANT_QUESTION\n"
)


# ── SQL 安全校验 ──

_DANGEROUS_KEYWORDS = re.compile(
    r'\b(DROP|DELETE|UPDATE|INSERT|ALTER|CREATE|TRUNCATE|REPLACE|EXEC|EXECUTE|GRANT|REVOKE)\b',
    re.IGNORECASE
)
_SELECT_PATTERN = re.compile(r'^\s*SELECT\b', re.IGNORECASE)
_LIMIT_PATTERN = re.compile(r'\bLIMIT\s+\d+', re.IGNORECASE)


def validate_sql(sql: str) -> str:
    """校验 LLM 生成的 SQL 安全性。返回清洗后的安全 SQL，不合法则抛出 ValueError。"""
    sql = re.sub(r'^```(?:sql)?\s*', '', sql.strip())
    sql = re.sub(r'```\s*$', '', sql.strip())
    sql = sql.strip().rstrip(';')

    if not sql:
        raise ValueError("LLM 返回了空的 SQL 语句")

    if sql.strip() == "IRRELEVANT_QUESTION":
        raise ValueError("IRRELEVANT_QUESTION")

    if sql.startswith("[TEXT_REPLY]"):
        return sql

    if not _SELECT_PATTERN.match(sql):
        raise ValueError(f"SQL 必须以 SELECT 开头，拒绝执行: {sql[:80]}")

    if _DANGEROUS_KEYWORDS.search(sql):
        raise ValueError(f"SQL 包含禁止的危险操作关键字，拒绝执行: {sql[:80]}")

    if not _LIMIT_PATTERN.search(sql):
        sql += " LIMIT 50"

    return sql


# ── LLM API 调用 ──

async def call_llm(question: str, history: list = None) -> str:
    """调用 OpenAI 兼容 API。返回 LLM 原始输出文本。"""
    if not LLM_API_KEY:
        raise ValueError("未配置 LLM_API_KEY，请在环境变量或 config.json 中设置")

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {LLM_API_KEY}",
    }

    messages = [{"role": "system", "content": SYSTEM_PROMPT}]

    if history:
        for msg in history[-10:]:
            if msg.get("role") in ["user", "assistant"]:
                content = msg.get("content", "")
                if msg.get("role") == "assistant" and msg.get("data"):
                    data_preview = msg["data"][:5] if isinstance(msg["data"], list) else []
                    content += "\n[查询结果数据]: " + json.dumps(data_preview, ensure_ascii=False)
                messages.append({"role": msg["role"], "content": content})

    messages.append({"role": "user", "content": question})

    payload = {
        "model": LLM_MODEL,
        "messages": messages,
        "temperature": 0.3,
        "max_tokens": 1024,
    }

    async with httpx.AsyncClient(timeout=120.0) as client:
        logger.info("[AI] 调用 LLM API: model=%s, question=%s", LLM_MODEL, question[:100])
        response = await client.post(LLM_API_URL, headers=headers, json=payload)
        response.raise_for_status()

    result = response.json()
    content = result["choices"][0]["message"]["content"].strip()
    logger.info("[AI] LLM 返回原始内容: %s", content[:200])
    return content


async def call_llm_stream(question: str, history: list = None):
    """流式调用 LLM API，逐 token yield 内容片段。"""
    if not LLM_API_KEY:
        raise ValueError("未配置 LLM_API_KEY")

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {LLM_API_KEY}",
    }

    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    if history:
        for msg in history[-10:]:
            if msg.get("role") in ["user", "assistant"]:
                content = msg.get("content", "")
                if msg.get("role") == "assistant" and msg.get("data"):
                    data_preview = msg["data"][:5] if isinstance(msg["data"], list) else []
                    content += "\n[查询结果数据]: " + json.dumps(data_preview, ensure_ascii=False)
                messages.append({"role": msg["role"], "content": content})
    messages.append({"role": "user", "content": question})

    payload = {
        "model": LLM_MODEL,
        "messages": messages,
        "temperature": 0.3,
        "max_tokens": 1024,
        "stream": True,
    }

    async with httpx.AsyncClient(timeout=120.0) as client:
        logger.info("[AI-Stream] 开始流式调用: %s", question[:80])
        async with client.stream("POST", LLM_API_URL, headers=headers, json=payload) as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                if not line.startswith("data: "):
                    continue
                data_str = line[6:]
                if data_str.strip() == "[DONE]":
                    break
                try:
                    chunk = json.loads(data_str)
                    delta = chunk.get("choices", [{}])[0].get("delta", {})
                    token = delta.get("content", "")
                    if token:
                        yield token
                except json.JSONDecodeError:
                    continue


# ── SQL 执行 ──

def execute_safe_sql(sql: str) -> list[dict]:
    """在 SQLite 上以只读方式执行经过校验的 SQL，返回字典列表。"""
    conn = sqlite3.connect(f"file:{SQLITE_DB}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


# ── 对外暴露的主函数 ──

async def ai_query(question: str, history: list = None) -> dict:
    """自然语言查询主入口，支持多轮对话。"""
    result = {
        "question": question,
        "sql": None,
        "data": [],
        "answer": "",
        "error": None,
    }

    try:
        raw_output = await call_llm(question, history)
        validated = validate_sql(raw_output)

        if validated.startswith("[TEXT_REPLY]"):
            result["answer"] = validated.replace("[TEXT_REPLY]", "").strip()
            logger.info("[AI] 纯文本分析回复成功")
        else:
            result["sql"] = validated
            data = execute_safe_sql(validated)
            result["data"] = data

            if len(data) == 0:
                result["answer"] = "查询完成，未找到符合条件的数据。"
            elif len(data) == 1 and len(data[0]) == 1:
                key = list(data[0].keys())[0]
                val = data[0][key]
                result["answer"] = f"查询结果：{key} = {val}"
            else:
                result["answer"] = f"查询完成，共返回 {len(data)} 条记录。"

            logger.info("[AI] 查询成功: %d 条结果, SQL: %s", len(data), validated[:100])

    except ValueError as e:
        error_msg = str(e)
        if error_msg == "IRRELEVANT_QUESTION":
            result["error"] = None
            result["answer"] = "抱歉，您的问题似乎与电商数据无关。请尝试询问关于销售额、订单、用户、品类等方面的问题。"
        else:
            result["error"] = error_msg
            result["answer"] = f"SQL 校验失败：{error_msg}"
            logger.warning("[AI] SQL 校验失败: %s", error_msg)

    except httpx.HTTPStatusError as e:
        result["error"] = f"LLM API 调用失败 (HTTP {e.response.status_code})"
        result["answer"] = "AI 服务暂时不可用，请稍后再试。"
        logger.error("[AI] LLM API HTTP 错误: %s", str(e))

    except Exception as e:
        result["error"] = str(e)
        result["answer"] = f"查询执行出错：{str(e)}"
        logger.error("[AI] 未预期的错误: %s", str(e), exc_info=True)

    return result
