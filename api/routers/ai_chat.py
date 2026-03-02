"""
AI 智能助手路由
POST /api/ai/chat — 用户自然语言提问，返回 SQL 查询结果。
POST /api/ai/stream — 流式输出版本（SSE），逐字返回 LLM 回复。
"""

import json
from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from typing import Optional

from services.ai_service import ai_query, call_llm_stream, validate_sql, execute_safe_sql
from core.logging import logger

router = APIRouter(prefix="/api/ai", tags=["AI 智能助手"])


class AiChatRequest(BaseModel):
    """AI 问答请求"""
    question: str = Field(..., min_length=1, max_length=500, description="用户的自然语言问题")
    history: Optional[list] = Field(default=[], description="包含 role 和 content 的历史消息列表")


class AiChatResponse(BaseModel):
    """AI 问答响应"""
    code: int = 200
    data: Optional[dict] = None
    message: str = "ok"


@router.post("/chat", response_model=AiChatResponse)
async def ai_chat(request: AiChatRequest):
    """非流式 AI 数据问答接口"""
    result = await ai_query(request.question, request.history)

    if result.get("error"):
        return AiChatResponse(code=500, data=result, message=result["error"])

    return AiChatResponse(code=200, data=result, message="ok")


@router.post("/stream")
async def ai_chat_stream(request: AiChatRequest):
    """
    流式 AI 数据问答接口（SSE）

    前端通过 fetch + ReadableStream 消费，实现逐字打字机效果。
    SSE 事件格式：
      - data: {"type":"token","content":"..."} — LLM 输出的每个 token
      - data: {"type":"sql","sql":"...","data":[...]} — SQL 查询完成后的结构化数据
      - data: {"type":"done"} — 流结束
      - data: {"type":"error","message":"..."} — 报错
    """
    async def event_generator():
        full_text = ""
        try:
            async for token in call_llm_stream(request.question, request.history):
                full_text += token
                yield f"data: {json.dumps({'type': 'token', 'content': token}, ensure_ascii=False)}\n\n"

            # 流结束后判断是否为 SQL 查询
            try:
                validated = validate_sql(full_text)
                if validated.startswith("[TEXT_REPLY]"):
                    # 纯文本分析，token 已经逐字发完了，只需发 done
                    pass
                else:
                    # 是 SQL，执行查询并返回结构化数据
                    data = execute_safe_sql(validated)
                    yield f"data: {json.dumps({'type': 'sql', 'sql': validated, 'data': data}, ensure_ascii=False)}\n\n"
            except ValueError as e:
                err_msg = str(e)
                if err_msg == "IRRELEVANT_QUESTION":
                    yield f"data: {json.dumps({'type': 'error', 'message': '抱歉，您的问题似乎与电商数据无关。'}, ensure_ascii=False)}\n\n"
                else:
                    yield f"data: {json.dumps({'type': 'error', 'message': err_msg}, ensure_ascii=False)}\n\n"

        except Exception as e:
            logger.error("[AI-Stream] 流式输出异常: %s", str(e), exc_info=True)
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)}, ensure_ascii=False)}\n\n"

        yield f"data: {json.dumps({'type': 'done'})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        }
    )
