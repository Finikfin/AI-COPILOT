from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database.session import get_session
from app.schemas.pipeline_chat_sch import PipelineGenerateRequest, PipelineGenerateResponse
from app.services.pipeline_service import PipelineService


router = APIRouter(tags=["Pipelines"])


@router.post("/generate", response_model=PipelineGenerateResponse)
async def generate_pipeline(
    payload: PipelineGenerateRequest,
    session: AsyncSession = Depends(get_session),
):
    service = PipelineService(session)
    try:
        result = await service.generate(
            dialog_id=payload.dialog_id,
            message=payload.message,
            user_id=payload.user_id,
            capability_ids=payload.capability_ids,
        )
    except Exception as exc:
        if "ollama" in str(exc).lower():
            message_ru = "Не удалось обратиться к локальной модели Ollama. Проверьте OLLAMA_HOST/OLLAMA_MODEL и повторите запрос."
            result = {
                "status": "cannot_build",
                "message_ru": message_ru,
                "chat_reply_ru": message_ru,
                "pipeline_id": None,
                "nodes": [],
                "edges": [],
                "missing_requirements": ["ollama_unavailable"],
                "context_summary": None,
            }
        else:
            raise
    return PipelineGenerateResponse(**result)
