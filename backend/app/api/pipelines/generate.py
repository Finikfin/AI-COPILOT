from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database.session import get_session
from app.models import User
from app.schemas.pipeline_chat_sch import PipelineGenerateRequest, PipelineGenerateResponse
from app.services.pipeline_dialog_service import DialogAccessError, PipelineDialogService
from app.services.pipeline_service import PipelineService
from app.utils.token_manager import get_current_user


router = APIRouter(tags=["Pipelines"])


@router.post("/generate", response_model=PipelineGenerateResponse)
async def generate_pipeline(
    payload: PipelineGenerateRequest,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    service = PipelineService(session)
    dialog_service = PipelineDialogService(session)
    try:
        await dialog_service.append_user_message(
            dialog_id=payload.dialog_id,
            user_id=current_user.id,
            content=payload.message,
        )
    except DialogAccessError as exc:
        detail = str(exc)
        if "denied" in detail:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=detail) from exc
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=detail) from exc

    try:
        result = await service.generate(
            dialog_id=payload.dialog_id,
            message=payload.message,
            user_id=current_user.id,
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

    response_payload = PipelineGenerateResponse(**result)
    try:
        await dialog_service.append_assistant_message(
            dialog_id=payload.dialog_id,
            user_id=current_user.id,
            content=response_payload.chat_reply_ru or response_payload.message_ru,
            assistant_payload=response_payload.model_dump(mode="json"),
        )
    except DialogAccessError as exc:
        detail = str(exc)
        if "denied" in detail:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=detail) from exc
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=detail) from exc

    return response_payload
