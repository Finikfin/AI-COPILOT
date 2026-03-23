from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database.session import get_session
from app.models import User
from app.schemas.pipeline_chat_sch import PipelineGenerateRequest, PipelineGenerateResponse
from app.services.pipeline_dialog_service import DialogAccessError, PipelineDialogService
from app.services.pipeline_service import PipelineService
from app.services.dialog_memory import DialogMemoryService
from app.utils.business_logger import log_business_event
from app.utils.token_manager import get_current_user


router = APIRouter(tags=["Pipelines"])


@router.post("/generate", response_model=PipelineGenerateResponse)
async def generate_pipeline(
    payload: PipelineGenerateRequest,
    request: Request,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    trace_id = getattr(request.state, "traceId", None)
    log_business_event(
        "pipeline_prompt_received",
        trace_id=trace_id,
        user_id=str(current_user.id),
        dialog_id=str(payload.dialog_id),
        message_len=len(payload.message),
        capability_ids_count=len(payload.capability_ids or []),
    )

    service = PipelineService(session)
    dialog_service = PipelineDialogService(session)
    dialog_memory = DialogMemoryService()
    
    try:
        await dialog_service.append_user_message(
            dialog_id=payload.dialog_id,
            user_id=current_user.id,
            content=payload.message,
        )
        dialog = await dialog_service.get_dialog(
            dialog_id=payload.dialog_id,
            user_id=current_user.id,
        )
    except DialogAccessError as exc:
        detail = str(exc)
        log_business_event(
            "pipeline_prompt_rejected",
            trace_id=trace_id,
            user_id=str(current_user.id),
            dialog_id=str(payload.dialog_id),
            reason=detail,
        )
        if "denied" in detail:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=detail) from exc
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=detail) from exc

    try:
        requested_capability_ids: list[UUID] | None = payload.capability_ids
        if requested_capability_ids is None:
            bound_capability_ids = await dialog_memory.get_bound_capability_ids(str(payload.dialog_id))
            if not bound_capability_ids:
                response_payload = PipelineGenerateResponse(
                    status="needs_input",
                    message_ru="Для этого чата нет импортированных OpenAPI capabilities.",
                    chat_reply_ru=(
                        "Сначала импортируйте OpenAPI в этом чате. "
                        "Capabilities из других чатов не используются."
                    ),
                    pipeline_id=None,
                    nodes=[],
                    edges=[],
                    missing_requirements=["dialog:capabilities_not_imported"],
                    context_summary=None,
                )
                await dialog_service.append_assistant_message(
                    dialog_id=payload.dialog_id,
                    user_id=current_user.id,
                    content=response_payload.chat_reply_ru,
                    assistant_payload=response_payload.model_dump(mode="json", exclude_none=True),
                )
                return response_payload
            requested_capability_ids = [UUID(item) for item in bound_capability_ids]

        # Only reuse previous pipeline if dialog has multiple messages (iterative refinement)
        # For fresh/single-message dialogs, always generate new pipeline
        previous_pipeline_id = None
        if dialog.messages and len(dialog.messages) > 1:
            # -1 because we just added a new message
            previous_pipeline_id = dialog.last_pipeline_id
        else:
            # First message in dialog - clean any stale memory from Redis
            await dialog_memory.reset(str(payload.dialog_id))
        result = await service.generate(
            dialog_id=payload.dialog_id,
            message=payload.message,
            user_id=current_user.id,
            capability_ids=requested_capability_ids,
            previous_pipeline_id=previous_pipeline_id,
        )
    except Exception as exc:
        if any(token in str(exc).lower() for token in ("llm provider", "openai", "yandex", "model")):
            message_ru = (
                "Не удалось обратиться к LLM-провайдеру. "
                "Проверьте настройки Yandex API (YANDEX_API_KEY/YANDEX_FOLDER_ID) "
                "или OpenAI API (LLM_BASE_URL/LLM_MODEL/LLM_API_KEY) и повторите запрос."
            )
            result = {
                "status": "cannot_build",
                "message_ru": message_ru,
                "chat_reply_ru": message_ru,
                "pipeline_id": None,
                "nodes": [],
                "edges": [],
                "missing_requirements": ["llm_unavailable"],
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
            assistant_payload=response_payload.model_dump(mode="json", exclude_none=True),
        )
    except DialogAccessError as exc:
        detail = str(exc)
        log_business_event(
            "pipeline_prompt_rejected",
            trace_id=trace_id,
            user_id=str(current_user.id),
            dialog_id=str(payload.dialog_id),
            reason=detail,
        )
        if "denied" in detail:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=detail) from exc
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=detail) from exc

    log_business_event(
        "pipeline_prompt_processed",
        trace_id=trace_id,
        user_id=str(current_user.id),
        dialog_id=str(payload.dialog_id),
        result_status=response_payload.status,
        pipeline_id=str(response_payload.pipeline_id) if response_payload.pipeline_id else None,
    )

    return response_payload
