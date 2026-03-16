from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database.session import get_session
from app.models import User
from app.schemas.pipeline_chat_sch import (
    PipelineDialogHistoryResponse,
    PipelineDialogMessageResponse,
)
from app.services.pipeline_dialog_service import DialogAccessError, PipelineDialogService
from app.utils.business_logger import log_business_event
from app.utils.token_manager import get_current_user


router = APIRouter(tags=["Pipelines"])


@router.get("/dialogs/{dialog_id}/history", response_model=PipelineDialogHistoryResponse)
async def get_pipeline_dialog_history(
    dialog_id: UUID,
    request: Request,
    limit: int = Query(default=30, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    trace_id = getattr(request.state, "traceId", None)
    dialog_service = PipelineDialogService(session)
    try:
        dialog, messages = await dialog_service.get_history(
            dialog_id=dialog_id,
            user_id=current_user.id,
            limit=limit,
            offset=offset,
        )
    except DialogAccessError as exc:
        detail = str(exc)
        log_business_event(
            "pipeline_dialog_history_rejected",
            trace_id=trace_id,
            user_id=str(current_user.id),
            dialog_id=str(dialog_id),
            reason=detail,
        )
        if "denied" in detail:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=detail) from exc
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=detail) from exc

    response = PipelineDialogHistoryResponse(
        dialog_id=dialog.id,
        title=dialog.title,
        messages=[
            PipelineDialogMessageResponse(
                id=message.id,
                role=message.role.value,
                content=message.content,
                assistant_payload=message.assistant_payload,
                created_at=message.created_at,
            )
            for message in messages
        ],
    )

    log_business_event(
        "pipeline_dialog_history_viewed",
        trace_id=trace_id,
        user_id=str(current_user.id),
        dialog_id=str(dialog.id),
        limit=limit,
        offset=offset,
        message_count=len(response.messages),
    )

    return response
