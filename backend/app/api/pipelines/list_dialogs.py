from __future__ import annotations

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database.session import get_session
from app.models import User
from app.schemas.pipeline_chat_sch import PipelineDialogListItemResponse
from app.services.pipeline_dialog_service import PipelineDialogService
from app.utils.business_logger import log_business_event
from app.utils.token_manager import get_current_user


router = APIRouter(tags=["Pipelines"])


@router.get("/dialogs", response_model=list[PipelineDialogListItemResponse])
async def list_pipeline_dialogs(
    request: Request,
    limit: int = Query(default=20, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    trace_id = getattr(request.state, "traceId", None)
    dialog_service = PipelineDialogService(session)
    dialogs = await dialog_service.list_dialogs(
        user_id=current_user.id,
        limit=limit,
        offset=offset,
    )
    response = [
        PipelineDialogListItemResponse(
            dialog_id=dialog.id,
            title=dialog.title,
            last_status=dialog.last_status,
            last_pipeline_id=dialog.last_pipeline_id,
            last_message_preview=dialog.last_message_preview,
            created_at=dialog.created_at,
            updated_at=dialog.updated_at,
        )
        for dialog in dialogs
    ]
    log_business_event(
        "pipeline_dialogs_listed",
        trace_id=trace_id,
        user_id=str(current_user.id),
        limit=limit,
        offset=offset,
        result_count=len(response),
    )
    return response
