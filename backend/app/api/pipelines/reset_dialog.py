from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database.session import get_session
from app.models import User
from app.schemas.pipeline_chat_sch import DialogResetRequest, DialogResetResponse
from app.services.pipeline_dialog_service import DialogAccessError, PipelineDialogService
from app.services.pipeline_service import PipelineService
from app.utils.business_logger import log_business_event
from app.utils.token_manager import get_current_user


router = APIRouter(tags=["Pipelines"])


@router.post("/dialog/reset", response_model=DialogResetResponse)
async def reset_pipeline_dialog(
    payload: DialogResetRequest,
    request: Request,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    trace_id = getattr(request.state, "traceId", None)
    dialog_service = PipelineDialogService(session)
    try:
        await dialog_service.get_dialog(
            dialog_id=payload.dialog_id,
            user_id=current_user.id,
        )
    except DialogAccessError as exc:
        detail = str(exc)
        log_business_event(
            "pipeline_dialog_reset_rejected",
            trace_id=trace_id,
            user_id=str(current_user.id),
            dialog_id=str(payload.dialog_id),
            reason=detail,
        )
        if "denied" in detail:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=detail) from exc
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=detail) from exc

    service = PipelineService(session)
    result = await service.reset_dialog(payload.dialog_id)
    log_business_event(
        "pipeline_dialog_reset",
        trace_id=trace_id,
        user_id=str(current_user.id),
        dialog_id=str(payload.dialog_id),
        result_status=result.get("status") if isinstance(result, dict) else None,
    )
    return DialogResetResponse(**result)
