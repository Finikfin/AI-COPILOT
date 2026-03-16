from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database.session import get_session
from app.models import User
from app.schemas.pipeline_chat_sch import DialogResetRequest, DialogResetResponse
from app.services.pipeline_dialog_service import DialogAccessError, PipelineDialogService
from app.services.pipeline_service import PipelineService
from app.utils.token_manager import get_current_user


router = APIRouter(tags=["Pipelines"])


@router.post("/dialog/reset", response_model=DialogResetResponse)
async def reset_pipeline_dialog(
    payload: DialogResetRequest,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    dialog_service = PipelineDialogService(session)
    try:
        await dialog_service.get_dialog(
            dialog_id=payload.dialog_id,
            user_id=current_user.id,
        )
    except DialogAccessError as exc:
        detail = str(exc)
        if "denied" in detail:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=detail) from exc
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=detail) from exc

    service = PipelineService(session)
    result = await service.reset_dialog(payload.dialog_id)
    return DialogResetResponse(**result)
