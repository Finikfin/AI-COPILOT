from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database.session import get_session
from app.schemas.pipeline_chat_sch import DialogResetRequest, DialogResetResponse
from app.services.pipeline_service import PipelineService


router = APIRouter(tags=["Pipelines"])


@router.post("/dialog/reset", response_model=DialogResetResponse)
async def reset_pipeline_dialog(
    payload: DialogResetRequest,
    session: AsyncSession = Depends(get_session),
):
    service = PipelineService(session)
    result = await service.reset_dialog(payload.dialog_id)
    return DialogResetResponse(**result)
