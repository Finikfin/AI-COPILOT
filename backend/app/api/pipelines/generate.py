from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database.session import get_session
from app.schemas.pipeline_chat_sch import PipelineGenerateRequest, PipelineGenerateResponse
from app.services.pipeline_generation import PipelineGenerationService


router = APIRouter(tags=["Pipelines"])


@router.post("/generate", response_model=PipelineGenerateResponse)
async def generate_pipeline(
    payload: PipelineGenerateRequest,
    session: AsyncSession = Depends(get_session),
):
    service = PipelineGenerationService(session)
    result = await service.generate(
        dialog_id=payload.dialog_id,
        message=payload.message,
        user_id=payload.user_id,
        capability_ids=payload.capability_ids,
    )
    return PipelineGenerateResponse(**result)
