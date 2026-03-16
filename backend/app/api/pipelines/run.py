from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database.session import get_session
from app.models import Pipeline, User, UserRole
from app.schemas.execution_sch import RunPipelineRequest, RunPipelineResponse
from app.services.execution_service import ExecutionService, ExecutionServiceError
from app.utils.token_manager import get_current_user


router = APIRouter(tags=["Pipelines"])


@router.post("/{pipeline_id}/run", response_model=RunPipelineResponse, status_code=status.HTTP_202_ACCEPTED)
async def run_pipeline(
    pipeline_id: UUID,
    payload: RunPipelineRequest,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    pipeline = await session.get(Pipeline, pipeline_id)
    if pipeline is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")

    if current_user.role != UserRole.ADMIN and pipeline.created_by != current_user.id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")

    service = ExecutionService(session)
    try:
        run = await service.create_run(
            pipeline_id=pipeline_id,
            inputs=payload.inputs,
            initiated_by=current_user.id,
        )
    except ExecutionServiceError as exc:
        message = str(exc)
        if "not found" in message.lower():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=message) from exc
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=message) from exc

    ExecutionService.start_background_execution(run.id)

    return RunPipelineResponse(
        run_id=run.id,
        pipeline_id=run.pipeline_id,
        status=run.status.value,
    )
