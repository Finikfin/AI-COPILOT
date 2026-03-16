from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database.session import get_session
from app.models import ExecutionRun, Pipeline, User, UserRole
from app.schemas.execution_sch import ExecutionRunListItemResponse
from app.utils.token_manager import get_current_user


router = APIRouter(tags=["Executions"])


@router.get("/", response_model=list[ExecutionRunListItemResponse])
async def list_executions(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    query = select(ExecutionRun).order_by(ExecutionRun.created_at.desc())

    if current_user.role != UserRole.ADMIN:
        query = query.join(Pipeline, Pipeline.id == ExecutionRun.pipeline_id).where(
            or_(
                ExecutionRun.initiated_by == current_user.id,
                and_(
                    ExecutionRun.initiated_by.is_(None),
                    Pipeline.created_by == current_user.id,
                ),
            )
        )

    query = query.limit(limit).offset(offset)
    result = await session.execute(query)
    return list(result.scalars().all())
