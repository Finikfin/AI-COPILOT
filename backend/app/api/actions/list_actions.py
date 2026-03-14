from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database.session import get_session
from app.models import Action, HttpMethod
from app.schemas.action_sch import ActionListItemResponse


router = APIRouter(tags=["Actions"])


@router.get("", response_model=list[ActionListItemResponse])
async def list_actions(
    method: HttpMethod | None = Query(default=None),
    source_filename: str | None = Query(default=None),
    search: str | None = Query(default=None, min_length=1),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_session),
):
    query = (
        select(Action)
        .where(Action.is_deleted.is_(False))
        .order_by(Action.created_at.desc())
        .limit(limit)
        .offset(offset)
    )

    if method is not None:
        query = query.where(Action.method == method)

    if source_filename:
        query = query.where(Action.source_filename == source_filename)

    if search:
        search_pattern = f"%{search}%"
        query = query.where(
            or_(
                Action.operation_id.ilike(search_pattern),
                Action.path.ilike(search_pattern),
                Action.summary.ilike(search_pattern),
            )
        )

    result = await session.execute(query)
    return result.scalars().all()
