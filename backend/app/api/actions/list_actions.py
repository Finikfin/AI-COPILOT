from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database.session import get_session
from app.models import Action, ActionIngestStatus, HttpMethod, User, UserRole
from app.schemas.action_sch import ActionListItemResponse
from app.utils.business_logger import log_business_event
from app.utils.token_manager import get_current_user


router = APIRouter(tags=["Actions"])


@router.get("/", response_model=list[ActionListItemResponse])
async def list_actions(
    request: Request,
    method: HttpMethod | None = Query(default=None),
    owner_id: UUID | None = Query(default=None),
    source_filename: str | None = Query(default=None),
    search: str | None = Query(default=None, min_length=1),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    trace_id = getattr(request.state, "traceId", None)
    query = (
        select(Action)
        .where(Action.is_deleted.is_(False))
        .where(Action.ingest_status == ActionIngestStatus.SUCCEEDED)
        .order_by(Action.created_at.desc())
        .limit(limit)
        .offset(offset)
    )

    if current_user.role == UserRole.ADMIN:
        if owner_id is not None:
            query = query.where(Action.user_id == owner_id)
    else:
        query = query.where(Action.user_id == current_user.id)

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
    actions = list(result.scalars().all())

    log_business_event(
        "actions_listed",
        trace_id=trace_id,
        user_id=str(current_user.id),
        method=method.value if method is not None else None,
        owner_id=str(owner_id) if owner_id is not None else None,
        source_filename=source_filename,
        search=search,
        limit=limit,
        offset=offset,
        result_count=len(actions),
    )

    return actions
