from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database.session import get_session
from app.models import User, UserRole
from app.schemas.capability_sch import CapabilityResponse
from app.services.capability_service import CapabilityService
from app.utils.token_manager import get_current_user


router = APIRouter(tags=["Capabilities"])


@router.get("/", response_model=list[CapabilityResponse])
async def list_capabilities(
    action_id: UUID | None = Query(default=None),
    owner_id: UUID | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    capability_service = CapabilityService(session)
    action_ids = [action_id] if action_id is not None else None
    include_all = current_user.role == UserRole.ADMIN
    owner_user_id = owner_id if include_all and owner_id is not None else current_user.id

    return await capability_service.get_capabilities(
        action_ids=action_ids,
        owner_user_id=owner_user_id,
        include_all=include_all and owner_id is None,
        limit=limit,
        offset=offset,
    )
