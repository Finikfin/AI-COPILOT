from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database.session import get_session
from app.schemas.capability_sch import CapabilityResponse
from app.services.capability_service import CapabilityService


router = APIRouter(tags=["Capabilities"])


@router.get("/", response_model=list[CapabilityResponse])
async def list_capabilities(
    action_id: UUID | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_session),
):
    capability_service = CapabilityService(session)
    action_ids = [action_id] if action_id is not None else None
    return await capability_service.get_capabilities(
        action_ids=action_ids,
        limit=limit,
        offset=offset,
    )
