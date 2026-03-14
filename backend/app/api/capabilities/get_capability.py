from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.capabilities.dependencies import get_capability_or_404
from app.core.database.session import get_session
from app.schemas.capability_sch import CapabilityResponse


router = APIRouter(tags=["Capabilities"])


@router.get("/{capability_id}", response_model=CapabilityResponse)
async def get_capability(
    capability_id: UUID,
    session: AsyncSession = Depends(get_session),
):
    return await get_capability_or_404(session, capability_id)
