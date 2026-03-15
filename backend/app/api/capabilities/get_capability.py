from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.capabilities.dependencies import get_capability_or_404
from app.core.database.session import get_session
from app.models import User
from app.schemas.capability_sch import CapabilityResponse
from app.utils.token_manager import get_current_user


router = APIRouter(tags=["Capabilities"])


@router.get("/{capability_id}", response_model=CapabilityResponse)
async def get_capability(
    capability_id: UUID,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    return await get_capability_or_404(session, capability_id, current_user)
