from __future__ import annotations

from uuid import UUID

from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Capability, User, UserRole
from app.services.capability_service import CapabilityService


async def get_capability_or_404(
    session: AsyncSession,
    capability_id: UUID,
    current_user: User,
) -> Capability:
    capability_service = CapabilityService(session)
    capability = await capability_service.get_capability(
        capability_id,
        owner_user_id=current_user.id,
        include_all=current_user.role == UserRole.ADMIN,
    )
    if capability is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Capability not found")
    return capability
