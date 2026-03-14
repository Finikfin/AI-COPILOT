from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.actions.dependencies import get_active_action_or_404
from app.core.database.session import get_session
from app.schemas.action_sch import ActionDetailResponse


router = APIRouter(tags=["Actions"])


@router.get("/{action_id}", response_model=ActionDetailResponse)
async def get_action(
    action_id: UUID,
    session: AsyncSession = Depends(get_session),
):
    return await get_active_action_or_404(session, action_id)
