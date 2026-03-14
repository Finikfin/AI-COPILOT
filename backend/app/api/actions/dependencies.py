from __future__ import annotations

from uuid import UUID

from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Action


async def get_active_action_or_404(session: AsyncSession, action_id: UUID) -> Action:
    action = await session.get(Action, action_id)
    if action is None or action.is_deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Action not found")
    return action
