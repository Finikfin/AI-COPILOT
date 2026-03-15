from __future__ import annotations

from uuid import UUID

from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Action, ActionIngestStatus, User, UserRole


async def get_active_action_or_404(
    session: AsyncSession,
    action_id: UUID,
    current_user: User,
) -> Action:
    action = await session.get(Action, action_id)
    if action is None or action.is_deleted or action.ingest_status != ActionIngestStatus.SUCCEEDED:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Action not found")
    if current_user.role != UserRole.ADMIN and action.user_id != current_user.id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Action not found")
    return action
