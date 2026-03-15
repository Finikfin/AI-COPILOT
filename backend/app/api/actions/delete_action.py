from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.actions.dependencies import get_active_action_or_404
from app.core.database.session import get_session
from app.models import User
from app.utils.token_manager import get_current_user


router = APIRouter(tags=["Actions"])


@router.delete("/{action_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_action(
    action_id: UUID,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    action = await get_active_action_or_404(session, action_id, current_user)
    action.is_deleted = True
    await session.commit()
    await session.refresh(action)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
