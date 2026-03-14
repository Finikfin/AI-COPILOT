from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.actions.dependencies import get_active_action_or_404
from app.core.database.session import get_session


router = APIRouter(tags=["Actions"])


@router.delete("/{action_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_action(
    action_id: UUID,
    session: AsyncSession = Depends(get_session),
):
    action = await get_active_action_or_404(session, action_id)
    action.is_deleted = True
    await session.commit()
    await session.refresh(action)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
