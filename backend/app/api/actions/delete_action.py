from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.actions.dependencies import get_active_action_or_404
from app.core.database.session import get_session
from app.models import User
from app.utils.business_logger import log_business_event
from app.utils.token_manager import get_current_user


router = APIRouter(tags=["Actions"])


@router.delete("/{action_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_action(
    action_id: UUID,
    request: Request,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    trace_id = getattr(request.state, "traceId", None)
    try:
        action = await get_active_action_or_404(session, action_id, current_user)
    except HTTPException:
        log_business_event(
            "action_delete_rejected",
            trace_id=trace_id,
            user_id=str(current_user.id),
            action_id=str(action_id),
            reason="action_not_found_or_forbidden",
        )
        raise

    action.is_deleted = True
    await session.commit()
    await session.refresh(action)

    log_business_event(
        "action_deleted",
        trace_id=trace_id,
        user_id=str(current_user.id),
        action_id=str(action.id),
    )

    return Response(status_code=status.HTTP_204_NO_CONTENT)
