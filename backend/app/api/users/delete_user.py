from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database.session import get_session
from app.models import User, UserRole
from app.utils.business_logger import log_business_event
from app.utils.token_manager import get_current_user

router = APIRouter(tags=["Users"])


@router.delete("/{user_id}", status_code=status.HTTP_200_OK)
async def delete_user(
    user_id: UUID,
    request: Request,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    trace_id = getattr(request.state, "traceId", None)

    if current_user.role != UserRole.ADMIN and current_user.id != user_id:
        log_business_event(
            "user_deactivation_rejected",
            trace_id=trace_id,
            user_id=str(current_user.id),
            target_user_id=str(user_id),
            reason="forbidden",
        )
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Нет доступа")

    user = await session.get(User, user_id)
    if user is None:
        log_business_event(
            "user_deactivation_rejected",
            trace_id=trace_id,
            user_id=str(current_user.id),
            target_user_id=str(user_id),
            reason="target_user_not_found",
        )
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)

    user.is_active = False
    await session.commit()

    log_business_event(
        "user_deactivated",
        trace_id=trace_id,
        user_id=str(current_user.id),
        target_user_id=str(user.id),
    )

    return {"message": "Пользователь успешно деактивирован"}