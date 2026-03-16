from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database.session import get_session
from app.models import User, UserRole
from app.schemas.users_sch import UserResponse, UserUpdate
from app.utils.business_logger import log_business_event
from app.utils.token_manager import check_permissions

router = APIRouter(tags=["Users"])


@router.patch("/{user_id}", response_model=UserResponse)
async def update_user(
    user_id: UUID,
    data: UserUpdate,
    request: Request,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(check_permissions([UserRole.ADMIN])),
):
    trace_id = getattr(request.state, "traceId", None)

    user = await session.get(User, user_id)
    if user is None:
        log_business_event(
            "user_update_rejected",
            trace_id=trace_id,
            user_id=str(current_user.id),
            target_user_id=str(user_id),
            reason="target_user_not_found",
        )
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)

    update_data = data.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(user, key, value)

    await session.commit()
    await session.refresh(user)

    log_business_event(
        "user_updated",
        trace_id=trace_id,
        user_id=str(current_user.id),
        target_user_id=str(user.id),
        updated_fields=sorted(update_data.keys()),
    )

    return user