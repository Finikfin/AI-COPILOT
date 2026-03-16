from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database.session import get_session
from app.models import User
from app.schemas.users_sch import UserResponse, UserUpdateMe
from app.utils.business_logger import log_business_event
from app.utils.token_manager import get_current_user

router = APIRouter(tags=["Users"])


@router.patch("/me", response_model=UserResponse)
async def update_me(
    data: UserUpdateMe,
    request: Request,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    trace_id = getattr(request.state, "traceId", None)

    if data.email and data.email != current_user.email:
        stmt = select(User).where(User.email == data.email)
        result = await session.execute(stmt)
        if result.scalar_one_or_none():
            log_business_event(
                "user_profile_update_rejected",
                trace_id=trace_id,
                user_id=str(current_user.id),
                reason="email_already_exists",
                requested_email=data.email,
            )
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Пользователь с таким email уже существует",
            )

    update_data = data.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(current_user, key, value)

    await session.commit()
    await session.refresh(current_user)
    log_business_event(
        "user_profile_updated",
        trace_id=trace_id,
        user_id=str(current_user.id),
        updated_fields=sorted(update_data.keys()),
    )
    return current_user