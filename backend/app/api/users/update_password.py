from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database.session import get_session
from app.models import User
from app.schemas.users_sch import PasswordUpdate
from app.utils.business_logger import log_business_event
from app.utils.hashing import hash_password, verify_password
from app.utils.token_manager import get_current_user

router = APIRouter(tags=["Users"])


@router.patch("/me/password", status_code=status.HTTP_200_OK)
async def update_password(
    data: PasswordUpdate,
    request: Request,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    trace_id = getattr(request.state, "traceId", None)

    if not verify_password(data.old_password, current_user.hashed_password):
        log_business_event(
            "user_password_update_rejected",
            trace_id=trace_id,
            user_id=str(current_user.id),
            reason="invalid_current_password",
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Неверный текущий пароль",
        )

    current_user.hashed_password = hash_password(data.new_password)
    await session.commit()

    log_business_event(
        "user_password_updated",
        trace_id=trace_id,
        user_id=str(current_user.id),
    )

    return {"message": "Пароль успешно обновлен"}