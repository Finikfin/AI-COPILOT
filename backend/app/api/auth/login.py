from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database.session import get_session
from app.models import User
from app.schemas.auth_sch import LoginIn
from app.utils.business_logger import log_business_event
from app.utils.hashing import verify_password
from app.utils.token_manager import create_access_token


router = APIRouter(prefix="/v1/auth", tags=["Auth"])


@router.post("/login", status_code=status.HTTP_200_OK)
async def login(
    data: LoginIn,
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    email = data.email.strip().lower()
    trace_id = getattr(request.state, "traceId", None)
    result = await session.execute(select(User).where(func.lower(User.email) == email))
    user = result.scalar_one_or_none()

    if user is None:
        log_business_event(
            "auth_login_failed",
            trace_id=trace_id,
            email=email,
            reason="user_not_found",
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"message": "Invalid email or password"},
        )

    if not verify_password(data.password, user.hashed_password):
        log_business_event(
            "auth_login_failed",
            trace_id=trace_id,
            email=email,
            reason="invalid_password",
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"message": "Invalid email or password"},
        )

    if not user.is_active:
        log_business_event(
            "auth_login_blocked",
            trace_id=trace_id,
            user_id=str(user.id),
            email=user.email,
            reason="user_inactive",
        )
        raise HTTPException(
            status_code=status.HTTP_423_LOCKED,
            detail={"message": "User account is deactivated"},
        )

    token, expires_in = create_access_token(sub=str(user.id), role=user.role.value)
    log_business_event(
        "auth_login_succeeded",
        trace_id=trace_id,
        user_id=str(user.id),
        email=user.email,
        role=user.role.value,
    )

    return {
        "accessToken": token,
        "expiresIn": expires_in,
        "user": {
            "id": str(user.id),
            "email": user.email,
            "fullName": user.full_name,
            "role": user.role.value,
            "isActive": user.is_active,
            "createdAt": user.created_at.isoformat(),
        },
    }
