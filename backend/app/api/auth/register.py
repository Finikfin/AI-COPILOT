from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database.session import get_session
from app.models import User, UserRole
from app.schemas.auth_sch import RegisterIn
from app.utils.business_logger import log_business_event
from app.utils.hashing import hash_password
from app.utils.token_manager import create_access_token


router = APIRouter(prefix="/v1/auth", tags=["Auth"])


@router.post("/register", status_code=status.HTTP_201_CREATED)
async def register(
    data: RegisterIn,
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    email = data.email.strip().lower()
    trace_id = getattr(request.state, "traceId", None)

    result = await session.execute(select(User).where(func.lower(User.email) == email))
    existing_user = result.scalar_one_or_none()

    if existing_user is not None:
        log_business_event(
            "auth_register_failed",
            trace_id=trace_id,
            email=email,
            reason="email_already_exists",
        )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"message": "Email already exists. Please login."},
        )

    user = User(
        email=email,
        full_name=data.full_name,
        hashed_password=hash_password(data.password),
        role=UserRole.USER,
        is_active=True,
    )

    session.add(user)
    await session.commit()
    await session.refresh(user)

    token, expires_in = create_access_token(sub=str(user.id), role=user.role.value)
    log_business_event(
        "auth_register_succeeded",
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
