from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database.session import get_session
from app.models import User, UserRole
from app.schemas.auth_sch import LoginIn
from app.utils.hashing import hash_password, verify_password
from app.utils.token_manager import create_access_token


router = APIRouter(prefix="/v1/auth", tags=["Auth"])


@router.post("/login", status_code=status.HTTP_200_OK)
async def login(data: LoginIn, session: AsyncSession = Depends(get_session)):
    email = data.email.lower()
    result = await session.execute(select(User).where(User.email == email))
    user = result.scalar_one_or_none()

    if user is None:
        full_name = (
            email.split("@", 1)[0]
            .replace(".", " ")
            .replace("_", " ")
            .title()
            or "New User"
        )
        user = User(
            email=email,
            full_name=full_name,
            hashed_password=hash_password(data.password),
            role=UserRole.USER,
            is_active=True,
        )
        session.add(user)
        await session.commit()
        await session.refresh(user)

    elif not verify_password(data.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"message": "Invalid email or password"},
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_423_LOCKED,
            detail={"message": "User account is deactivated"},
        )

    token, expires_in = create_access_token(sub=str(user.id), role=user.role.value)

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
