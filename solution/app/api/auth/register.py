from fastapi import APIRouter, HTTPException, status, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database.session import get_session
from app.core.database.models import User, UserRole
from app.schemas.auth_sch import RegisterIn
from app.utils.hashing import hash_password
from app.utils.token_manager import create_access_token

router = APIRouter(prefix="/auth", tags=["Auth"])

@router.post("/register", status_code=status.HTTP_201_CREATED)
async def register(data: RegisterIn, session: AsyncSession = Depends(get_session)):
    res = await session.execute(select(User).where(User.email == data.email))
    if res.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"message": "Пользователь с таким email уже существует"}
        )

    user = User(
        email=data.email,
        full_name=data.full_name,
        hashed_password=hash_password(data.password),
        role=UserRole.USER,
        is_active=True
    )
    
    session.add(user)
    await session.commit()
    await session.refresh(user)

    token, expires_in = create_access_token(sub=str(user.id), role=user.role)

    return {
        "accessToken": token,
        "expiresIn": expires_in,
        "user": {
            "id": str(user.id),
            "email": user.email,
            "fullName": user.full_name,
            "role": user.role,
            "isActive": user.is_active,
            "createdAt": user.created_at
        },
    }