from fastapi import APIRouter, HTTPException, status, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database.session import get_session
from app.core.database.models import User
from app.schemas.auth_sch import LoginIn
from app.utils.hashing import verify_password
from app.utils.token_manager import create_access_token

router = APIRouter(prefix="/auth", tags=["Auth"])

@router.post("/login", status_code=status.HTTP_200_OK)
async def login(data: LoginIn, session: AsyncSession = Depends(get_session)):
    result = await session.execute(select(User).where(User.email == data.email))
    user = result.scalar_one_or_none()
    
    if not user or not verify_password(data.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"message": "Неверный email или пароль"}
        )
    
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_423_LOCKED,
            detail={"message": "Пользователь неактивен"}
        )
    
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