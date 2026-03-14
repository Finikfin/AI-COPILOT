from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.core.database.session import get_session
from app.models import User
from app.utils.token_manager import get_current_user
from app.schemas.users_sch import UserUpdateMe, UserResponse

router = APIRouter(tags=["Users"])

@router.patch("/me", response_model=UserResponse)
async def update_me(
    data: UserUpdateMe,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user)
):
    if data.email and data.email != current_user.email:
        stmt = select(User).where(User.email == data.email)
        result = await session.execute(stmt)
        if result.scalar_one_or_none():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Пользователь с таким email уже существует"
            )

    update_data = data.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(current_user, key, value)

    await session.commit()
    await session.refresh(current_user)
    return current_user
