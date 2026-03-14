from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from uuid import UUID
from app.core.database.session import get_session
from app.models import User, UserRole
from app.utils.token_manager import check_permissions
from app.schemas.users_sch import UserUpdate, UserResponse

router = APIRouter(tags=["Users"])

@router.patch("/{user_id}", response_model=UserResponse)
async def update_user(
    user_id: UUID,
    data: UserUpdate,
    session: AsyncSession = Depends(get_session),
    current_user = Depends(check_permissions([UserRole.ADMIN]))
):
    user = await session.get(User, user_id)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)

    update_data = data.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(user, key, value)

    await session.commit()
    await session.refresh(user)
    return user
