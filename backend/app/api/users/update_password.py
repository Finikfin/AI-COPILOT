from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database.session import get_session
from app.models import User
from app.utils.token_manager import get_current_user
from app.utils.hashing import verify_password, hash_password
from app.schemas.users_sch import PasswordUpdate

router = APIRouter(tags=["Users"])

@router.patch("/me/password", status_code=status.HTTP_200_OK)
async def update_password(
    data: PasswordUpdate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user)
):
    if not verify_password(data.old_password, current_user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Неверный текущий пароль"
        )

    current_user.hashed_password = hash_password(data.new_password)
    await session.commit()
    return {"message": "Пароль успешно обновлен"}
