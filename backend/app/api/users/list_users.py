from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.core.database.session import get_session
from app.models import User, UserRole
from app.utils.token_manager import check_permissions
from app.schemas.users_sch import UserResponse

router = APIRouter(tags=["Users"])

@router.get("/", response_model=list[UserResponse])
async def list_users(
    session: AsyncSession = Depends(get_session),
    current_user = Depends(check_permissions([UserRole.ADMIN]))
):
    result = await session.execute(select(User))
    return result.scalars().all()
