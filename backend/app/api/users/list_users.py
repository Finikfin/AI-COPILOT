from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.core.database.session import get_session
from app.models import User, UserRole
from app.utils.business_logger import log_business_event
from app.utils.token_manager import check_permissions
from app.schemas.users_sch import UserResponse

router = APIRouter(tags=["Users"])

@router.get("/", response_model=list[UserResponse])
async def list_users(
    request: Request,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(check_permissions([UserRole.ADMIN])),
):
    result = await session.execute(select(User))
    users = result.scalars().all()
    trace_id = getattr(request.state, "traceId", None)
    log_business_event(
        "users_listed",
        trace_id=trace_id,
        user_id=str(current_user.id),
        result_count=len(users),
    )
    return users
