from fastapi import APIRouter, Depends, Request

from app.models import User
from app.schemas.users_sch import UserResponse
from app.utils.business_logger import log_business_event
from app.utils.token_manager import get_current_user

router = APIRouter(tags=["Users"])

@router.get("/me", response_model=UserResponse)
async def get_me(
    request: Request,
    current_user: User = Depends(get_current_user),
):
    trace_id = getattr(request.state, "traceId", None)
    log_business_event(
        "user_profile_viewed",
        trace_id=trace_id,
        user_id=str(current_user.id),
    )
    return current_user
