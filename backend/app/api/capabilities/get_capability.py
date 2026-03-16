from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.capabilities.dependencies import get_capability_or_404
from app.core.database.session import get_session
from app.models import User
from app.schemas.capability_sch import CapabilityResponse
from app.utils.business_logger import log_business_event
from app.utils.token_manager import get_current_user


router = APIRouter(tags=["Capabilities"])


@router.get("/{capability_id}", response_model=CapabilityResponse)
async def get_capability(
    capability_id: UUID,
    request: Request,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    trace_id = getattr(request.state, "traceId", None)
    try:
        capability = await get_capability_or_404(session, capability_id, current_user)
    except HTTPException:
        log_business_event(
            "capability_fetch_rejected",
            trace_id=trace_id,
            user_id=str(current_user.id),
            capability_id=str(capability_id),
            reason="capability_not_found_or_forbidden",
        )
        raise

    log_business_event(
        "capability_fetched",
        trace_id=trace_id,
        user_id=str(current_user.id),
        capability_id=str(capability.id),
        capability_type=capability.type.value if hasattr(capability.type, "value") else str(capability.type),
    )
    return capability
