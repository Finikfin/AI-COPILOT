from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database.session import get_session
from app.models import User, UserRole
from app.schemas.capability_sch import CapabilityResponse, CreateCompositeCapabilityRequest
from app.services.capability_service import (
    CapabilityService,
    CompositeRecipeValidationError,
)
from app.utils.token_manager import get_current_user


router = APIRouter(tags=["Capabilities"])


@router.post(
    "/composite",
    response_model=CapabilityResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_composite_capability(
    payload: CreateCompositeCapabilityRequest,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    capability_service = CapabilityService(session)
    try:
        capability = await capability_service.create_validated_composite_capability(
            owner_user_id=current_user.id,
            name=payload.name,
            description=payload.description,
            input_schema=payload.input_schema,
            output_schema=payload.output_schema,
            recipe=payload.recipe.model_dump(mode="python"),
            include_all=current_user.role == UserRole.ADMIN,
        )
        await session.commit()
        await session.refresh(capability)
        return capability
    except CompositeRecipeValidationError as exc:
        await session.rollback()
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "message": "Composite recipe validation failed",
                "errors": exc.errors,
            },
        ) from exc
