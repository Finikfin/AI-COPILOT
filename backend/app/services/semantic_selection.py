from __future__ import annotations

from typing import NamedTuple
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Capability


class SelectedCapability(NamedTuple):
    capability: Capability
    score: float


class SemanticSelectionService:
    async def select_capabilities(
        self,
        session: AsyncSession,
        user_query: str,
        owner_user_id: UUID | None = None,
        limit: int = 10,
    ) -> list[SelectedCapability]:
        _ = user_query
        query = select(Capability).order_by(Capability.created_at.asc())
        if owner_user_id is not None:
            query = query.where(Capability.user_id == owner_user_id)
        query = query.limit(limit)
        result = await session.execute(query)
        capabilities = list(result.scalars().all())
        return [SelectedCapability(capability=capability, score=1.0) for capability in capabilities]
