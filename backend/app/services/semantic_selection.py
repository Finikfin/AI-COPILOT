from __future__ import annotations

<<<<<<< HEAD
from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class SelectedCapability:
    capability: Any
=======
from typing import NamedTuple

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Capability


class SelectedCapability(NamedTuple):
    capability: Capability
>>>>>>> 7432131743d16388bdcffb9fc4bc8646d279ac9e
    score: float


class SemanticSelectionService:
    async def select_capabilities(
        self,
<<<<<<< HEAD
        session: Any,
        message: str,
        limit: int = 10,
    ) -> list[SelectedCapability]:
        # Fallback mock selector: until semantic matching is implemented,
        # return no candidates and let the caller ask for explicit capabilities.
        _ = (session, message, limit)
        return []
=======
        session: AsyncSession,
        user_query: str,
        limit: int = 10,
    ) -> list[SelectedCapability]:
        query = select(Capability).order_by(Capability.created_at.asc()).limit(limit)
        result = await session.execute(query)
        capabilities = list(result.scalars().all())
        return [SelectedCapability(capability=capability, score=1.0) for capability in capabilities]
>>>>>>> 7432131743d16388bdcffb9fc4bc8646d279ac9e
