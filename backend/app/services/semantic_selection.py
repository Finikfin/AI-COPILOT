from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class SelectedCapability:
    capability: Any
    score: float


class SemanticSelectionService:
    async def select_capabilities(
        self,
        session: Any,
        message: str,
        limit: int = 10,
    ) -> list[SelectedCapability]:
        # Fallback mock selector: until semantic matching is implemented,
        # return no candidates and let the caller ask for explicit capabilities.
        _ = (session, message, limit)
        return []
