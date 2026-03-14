from __future__ import annotations

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Action, Capability
from app.utils.ollama_client import build_capability_from_action


class CapabilityService:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    @staticmethod
    def build_from_actions(actions: list[Action]) -> list[Capability]:
        capabilities: list[Capability] = []

        for action in actions:
            capability_payload = build_capability_from_action(action)
            capabilities.append(
                Capability(
                    action_id=action.id,
                    name=capability_payload["name"],
                    description=capability_payload.get("description"),
                    input_schema=capability_payload.get("input_schema"),
                    output_schema=capability_payload.get("output_schema"),
                    data_format=capability_payload.get("data_format"),
                    llm_payload=capability_payload.get("llm_payload"),
                )
            )

        return capabilities

    async def create_from_actions(
        self,
        actions: list[Action],
        *,
        refresh: bool = True,
    ) -> list[Capability]:
        capabilities = self.build_from_actions(actions)
        if not capabilities:
            return []

        self.session.add_all(capabilities)
        await self.session.flush()

        if refresh:
            for capability in capabilities:
                await self.session.refresh(capability)

        return capabilities

    async def get_capabilities(
        self,
        *,
        capability_ids: list[UUID] | None = None,
        action_ids: list[UUID] | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[Capability]:
        query = select(Capability).order_by(Capability.created_at.asc())

        if capability_ids:
            query = query.where(Capability.id.in_(capability_ids))

        if action_ids:
            query = query.where(Capability.action_id.in_(action_ids))

        if offset:
            query = query.offset(offset)

        if limit is not None:
            query = query.limit(limit)

        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def get_capability(self, capability_id: UUID) -> Capability | None:
        return await self.session.get(Capability, capability_id)
