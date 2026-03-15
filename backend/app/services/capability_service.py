from __future__ import annotations

import re
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Action, Capability


class CapabilityService:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    @staticmethod
    def build_from_actions(actions: list[Action]) -> list[Capability]:
        from app.models.capability import CapabilityType
        capabilities: list[Capability] = []

        for action in actions:
            capability_payload = CapabilityService._build_capability_payload(action)
            capabilities.append(
                Capability(
                    action_id=action.id,
                    type=CapabilityType.ATOMIC,
                    name=capability_payload["name"],
                    description=capability_payload.get("description"),
                    input_schema=capability_payload.get("input_schema"),
                    output_schema=capability_payload.get("output_schema"),
                    data_format=capability_payload.get("data_format"),
                    llm_payload=capability_payload.get("llm_payload"),
                )
            )

        return capabilities

    async def create_composite_capability(
        self,
        *,
        name: str,
        description: str | None = None,
        input_schema: dict[str, Any] | None = None,
        output_schema: dict[str, Any] | None = None,
        recipe: dict[str, Any],
    ) -> Capability:
        from app.models.capability import CapabilityType
        capability = Capability(
            type=CapabilityType.COMPOSITE,
            name=name,
            description=description,
            input_schema=input_schema,
            output_schema=output_schema,
            recipe=recipe,
        )
        self.session.add(capability)
        await self.session.flush()
        await self.session.refresh(capability)
        return capability

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

    @staticmethod
    def _build_capability_payload(action: Action) -> dict[str, Any]:
        return {
            "name": CapabilityService._build_capability_name(action),
            "description": CapabilityService._build_capability_description(action),
            "input_schema": CapabilityService._build_input_schema(action),
            "output_schema": getattr(action, "response_schema", None),
            "data_format": CapabilityService._build_data_format(action),
            "llm_payload": {
                "source": "deterministic",
            },
        }

    @staticmethod
    def _build_capability_name(action: Action) -> str:
        operation_id = getattr(action, "operation_id", None)
        if operation_id:
            return str(operation_id)

        method = getattr(action, "method", None)
        method_value = method.value.lower() if method is not None else "call"
        path = getattr(action, "path", "") or ""
        normalized_path = re.sub(r"[{}]", "", path).strip("/")
        normalized_path = re.sub(r"[^a-zA-Z0-9/]+", "_", normalized_path)
        normalized_path = normalized_path.replace("/", "_") or "root"
        return f"{method_value}_{normalized_path.lower()}"

    @staticmethod
    def _build_capability_description(action: Action) -> str:
        summary = getattr(action, "summary", None)
        description = getattr(action, "description", None)
        operation_id = getattr(action, "operation_id", None)
        return str(summary or description or operation_id or CapabilityService._build_capability_name(action))

    @staticmethod
    def _build_input_schema(action: Action) -> dict[str, Any] | None:
        parameters_schema = getattr(action, "parameters_schema", None)
        request_body_schema = getattr(action, "request_body_schema", None)

        if parameters_schema and request_body_schema:
            return {
                "type": "object",
                "properties": {
                    "parameters": parameters_schema,
                    "request_body": request_body_schema,
                },
            }
        if parameters_schema:
            return parameters_schema
        if request_body_schema:
            return request_body_schema
        return None

    @staticmethod
    def _build_data_format(action: Action) -> dict[str, Any]:
        parameters_schema = getattr(action, "parameters_schema", None) or {}
        request_body_schema = getattr(action, "request_body_schema", None) or {}
        response_schema = getattr(action, "response_schema", None) or {}

        parameter_locations: list[str] = []
        if isinstance(parameters_schema, dict):
            properties = parameters_schema.get("properties", {})
            if isinstance(properties, dict):
                for property_schema in properties.values():
                    if not isinstance(property_schema, dict):
                        continue
                    location = property_schema.get("x-parameter-location")
                    if isinstance(location, str) and location not in parameter_locations:
                        parameter_locations.append(location)

        request_content_type = request_body_schema.get("x-content-type") if isinstance(request_body_schema, dict) else None
        response_content_type = response_schema.get("x-content-type") if isinstance(response_schema, dict) else None

        return {
            "parameter_locations": parameter_locations,
            "request_content_types": [request_content_type] if isinstance(request_content_type, str) else [],
            "request_schema_type": request_body_schema.get("type") if isinstance(request_body_schema, dict) else None,
            "response_content_types": [response_content_type] if isinstance(response_content_type, str) else [],
            "response_schema_types": [response_schema.get("type")] if isinstance(response_schema, dict) and isinstance(response_schema.get("type"), str) else [],
        }
