from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from app.schemas.action_sch import ActionIngestItemResponse


class CapabilityDataFormat(BaseModel):
    parameter_locations: list[str] = []
    request_content_types: list[str] = []
    request_schema_type: str | None = None
    response_content_types: list[str] = []
    response_schema_types: list[str] = []


class CapabilityResponse(BaseModel):
    id: UUID
    user_id: UUID | None = None
    action_id: UUID | None = None
    type: str = "ATOMIC"
    name: str
    description: str | None = None
    input_schema: dict[str, Any] | None = None
    output_schema: dict[str, Any] | None = None
    recipe: dict[str, Any] | None = None
    data_format: CapabilityDataFormat | None = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class CapabilityIngestItemResponse(BaseModel):
    id: UUID
    user_id: UUID | None = None
    action_id: UUID | None = None
    type: str = "ATOMIC"
    name: str
    description: str | None = None

    model_config = ConfigDict(from_attributes=True)


class ActionIngestWithCapabilitiesResponse(BaseModel):
    succeeded_count: int
    failed_count: int
    created_capabilities_count: int
    succeeded_actions: list[ActionIngestItemResponse]
    failed_actions: list[ActionIngestItemResponse]
    capabilities: list[CapabilityIngestItemResponse]
