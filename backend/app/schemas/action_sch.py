from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, computed_field

from app.models import ActionIngestStatus, HttpMethod


class ActionListItemResponse(BaseModel):
    id: UUID
    user_id: UUID | None = None
    operation_id: str | None = None
    method: HttpMethod
    path: str
    base_url: str | None = None
    summary: str | None = None
    description: str | None = None
    tags: list[str] | None = None
    source_filename: str | None = None
    ingest_status: ActionIngestStatus
    ingest_error: str | None = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class ActionIngestItemResponse(BaseModel):
    id: UUID
    user_id: UUID | None = None
    operation_id: str | None = None
    method: HttpMethod
    path: str
    summary: str | None = None
    source_filename: str | None = None
    ingest_status: ActionIngestStatus
    ingest_error: str | None = None

    model_config = ConfigDict(from_attributes=True)


class ActionDetailResponse(ActionListItemResponse):
    parameters_schema: dict[str, Any] | None = None
    request_body_schema: dict[str, Any] | None = None
    response_schema: dict[str, Any] | None = None
    raw_spec: dict[str, Any] | None = None

    @computed_field(return_type=dict[str, Any] | None)
    @property
    def json_schema(self) -> dict[str, Any] | None:
        if not any((self.parameters_schema, self.request_body_schema, self.response_schema, self.raw_spec)):
            return None

        return {
            "parameters": self.parameters_schema,
            "request_body": self.request_body_schema,
            "response": self.response_schema,
            "raw_spec": self.raw_spec,
        }


class ActionIngestResponse(BaseModel):
    succeeded_count: int
    failed_count: int
    succeeded_actions: list[ActionDetailResponse]
    failed_actions: list[ActionDetailResponse]
