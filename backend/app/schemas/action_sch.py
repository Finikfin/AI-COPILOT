from __future__ import annotations

from datetime import datetime
from uuid import UUID
from typing import Any

from pydantic import BaseModel, ConfigDict

from app.models import HttpMethod


class ActionResponse(BaseModel):
    id: UUID
    operation_id: str | None = None
    method: HttpMethod
    path: str
    base_url: str | None = None
    summary: str | None = None
    description: str | None = None
    tags: list[str] | None = None
    parameters_schema: dict[str, Any] | None = None
    request_body_schema: dict[str, Any] | None = None
    response_schema: dict[str, Any] | None = None
    source_filename: str | None = None
    raw_spec: dict[str, Any] | None = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class ActionIngestResponse(BaseModel):
    created_count: int
    actions: list[ActionResponse]
