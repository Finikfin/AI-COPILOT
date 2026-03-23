from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class RunPipelineRequest(BaseModel):
    inputs: dict[str, Any] = Field(default_factory=dict)
    dialog_id: UUID | None = None


class RunPipelineResponse(BaseModel):
    run_id: UUID
    pipeline_id: UUID
    status: Literal["QUEUED", "RUNNING"]


class ExecutionRunListItemResponse(BaseModel):
    id: UUID
    pipeline_id: UUID
    status: Literal["QUEUED", "RUNNING", "SUCCEEDED", "FAILED", "PARTIAL_FAILED"]
    error: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class ExecutionStepRunResponse(BaseModel):
    step: int
    name: str | None = None
    capability_id: UUID | None = None
    action_id: UUID | None = None
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"] | None = None
    status_code: int | None = None
    status: Literal["PENDING", "RUNNING", "SUCCEEDED", "FAILED", "SKIPPED"]
    resolved_inputs: dict[str, Any] | None = None
    accepted_payload: Any = None
    output_payload: Any = None
    request_snapshot: dict[str, Any] | None = None
    response_snapshot: dict[str, Any] | None = None
    error: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_ms: int | None = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class ExecutionRunDetailResponse(BaseModel):
    id: UUID
    pipeline_id: UUID
    status: Literal["QUEUED", "RUNNING", "SUCCEEDED", "FAILED", "PARTIAL_FAILED"]
    inputs: dict[str, Any] = Field(default_factory=dict)
    summary: dict[str, Any] | None = None
    error: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    created_at: datetime
    updated_at: datetime
    steps: list[ExecutionStepRunResponse] = Field(default_factory=list)
