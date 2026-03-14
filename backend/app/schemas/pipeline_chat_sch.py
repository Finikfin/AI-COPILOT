from __future__ import annotations

from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field


class PipelineGraphNode(BaseModel):
    id: str
    capability_id: UUID
    label: str
    description: str | None = None
    input_mapping: dict[str, Any] | None = None
    position: dict[str, float] | None = None


class PipelineGraphEdge(BaseModel):
    id: str
    source: str
    target: str
    condition: str | None = None


class PipelineGenerateRequest(BaseModel):
    dialog_id: UUID
    message: str = Field(min_length=1)
    user_id: UUID | None = None
    capability_ids: list[UUID] | None = None


class PipelineGenerateResponse(BaseModel):
    status: Literal["ready", "needs_input", "cannot_build"]
    message_ru: str
    pipeline_id: UUID | None = None
    nodes: list[PipelineGraphNode] = Field(default_factory=list)
    edges: list[PipelineGraphEdge] = Field(default_factory=list)
    missing_requirements: list[str] = Field(default_factory=list)
    context_summary: str | None = None


class DialogResetRequest(BaseModel):
    dialog_id: UUID


class DialogResetResponse(BaseModel):
    status: Literal["ok"]
    message_ru: str
