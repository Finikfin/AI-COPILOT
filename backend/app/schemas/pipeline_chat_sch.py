from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class PipelineInputTypeFromPrevious(BaseModel):
    from_step: int
    type: str


class PipelineStepEndpoint(BaseModel):
    name: str
    capability_id: UUID
    action_id: UUID
    input_type: str | dict[str, Any] | None = None
    output_type: str | dict[str, Any] | None = None


class PipelineGraphNode(BaseModel):
    step: int
    name: str
    description: str | None = None
    input_connected_from: list[int] = Field(default_factory=list)
    output_connected_to: list[int] = Field(default_factory=list)
    input_data_type_from_previous: list[PipelineInputTypeFromPrevious] = Field(default_factory=list)
    external_inputs: list[str] = Field(default_factory=list)
    endpoints: list[PipelineStepEndpoint] = Field(default_factory=list)


class PipelineGraphEdge(BaseModel):
    from_step: int
    to_step: int
    type: str


class PipelineGenerateRequest(BaseModel):
    dialog_id: UUID
    message: str = Field(min_length=1)
    user_id: UUID | None = None
    capability_ids: list[UUID] | None = None


class PipelineGenerateResponse(BaseModel):
    status: Literal["ready", "needs_input", "cannot_build"]
    message_ru: str
    chat_reply_ru: str
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


class PipelineDialogListItemResponse(BaseModel):
    dialog_id: UUID
    title: str | None = None
    last_status: str | None = None
    last_pipeline_id: UUID | None = None
    last_message_preview: str | None = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class PipelineDialogMessageResponse(BaseModel):
    id: UUID
    role: Literal["user", "assistant"]
    content: str
    assistant_payload: dict[str, Any] | None = None
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class PipelineDialogHistoryResponse(BaseModel):
    dialog_id: UUID
    title: str | None = None
    messages: list[PipelineDialogMessageResponse] = Field(default_factory=list)
