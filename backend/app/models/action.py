from __future__ import annotations

import enum
import uuid
from typing import Any

from sqlalchemy import Boolean, Enum, ForeignKey, Index, String, Text
from sqlalchemy.dialects.postgresql import JSON, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin


class HttpMethod(str, enum.Enum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"
    HEAD = "HEAD"
    OPTIONS = "OPTIONS"


class ActionIngestStatus(str, enum.Enum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class Action(TimestampMixin, Base):
    __tablename__ = "actions"
    __table_args__ = (
        Index("ix_actions_method_path", "method", "path"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    user_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
        comment="Owner of imported action",
    )
    operation_id: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        index=True,
    )
    method: Mapped[HttpMethod] = mapped_column(
        Enum(HttpMethod, name="http_method"),
        nullable=False,
    )
    path: Mapped[str] = mapped_column(
        String(2048),
        nullable=False,
    )
    base_url: Mapped[str | None] = mapped_column(
        String(2048),
        nullable=True,
    )
    summary: Mapped[str | None] = mapped_column(
        String(512),
        nullable=True,
    )
    description: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    tags: Mapped[list[str] | None] = mapped_column(
        JSON,
        nullable=True,
    )
    parameters_schema: Mapped[dict[str, Any] | None] = mapped_column(
        JSON,
        nullable=True,
    )
    request_body_schema: Mapped[dict[str, Any] | None] = mapped_column(
        JSON,
        nullable=True,
    )
    response_schema: Mapped[dict[str, Any] | None] = mapped_column(
        JSON,
        nullable=True,
    )
    source_filename: Mapped[str | None] = mapped_column(
        String(512),
        nullable=True,
    )
    raw_spec: Mapped[dict[str, Any] | None] = mapped_column(
        JSON,
        nullable=True,
    )
    ingest_status: Mapped[ActionIngestStatus] = mapped_column(
        Enum(ActionIngestStatus, name="action_ingest_status", native_enum=False),
        nullable=False,
        default=ActionIngestStatus.SUCCEEDED,
        server_default=ActionIngestStatus.SUCCEEDED.value,
        index=True,
    )
    ingest_error: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    is_deleted: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default="false",
        index=True,
    )

    owner = relationship("User", lazy="select")
