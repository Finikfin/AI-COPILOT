from __future__ import annotations

import enum
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Enum, ForeignKey, Index, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin


class DialogMessageRole(str, enum.Enum):
    USER = "user"
    ASSISTANT = "assistant"


class PipelineDialog(TimestampMixin, Base):
    __tablename__ = "pipeline_dialogs"
    __table_args__ = (
        Index("ix_pipeline_dialogs_user_updated_at", "user_id", "updated_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    title: Mapped[str | None] = mapped_column(
        String(256),
        nullable=True,
    )
    last_status: Mapped[str | None] = mapped_column(
        String(32),
        nullable=True,
    )
    last_pipeline_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("pipelines.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    last_message_preview: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )

    user = relationship(
        "User",
        back_populates="pipeline_dialogs",
        lazy="select",
    )
    last_pipeline = relationship(
        "Pipeline",
        back_populates="dialogs",
        lazy="select",
    )
    messages = relationship(
        "PipelineDialogMessage",
        back_populates="dialog",
        cascade="all, delete-orphan",
        passive_deletes=True,
        lazy="selectin",
    )


class PipelineDialogMessage(Base):
    __tablename__ = "pipeline_dialog_messages"
    __table_args__ = (
        Index(
            "ix_pipeline_dialog_messages_dialog_created_at",
            "dialog_id",
            "created_at",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    dialog_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("pipeline_dialogs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    role: Mapped[DialogMessageRole] = mapped_column(
        Enum(DialogMessageRole, name="dialog_message_role"),
        nullable=False,
        index=True,
    )
    content: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )
    assistant_payload: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        index=True,
    )

    dialog = relationship(
        "PipelineDialog",
        back_populates="messages",
        lazy="select",
    )
