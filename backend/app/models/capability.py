from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import ForeignKey, Index, String, Text
from sqlalchemy.dialects.postgresql import JSON, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin


class Capability(TimestampMixin, Base):
    __tablename__ = "capabilities"
    __table_args__ = (
        Index("ix_capabilities_action_id", "action_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    action_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("actions.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        comment="Action, from which this capability was built",
    )
    name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        index=True,
        comment="Machine-friendly capability name",
    )
    description: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="Business description for PM/LLM use",
    )
    input_schema: Mapped[dict[str, Any] | None] = mapped_column(
        JSON,
        nullable=True,
        comment="Public input schema of the capability",
    )
    output_schema: Mapped[dict[str, Any] | None] = mapped_column(
        JSON,
        nullable=True,
        comment="Public output schema of the capability",
    )
    data_format: Mapped[dict[str, Any] | None] = mapped_column(
        JSON,
        nullable=True,
        comment="Structured request/response data format metadata",
    )
    llm_payload: Mapped[dict[str, Any] | None] = mapped_column(
        JSON,
        nullable=True,
        comment="Raw normalized LLM payload for debugging",
    )

    action = relationship("Action", lazy="select")
