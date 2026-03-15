from __future__ import annotations

import enum
import uuid
from typing import Any

from sqlalchemy import Enum, ForeignKey, Index, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSON, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin


class CapabilityType(str, enum.Enum):
    ATOMIC = "ATOMIC"
    COMPOSITE = "COMPOSITE"


class Capability(TimestampMixin, Base):
    __tablename__ = "capabilities"
    __table_args__ = (
        Index("ix_capabilities_action_id", "action_id"),
        UniqueConstraint(
            "user_id",
            "action_id",
            name="uq_capabilities_user_action",
        ),
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
        comment="Owner of capability",
    )
    action_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("actions.id", ondelete="CASCADE"),
        nullable=True,
        comment="Action source for atomic capability",
    )
    type: Mapped[CapabilityType] = mapped_column(
        Enum(CapabilityType, name="capability_type", native_enum=False),
        nullable=False,
        default=CapabilityType.ATOMIC,
        server_default=CapabilityType.ATOMIC.value,
        index=True,
    )
    name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        index=True,
    )
    description: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    input_schema: Mapped[dict[str, Any] | None] = mapped_column(
        JSON,
        nullable=True,
    )
    output_schema: Mapped[dict[str, Any] | None] = mapped_column(
        JSON,
        nullable=True,
    )
    recipe: Mapped[dict[str, Any] | None] = mapped_column(
        JSON,
        nullable=True,
    )
    data_format: Mapped[dict[str, Any] | None] = mapped_column(
        JSON,
        nullable=True,
    )
    llm_payload: Mapped[dict[str, Any] | None] = mapped_column(
        JSON,
        nullable=True,
    )

    action = relationship("Action", lazy="select")
    owner = relationship("User", lazy="select")
