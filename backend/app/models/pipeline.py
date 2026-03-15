import enum
import uuid
from typing import Any

from sqlalchemy import Enum, ForeignKey, String, Text
from sqlalchemy.dialects.postgresql import JSON, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin


class PipelineStatus(str, enum.Enum):
    DRAFT = "DRAFT"
    READY = "READY"
    ARCHIVED = "ARCHIVED"


class Pipeline(TimestampMixin, Base):
    """
    Сценарный слой.
    Коллекция нод и связей между ними — полная структура графа,
    сгенерированного SynthesisService и отображаемого на канвасе (React Flow).
    """
    __tablename__ = "pipelines"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )

    name: Mapped[str] = mapped_column(
        String(512),
        nullable=False,
        comment="Человекочитаемое название пайплайна",
    )
    description: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="Подробное описание того, что делает этот сценарий",
    )

    user_prompt: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="Оригинальный текстовый запрос PM из чата, породивший этот граф",
    )

    nodes: Mapped[list[dict[str, Any]]] = mapped_column(
        JSON,
        nullable=False,
        default=list,
        comment="Список нод графа. Каждая нода ссылается на Capability и хранит индивидуальные параметры",
    )

    edges: Mapped[list[dict[str, Any]]] = mapped_column(
        JSON,
        nullable=False,
        default=list,
        comment="Список рёбер графа. Определяет порядок выполнения нод (DAG)",
    )

    status: Mapped[PipelineStatus] = mapped_column(
        Enum(PipelineStatus, name="pipeline_status"),
        nullable=False,
        default=PipelineStatus.DRAFT,
        server_default=PipelineStatus.DRAFT.value,
        comment="Статус пайплайна: DRAFT → READY → ARCHIVED",
    )

    created_by: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
        comment="UUID пользователя (PM), создавшего или запустившего генерацию",
    )

    creator = relationship("User", lazy="select")
    dialogs = relationship(
        "PipelineDialog",
        back_populates="last_pipeline",
        passive_deletes=True,
        lazy="selectin",
    )
