import enum
import uuid
from typing import Any

from sqlalchemy import Enum, String, Text
from sqlalchemy.dialects.postgresql import JSON, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin


class HttpMethod(str, enum.Enum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"
    HEAD = "HEAD"
    OPTIONS = "OPTIONS"


class Action(TimestampMixin, Base):
    """
    Технический слой.
    Хранит детали одного эндпоинта, импортированного из OpenAPI/Swagger файла.
    """
    __tablename__ = "actions"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )

    # --- Идентификация внутри спецификации ---
    operation_id: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        index=True,
        comment="operationId из OpenAPI-спецификации",
    )

    # --- HTTP-детали ---
    method: Mapped[HttpMethod] = mapped_column(
        Enum(HttpMethod, name="http_method"),
        nullable=False,
        comment="HTTP-метод эндпоинта (GET, POST, ...)",
    )
    path: Mapped[str] = mapped_column(
        String(2048),
        nullable=False,
        comment="URL-путь эндпоинта, например /users/{id}",
    )
    base_url: Mapped[str | None] = mapped_column(
        String(2048),
        nullable=True,
        comment="Базовый URL сервера из секции servers[] спецификации",
    )

    summary: Mapped[str | None] = mapped_column(
        String(512),
        nullable=True,
        comment="Краткое описание (поле summary из OpenAPI)",
    )
    description: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="Подробное описание (поле description из OpenAPI)",
    )
    tags: Mapped[list[str] | None] = mapped_column(
        JSON,
        nullable=True,
        comment="Список тегов из OpenAPI для группировки",
    )

    # --- JSON-схемы ---
    parameters_schema: Mapped[dict[str, Any] | None] = mapped_column(
        JSON,
        nullable=True,
        comment="JSON Schema query/path/header параметров",
    )
    request_body_schema: Mapped[dict[str, Any] | None] = mapped_column(
        JSON,
        nullable=True,
        comment="JSON Schema тела запроса (requestBody)",
    )
    response_schema: Mapped[dict[str, Any] | None] = mapped_column(
        JSON,
        nullable=True,
        comment="JSON Schema успешного ответа (2xx)",
    )

    source_filename: Mapped[str | None] = mapped_column(
        String(512),
        nullable=True,
        comment="Имя загруженного OpenAPI-файла",
    )
    raw_spec: Mapped[dict[str, Any] | None] = mapped_column(
        JSON,
        nullable=True,
        comment="Оригинальный JSON-фрагмент операции из спецификации",
    )
