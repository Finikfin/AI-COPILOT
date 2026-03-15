from __future__ import annotations

from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import DialogMessageRole, PipelineDialog, PipelineDialogMessage


class DialogAccessError(Exception):
    pass


class PipelineDialogService:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def list_dialogs(
        self,
        *,
        user_id: UUID,
        limit: int,
        offset: int,
    ) -> list[PipelineDialog]:
        query = (
            select(PipelineDialog)
            .where(PipelineDialog.user_id == user_id)
            .order_by(PipelineDialog.updated_at.desc())
            .limit(limit)
            .offset(offset)
        )
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def get_history(
        self,
        *,
        dialog_id: UUID,
        user_id: UUID,
        limit: int,
        offset: int,
    ) -> tuple[PipelineDialog, list[PipelineDialogMessage]]:
        dialog = await self._get_dialog_owned_by_user(dialog_id=dialog_id, user_id=user_id)

        query = (
            select(PipelineDialogMessage)
            .where(PipelineDialogMessage.dialog_id == dialog.id)
            .order_by(PipelineDialogMessage.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        result = await self.session.execute(query)
        messages_desc = list(result.scalars().all())
        return dialog, list(reversed(messages_desc))

    async def get_dialog(
        self,
        *,
        dialog_id: UUID,
        user_id: UUID,
    ) -> PipelineDialog:
        return await self._get_dialog_owned_by_user(dialog_id=dialog_id, user_id=user_id)

    async def append_user_message(
        self,
        *,
        dialog_id: UUID,
        user_id: UUID,
        content: str,
    ) -> PipelineDialogMessage:
        return await self._append_message(
            dialog_id=dialog_id,
            user_id=user_id,
            role=DialogMessageRole.USER,
            content=content,
            assistant_payload=None,
            create_dialog_if_missing=True,
        )

    async def append_assistant_message(
        self,
        *,
        dialog_id: UUID,
        user_id: UUID,
        content: str,
        assistant_payload: dict[str, Any],
    ) -> PipelineDialogMessage:
        return await self._append_message(
            dialog_id=dialog_id,
            user_id=user_id,
            role=DialogMessageRole.ASSISTANT,
            content=content,
            assistant_payload=assistant_payload,
            create_dialog_if_missing=False,
        )

    async def _append_message(
        self,
        *,
        dialog_id: UUID,
        user_id: UUID,
        role: DialogMessageRole,
        content: str,
        assistant_payload: dict[str, Any] | None,
        create_dialog_if_missing: bool,
    ) -> PipelineDialogMessage:
        dialog = await self.session.get(PipelineDialog, dialog_id)
        if dialog is None:
            if not create_dialog_if_missing:
                raise DialogAccessError("Dialog not found")
            dialog = PipelineDialog(
                id=dialog_id,
                user_id=user_id,
                title=self._build_title(content),
            )
            self.session.add(dialog)
            await self.session.flush()
        elif dialog.user_id != user_id:
            raise DialogAccessError("Dialog access denied")

        if role == DialogMessageRole.USER and not dialog.title:
            dialog.title = self._build_title(content)

        message = PipelineDialogMessage(
            dialog_id=dialog.id,
            role=role,
            content=content,
            assistant_payload=assistant_payload,
        )
        self.session.add(message)

        dialog.last_message_preview = self._build_preview(content)
        if role == DialogMessageRole.ASSISTANT and assistant_payload:
            status = assistant_payload.get("status")
            if isinstance(status, str):
                dialog.last_status = status
            pipeline_id = self._parse_uuid(assistant_payload.get("pipeline_id"))
            if pipeline_id is not None:
                # Preserve the last valid graph reference for non-ready statuses.
                dialog.last_pipeline_id = pipeline_id

        await self.session.commit()
        return message

    async def _get_dialog_owned_by_user(
        self,
        *,
        dialog_id: UUID,
        user_id: UUID,
    ) -> PipelineDialog:
        dialog = await self.session.get(PipelineDialog, dialog_id)
        if dialog is None:
            raise DialogAccessError("Dialog not found")
        if dialog.user_id != user_id:
            raise DialogAccessError("Dialog access denied")
        return dialog

    def _build_title(self, content: str) -> str:
        text = (content or "").strip().replace("\n", " ")
        return (text[:120] or "Pipeline dialog")

    def _build_preview(self, content: str) -> str:
        text = (content or "").strip().replace("\n", " ")
        return text[:280]

    def _parse_uuid(self, value: Any) -> UUID | None:
        if isinstance(value, UUID):
            return value
        if isinstance(value, str):
            try:
                return UUID(value)
            except ValueError:
                return None
        return None
