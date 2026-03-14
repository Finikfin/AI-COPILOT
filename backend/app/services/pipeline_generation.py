from __future__ import annotations

import json
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Capability, Pipeline, PipelineStatus
from app.schemas.pipeline_chat_sch import PipelineGraphEdge, PipelineGraphNode
from app.services.capability_service import CapabilityService
from app.services.dialog_memory import DialogMemoryService
from app.utils.ollama_client import chat_json


class PipelineGenerationService:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.dialog_memory = DialogMemoryService()

    async def generate(
        self,
        dialog_id: UUID,
        message: str,
        user_id: UUID | None = None,
        capability_ids: list[UUID] | None = None,
    ) -> dict[str, Any]:
        capabilities = await self._load_capabilities(capability_ids)
        if not capabilities:
            summary = await self.dialog_memory.append_and_summarize(str(dialog_id), "user", message)
            return {
                "status": "needs_input",
                "message_ru": "Сейчас у меня нет подходящих capability. Добавьте Swagger/OpenAPI или документы по API.",
                "pipeline_id": None,
                "nodes": [],
                "edges": [],
                "missing_requirements": ["swagger_or_openapi"],
                "context_summary": summary,
            }

        history, summary = await self.dialog_memory.get_context(str(dialog_id))
        llm_payload = chat_json(
            system_prompt=(
                "Ты строишь pipeline-граф по бизнес-задаче. "
                "Верни только JSON с полями: "
                "status, message_ru, nodes, edges, missing_requirements, pipeline_name, pipeline_description. "
                "status может быть только ready, needs_input, cannot_build. "
                "Все объяснения для пользователя должны быть на русском."
            ),
            user_prompt=self._build_generation_prompt(
                summary=summary,
                message=message,
                capabilities=capabilities,
                history=history,
            ),
        )

        response = self._normalize_llm_response(llm_payload, capabilities)
        if response["status"] == "ready":
            pipeline = await self._save_pipeline(
                message=message,
                user_id=user_id,
                pipeline_name=response["pipeline_name"],
                pipeline_description=response["pipeline_description"],
                nodes=response["nodes"],
                edges=response["edges"],
            )
            response["pipeline_id"] = pipeline.id
        else:
            response["pipeline_id"] = None

        updated_summary = await self.dialog_memory.append_and_summarize(str(dialog_id), "user", message)
        assistant_summary_line = response["message_ru"]
        updated_summary = await self.dialog_memory.append_and_summarize(str(dialog_id), "assistant", assistant_summary_line)
        response["context_summary"] = updated_summary or summary
        return {
            "status": response["status"],
            "message_ru": response["message_ru"],
            "pipeline_id": response["pipeline_id"],
            "nodes": response["nodes"],
            "edges": response["edges"],
            "missing_requirements": response["missing_requirements"],
            "context_summary": response["context_summary"],
        }

    async def reset_dialog(self, dialog_id: UUID) -> dict[str, str]:
        await self.dialog_memory.reset(str(dialog_id))
        return {
            "status": "ok",
            "message_ru": "Контекст диалога сброшен.",
        }

    async def _load_capabilities(self, capability_ids: list[UUID] | None) -> list[Capability]:
        capability_service = CapabilityService(self.session)
        return await capability_service.get_capabilities(capability_ids=capability_ids)

    def _build_generation_prompt(
        self,
        *,
        summary: str | None,
        message: str,
        capabilities: list[Capability],
        history: list[dict[str, Any]],
    ) -> str:
        capabilities_payload = [
            {
                "id": str(capability.id),
                "name": capability.name,
                "description": capability.description,
                "input_schema": capability.input_schema,
                "output_schema": capability.output_schema,
                "data_format": capability.data_format,
            }
            for capability in capabilities
        ]
        payload = {
            "dialog_summary": summary,
            "recent_history": history[-6:],
            "user_message": message,
            "capabilities": capabilities_payload,
        }
        return json.dumps(payload, ensure_ascii=False, indent=2)

    def _normalize_llm_response(
        self,
        payload: dict[str, Any] | None,
        capabilities: list[Capability],
    ) -> dict[str, Any]:
        capability_by_id = {str(capability.id): capability for capability in capabilities}
        if not isinstance(payload, dict):
            return {
                "status": "cannot_build",
                "message_ru": "Я не смог собрать корректный граф. Уточните задачу или добавьте больше OpenAPI/Swagger.",
                "nodes": [],
                "edges": [],
                "missing_requirements": ["insufficient_information"],
                "pipeline_name": None,
                "pipeline_description": None,
            }

        status = payload.get("status")
        if status not in {"ready", "needs_input", "cannot_build"}:
            status = "cannot_build"

        nodes = self._normalize_nodes(payload.get("nodes"), capability_by_id)
        edges = self._normalize_edges(payload.get("edges"), {node["id"] for node in nodes})
        missing_requirements = self._normalize_string_list(payload.get("missing_requirements"))
        message_ru = payload.get("message_ru")
        if not isinstance(message_ru, str) or not message_ru.strip():
            message_ru = self._default_message(status)

        if status == "ready" and (not nodes or not edges):
            status = "cannot_build"
            message_ru = "Я не смог собрать исполнимый граф. Уточните задачу или добавьте больше API-контекста."

        if status != "ready":
            nodes = []
            edges = []

        return {
            "status": status,
            "message_ru": message_ru,
            "nodes": nodes,
            "edges": edges,
            "missing_requirements": missing_requirements,
            "pipeline_name": payload.get("pipeline_name"),
            "pipeline_description": payload.get("pipeline_description"),
        }

    def _normalize_nodes(
        self,
        payload: Any,
        capability_by_id: dict[str, Capability],
    ) -> list[dict[str, Any]]:
        if not isinstance(payload, list):
            return []
        nodes: list[dict[str, Any]] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            capability_id = str(item.get("capability_id", ""))
            capability = capability_by_id.get(capability_id)
            node_id = item.get("id")
            if capability is None or not isinstance(node_id, str) or not node_id:
                continue
            position = item.get("position")
            normalized_position = None
            if isinstance(position, dict):
                x = position.get("x")
                y = position.get("y")
                if isinstance(x, (int, float)) and isinstance(y, (int, float)):
                    normalized_position = {"x": float(x), "y": float(y)}
            node = PipelineGraphNode(
                id=node_id,
                capability_id=capability.id,
                label=str(item.get("label") or capability.name),
                description=str(item.get("description")) if item.get("description") is not None else capability.description,
                input_mapping=item.get("input_mapping") if isinstance(item.get("input_mapping"), dict) else None,
                position=normalized_position,
            )
            nodes.append(node.model_dump())
        return nodes

    def _normalize_edges(self, payload: Any, valid_node_ids: set[str]) -> list[dict[str, Any]]:
        if not isinstance(payload, list):
            return []
        edges: list[dict[str, Any]] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            source = item.get("source")
            target = item.get("target")
            edge_id = item.get("id")
            if not all(isinstance(value, str) and value for value in (source, target, edge_id)):
                continue
            if source not in valid_node_ids or target not in valid_node_ids:
                continue
            edge = PipelineGraphEdge(
                id=edge_id,
                source=source,
                target=target,
                condition=str(item.get("condition")) if item.get("condition") is not None else None,
            )
            edges.append(edge.model_dump())
        return edges

    async def _save_pipeline(
        self,
        *,
        message: str,
        user_id: UUID | None,
        pipeline_name: Any,
        pipeline_description: Any,
        nodes: list[dict[str, Any]],
        edges: list[dict[str, Any]],
    ) -> Pipeline:
        pipeline = Pipeline(
            name=str(pipeline_name or self._default_pipeline_name(message)),
            description=str(pipeline_description) if pipeline_description is not None else None,
            user_prompt=message,
            nodes=nodes,
            edges=edges,
            status=PipelineStatus.DRAFT,
            created_by=user_id,
        )
        self.session.add(pipeline)
        await self.session.commit()
        await self.session.refresh(pipeline)
        return pipeline

    def _default_pipeline_name(self, message: str) -> str:
        compact = " ".join(message.split())
        return compact[:120] or "Generated pipeline"

    def _normalize_string_list(self, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return [str(item) for item in value if item is not None]
        return [str(value)]

    def _default_message(self, status: str) -> str:
        if status == "needs_input":
            return "Мне не хватает данных для построения пайплайна. Добавьте Swagger/OpenAPI или уточните задачу."
        if status == "ready":
            return "Пайплайн успешно собран."
        return "Я не могу реализовать этот сценарий с текущими данными."
