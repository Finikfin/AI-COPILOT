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
            summary = await self._safe_append_and_summarize(str(dialog_id), "user", message)
            return {
                "status": "needs_input",
                "message_ru": "Сейчас у меня нет подходящих capability. Добавьте Swagger/OpenAPI или документы по API.",
                "pipeline_id": None,
                "nodes": [],
                "edges": [],
                "missing_requirements": ["swagger_or_openapi"],
                "context_summary": summary,
            }

        history, summary = await self._safe_get_context(str(dialog_id))
        llm_payload = None
        try:
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
        except Exception:
            llm_payload = None

        response = self._normalize_llm_response(llm_payload, capabilities)
        if response["status"] != "ready":
            heuristic_response = self._build_heuristic_response(message, capabilities)
            if heuristic_response is not None:
                response = heuristic_response

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

        updated_summary = await self._safe_append_and_summarize(str(dialog_id), "user", message)
        assistant_summary_line = response["message_ru"]
        updated_summary = await self._safe_append_and_summarize(str(dialog_id), "assistant", assistant_summary_line)
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
        try:
            await self.dialog_memory.reset(str(dialog_id))
        except Exception:
            pass
        return {
            "status": "ok",
            "message_ru": "Контекст диалога сброшен.",
        }

    async def _safe_get_context(self, dialog_id: str) -> tuple[list[dict[str, Any]], str | None]:
        try:
            return await self.dialog_memory.get_context(dialog_id)
        except Exception:
            return [], None

    async def _safe_append_and_summarize(self, dialog_id: str, role: str, content: str) -> str | None:
        try:
            return await self.dialog_memory.append_and_summarize(dialog_id, role, content)
        except Exception:
            return None

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
            return self._cannot_build_response(
                "Я не смог собрать корректный граф. Уточните задачу или добавьте больше OpenAPI/Swagger."
            )

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
                action_id=capability.action_id,
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

    def _build_heuristic_response(
        self,
        message: str,
        capabilities: list[Capability],
    ) -> dict[str, Any] | None:
        lowered_message = message.lower()
        need_users = any(token in lowered_message for token in ("user", "users", "юзер", "пользоват", "клиент"))
        need_hotels = any(token in lowered_message for token in ("hotel", "hotels", "отел", "гостиниц"))
        need_email = any(token in lowered_message for token in ("email", "e-mail", "mail", "почт", "оффер", "offer", "рассыл"))

        if not any((need_users, need_hotels, need_email)):
            return None

        role_keywords = {
            "users": ("user", "users", "юзер", "пользоват", "client", "customer"),
            "hotels": ("hotel", "hotels", "отел", "гостиниц"),
            "segments": ("segment", "segments", "сегмент", "cluster", "group"),
            "assignments": ("assign", "assignment", "распредел", "подбор", "match", "allocate"),
            "emails": ("email", "mail", "почт", "offer", "оффер", "рассыл", "campaign"),
        }

        selected: dict[str, Capability] = {}
        for role, keywords in role_keywords.items():
            capability = self._best_capability_for_role(capabilities, keywords)
            if capability is not None:
                selected[role] = capability

        missing: list[str] = []
        if need_users and "users" not in selected:
            missing.append("users_source")
        if need_hotels and "hotels" not in selected:
            missing.append("hotels_source")
        if need_email and "emails" not in selected:
            missing.append("email_sender")
        if need_users and need_hotels and "assignments" not in selected and "segments" not in selected:
            missing.append("user_hotel_matching")

        if missing:
            return {
                "status": "needs_input",
                "message_ru": "Не хватает capability для сборки графа. Добавьте Swagger/OpenAPI для недостающих шагов.",
                "nodes": [],
                "edges": [],
                "missing_requirements": missing,
                "pipeline_name": None,
                "pipeline_description": None,
            }

        ordered_roles: list[str] = []
        if need_users and "users" in selected:
            ordered_roles.append("users")
        if need_hotels and "hotels" in selected:
            ordered_roles.append("hotels")
        if "segments" in selected and "users" in selected:
            ordered_roles.append("segments")
        if need_users and need_hotels and "assignments" in selected:
            ordered_roles.append("assignments")
        if need_email and "emails" in selected:
            ordered_roles.append("emails")

        if len(ordered_roles) < 2:
            return None

        # Удаляем дубли по capability_id, чтобы не создавать повторяющиеся узлы.
        unique_roles: list[str] = []
        seen_capability_ids: set[UUID] = set()
        for role in ordered_roles:
            capability = selected.get(role)
            if capability is None or capability.id in seen_capability_ids:
                continue
            unique_roles.append(role)
            seen_capability_ids.add(capability.id)

        if len(unique_roles) < 2:
            return None

        node_by_role: dict[str, dict[str, Any]] = {}
        nodes: list[dict[str, Any]] = []
        x_position = 0.0
        for index, role in enumerate(unique_roles, start=1):
            capability = selected[role]
            node = PipelineGraphNode(
                id=f"node_{index}",
                capability_id=capability.id,
                action_id=capability.action_id,
                label=self._build_node_label(role, capability),
                description=capability.description,
                input_mapping=self._build_input_mapping(role, node_by_role),
                position={"x": x_position, "y": 0.0},
            )
            dumped = node.model_dump()
            node_by_role[role] = dumped
            nodes.append(dumped)
            x_position += 320.0

        edges: list[dict[str, Any]] = self._build_heuristic_edges(node_by_role, unique_roles)
        if not edges:
            return None

        return {
            "status": "ready",
            "message_ru": "Собрал граф по задаче: отбор пользователей, подбор отелей и отправка офферов на email.",
            "nodes": nodes,
            "edges": edges,
            "missing_requirements": [],
            "pipeline_name": "Подбор отелей и email-офферы",
            "pipeline_description": "Автосборка пайплайна из capability: пользователи -> отели -> распределение -> email.",
        }

    def _best_capability_for_role(
        self,
        capabilities: list[Capability],
        keywords: tuple[str, ...],
    ) -> Capability | None:
        scored: list[tuple[int, Capability]] = []
        for capability in capabilities:
            blob = self._capability_blob(capability)
            score = 0
            for keyword in keywords:
                if keyword in blob:
                    score += 2 if keyword in capability.name.lower() else 1
            if score > 0:
                scored.append((score, capability))

        if not scored:
            return None
        scored.sort(key=lambda item: item[0], reverse=True)
        return scored[0][1]

    def _capability_blob(self, capability: Capability) -> str:
        fragments = [
            capability.name or "",
            capability.description or "",
            json.dumps(capability.input_schema or {}, ensure_ascii=False),
            json.dumps(capability.output_schema or {}, ensure_ascii=False),
            json.dumps(capability.data_format or {}, ensure_ascii=False),
        ]
        return " ".join(fragments).lower()

    def _build_node_label(self, role: str, capability: Capability) -> str:
        labels = {
            "users": "Отобрать пользователей",
            "hotels": "Подобрать отели",
            "segments": "Сегментировать пользователей",
            "assignments": "Распределить пользователей по отелям",
            "emails": "Отправить офферы на email",
        }
        return labels.get(role, capability.name)

    def _build_input_mapping(self, role: str, node_by_role: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
        users_node = node_by_role.get("users")
        hotels_node = node_by_role.get("hotels")
        segments_node = node_by_role.get("segments")
        assignments_node = node_by_role.get("assignments")

        if role == "segments" and users_node:
            return {"users": f"{{{{{users_node['id']}.output.users}}}}"}
        if role == "assignments":
            payload: dict[str, Any] = {}
            if segments_node:
                payload["segments"] = f"{{{{{segments_node['id']}.output.segments}}}}"
            elif users_node:
                payload["users"] = f"{{{{{users_node['id']}.output.users}}}}"
            if hotels_node:
                payload["hotels"] = f"{{{{{hotels_node['id']}.output.hotels}}}}"
            return payload or None
        if role == "emails":
            payload = {}
            if assignments_node:
                payload["assignments"] = f"{{{{{assignments_node['id']}.output.assignments}}}}"
            if users_node:
                payload["users"] = f"{{{{{users_node['id']}.output.users}}}}"
            return payload or None
        return None

    def _build_heuristic_edges(
        self,
        node_by_role: dict[str, dict[str, Any]],
        unique_roles: list[str],
    ) -> list[dict[str, Any]]:
        edges: list[dict[str, Any]] = []
        edge_index = 1

        def add_edge(source_role: str, target_role: str) -> None:
            nonlocal edge_index
            source_node = node_by_role.get(source_role)
            target_node = node_by_role.get(target_role)
            if source_node is None or target_node is None:
                return
            edge = PipelineGraphEdge(
                id=f"edge_{edge_index}",
                source=source_node["id"],
                target=target_node["id"],
                condition=None,
            )
            edges.append(edge.model_dump())
            edge_index += 1

        add_edge("users", "segments")
        add_edge("users", "assignments")
        add_edge("hotels", "assignments")
        add_edge("segments", "assignments")
        add_edge("assignments", "emails")
        add_edge("users", "emails")

        if edges:
            return edges

        # Линейный fallback, если роли нестандартные.
        for index in range(len(unique_roles) - 1):
            add_edge(unique_roles[index], unique_roles[index + 1])
        return edges

    def _cannot_build_response(self, message_ru: str) -> dict[str, Any]:
        return {
            "status": "cannot_build",
            "message_ru": message_ru,
            "nodes": [],
            "edges": [],
            "missing_requirements": ["insufficient_information"],
            "pipeline_name": None,
            "pipeline_description": None,
        }
