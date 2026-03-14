from __future__ import annotations

import json
import re
from collections import defaultdict, deque
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Action, Capability, Pipeline, PipelineStatus
from app.schemas.pipeline_chat_sch import (
    PipelineGraphEdge,
    PipelineGraphNode,
    PipelineInputTypeFromPrevious,
    PipelineStepEndpoint,
)
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
            message_ru = "Сейчас у меня нет подходящих capability. Добавьте Swagger/OpenAPI или документы по API."
            summary = await self._safe_append_and_summarize(str(dialog_id), "user", message)
            return {
                "status": "needs_input",
                "message_ru": message_ru,
                "chat_reply_ru": self._build_chat_reply(
                    status="needs_input",
                    message_ru=message_ru,
                    nodes=[],
                    missing_requirements=["swagger_or_openapi"],
                ),
                "pipeline_id": None,
                "nodes": [],
                "edges": [],
                "missing_requirements": ["swagger_or_openapi"],
                "context_summary": summary,
            }

        action_by_id = await self._load_actions(capabilities)
        history, summary = await self._safe_get_context(str(dialog_id))

        llm_payload = None
        try:
            llm_payload = chat_json(
                system_prompt=(
                    "Ты проектируешь DAG workflow. "
                    "Возвращай только JSON без markdown и без текста вне JSON."
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

        response = self._normalize_llm_response(llm_payload, capabilities, action_by_id)
        if response["status"] != "ready":
            heuristic_response = self._build_heuristic_response(message, capabilities, action_by_id)
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
        updated_summary = await self._safe_append_and_summarize(str(dialog_id), "assistant", response["message_ru"])
        response["context_summary"] = updated_summary or summary
        response["chat_reply_ru"] = self._build_chat_reply(
            status=response["status"],
            message_ru=response["message_ru"],
            nodes=response["nodes"],
            missing_requirements=response["missing_requirements"],
        )

        return {
            "status": response["status"],
            "message_ru": response["message_ru"],
            "chat_reply_ru": response["chat_reply_ru"],
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

    async def _load_actions(self, capabilities: list[Capability]) -> dict[str, Action]:
        action_ids = [capability.action_id for capability in capabilities if capability.action_id is not None]
        if not action_ids:
            return {}
        result = await self.session.execute(select(Action).where(Action.id.in_(action_ids)))
        actions = list(result.scalars().all())
        return {str(action.id): action for action in actions}

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
            }
            for capability in capabilities
        ]

        data = {
            "business_task": message,
            "dialog_summary": summary,
            "recent_history": history[-6:],
            "capabilities": capabilities_payload,
        }

        instruction = (
            "Дано:\n"
            "- business_task — текст бизнес-задачи\n"
            "- capabilities — список [{id, name, description}]\n\n"
            "Задача:\n"
            "Построить DAG workflow, который решает business_task, используя только подходящие capabilities.\n\n"
            "Правила:\n"
            "- выбирай только нужные capabilities\n"
            "- определяй зависимости по смыслу бизнес-процесса\n"
            "- не строй автоматически линейную цепочку\n"
            "- если шагу нужны результаты нескольких предыдущих шагов, добавь несколько входящих связей\n"
            "- финальный шаг должен напрямую решать business_task\n"
            "- граф должен быть ацикличным\n"
            "- если шагу нужен параметр, который не создаётся предыдущими шагами, добавь его в external_inputs\n"
            "- type у связи — короткое смысловое имя данных (например users, hotels, segments, assignments, offers)\n\n"
            "Верни только JSON:\n"
            "{\n"
            '  "nodes": [\n'
            "    {\n"
            '      "step": 1,\n'
            '      "name": "",\n'
            '      "description": "",\n'
            '      "input_connected_from": [],\n'
            '      "output_connected_to": [],\n'
            '      "input_data_type_from_previous": [],\n'
            '      "external_inputs": [],\n'
            '      "endpoints": [""]\n'
            "    }\n"
            "  ],\n"
            '  "edges": [\n'
            "    {\n"
            '      "from_step": 1,\n'
            '      "to_step": 2,\n'
            '      "type": ""\n'
            "    }\n"
            "  ]\n"
            "}"
        )
        return f"{instruction}\n\nINPUT:\n{json.dumps(data, ensure_ascii=False, indent=2)}"

    def _normalize_llm_response(
        self,
        payload: dict[str, Any] | None,
        capabilities: list[Capability],
        action_by_id: dict[str, Action],
    ) -> dict[str, Any]:
        if not isinstance(payload, dict):
            return self._cannot_build_response(
                "Я не смог собрать корректный граф. Уточните задачу или добавьте больше OpenAPI/Swagger."
            )

        raw_status = payload.get("status")
        status = raw_status if raw_status in {"ready", "needs_input", "cannot_build"} else "ready"
        message_ru = payload.get("message_ru")
        if not isinstance(message_ru, str) or not message_ru.strip():
            message_ru = self._default_message(status)

        if status != "ready" and not payload.get("nodes"):
            return {
                "status": status,
                "message_ru": message_ru,
                "nodes": [],
                "edges": [],
                "missing_requirements": self._normalize_string_list(payload.get("missing_requirements")),
                "pipeline_name": payload.get("pipeline_name"),
                "pipeline_description": payload.get("pipeline_description"),
            }

        normalized = self._normalize_workflow(payload, capabilities, action_by_id)
        if normalized is None:
            return self._cannot_build_response(
                "Я не смог собрать корректный граф. Уточните задачу или добавьте больше OpenAPI/Swagger."
            )

        nodes, edges = normalized
        if not nodes or not edges:
            return self._cannot_build_response(
                "Я не смог собрать исполнимый граф. Уточните задачу или добавьте больше API-контекста."
            )

        return {
            "status": "ready",
            "message_ru": payload.get("message_ru") or "Пайплайн успешно собран.",
            "nodes": nodes,
            "edges": edges,
            "missing_requirements": self._normalize_string_list(payload.get("missing_requirements")),
            "pipeline_name": payload.get("pipeline_name"),
            "pipeline_description": payload.get("pipeline_description"),
        }

    def _normalize_workflow(
        self,
        payload: dict[str, Any],
        capabilities: list[Capability],
        action_by_id: dict[str, Action],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]] | None:
        nodes_payload = payload.get("nodes")
        edges_payload = payload.get("edges")
        if not isinstance(nodes_payload, list) or not isinstance(edges_payload, list):
            return None

        capability_by_id = {str(capability.id): capability for capability in capabilities}

        parsed_nodes: dict[int, dict[str, Any]] = {}
        for node_raw in nodes_payload:
            if not isinstance(node_raw, dict):
                continue
            step = self._to_int(node_raw.get("step"))
            if step is None or step < 1:
                continue

            endpoints = self._normalize_endpoints(
                raw_endpoints=node_raw.get("endpoints"),
                node_name=node_raw.get("name"),
                capabilities=capabilities,
                action_by_id=action_by_id,
            )
            if not endpoints:
                continue

            parsed_nodes[step] = {
                "step": step,
                "name": str(node_raw.get("name") or endpoints[0]["name"]),
                "description": str(node_raw.get("description")) if node_raw.get("description") is not None else None,
                "endpoints": endpoints,
                "external_inputs": self._normalize_string_list(node_raw.get("external_inputs")),
            }

        if not parsed_nodes:
            return None

        parsed_edges: list[dict[str, Any]] = []
        for edge_raw in edges_payload:
            edge = self._normalize_edge(edge_raw, parsed_nodes)
            if edge is not None:
                parsed_edges.append(edge)

        if not parsed_edges and len(parsed_nodes) > 1:
            # Если модель не вернула edges, строим простой safe fallback по порядку шага.
            ordered_steps = sorted(parsed_nodes)
            for index in range(len(ordered_steps) - 1):
                from_step = ordered_steps[index]
                to_step = ordered_steps[index + 1]
                edge_type = self._infer_edge_type_from_node(parsed_nodes[from_step])
                parsed_edges.append(
                    PipelineGraphEdge(from_step=from_step, to_step=to_step, type=edge_type).model_dump()
                )

        parsed_edges = self._dedupe_edges(parsed_edges)
        if self._has_cycle(parsed_nodes.keys(), parsed_edges):
            return None

        incoming_by_step: dict[int, list[int]] = defaultdict(list)
        outgoing_by_step: dict[int, list[int]] = defaultdict(list)
        incoming_types_by_step: dict[int, list[PipelineInputTypeFromPrevious]] = defaultdict(list)

        for edge in parsed_edges:
            from_step = edge["from_step"]
            to_step = edge["to_step"]
            edge_type = edge["type"]
            outgoing_by_step[from_step].append(to_step)
            incoming_by_step[to_step].append(from_step)
            incoming_types_by_step[to_step].append(
                PipelineInputTypeFromPrevious(from_step=from_step, type=edge_type)
            )

        normalized_nodes: list[dict[str, Any]] = []
        for step in sorted(parsed_nodes):
            node = parsed_nodes[step]
            endpoint_capability = capability_by_id.get(str(node["endpoints"][0]["capability_id"]))
            incoming_types = [item.type for item in incoming_types_by_step.get(step, [])]
            external_inputs = node["external_inputs"] or self._infer_external_inputs(endpoint_capability, incoming_types)

            normalized_node = PipelineGraphNode(
                step=step,
                name=node["name"],
                description=node["description"],
                input_connected_from=sorted(set(incoming_by_step.get(step, []))),
                output_connected_to=sorted(set(outgoing_by_step.get(step, []))),
                input_data_type_from_previous=incoming_types_by_step.get(step, []),
                external_inputs=external_inputs,
                endpoints=node["endpoints"],
            )
            normalized_nodes.append(normalized_node.model_dump())

        return normalized_nodes, parsed_edges

    def _normalize_endpoints(
        self,
        *,
        raw_endpoints: Any,
        node_name: Any,
        capabilities: list[Capability],
        action_by_id: dict[str, Action],
    ) -> list[dict[str, Any]]:
        if not isinstance(raw_endpoints, list):
            raw_endpoints = []

        endpoints: list[dict[str, Any]] = []
        for endpoint_raw in raw_endpoints:
            capability = self._resolve_capability_from_endpoint(endpoint_raw, node_name, capabilities)
            if capability is None:
                continue
            action = action_by_id.get(str(capability.action_id))

            if isinstance(endpoint_raw, dict):
                endpoint_name = str(endpoint_raw.get("name") or capability.name)
                input_type = endpoint_raw.get("input_type", self._infer_input_type(capability))
                output_type = endpoint_raw.get("output_type", self._infer_output_type(capability, action))
            elif isinstance(endpoint_raw, str):
                endpoint_name = endpoint_raw
                input_type = self._infer_input_type(capability)
                output_type = self._infer_output_type(capability, action)
            else:
                endpoint_name = capability.name
                input_type = self._infer_input_type(capability)
                output_type = self._infer_output_type(capability, action)

            endpoint = PipelineStepEndpoint(
                name=endpoint_name,
                capability_id=capability.id,
                action_id=capability.action_id,
                input_type=input_type,
                output_type=output_type,
            )
            endpoints.append(endpoint.model_dump())

        if endpoints:
            return endpoints

        fallback_capability = self._find_capability_by_name(capabilities, str(node_name) if node_name is not None else "")
        if fallback_capability is None:
            return []
        action = action_by_id.get(str(fallback_capability.action_id))
        endpoint = PipelineStepEndpoint(
            name=fallback_capability.name,
            capability_id=fallback_capability.id,
            action_id=fallback_capability.action_id,
            input_type=self._infer_input_type(fallback_capability),
            output_type=self._infer_output_type(fallback_capability, action),
        )
        return [endpoint.model_dump()]

    def _resolve_capability_from_endpoint(
        self,
        endpoint_raw: Any,
        node_name: Any,
        capabilities: list[Capability],
    ) -> Capability | None:
        capability_by_id = {str(capability.id): capability for capability in capabilities}

        if isinstance(endpoint_raw, dict):
            capability_id_raw = endpoint_raw.get("capability_id")
            if capability_id_raw is not None:
                capability = capability_by_id.get(str(capability_id_raw))
                if capability is not None:
                    return capability

            endpoint_name = endpoint_raw.get("name")
            if isinstance(endpoint_name, str):
                capability = self._find_capability_by_name(capabilities, endpoint_name)
                if capability is not None:
                    return capability

        if isinstance(endpoint_raw, str):
            capability = self._find_capability_by_name(capabilities, endpoint_raw)
            if capability is not None:
                return capability

        if isinstance(node_name, str):
            return self._find_capability_by_name(capabilities, node_name)
        return None

    def _find_capability_by_name(self, capabilities: list[Capability], name: str) -> Capability | None:
        if not name:
            return None
        candidate = self._normalize_token(name)
        for capability in capabilities:
            cap_name = self._normalize_token(capability.name)
            if candidate == cap_name or candidate in cap_name or cap_name in candidate:
                return capability
        return None

    def _normalize_edge(self, edge_raw: Any, parsed_nodes: dict[int, dict[str, Any]]) -> dict[str, Any] | None:
        if not isinstance(edge_raw, dict):
            return None

        from_step = self._to_int(edge_raw.get("from_step"))
        to_step = self._to_int(edge_raw.get("to_step"))
        if from_step is None or to_step is None:
            return None
        if from_step not in parsed_nodes or to_step not in parsed_nodes:
            return None

        edge_type_raw = edge_raw.get("type")
        edge_type = str(edge_type_raw).strip() if edge_type_raw is not None else ""
        if not edge_type:
            edge_type = self._infer_edge_type_from_node(parsed_nodes[from_step])

        edge = PipelineGraphEdge(from_step=from_step, to_step=to_step, type=edge_type)
        return edge.model_dump()

    def _dedupe_edges(self, edges: list[dict[str, Any]]) -> list[dict[str, Any]]:
        deduped: list[dict[str, Any]] = []
        seen: set[tuple[int, int, str]] = set()
        for edge in edges:
            key = (edge["from_step"], edge["to_step"], edge["type"])
            if key in seen:
                continue
            seen.add(key)
            deduped.append(edge)
        return deduped

    def _has_cycle(self, steps: Any, edges: list[dict[str, Any]]) -> bool:
        step_set = {int(step) for step in steps}
        indegree: dict[int, int] = {step: 0 for step in step_set}
        outgoing: dict[int, list[int]] = defaultdict(list)

        for edge in edges:
            from_step = edge["from_step"]
            to_step = edge["to_step"]
            if from_step not in indegree or to_step not in indegree:
                continue
            outgoing[from_step].append(to_step)
            indegree[to_step] += 1

        queue = deque(step for step, deg in indegree.items() if deg == 0)
        visited = 0
        while queue:
            step = queue.popleft()
            visited += 1
            for nxt in outgoing.get(step, []):
                indegree[nxt] -= 1
                if indegree[nxt] == 0:
                    queue.append(nxt)
        return visited != len(step_set)

    def _infer_edge_type_from_node(self, node: dict[str, Any]) -> str:
        endpoints = node.get("endpoints")
        if isinstance(endpoints, list) and endpoints:
            endpoint = endpoints[0]
            if isinstance(endpoint, dict):
                output_type = endpoint.get("output_type")
                return self._edge_type_from_output(output_type)
        return "data"

    def _edge_type_from_output(self, output_type: Any) -> str:
        if isinstance(output_type, str):
            normalized = output_type.strip().lower()
            normalized = normalized.replace("[]", "")
            normalized = re.sub(r"[^a-z0-9_]+", "", normalized)
            return normalized or "data"
        if isinstance(output_type, dict) and output_type:
            first_key = next(iter(output_type.keys()))
            normalized = self._normalize_token(str(first_key))
            return normalized or "data"
        return "data"

    def _infer_input_type(self, capability: Capability) -> str | dict[str, Any] | None:
        return self._schema_to_type(capability.input_schema, semantic_hint=None)

    def _infer_output_type(self, capability: Capability, action: Action | None) -> str | dict[str, Any] | None:
        semantic_hint = self._semantic_collection_hint(capability.name)
        inferred = self._schema_to_type(capability.output_schema, semantic_hint=None)
        if semantic_hint is not None and (
            inferred is None
            or inferred == "object"
            or (isinstance(inferred, dict) and len(inferred) <= 1)
        ):
            return semantic_hint
        if inferred is not None:
            return inferred
        if action is not None and action.operation_id:
            return self._semantic_collection_hint(action.operation_id)
        return None

    def _semantic_collection_hint(self, text: str | None) -> str | None:
        if not text:
            return None
        normalized = text.lower()
        if "users" in normalized or "user" in normalized or "пользоват" in normalized:
            return "users[]"
        if "hotels" in normalized or "hotel" in normalized or "отел" in normalized:
            return "hotels[]"
        if "segments" in normalized or "segment" in normalized or "сегмент" in normalized:
            return "segments[]"
        if "assignments" in normalized or "assignment" in normalized or "распредел" in normalized:
            return "assignments[]"
        if "offers" in normalized or "offer" in normalized or "оффер" in normalized:
            return "offers[]"
        return None

    def _schema_to_type(self, schema: Any, *, semantic_hint: str | None) -> str | dict[str, Any] | None:
        if semantic_hint:
            return semantic_hint
        if not isinstance(schema, dict):
            return None

        schema_type = schema.get("type")
        if schema_type == "array":
            item_schema = schema.get("items")
            item_type = self._schema_type_scalar("item", item_schema)
            return f"{item_type}[]" if item_type else "array[]"

        if schema_type == "object" or isinstance(schema.get("properties"), dict):
            properties = schema.get("properties")
            if not isinstance(properties, dict) or not properties:
                return "object"
            result: dict[str, Any] = {}
            for key, value in properties.items():
                result[str(key)] = self._schema_type_scalar(str(key), value) or "unknown"
            return result

        if isinstance(schema_type, str):
            return schema_type
        return None

    def _schema_type_scalar(self, key: str, schema: Any) -> str | None:
        if not isinstance(schema, dict):
            return None
        schema_type = schema.get("type")
        if schema_type == "array":
            item_schema = schema.get("items")
            if isinstance(item_schema, dict):
                item_type = item_schema.get("type")
                if isinstance(item_type, str):
                    if item_type == "object":
                        return f"{key}[]"
                    return f"{item_type}[]"
            return f"{key}[]"
        if isinstance(schema_type, str):
            return schema_type
        if isinstance(schema.get("properties"), dict):
            return "object"
        return None

    def _infer_external_inputs(self, capability: Capability | None, incoming_types: list[str]) -> list[str]:
        if capability is None or not isinstance(capability.input_schema, dict):
            return []

        required = capability.input_schema.get("required")
        if not isinstance(required, list):
            return []

        incoming_norm = {self._normalize_token(item) for item in incoming_types}
        external_inputs: list[str] = []
        for field in required:
            field_name = str(field)
            normalized_field = self._normalize_token(field_name)
            covered = any(
                normalized_field == token
                or normalized_field.startswith(token)
                or token.startswith(normalized_field)
                for token in incoming_norm
                if token
            )
            if not covered:
                external_inputs.append(field_name)
        return external_inputs

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

    def _to_int(self, value: Any) -> int | None:
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.strip().isdigit():
            return int(value.strip())
        return None

    def _normalize_token(self, value: str) -> str:
        lowered = value.lower().strip()
        lowered = lowered.replace("[]", "")
        lowered = re.sub(r"[^a-z0-9а-я_]+", "_", lowered)
        return lowered.strip("_")

    def _default_message(self, status: str) -> str:
        if status == "needs_input":
            return "Мне не хватает данных для построения пайплайна. Добавьте Swagger/OpenAPI или уточните задачу."
        if status == "ready":
            return "Пайплайн успешно собран."
        return "Я не могу реализовать этот сценарий с текущими данными."

    def _build_chat_reply(
        self,
        *,
        status: str,
        message_ru: str,
        nodes: list[dict[str, Any]],
        missing_requirements: list[str],
    ) -> str:
        if status == "ready" and nodes:
            ordered = sorted(nodes, key=lambda item: int(item.get("step", 0)))
            names = [str(node.get("name")) for node in ordered if node.get("name")]
            if names:
                chain = " -> ".join(names[:6])
                return f"{message_ru} План шагов: {chain}."
            return message_ru
        if status == "needs_input" and missing_requirements:
            return f"{message_ru} Нужны уточнения: {', '.join(missing_requirements)}."
        return message_ru

    def _build_heuristic_response(
        self,
        message: str,
        capabilities: list[Capability],
        action_by_id: dict[str, Action],
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

        role_to_step = {role: index for index, role in enumerate(unique_roles, start=1)}

        edges: list[dict[str, Any]] = []

        def add_edge(from_role: str, to_role: str, edge_type: str) -> None:
            from_step = role_to_step.get(from_role)
            to_step = role_to_step.get(to_role)
            if from_step is None or to_step is None:
                return
            edge = PipelineGraphEdge(from_step=from_step, to_step=to_step, type=edge_type)
            edges.append(edge.model_dump())

        add_edge("users", "segments", "users")
        add_edge("users", "assignments", "users")
        add_edge("hotels", "assignments", "hotels")
        add_edge("segments", "assignments", "segments")
        add_edge("assignments", "emails", "assignments")

        if not edges:
            for index in range(len(unique_roles) - 1):
                from_role = unique_roles[index]
                to_role = unique_roles[index + 1]
                edge_type = self._edge_type_from_output(self._semantic_collection_hint(from_role) or from_role)
                add_edge(from_role, to_role, edge_type)

        incoming_by_step: dict[int, list[int]] = defaultdict(list)
        outgoing_by_step: dict[int, list[int]] = defaultdict(list)
        incoming_types_by_step: dict[int, list[PipelineInputTypeFromPrevious]] = defaultdict(list)
        for edge in edges:
            from_step = edge["from_step"]
            to_step = edge["to_step"]
            edge_type = edge["type"]
            incoming_by_step[to_step].append(from_step)
            outgoing_by_step[from_step].append(to_step)
            incoming_types_by_step[to_step].append(PipelineInputTypeFromPrevious(from_step=from_step, type=edge_type))

        nodes: list[dict[str, Any]] = []
        for role in unique_roles:
            step = role_to_step[role]
            capability = selected[role]
            action = action_by_id.get(str(capability.action_id))
            endpoint = PipelineStepEndpoint(
                name=capability.name,
                capability_id=capability.id,
                action_id=capability.action_id,
                input_type=self._infer_input_type(capability),
                output_type=self._infer_output_type(capability, action),
            )

            incoming_types = [item.type for item in incoming_types_by_step.get(step, [])]
            node = PipelineGraphNode(
                step=step,
                name=self._build_node_name(role, capability),
                description=capability.description,
                input_connected_from=sorted(set(incoming_by_step.get(step, []))),
                output_connected_to=sorted(set(outgoing_by_step.get(step, []))),
                input_data_type_from_previous=incoming_types_by_step.get(step, []),
                external_inputs=self._infer_external_inputs(capability, incoming_types),
                endpoints=[endpoint],
            )
            nodes.append(node.model_dump())

        edges = self._dedupe_edges(edges)
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

    def _build_node_name(self, role: str, capability: Capability) -> str:
        labels = {
            "users": "get_users_recent",
            "hotels": "get_hotels_top",
            "segments": "post_segments_hotel",
            "assignments": "post_assignments_hotels",
            "emails": "post_emails_send_offers",
        }
        return labels.get(role, capability.name)

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
