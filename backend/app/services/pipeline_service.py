from __future__ import annotations

import json
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Pipeline, PipelineStatus
from app.services.capability_service import CapabilityService
from app.services.dialog_memory import DialogMemoryService
from app.services.semantic_selection import SelectedCapability, SemanticSelectionService
from app.utils.ollama_client import chat_json, reset_model_session


class PipelineServiceError(Exception):
    pass


class PipelineService:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.capability_service = CapabilityService(session)
        self.semantic_selector = SemanticSelectionService()
        self.dialog_memory = DialogMemoryService()

    async def reset_dialog(self, dialog_id: UUID) -> dict[str, Any]:
        await self.dialog_memory.reset(str(dialog_id))
        return {
            "status": "ok",
            "message_ru": "Диалог сброшен.",
        }

    async def generate(
        self,
        *,
        dialog_id: UUID,
        message: str,
        user_id: UUID | None = None,
        capability_ids: list[UUID] | None = None,
    ) -> dict[str, Any]:
        if capability_ids:
            capabilities = await self.capability_service.get_capabilities(capability_ids=capability_ids)
            selected_capabilities = [SelectedCapability(capability=c, score=1.0) for c in capabilities]
        else:
            selected_capabilities = await self.semantic_selector.select_capabilities(
                self.session,
                message,
                limit=10,
            )

        if not selected_capabilities:
            return {
                "status": "needs_input",
                "message_ru": "Не удалось найти подходящих инструментов. Добавьте OpenAPI/Swagger.",
                "chat_reply_ru": "Пожалуйста, загрузите OpenAPI/Swagger спецификацию, чтобы я мог построить сценарий.",
                "nodes": [],
                "edges": [],
                "context_summary": None,
            }

        dialog_messages, dialog_summary = await self.dialog_memory.get_context(str(dialog_id))
        prompt = self._build_generation_prompt(
            user_query=message,
            selected_capabilities=selected_capabilities,
            dialog_messages=dialog_messages,
            dialog_summary=dialog_summary,
        )

        try:
            raw_graph = self.generate_raw_graph(message, selected_capabilities, prompt)
        except PipelineServiceError as exc:
            return {
                "status": "cannot_build",
                "message_ru": str(exc),
                "chat_reply_ru": "Не удалось построить сценарий. Попробуйте уточнить запрос.",
                "nodes": [],
                "edges": [],
                "context_summary": dialog_summary,
            }

        normalized_nodes, normalized_edges = self._normalize_workflow(raw_graph, selected_capabilities)
        normalized_edges = self._repair_edges_with_data_flow(normalized_nodes, normalized_edges)
        self._ensure_external_inputs(normalized_nodes, normalized_edges)

        is_ready, missing = self._validate_ready_graph(normalized_nodes, normalized_edges)
        chat_reply = self._build_chat_reply_ru(normalized_nodes, normalized_edges)

        if not is_ready:
            await self.dialog_memory.append_and_summarize(str(dialog_id), "user", message)
            await self.dialog_memory.append_and_summarize(str(dialog_id), "assistant", chat_reply)
            return {
                "status": "cannot_build",
                "message_ru": "Сценарий не готов к выполнению. Не хватает входных данных.",
                "chat_reply_ru": chat_reply,
                "nodes": normalized_nodes,
                "edges": normalized_edges,
                "missing_requirements": missing,
                "context_summary": dialog_summary,
            }

        pipeline = Pipeline(
            name=self._build_pipeline_name(message),
            description=None,
            user_prompt=message,
            nodes=normalized_nodes,
            edges=normalized_edges,
            status=PipelineStatus.READY,
            created_by=user_id,
        )
        self.session.add(pipeline)
        await self.session.flush()
        await self.session.refresh(pipeline)
        await self.session.commit()

        await self.dialog_memory.append_and_summarize(str(dialog_id), "user", message)
        await self.dialog_memory.append_and_summarize(str(dialog_id), "assistant", chat_reply)

        return {
            "status": "ready",
            "message_ru": "Сценарий готов.",
            "chat_reply_ru": chat_reply,
            "pipeline_id": pipeline.id,
            "nodes": normalized_nodes,
            "edges": normalized_edges,
            "context_summary": dialog_summary,
        }

    def generate_raw_graph(
        self,
        user_query: str,
        selected_capabilities: list[SelectedCapability],
        prompt: str,
    ) -> dict[str, Any]:
        system_prompt = (
            "You are an assistant that generates pipeline graphs. "
            "Return ONLY valid JSON without markdown or comments."
        )
        reset_model_session()
        payload = chat_json(system_prompt=system_prompt, user_prompt=prompt)
        if not isinstance(payload, dict):
            raise PipelineServiceError("Failed to call Ollama")
        return payload

    def _build_generation_prompt(
        self,
        *,
        user_query: str,
        selected_capabilities: list[SelectedCapability],
        dialog_messages: list[dict[str, Any]],
        dialog_summary: str | None,
    ) -> str:
        capabilities_payload = []
        for sc in selected_capabilities:
            cap = sc.capability
            capabilities_payload.append(
                {
                    "id": str(cap.id),
                    "action_id": str(cap.action_id),
                    "name": cap.name,
                    "description": cap.description,
                    "input_type": cap.input_schema,
                    "output_type": cap.output_schema,
                    "required_inputs": self._extract_required_inputs(cap.input_schema),
                    "data_format": cap.data_format,
                }
            )

        context_payload = {
            "summary": dialog_summary,
            "recent_messages": dialog_messages[-6:],
        }

        instruction = (
            "Сгенерируй граф сценария для запроса пользователя. "
            "Ответь ТОЛЬКО валидным JSON без текста и markdown. "
            "Граф — DAG: связи только по данным, не по порядку. "
            "Если узлу нужно несколько входов — создай несколько связей."
        )

        return (
            f"{instruction}\n\n"
            f"USER_QUERY:\n{user_query}\n\n"
            f"DIALOG_CONTEXT:\n{json.dumps(context_payload, ensure_ascii=False)}\n\n"
            f"CAPABILITIES:\n{json.dumps(capabilities_payload, ensure_ascii=False)}\n\n"
            "OUTPUT_SCHEMA:\n"
            "{\n"
            "  \"nodes\": [\n"
            "    {\n"
            "      \"step\": 1,\n"
            "      \"name\": \"Step name\",\n"
            "      \"description\": \"What this step does\",\n"
            "      \"capability_id\": \"UUID\",\n"
            "      \"input_connected_from\": [],\n"
            "      \"output_connected_to\": [],\n"
            "      \"input_data_type_from_previous\": [],\n"
            "      \"external_inputs\": [],\n"
            "      \"endpoints\": []\n"
            "    }\n"
            "  ],\n"
            "  \"edges\": [\n"
            "    {\n"
            "      \"from_step\": 1,\n"
            "      \"to_step\": 2,\n"
            "      \"type\": \"field_name_or_type\"\n"
            "    }\n"
            "  ],\n"
            "  \"variable_injections\": []\n"
            "}\n"
        )

    def _normalize_workflow(
        self,
        raw_graph: dict[str, Any],
        selected_capabilities: list[SelectedCapability],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        capabilities_by_id = {
            str(sc.capability.id): sc.capability for sc in selected_capabilities
        }

        raw_nodes = raw_graph.get("nodes") if isinstance(raw_graph, dict) else None
        if not isinstance(raw_nodes, list):
            raw_nodes = []

        normalized_nodes: list[dict[str, Any]] = []
        step_map: dict[Any, int] = {}
        next_step = 1

        for raw_node in raw_nodes:
            if not isinstance(raw_node, dict):
                continue

            raw_step = raw_node.get("step") or raw_node.get("id")
            if isinstance(raw_step, int):
                step = raw_step
            else:
                step = next_step
                next_step += 1
            step_map[raw_step] = step

            capability_id = raw_node.get("capability_id")
            if not capability_id and isinstance(raw_node.get("endpoints"), list):
                endpoints = raw_node.get("endpoints")
                if endpoints and isinstance(endpoints[0], dict):
                    capability_id = endpoints[0].get("capability_id")

            cap = capabilities_by_id.get(str(capability_id)) if capability_id else None
            if cap is None:
                endpoints_payload = []
            else:
                endpoints_payload = [
                    {
                        "name": cap.name,
                        "capability_id": str(cap.id),
                        "action_id": str(cap.action_id),
                        "input_type": cap.input_schema,
                        "output_type": cap.output_schema,
                    }
                ]

            normalized_nodes.append(
                {
                    "step": step,
                    "name": raw_node.get("name") or (cap.name if cap else f"Шаг {step}"),
                    "description": raw_node.get("description"),
                    "input_connected_from": self._normalize_int_list(raw_node.get("input_connected_from")),
                    "output_connected_to": self._normalize_int_list(raw_node.get("output_connected_to")),
                    "input_data_type_from_previous": self._normalize_input_data_types(raw_node.get("input_data_type_from_previous")),
                    "external_inputs": self._normalize_str_list(raw_node.get("external_inputs")),
                    "endpoints": endpoints_payload,
                }
            )

        raw_edges = raw_graph.get("edges") if isinstance(raw_graph, dict) else None
        if not isinstance(raw_edges, list):
            raw_edges = []

        normalized_edges: list[dict[str, Any]] = []
        for raw_edge in raw_edges:
            if not isinstance(raw_edge, dict):
                continue
            from_step = raw_edge.get("from_step") or raw_edge.get("from") or raw_edge.get("source")
            to_step = raw_edge.get("to_step") or raw_edge.get("to") or raw_edge.get("target")
            edge_type = raw_edge.get("type")
            if from_step in step_map:
                from_step = step_map[from_step]
            if to_step in step_map:
                to_step = step_map[to_step]
            if not isinstance(from_step, int) or not isinstance(to_step, int):
                continue
            if not isinstance(edge_type, str) or not edge_type.strip():
                continue
            normalized_edges.append(
                {
                    "from_step": from_step,
                    "to_step": to_step,
                    "type": edge_type.strip(),
                }
            )

        return normalized_nodes, normalized_edges

    def _repair_edges_with_data_flow(
        self,
        nodes: list[dict[str, Any]],
        edges: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        existing = {(e["from_step"], e["to_step"], e["type"]) for e in edges}
        node_by_step = {n["step"]: n for n in nodes if isinstance(n.get("step"), int)}

        def add_edge(from_step: int, to_step: int, edge_type: str) -> None:
            if (from_step, to_step, edge_type) in existing:
                return
            if self._edge_creates_cycle(edges, from_step, to_step):
                return
            edges.append({"from_step": from_step, "to_step": to_step, "type": edge_type})
            existing.add((from_step, to_step, edge_type))

        for from_step, from_node in node_by_step.items():
            output_type = self._extract_primary_type(from_node, "output_type")
            if not output_type:
                continue
            for to_step, to_node in node_by_step.items():
                if from_step == to_step:
                    continue
                input_type = self._extract_primary_type(to_node, "input_type")
                if output_type == input_type:
                    add_edge(from_step, to_step, output_type)

        return edges

    def _ensure_external_inputs(
        self,
        nodes: list[dict[str, Any]],
        edges: list[dict[str, Any]],
    ) -> None:
        edges_by_target: dict[int, set[str]] = {}
        for edge in edges:
            to_step = edge.get("to_step")
            edge_type = edge.get("type")
            if isinstance(to_step, int) and isinstance(edge_type, str):
                edges_by_target.setdefault(to_step, set()).add(edge_type)

        for node in nodes:
            step = node.get("step")
            required_inputs = self._extract_required_inputs_from_node(node)
            if not required_inputs:
                continue
            external_inputs = set(self._normalize_str_list(node.get("external_inputs")))
            incoming_types = edges_by_target.get(step, set())
            for required_input in required_inputs:
                if required_input in incoming_types:
                    continue
                external_inputs.add(required_input)
            node["external_inputs"] = sorted(external_inputs)

    def _validate_ready_graph(
        self,
        nodes: list[dict[str, Any]],
        edges: list[dict[str, Any]],
    ) -> tuple[bool, list[str]]:
        missing: list[str] = []
        edges_by_target: dict[int, set[str]] = {}
        for edge in edges:
            to_step = edge.get("to_step")
            edge_type = edge.get("type")
            if isinstance(to_step, int) and isinstance(edge_type, str):
                edges_by_target.setdefault(to_step, set()).add(edge_type)

        for node in nodes:
            step = node.get("step")
            endpoints = node.get("endpoints") or []
            if not endpoints:
                missing.append(f"node_{step}: missing_endpoint")
                continue

            for endpoint in endpoints:
                if not endpoint.get("capability_id") or not endpoint.get("action_id"):
                    missing.append(f"node_{step}: invalid_endpoint")

            required_inputs = self._extract_required_inputs_from_node(node)
            if not required_inputs:
                continue

            external_inputs = set(self._normalize_str_list(node.get("external_inputs")))
            incoming_types = edges_by_target.get(step, set())
            for required_input in required_inputs:
                if required_input in external_inputs:
                    continue
                if required_input in incoming_types:
                    continue
                missing.append(f"node_{step}: missing_input:{required_input}")

        return len(missing) == 0, missing

    def _build_chat_reply_ru(self, nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> str:
        if not nodes:
            return "Мне не удалось построить шаги выполнения."

        steps = sorted(nodes, key=lambda n: n.get("step", 0))
        is_linear = self._is_linear_chain(steps, edges)

        if is_linear:
            names = [f"{n.get('step')}. {n.get('name')}" for n in steps]
            return "План выполнения: " + " -> ".join(names)

        lines = ["План выполнения:"]
        for node in steps:
            lines.append(f"Шаг {node.get('step')}: {node.get('name')}")
        return "\n".join(lines)

    def _build_pipeline_name(self, message: str) -> str:
        cleaned = message.strip().split("\n", 1)[0]
        return cleaned[:120] if cleaned else "Generated pipeline"

    def _extract_primary_type(self, node: dict[str, Any], field: str) -> str | None:
        endpoints = node.get("endpoints") or []
        if not endpoints:
            return None
        endpoint = endpoints[0]
        raw = endpoint.get(field)
        if isinstance(raw, str):
            return raw
        if isinstance(raw, dict):
            return raw.get("type") if isinstance(raw.get("type"), str) else None
        return None

    def _extract_required_inputs(self, input_schema: dict[str, Any] | None) -> list[str]:
        if not isinstance(input_schema, dict):
            return []
        required = input_schema.get("required")
        if isinstance(required, list):
            return [str(item) for item in required if item]
        return []

    def _extract_required_inputs_from_node(self, node: dict[str, Any]) -> list[str]:
        endpoints = node.get("endpoints") or []
        if not endpoints:
            return []
        input_type = endpoints[0].get("input_type")
        return self._extract_required_inputs(input_type) if isinstance(input_type, dict) else []

    def _normalize_int_list(self, value: Any) -> list[int]:
        if not isinstance(value, list):
            return []
        result = []
        for item in value:
            if isinstance(item, int):
                result.append(item)
        return result

    def _normalize_input_data_types(self, value: Any) -> list[dict[str, Any]]:
        if not isinstance(value, list):
            return []
        result = []
        for item in value:
            if not isinstance(item, dict):
                continue
            from_step = item.get("from_step") or item.get("step")
            edge_type = item.get("type") or item.get("output") or item.get("data_type")
            if isinstance(from_step, int) and isinstance(edge_type, str):
                result.append({"from_step": from_step, "type": edge_type})
        return result

    def _normalize_str_list(self, value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        return [str(item) for item in value if isinstance(item, (str, int))]

    def _edge_creates_cycle(self, edges: list[dict[str, Any]], from_step: int, to_step: int) -> bool:
        adjacency: dict[int, set[int]] = {}
        for edge in edges:
            src = edge.get("from_step")
            dst = edge.get("to_step")
            if isinstance(src, int) and isinstance(dst, int):
                adjacency.setdefault(src, set()).add(dst)
        adjacency.setdefault(from_step, set()).add(to_step)

        visiting: set[int] = set()
        visited: set[int] = set()

        def dfs(node: int) -> bool:
            if node in visiting:
                return True
            if node in visited:
                return False
            visiting.add(node)
            for neighbor in adjacency.get(node, set()):
                if dfs(neighbor):
                    return True
            visiting.remove(node)
            visited.add(node)
            return False

        return any(dfs(node) for node in list(adjacency.keys()))

    def _is_linear_chain(self, nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> bool:
        if not nodes:
            return False
        if len(nodes) == 1:
            return True

        outgoing = {n["step"]: set() for n in nodes}
        incoming = {n["step"]: set() for n in nodes}
        for edge in edges:
            src = edge.get("from_step")
            dst = edge.get("to_step")
            if src in outgoing and dst in incoming:
                outgoing[src].add(dst)
                incoming[dst].add(src)

        for step in outgoing:
            if len(outgoing[step]) > 1:
                return False
        for step in incoming:
            if len(incoming[step]) > 1:
                return False

        return True
