from __future__ import annotations

import json
import re
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Pipeline, PipelineStatus
from app.services.capability_service import CapabilityService
from app.services.dialog_memory import DialogMemoryService
from app.services.semantic_selection import SelectedCapability, SemanticSelectionService
from app.utils.llm_client import chat_json, reset_model_session


class PipelineServiceError(Exception):
    pass


class PipelineService:
    # Clarification loop is disabled: service should attempt a full graph in one shot.
    LOW_CONFIDENCE_MAX_QUESTIONS = 0
    LOW_CONFIDENCE_QUESTION_MARKER = "нужно уточнить цель, чтобы построить точный сценарий"
    LOW_CONFIDENCE_DIALOG_MARKER = "[[low_confidence_question]]"
    STRICT_CAPABILITY_ISSUES = {
        "graph:invalid_capability_ref",
        "graph:missing_capability_ref",
    }
    CAPABILITY_SELECTION_LIMIT = 8
    FALLBACK_CAPABILITIES_LIMIT = 8

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
        previous_pipeline_id: UUID | None = None,
    ) -> dict[str, Any]:
        dialog_messages, dialog_summary = await self.dialog_memory.get_context(
            str(dialog_id)
        )

        if self._is_low_quality_message(message):
            return {
                "status": "needs_input",
                "message_ru": "Запрос слишком общий или случайный. Нужна цель сценария.",
                "chat_reply_ru": (
                    "Похоже, в запросе не хватает цели. "
                    "Опишите, какой результат нужен (например: получить активных пользователей, "
                    "сегментировать их и назначить отели)."
                ),
                "nodes": [],
                "edges": [],
                "missing_requirements": ["query:low_quality"],
                "context_summary": dialog_summary,
            }

        if capability_ids:
            try:
                capabilities = await self.capability_service.get_capabilities(
                    capability_ids=capability_ids,
                    owner_user_id=user_id,
                    include_all=False,
                )
            except TypeError:
                # Backward-compatible path for simplified test doubles.
                capabilities = await self.capability_service.get_capabilities(
                    capability_ids=capability_ids,
                )
            if len(capabilities) != len(set(capability_ids)):
                return {
                    "status": "needs_input",
                    "message_ru": "Часть выбранных capabilities недоступна для этого пользователя.",
                    "chat_reply_ru": "Некоторые capabilities вам не принадлежат или были удалены. Выберите доступные.",
                    "nodes": [],
                    "edges": [],
                    "context_summary": None,
                }
            selected_capabilities = [
                SelectedCapability(capability=c, score=1.0, confidence_tier="high")
                for c in capabilities
            ]
        else:
            selection_query = self._build_selection_query(
                message=message,
                dialog_messages=dialog_messages,
                dialog_summary=dialog_summary,
            )
            selected_capabilities = await self.semantic_selector.select_capabilities(
                self.session,
                selection_query,
                owner_user_id=user_id,
                limit=self.CAPABILITY_SELECTION_LIMIT,
            )

        selected_capabilities = self._reorder_selected_capabilities_by_plan(
            message=message,
            selected_capabilities=selected_capabilities,
        )
        selected_capabilities = self._filter_capabilities_by_explicit_plan(
            message=message,
            selected_capabilities=selected_capabilities,
        )
        selected_capabilities = self._filter_capabilities_by_negative_intent(
            message=message,
            selected_capabilities=selected_capabilities,
        )
        selected_capabilities = self._filter_send_capabilities_by_negative_intent(
            message=message,
            selected_capabilities=selected_capabilities,
        )
        selected_capabilities = self._filter_capabilities_by_positive_intent(
            message=message,
            selected_capabilities=selected_capabilities,
        )

        plan_aligned_capabilities = self._select_capabilities_by_explicit_plan_terms(
            message=message,
            selected_capabilities=selected_capabilities,
        )
        use_explicit_plan_mode = len(plan_aligned_capabilities) >= 2
        if use_explicit_plan_mode:
            selected_capabilities = plan_aligned_capabilities

        previous_pipeline: Pipeline | None = None
        previous_nodes: list[dict[str, Any]] = []
        previous_edges: list[dict[str, Any]] = []
        if previous_pipeline_id is not None and self._should_reuse_previous_graph(
            message=message,
            dialog_messages=dialog_messages,
        ):
            candidate = await self.session.get(Pipeline, previous_pipeline_id)
            if candidate is not None and (
                user_id is None or candidate.created_by in (None, user_id)
            ):
                previous_pipeline = candidate
                previous_nodes = (
                    candidate.nodes
                    if isinstance(candidate.nodes, list)
                    else []
                )
                previous_edges = (
                    candidate.edges
                    if isinstance(candidate.edges, list)
                    else []
                )

        if not selected_capabilities:
            return {
                "status": "needs_input",
                "message_ru": "Не удалось найти доступные инструменты. Загрузите OpenAPI/Swagger.",
                "chat_reply_ru": (
                    "Для вашего аккаунта пока нет capabilities. "
                    "Загрузите OpenAPI/Swagger, чтобы я смог собрать исполнимый pipeline."
                ),
                "nodes": [],
                "edges": [],
                "missing_requirements": ["selection:no_matches"],
                "context_summary": dialog_summary,
            }
        
        # If confidence is too low or only 1 capability selected with low confidence, ask for clarification
        if self._selection_is_low_confidence(selected_capabilities) and len(selected_capabilities) <= 2:
            clarification = self._build_low_confidence_question_ru(
                question_number=1,
                message=message,
                dialog_messages=dialog_messages,
                selected_capabilities=selected_capabilities,
            )
            return {
                "status": "needs_input",
                "message_ru": "Сценарий не на 100% уверенный. Нужны уточнения.",
                "chat_reply_ru": clarification,
                "nodes": [],
                "edges": [],
                "missing_requirements": ["selection:low_confidence"],
                "context_summary": dialog_summary,
            }

        raw_graph: dict[str, Any] | None = None
        if use_explicit_plan_mode:
            raw_graph = self._build_minimal_raw_graph(
                selected_capabilities,
                user_query=message,
            )

        if raw_graph is None:
            prompt = self._build_generation_prompt(
                user_query=message,
                selected_capabilities=selected_capabilities,
                dialog_messages=dialog_messages,
                dialog_summary=dialog_summary,
                previous_nodes=previous_nodes,
                previous_edges=previous_edges,
            )

            try:
                raw_graph = self.generate_raw_graph(message, selected_capabilities, prompt)
            except Exception:
                top_capabilities = self._select_fallback_capabilities(
                    message=message,
                    selected_capabilities=selected_capabilities,
                    max_items=self.FALLBACK_CAPABILITIES_LIMIT,
                )
                raw_graph = self._build_minimal_raw_graph(
                    top_capabilities,
                    user_query=message,
                )
                if raw_graph is None:
                    return {
                        "status": "cannot_build",
                        "message_ru": "Не удалось построить сценарий. Нет доступной исполнимой capability.",
                        "chat_reply_ru": "Не удалось построить сценарий. Попробуйте уточнить запрос.",
                        "nodes": [],
                        "edges": [],
                        "context_summary": dialog_summary,
                    }

        normalized_nodes, normalized_edges, is_ready, missing = self._prepare_graph(
            raw_graph=raw_graph,
            selected_capabilities=selected_capabilities,
        )
        normalized_nodes, normalized_edges = self._apply_user_plan_order_hint(
            message=message,
            nodes=normalized_nodes,
            edges=normalized_edges,
            selected_capabilities=selected_capabilities,
        )
        normalized_nodes, normalized_edges = self._enforce_segment_merge_edges(
            message=message,
            nodes=normalized_nodes,
            edges=normalized_edges,
            selected_capabilities=selected_capabilities,
        )
        normalized_nodes, normalized_edges = self._enforce_explicit_plan_step_count(
            message=message,
            nodes=normalized_nodes,
            edges=normalized_edges,
        )
        normalized_nodes, normalized_edges = self._prune_unsolicited_send_nodes(
            message=message,
            nodes=normalized_nodes,
            edges=normalized_edges,
            selected_capabilities=selected_capabilities,
        )
        normalized_nodes, normalized_edges = self._prune_unsolicited_assign_nodes(
            message=message,
            nodes=normalized_nodes,
            edges=normalized_edges,
            selected_capabilities=selected_capabilities,
        )

        if len(selected_capabilities) >= 4 and len(normalized_nodes) < 3:
            top_capabilities = self._select_fallback_capabilities(
                message=message,
                selected_capabilities=selected_capabilities,
                max_items=self.FALLBACK_CAPABILITIES_LIMIT,
            )
            fallback_raw_graph = self._build_minimal_raw_graph(
                top_capabilities,
                user_query=message,
            )
            if fallback_raw_graph is not None:
                fallback_nodes, fallback_edges, fallback_ready, fallback_missing = self._prepare_graph(
                    raw_graph=fallback_raw_graph,
                    selected_capabilities=top_capabilities,
                )
                fallback_nodes, fallback_edges = self._apply_user_plan_order_hint(
                    message=message,
                    nodes=fallback_nodes,
                    edges=fallback_edges,
                    selected_capabilities=top_capabilities,
                )
                fallback_nodes, fallback_edges = self._enforce_segment_merge_edges(
                    message=message,
                    nodes=fallback_nodes,
                    edges=fallback_edges,
                    selected_capabilities=top_capabilities,
                )
                fallback_nodes, fallback_edges = self._enforce_explicit_plan_step_count(
                    message=message,
                    nodes=fallback_nodes,
                    edges=fallback_edges,
                )
                fallback_nodes, fallback_edges = self._prune_unsolicited_send_nodes(
                    message=message,
                    nodes=fallback_nodes,
                    edges=fallback_edges,
                    selected_capabilities=top_capabilities,
                )
                fallback_nodes, fallback_edges = self._prune_unsolicited_assign_nodes(
                    message=message,
                    nodes=fallback_nodes,
                    edges=fallback_edges,
                    selected_capabilities=top_capabilities,
                )
                if fallback_nodes:
                    normalized_nodes = fallback_nodes
                    normalized_edges = fallback_edges
                    is_ready = fallback_ready
                    missing = fallback_missing

        chat_reply = self._build_chat_reply_ru(normalized_nodes, normalized_edges)
        has_strict_capability_issues = self._has_strict_capability_issues(missing)

        if not is_ready and not has_strict_capability_issues:
            top_capabilities = self._select_fallback_capabilities(
                message=message,
                selected_capabilities=selected_capabilities,
                max_items=self.FALLBACK_CAPABILITIES_LIMIT,
            )
            fallback_raw_graph = self._build_minimal_raw_graph(
                top_capabilities,
                user_query=message,
            )
            if fallback_raw_graph is not None:
                fallback_nodes, fallback_edges, fallback_ready, fallback_missing = self._prepare_graph(
                    raw_graph=fallback_raw_graph,
                    selected_capabilities=top_capabilities,
                )
                fallback_nodes, fallback_edges = self._apply_user_plan_order_hint(
                    message=message,
                    nodes=fallback_nodes,
                    edges=fallback_edges,
                    selected_capabilities=top_capabilities,
                )
                fallback_nodes, fallback_edges = self._enforce_segment_merge_edges(
                    message=message,
                    nodes=fallback_nodes,
                    edges=fallback_edges,
                    selected_capabilities=top_capabilities,
                )
                fallback_nodes, fallback_edges = self._enforce_explicit_plan_step_count(
                    message=message,
                    nodes=fallback_nodes,
                    edges=fallback_edges,
                )
                fallback_nodes, fallback_edges = self._prune_unsolicited_send_nodes(
                    message=message,
                    nodes=fallback_nodes,
                    edges=fallback_edges,
                    selected_capabilities=top_capabilities,
                )
                fallback_nodes, fallback_edges = self._prune_unsolicited_assign_nodes(
                    message=message,
                    nodes=fallback_nodes,
                    edges=fallback_edges,
                    selected_capabilities=top_capabilities,
                )
                if fallback_ready:
                    normalized_nodes = fallback_nodes
                    normalized_edges = fallback_edges
                    is_ready = True
                    missing = []
                    chat_reply = self._build_chat_reply_ru(normalized_nodes, normalized_edges)
                else:
                    missing = fallback_missing

        if not is_ready:
            if has_strict_capability_issues:
                chat_reply = (
                    "Не удалось безопасно собрать сценарий: модель вернула шаги "
                    "без подтвержденных capability_id. Уточните задачу и повторите запрос."
                )
                message_ru = (
                    "Сценарий отклонен: обнаружены неподтвержденные ссылки на capability."
                )
            else:
                message_ru = "Сценарий не готов к выполнению. Не хватает входных данных."
            await self.dialog_memory.append_and_summarize(
                str(dialog_id), "user", message
            )
            await self.dialog_memory.append_and_summarize(
                str(dialog_id), "assistant", chat_reply
            )
            return {
                "status": "cannot_build",
                "message_ru": message_ru,
                "chat_reply_ru": chat_reply,
                "nodes": normalized_nodes,
                "edges": normalized_edges,
                "missing_requirements": missing,
                "context_summary": dialog_summary,
            }

        if previous_pipeline is not None:
            previous_pipeline.name = self._build_pipeline_name(message)
            previous_pipeline.description = None
            previous_pipeline.user_prompt = message
            previous_pipeline.nodes = normalized_nodes
            previous_pipeline.edges = normalized_edges
            previous_pipeline.status = PipelineStatus.READY
            if previous_pipeline.created_by is None:
                previous_pipeline.created_by = user_id
            pipeline = previous_pipeline
        else:
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
        await self.dialog_memory.append_and_summarize(
            str(dialog_id), "assistant", chat_reply
        )

        return {
            "status": "ready",
            "message_ru": "Сценарий готов.",
            "chat_reply_ru": chat_reply,
            "pipeline_id": pipeline.id,
            "nodes": normalized_nodes,
            "edges": normalized_edges,
            "context_summary": dialog_summary,
        }

    def _prepare_graph(
        self,
        *,
        raw_graph: dict[str, Any],
        selected_capabilities: list[SelectedCapability],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], bool, list[str]]:
        normalized_nodes, normalized_edges, normalization_issues = self._normalize_workflow(
            raw_graph, selected_capabilities
        )
        normalized_edges = self._repair_edges_with_data_flow(normalized_nodes, normalized_edges)
        normalized_nodes, normalized_edges = self._compact_step_sequence(
            normalized_nodes,
            normalized_edges,
        )
        self._sync_node_connections(normalized_nodes, normalized_edges)
        self._ensure_external_inputs(normalized_nodes, normalized_edges)

        reviewed_nodes, reviewed_edges = self._review_graph_with_llm(
            normalized_nodes,
            normalized_edges,
            selected_capabilities,
        )
        if len(normalized_nodes) >= 4 and len(reviewed_nodes) < 3:
            reviewed_nodes = normalized_nodes
            reviewed_edges = normalized_edges
        reviewed_edges = self._repair_edges_with_data_flow(reviewed_nodes, reviewed_edges)
        reviewed_edges = self._prune_edges_for_terminal_goal(reviewed_nodes, reviewed_edges)
        reviewed_edges = self._prune_edges_by_required_inputs(reviewed_nodes, reviewed_edges)
        reviewed_nodes, reviewed_edges = self._prune_disconnected_nodes(
            reviewed_nodes,
            reviewed_edges,
        )
        reviewed_nodes, reviewed_edges = self._compact_step_sequence(
            reviewed_nodes,
            reviewed_edges,
        )
        self._sync_node_connections(reviewed_nodes, reviewed_edges)
        self._ensure_external_inputs(reviewed_nodes, reviewed_edges)
        is_ready, missing = self._validate_ready_graph(reviewed_nodes, reviewed_edges)
        if normalization_issues:
            missing = sorted(set(missing + normalization_issues))
            is_ready = False
        return reviewed_nodes, reviewed_edges, is_ready, missing

    def _compact_step_sequence(
        self,
        nodes: list[dict[str, Any]],
        edges: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        step_values = sorted(
            {
                step
                for node in nodes
                if isinstance((step := node.get("step")), int)
            }
        )
        if not step_values:
            return nodes, edges

        target = list(range(1, len(step_values) + 1))
        if step_values == target:
            return nodes, edges

        step_map = {
            old_step: new_step
            for new_step, old_step in enumerate(step_values, start=1)
        }

        compact_nodes: list[dict[str, Any]] = []
        for node in nodes:
            step = node.get("step")
            if not isinstance(step, int) or step not in step_map:
                continue

            compact_node = dict(node)
            compact_node["step"] = step_map[step]
            compact_node["input_connected_from"] = sorted(
                {
                    step_map[src]
                    for src in self._normalize_int_list(node.get("input_connected_from"))
                    if src in step_map
                }
            )
            compact_node["output_connected_to"] = sorted(
                {
                    step_map[dst]
                    for dst in self._normalize_int_list(node.get("output_connected_to"))
                    if dst in step_map
                }
            )

            compact_input_types: list[dict[str, Any]] = []
            for item in self._normalize_input_data_types(
                node.get("input_data_type_from_previous")
            ):
                from_step = item.get("from_step")
                edge_type = item.get("type")
                if (
                    isinstance(from_step, int)
                    and from_step in step_map
                    and isinstance(edge_type, str)
                ):
                    compact_input_types.append(
                        {
                            "from_step": step_map[from_step],
                            "type": edge_type,
                        }
                    )
            compact_node["input_data_type_from_previous"] = compact_input_types
            compact_nodes.append(compact_node)

        compact_edges: list[dict[str, Any]] = []
        seen_edges: set[tuple[int, int, str]] = set()
        for edge in edges:
            src = edge.get("from_step")
            dst = edge.get("to_step")
            edge_type = edge.get("type")
            if (
                not isinstance(src, int)
                or not isinstance(dst, int)
                or src not in step_map
                or dst not in step_map
                or not isinstance(edge_type, str)
                or not edge_type.strip()
            ):
                continue
            remapped = (step_map[src], step_map[dst], edge_type.strip())
            if remapped[0] == remapped[1] or remapped in seen_edges:
                continue
            seen_edges.add(remapped)
            compact_edges.append(
                {
                    "from_step": remapped[0],
                    "to_step": remapped[1],
                    "type": remapped[2],
                }
            )

        compact_nodes.sort(key=lambda item: item.get("step", 0))
        compact_edges.sort(
            key=lambda item: (
                item.get("from_step", 0),
                item.get("to_step", 0),
                str(item.get("type", "")),
            )
        )
        return compact_nodes, compact_edges

    def _build_minimal_raw_graph(
        self,
        selected_capabilities: list[SelectedCapability],
        user_query: str | None = None,
    ) -> dict[str, Any] | None:
        executable_caps: list[Any] = []
        for item in selected_capabilities:
            cap = item.capability
            cap_id = getattr(cap, "id", None)
            if cap_id is None:
                continue
            cap_type = self._capability_type_value(cap)
            action_id = getattr(cap, "action_id", None)
            if cap_type == "ATOMIC" and action_id is None:
                continue
            if cap_type == "COMPOSITE" and not self._recipe_is_executable(
                getattr(cap, "recipe", None)
            ):
                continue
            executable_caps.append(cap)

        if not executable_caps:
            return None

        has_explicit_plan = bool(self._extract_user_plan_terms(user_query or ""))
        if has_explicit_plan:
            ordered_caps = executable_caps
        else:
            ordered_caps = sorted(executable_caps, key=self._fallback_capability_order_key)

        nodes: list[dict[str, Any]] = []
        edges: list[dict[str, Any]] = []
        required_by_step: dict[int, list[str]] = {}
        outputs_by_step: dict[int, list[str]] = {}

        for index, cap in enumerate(ordered_caps, start=1):
            required_inputs = self._extract_required_inputs(
                getattr(cap, "input_schema", None)
            )
            required_by_step[index] = required_inputs
            outputs_by_step[index] = self._extract_schema_field_names(
                getattr(cap, "output_schema", None)
            )
            nodes.append(
                {
                    "step": index,
                    "name": str(getattr(cap, "name", f"Step {index}") or f"Step {index}"),
                    "description": getattr(cap, "description", None),
                    "capability_id": str(getattr(cap, "id")),
                    "input_connected_from": [],
                    "output_connected_to": [],
                    "input_data_type_from_previous": [],
                    "external_inputs": [],
                }
            )

        for to_step in range(1, len(ordered_caps) + 1):
            if to_step == 1:
                continue
            to_cap = ordered_caps[to_step - 1]
            to_signature = self._fallback_capability_signature(to_cap)
            to_required = required_by_step.get(to_step, [])
            to_required_keys = {
                self._normalize_lookup_token(item): item
                for item in to_required
                if isinstance(item, str) and item.strip()
            }

            selected_parents: list[tuple[int, str]] = []
            for from_step in range(1, to_step):
                from_fields = outputs_by_step.get(from_step, [])
                matched_type = None
                for field in from_fields:
                    field_key = self._normalize_lookup_token(field)
                    if field_key and field_key in to_required_keys:
                        matched_type = to_required_keys[field_key]
                        break
                if matched_type is not None:
                    selected_parents.append((from_step, matched_type))

            # Domain heuristic: segmentation typically merges users + hotels branches.
            if "segment" in to_signature and len(selected_parents) < 2:
                user_parent = None
                hotel_parent = None
                for from_step in range(1, to_step):
                    from_signature = self._fallback_capability_signature(
                        ordered_caps[from_step - 1]
                    )
                    if user_parent is None and (
                        "user" in from_signature or "client" in from_signature
                    ):
                        user_parent = from_step
                    if hotel_parent is None and "hotel" in from_signature:
                        hotel_parent = from_step

                seen_parent_steps = {item[0] for item in selected_parents}
                if user_parent is not None and user_parent not in seen_parent_steps:
                    selected_parents.append(
                        (
                            user_parent,
                            self._infer_fallback_edge_type(
                                ordered_caps[user_parent - 1],
                                to_cap,
                            ),
                        )
                    )
                    seen_parent_steps.add(user_parent)
                if hotel_parent is not None and hotel_parent not in seen_parent_steps:
                    selected_parents.append(
                        (
                            hotel_parent,
                            self._infer_fallback_edge_type(
                                ordered_caps[hotel_parent - 1],
                                to_cap,
                            ),
                        )
                    )

            if not selected_parents:
                segment_parent = None
                assign_parent = None
                send_parent = None
                for from_step in range(1, to_step):
                    from_signature = self._fallback_capability_signature(
                        ordered_caps[from_step - 1]
                    )
                    if segment_parent is None and "segment" in from_signature:
                        segment_parent = from_step
                    if assign_parent is None and "assign" in from_signature:
                        assign_parent = from_step
                    if send_parent is None and (
                        "send" in from_signature or "email" in from_signature
                    ):
                        send_parent = from_step

                if "assign" in to_signature and segment_parent is not None:
                    edge_type = self._semantic_crm_edge_type(
                        src_stage="segment",
                        dst_stage="assign",
                        required_inputs=to_required,
                    )
                    selected_parents.append((segment_parent, edge_type))
                elif (
                    ("send" in to_signature or "email" in to_signature)
                    and assign_parent is not None
                ):
                    edge_type = self._semantic_crm_edge_type(
                        src_stage="assign",
                        dst_stage="send",
                        required_inputs=to_required,
                    )
                    selected_parents.append((assign_parent, edge_type))
                elif ("qualify" in to_signature or "lead" in to_signature):
                    preferred_parent = send_parent or assign_parent or segment_parent
                    if preferred_parent is not None:
                        from_stage = None
                        if preferred_parent == send_parent:
                            from_stage = "send"
                        elif preferred_parent == assign_parent:
                            from_stage = "assign"
                        else:
                            from_stage = "segment"
                        edge_type = self._semantic_crm_edge_type(
                            src_stage=from_stage,
                            dst_stage="qualify",
                            required_inputs=to_required,
                        )
                        selected_parents.append((preferred_parent, edge_type))

            if not selected_parents and to_required:
                # Keep DAG connected when schemas are too weak to match fields exactly.
                fallback_from = to_step - 1
                selected_parents.append(
                    (
                        fallback_from,
                        self._infer_fallback_edge_type(ordered_caps[fallback_from - 1], to_cap),
                    )
                )

            for from_step, edge_type in selected_parents:
                edges.append(
                    {
                        "from_step": from_step,
                        "to_step": to_step,
                        "type": edge_type,
                    }
                )

        self._sync_node_connections(nodes, edges)

        for node in nodes:
            step = node.get("step")
            if not isinstance(step, int):
                continue
            incoming_types = {
                str(edge.get("type"))
                for edge in edges
                if edge.get("to_step") == step and isinstance(edge.get("type"), str)
            }
            required_inputs = required_by_step.get(step, [])
            node["input_data_type_from_previous"] = [
                {
                    "from_step": edge["from_step"],
                    "type": edge["type"],
                }
                for edge in edges
                if edge.get("to_step") == step
            ]
            node["external_inputs"] = [
                required_input
                for required_input in required_inputs
                if not self._is_input_satisfied_by_values(required_input, incoming_types)
            ]

        return {
            "nodes": nodes,
            "edges": edges,
        }

    def _fallback_capability_order_key(self, capability: Any) -> tuple[int, str]:
        context = self._extract_capability_action_context(capability)
        name = str(getattr(capability, "name", "") or "").lower()
        operation_id = str(context.get("operation_id", "") or "").lower()
        path = str(context.get("path", "") or "").lower()
        method = str(context.get("method", "") or "").lower()
        signature = " ".join(part for part in (name, operation_id, method, path) if part)

        # Prefer canonical travel/crm sequence when present.
        if "recent" in signature and ("users" in signature or "/users" in signature):
            return (10, signature)
        if "top" in signature and ("hotel" in signature or "/hotels" in signature):
            return (20, signature)
        if "segment" in signature:
            return (30, signature)
        if "assign" in signature:
            return (40, signature)
        if "send" in signature and ("offer" in signature or "email" in signature):
            return (50, signature)
        if "qualify" in signature or "lead" in signature:
            return (60, signature)
        return (100, signature)

    def _infer_fallback_edge_type(self, from_capability: Any, to_capability: Any) -> str:
        to_required = self._extract_required_inputs(getattr(to_capability, "input_schema", None))
        from_output_fields = self._extract_schema_field_names(getattr(from_capability, "output_schema", None))
        from_signature = self._fallback_capability_signature(from_capability)

        semantic_required = self._select_required_input_by_signature(
            to_required,
            from_signature,
        )
        if semantic_required is not None:
            return semantic_required

        normalized_from = {self._normalize_lookup_token(item): item for item in from_output_fields}
        for required in to_required:
            key = self._normalize_lookup_token(required)
            if key in normalized_from:
                return required

        if to_required:
            return to_required[0]
        if from_output_fields:
            return from_output_fields[0]
        return "data"

    def _fallback_capability_signature(self, capability: Any) -> str:
        context = self._extract_capability_action_context(capability)
        name = str(getattr(capability, "name", "") or "").lower()
        operation_id = str(context.get("operation_id", "") or "").lower()
        path = str(context.get("path", "") or "").lower()
        method = str(context.get("method", "") or "").lower()
        return " ".join(
            part for part in (name, operation_id, method, path) if part
        )

    def _select_required_input_by_signature(
        self,
        required_inputs: list[str],
        from_signature: str,
    ) -> str | None:
        if not required_inputs:
            return None

        lower_required = [str(item).strip().lower() for item in required_inputs]

        if "hotel" in from_signature:
            for idx, required in enumerate(lower_required):
                if "hotel" in required:
                    return required_inputs[idx]

        if "user" in from_signature or "client" in from_signature:
            for idx, required in enumerate(lower_required):
                if "user" in required or "client" in required:
                    return required_inputs[idx]

        if "segment" in from_signature:
            for idx, required in enumerate(lower_required):
                if "segment" in required:
                    return required_inputs[idx]

        if "assign" in from_signature:
            for idx, required in enumerate(lower_required):
                if "assign" in required:
                    return required_inputs[idx]

        if "offer" in from_signature or "email" in from_signature:
            for idx, required in enumerate(lower_required):
                if "offer" in required or "email" in required:
                    return required_inputs[idx]

        if "lead" in from_signature:
            for idx, required in enumerate(lower_required):
                if "lead" in required:
                    return required_inputs[idx]

        return None

    def _semantic_crm_edge_type(
        self,
        src_stage: str,
        dst_stage: str,
        required_inputs: list[str],
    ) -> str:
        domain_types = {
            ("segment", "assign"): "segmentedUsers",
            ("assign", "send"): "assignments",
            ("send", "qualify"): "sentOffers",
            ("assign", "qualify"): "assignments",
            ("segment", "qualify"): "segmentedUsers",
        }
        if (src_stage, dst_stage) in domain_types:
            return domain_types[(src_stage, dst_stage)]

        if not required_inputs:
            return "data"

        lower_required = [str(item).strip().lower() for item in required_inputs]
        if "assignment" in lower_required[0]:
            return "assignments"
        if "offer" in lower_required[0]:
            return "sentOffers"
        if "segment" in lower_required[0]:
            return "segmentedUsers"

        return lower_required[0] if lower_required else "data"

    def _extract_schema_field_names(self, schema: Any) -> list[str]:
        if not isinstance(schema, dict):
            return []
        properties = schema.get("properties")
        if not isinstance(properties, dict):
            return []
        result: list[str] = []
        for key in properties.keys():
            if isinstance(key, str) and key.strip():
                result.append(key.strip())
        return result

    def generate_raw_graph(
        self,
        user_query: str,
        selected_capabilities: list[SelectedCapability],
        prompt: str,
    ) -> dict[str, Any]:
        system_prompt = (
            "You are Qwen2.5-Coder (7B) building executable workflow DAGs. "
            "Output MUST be a single valid JSON object only. "
            "No markdown, no comments, no extra keys, no prose."
        )
        reset_model_session()
        payload = chat_json(system_prompt=system_prompt, user_prompt=prompt)
        if not isinstance(payload, dict):
            raise PipelineServiceError("Failed to call LLM provider")
        return payload

    def _build_generation_prompt(
        self,
        *,
        user_query: str,
        selected_capabilities: list[SelectedCapability],
        dialog_messages: list[dict[str, Any]],
        dialog_summary: str | None,
        previous_nodes: list[dict[str, Any]] | None = None,
        previous_edges: list[dict[str, Any]] | None = None,
    ) -> str:
        capabilities_payload = []
        allowed_capability_ids: list[str] = []
        for sc in selected_capabilities:
            cap = sc.capability
            capabilities_payload.append(self._build_capability_prompt_payload(cap))
            cap_id = str(getattr(cap, "id", "") or "").strip()
            if cap_id:
                allowed_capability_ids.append(cap_id)

        context_payload = {
            "summary": dialog_summary,
            "recent_messages": dialog_messages[-6:],
            "previous_graph": {
                "nodes": previous_nodes or [],
                "edges": previous_edges or [],
            },
        }

        instruction = (
            "MODEL_PROFILE: qwen2.5:7b-coder\n"
            "TASK: Build an executable DAG pipeline from USER_QUERY, CONTEXT and CAPABILITIES.\n"
            "LANGUAGE: Keep values human-readable; structure and keys must follow OUTPUT_SCHEMA.\n\n"
            "HARD_RULES:\n"
            "1) Return ONLY a single JSON object matching OUTPUT_SCHEMA.\n"
            "2) Graph must be a DAG (no cycles, no self-links).\n"
            "3) Use ONLY capability_id values from ALLOWED_CAPABILITY_IDS.\n"
            "4) Never replace capability_id with name/path/operation_id/action_id.\n"
            "   Никогда не подменяй capability_id значениями name/path/operation_id/action_id.\n"
            "5) Edges must represent data-flow, not just chronological order.\n"
            "6) For each edge from_step->to_step:\n"
            "   - to_step must be in from_step.output_connected_to\n"
            "   - from_step must be in to_step.input_connected_from\n"
            "7) COMPOSITE capability must remain one node (do not expand substeps).\n"
            "8) If PREVIOUS_GRAPH is non-empty, edit it in-place and keep unchanged valid parts.\n"
            "9) If exact capability choice is impossible, return empty graph: {\"nodes\": [], \"edges\": []}.\n\n"
            "MERGE_PATTERN_EXAMPLE:\n"
            "- Step 1 produce users\n"
            "- Step 2 produce hotels\n"
            "- Step 3 consumes users and hotels\n"
            "Expected edges:\n"
            '- (1->3, type=\"users\"), (2->3, type=\"hotels\")\n'
            "Expected links:\n"
            "- node[3].input_connected_from = [1,2]\n"
            "- node[1].output_connected_to contains 3\n"
            "- node[2].output_connected_to contains 3\n\n"
        )
        
        # Suggest canonical CRM flow only for stages explicitly requested by user.
        user_query_lower = (user_query or "").lower()
        mentions_crm_flow = any(
            keyword in user_query_lower
            for keyword in ["отел", "hotel", "пользоват", "user", "segment", "assign", "offer", "email"]
        )
        mentions_send = any(
            keyword in user_query_lower for keyword in ["send", "offer", "email", "разосл", "письм"]
        )
        mentions_assign = any(
            keyword in user_query_lower for keyword in ["assign", "назнач", "назначи", "назначить", "назначения"]
        )
        if self._message_forbids_send_offers(user_query):
            mentions_send = False
        mentions_qualify = any(
            keyword in user_query_lower for keyword in ["qualify", "lead", "лид", "квалиф"]
        )
        forbid_qualify = self._message_forbids_lead_qualification(user_query)

        if mentions_crm_flow:
            instruction += (
                "TARGET_LINEAR_PATTERN_IF_RELEVANT:\n"
                "- Step 1: get recently active users\n"
                "- Step 2: get top hotels\n"
                "- Step 3: segment users by hotel interests (consumes 1+2)\n"
            )
            if mentions_assign:
                instruction += "- Step 4: assign specific hotels to users (consumes 3)\n"
            if mentions_send:
                if mentions_assign:
                    instruction += "- Step 5: send personalized offers to users (consumes 4)\n"
                else:
                    instruction += "- Step 4: send personalized offers to users (consumes 3)\n"
            if mentions_qualify and not forbid_qualify:
                if mentions_send:
                    if mentions_assign:
                        instruction += "- Step 6: evaluate lead quality (consumes 5 and/or 4)\n"
                    else:
                        instruction += "- Step 5: evaluate lead quality (consumes 4 and/or 3)\n"
                else:
                    if mentions_assign:
                        instruction += "- Step 5: evaluate lead quality (consumes 4)\n"
                    else:
                        instruction += "- Step 4: evaluate lead quality (consumes 3)\n"

        ordered_terms = self._extract_user_plan_terms(user_query)
        if ordered_terms:
            instruction += (
                "\nORDER_CONSTRAINT:\n"
                "- USER_QUERY contains explicit step order hints. Preserve this order if matching capabilities exist.\n"
                f"- Ordered hints: {json.dumps(ordered_terms, ensure_ascii=False)}\n"
            )

        return (
            f"{instruction}\n\n"
            f"USER_QUERY:\n{user_query}\n\n"
            f"DIALOG_CONTEXT:\n{json.dumps(context_payload, ensure_ascii=False)}\n\n"
            f"ALLOWED_CAPABILITY_IDS:\n{json.dumps(allowed_capability_ids, ensure_ascii=False)}\n\n"
            f"CAPABILITIES:\n{json.dumps(capabilities_payload, ensure_ascii=False)}\n\n"
            "OUTPUT_SCHEMA:\n"
            "{\n"
            '  "nodes": [\n'
            "    {\n"
            '      "step": 1,\n'
            '      "name": "Step name",\n'
            '      "capability_id": "UUID from CAPABILITIES",\n'
            '      "description": "Step purpose",\n'
            '      "input_connected_from": [],\n'
            '      "output_connected_to": [],\n'
            '      "input_data_type_from_previous": [],\n'
            '      "external_inputs": []\n'
            "    }\n"
            "  ],\n"
            '  "edges": [\n'
            '    {"from_step": 1, "to_step": 2, "type": "field_name"}\n'
            "  ]\n"
            "}\n\n"
            "SELF-CHECK (INTERNAL ONLY):\n"
            "- Verify every node.capability_id is in ALLOWED_CAPABILITY_IDS.\n"
            "- Verify every required input has either upstream edge or external_inputs.\n"
            "- Verify edges and node links are synchronized.\n"
            "- Output final JSON only.\n"
        )

    def _has_strict_capability_issues(self, issues: list[str]) -> bool:
        return any(issue in self.STRICT_CAPABILITY_ISSUES for issue in issues)

    def _build_selection_query(
        self,
        *,
        message: str,
        dialog_messages: list[dict[str, Any]],
        dialog_summary: str | None,
    ) -> str:
        recent_chunks = [
            str(item.get("content", ""))
            for item in dialog_messages[-4:]
            if isinstance(item, dict) and item.get("role") == "user"
        ]
        parts = [str(message or "").strip()]
        if dialog_summary:
            parts.append(self._strip_low_confidence_marker(dialog_summary))
        parts.extend(chunk for chunk in recent_chunks if chunk)
        return "\n".join(part for part in parts if part)

    def _should_reuse_previous_graph(
        self,
        *,
        message: str,
        dialog_messages: list[dict[str, Any]],
    ) -> bool:
        if self._is_low_quality_message(message):
            return False
        if self._extract_user_plan_terms(message):
            return False

        normalized = (message or "").strip().lower()
        if not normalized:
            return False

        refinement_markers = (
            "измени",
            "исправ",
            "добав",
            "убери",
            "замени",
            "обнов",
            "перестро",
            "допол",
            "тот же",
            "этот граф",
            "предыдущ",
            "modify",
            "update",
            "refine",
            "change",
            "same graph",
            "previous graph",
        )
        if any(marker in normalized for marker in refinement_markers):
            return True

        # Reuse only when user is clearly continuing within the same dialog context.
        recent_user_messages = [
            str(item.get("content", ""))
            for item in dialog_messages[-4:]
            if isinstance(item, dict) and str(item.get("role", "")).lower() == "user"
        ]
        return bool(recent_user_messages and len(self._tokenize_text(normalized)) <= 5)

    def _extract_user_plan_terms(self, message: str) -> list[str]:
        text = str(message or "")
        if not text.strip():
            return []

        terms: list[str] = []

        numbered = re.findall(r"\b\d+\s*[\.)]\s*([A-Za-z_][A-Za-z0-9_]*)", text)
        for item in numbered:
            cleaned = item.strip()
            if cleaned:
                terms.append(cleaned)

        if not terms and ("план выполнения" in text.lower() or "execution plan" in text.lower()):
            right_part = text.split(":", 1)[-1]
            chunks = [chunk.strip() for chunk in right_part.split("->") if chunk.strip()]
            for chunk in chunks:
                token_match = re.search(r"([A-Za-z_][A-Za-z0-9_]*)", chunk)
                if token_match:
                    terms.append(token_match.group(1))

        if not terms:
            return []

        raw_terms_normalized = [self._normalize_lookup_token(item) for item in terms if item]
        raw_terms_normalized = [item for item in raw_terms_normalized if item]

        # Reject malformed plans with duplicated explicit steps.
        if len(raw_terms_normalized) != len(set(raw_terms_normalized)):
            return []

        unique_terms: list[str] = []
        seen: set[str] = set()
        for term in terms:
            key = term.lower()
            if key in seen:
                continue
            seen.add(key)
            unique_terms.append(term)

        if not self._is_plan_terms_consistent_with_message(message=message, terms=unique_terms):
            return []
        return unique_terms

    def _message_forbids_lead_qualification(self, message: str) -> bool:
        message_lower = (message or "").lower()
        has_lead_term = any(
            token in message_lower
            for token in ["lead", "qualify", "лид", "квалиф", "оценк"]
        )
        if not has_lead_term:
            return False

        negative_markers = [
            "не нужно",
            "не надо",
            "не должен",
            "не должна",
            "не должно",
            "без",
            "исключ",
            "убери",
            "remove",
            "without",
            "no lead",
            "do not",
            "don't",
        ]
        return any(marker in message_lower for marker in negative_markers)

    def _message_forbids_send_offers(self, message: str) -> bool:
        message_lower = (message or "").lower()
        has_send_term = any(
            token in message_lower
            for token in ["send", "email", "offer", "почт", "разосл", "письм", "присыл"]
        )
        if not has_send_term:
            return False

        negative_markers = [
            "не нужно",
            "не надо",
            "не должен",
            "не должна",
            "не должно",
            "не просил",
            "без",
            "исключ",
            "убери",
            "remove",
            "without",
            "do not",
            "don't",
            "no email",
            "no send",
        ]
        return any(marker in message_lower for marker in negative_markers)

    def _message_requests_send_offers(self, message: str) -> bool:
        message_lower = (message or "").lower()
        if self._message_forbids_send_offers(message):
            return False
        return any(
            token in message_lower
            for token in ["send", "email", "offer", "почт", "разосл", "письм", "присыл"]
        )

    def _message_requests_assign_stage(self, message: str) -> bool:
        message_lower = (message or "").lower()
        return any(
            token in message_lower
            for token in ["assign", "назнач", "назначи", "назначить", "назначения"]
        )

    def _message_requests_lead_qualification(self, message: str) -> bool:
        message_lower = (message or "").lower()
        return any(
            token in message_lower
            for token in ["lead", "qualify", "лид", "квалиф", "оценк лид"]
        )

    def _filter_capabilities_by_negative_intent(
        self,
        *,
        message: str,
        selected_capabilities: list[SelectedCapability],
    ) -> list[SelectedCapability]:
        if not selected_capabilities:
            return selected_capabilities
        if not self._message_forbids_lead_qualification(message):
            return selected_capabilities

        filtered = [
            item
            for item in selected_capabilities
            if all(
                token not in self._fallback_capability_signature(item.capability)
                for token in ("lead", "qualify")
            )
        ]
        return filtered or selected_capabilities

    def _filter_send_capabilities_by_negative_intent(
        self,
        *,
        message: str,
        selected_capabilities: list[SelectedCapability],
    ) -> list[SelectedCapability]:
        if not selected_capabilities:
            return selected_capabilities
        if not self._message_forbids_send_offers(message):
            return selected_capabilities

        filtered = [
            item
            for item in selected_capabilities
            if all(
                token not in self._fallback_capability_signature(item.capability)
                for token in ("send", "email", "offer")
            )
        ]
        return filtered or selected_capabilities

    def _filter_capabilities_by_positive_intent(
        self,
        *,
        message: str,
        selected_capabilities: list[SelectedCapability],
    ) -> list[SelectedCapability]:
        if not selected_capabilities:
            return selected_capabilities
        if self._message_requests_lead_qualification(message):
            return selected_capabilities

        filtered = [
            item
            for item in selected_capabilities
            if all(
                token not in self._fallback_capability_signature(item.capability)
                for token in ("lead", "qualify")
            )
        ]
        return filtered or selected_capabilities

    def _is_plan_terms_consistent_with_message(
        self,
        *,
        message: str,
        terms: list[str],
    ) -> bool:
        if not terms:
            return False

        message_lower = (message or "").lower()
        term_tokens = [self._normalize_lookup_token(term) for term in terms]

        has_segment_in_plan = any("segment" in token for token in term_tokens)
        has_user_in_plan = any("user" in token or "client" in token for token in term_tokens)
        has_hotel_in_plan = any("hotel" in token for token in term_tokens)
        has_assign_in_plan = any("assign" in token for token in term_tokens)
        has_send_in_plan = any("send" in token or "offer" in token or "email" in token for token in term_tokens)
        has_qualify_in_plan = any("qualify" in token or "lead" in token for token in term_tokens)

        if has_send_in_plan and self._message_forbids_send_offers(message):
            return False
        if has_qualify_in_plan and self._message_forbids_lead_qualification(message):
            return False

        mentions_users = ("пользоват" in message_lower) or ("user" in message_lower)
        mentions_hotels = ("отел" in message_lower) or ("hotel" in message_lower)
        mentions_assign = ("назнач" in message_lower) or ("assign" in message_lower)
        mentions_send = ("разосл" in message_lower) or ("send" in message_lower) or ("offer" in message_lower)

        # If user asks for segmentation by hotel interests, plan must include user and hotel collectors.
        if has_segment_in_plan and mentions_users and mentions_hotels:
            if not (has_user_in_plan and has_hotel_in_plan):
                return False

        # If user asks for lead qualification flow, plan should include upstream assign/send stages.
        if has_qualify_in_plan and (mentions_assign or mentions_send):
            if not (has_assign_in_plan or has_send_in_plan):
                return False

        return True

    def _reorder_selected_capabilities_by_plan(
        self,
        *,
        message: str,
        selected_capabilities: list[SelectedCapability],
    ) -> list[SelectedCapability]:
        if len(selected_capabilities) < 2:
            return selected_capabilities

        ordered_terms = self._extract_user_plan_terms(message)
        if len(ordered_terms) < 2:
            return selected_capabilities

        term_keys = [self._normalize_lookup_token(term) for term in ordered_terms]
        indexed: list[tuple[int, int, SelectedCapability]] = []
        for index, item in enumerate(selected_capabilities):
            aliases = self._capability_aliases(item.capability)
            rank = len(term_keys) + index
            for term_index, term_key in enumerate(term_keys):
                if not term_key:
                    continue
                if any(term_key in alias or alias in term_key for alias in aliases):
                    rank = term_index
                    break
            indexed.append((rank, index, item))

        indexed.sort(key=lambda row: (row[0], row[1]))
        return [row[2] for row in indexed]

    def _filter_capabilities_by_explicit_plan(
        self,
        *,
        message: str,
        selected_capabilities: list[SelectedCapability],
    ) -> list[SelectedCapability]:
        if len(selected_capabilities) < 2:
            return selected_capabilities

        ordered_terms = self._extract_user_plan_terms(message)
        if len(ordered_terms) < 2:
            return selected_capabilities

        term_keys = [self._normalize_lookup_token(term) for term in ordered_terms]
        term_keys = [key for key in term_keys if key]
        if len(term_keys) < 2:
            return selected_capabilities

        matched: list[SelectedCapability] = []
        seen_ids: set[str] = set()

        for term_key in term_keys:
            for item in selected_capabilities:
                cap_id = str(getattr(item.capability, "id", "") or "").strip()
                if cap_id and cap_id in seen_ids:
                    continue
                aliases = self._capability_aliases(item.capability)
                if any(term_key in alias or alias in term_key for alias in aliases):
                    matched.append(item)
                    if cap_id:
                        seen_ids.add(cap_id)
                    break

        # Keep strict plan filtering only when it has useful coverage.
        if len(matched) >= max(2, len(term_keys) // 2):
            return matched
        return selected_capabilities

    def _select_capabilities_by_explicit_plan_terms(
        self,
        *,
        message: str,
        selected_capabilities: list[SelectedCapability],
    ) -> list[SelectedCapability]:
        ordered_terms = self._extract_user_plan_terms(message)
        if len(ordered_terms) < 2:
            return []

        term_keys = [self._normalize_lookup_token(term) for term in ordered_terms]
        term_keys = [key for key in term_keys if key]
        if len(term_keys) < 2:
            return []

        result: list[SelectedCapability] = []
        seen_ids: set[str] = set()
        for term_key in term_keys:
            matched_item: SelectedCapability | None = None
            for item in selected_capabilities:
                cap_id = str(getattr(item.capability, "id", "") or "").strip()
                if cap_id and cap_id in seen_ids:
                    continue
                aliases = self._capability_aliases(item.capability)
                if any(term_key in alias or alias in term_key for alias in aliases):
                    matched_item = item
                    break
            if matched_item is None:
                continue
            cap_id = str(getattr(matched_item.capability, "id", "") or "").strip()
            if cap_id:
                seen_ids.add(cap_id)
            result.append(matched_item)

        # Require good coverage before enabling strict deterministic plan mode.
        if len(result) >= max(2, len(term_keys) // 2):
            return result
        return []

    def _apply_user_plan_order_hint(
        self,
        *,
        message: str,
        nodes: list[dict[str, Any]],
        edges: list[dict[str, Any]],
        selected_capabilities: list[SelectedCapability],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        ordered_terms = self._extract_user_plan_terms(message)
        if len(ordered_terms) < 2 or len(nodes) < 2:
            return nodes, edges

        capabilities_by_id = {
            str(item.capability.id): item.capability for item in selected_capabilities
        }

        step_to_rank: dict[int, int] = {}
        normalized_terms = [self._normalize_lookup_token(term) for term in ordered_terms]
        for node in nodes:
            step = node.get("step")
            if not isinstance(step, int):
                continue
            endpoints = node.get("endpoints")
            if not isinstance(endpoints, list) or not endpoints:
                continue
            first_endpoint = endpoints[0] if isinstance(endpoints[0], dict) else {}
            cap_id = str(first_endpoint.get("capability_id", "") or "").strip()
            capability = capabilities_by_id.get(cap_id)
            if capability is None:
                continue
            aliases = self._capability_aliases(capability)
            for term_index, term in enumerate(normalized_terms):
                if not term:
                    continue
                if any(term in alias or alias in term for alias in aliases):
                    step_to_rank[step] = term_index
                    break

        if len(step_to_rank) < 2:
            return nodes, edges

        sorted_nodes = sorted(
            nodes,
            key=lambda node: (
                step_to_rank.get(node.get("step"), 10_000),
                node.get("step", 10_000),
            ),
        )
        step_remap: dict[int, int] = {}
        remapped_nodes: list[dict[str, Any]] = []
        for index, node in enumerate(sorted_nodes, start=1):
            old_step = node.get("step")
            if isinstance(old_step, int):
                step_remap[old_step] = index
            cloned = dict(node)
            cloned["step"] = index
            remapped_nodes.append(cloned)

        remapped_edges: list[dict[str, Any]] = []
        seen_edges: set[tuple[int, int, str]] = set()
        for edge in edges:
            src = edge.get("from_step")
            dst = edge.get("to_step")
            edge_type = str(edge.get("type", "") or "").strip()
            if not isinstance(src, int) or not isinstance(dst, int):
                continue
            if src not in step_remap or dst not in step_remap or not edge_type:
                continue
            remapped = (step_remap[src], step_remap[dst], edge_type)
            if remapped[0] == remapped[1] or remapped in seen_edges:
                continue
            seen_edges.add(remapped)
            remapped_edges.append(
                {
                    "from_step": remapped[0],
                    "to_step": remapped[1],
                    "type": remapped[2],
                }
            )

        self._sync_node_connections(remapped_nodes, remapped_edges)
        self._ensure_external_inputs(remapped_nodes, remapped_edges)
        return remapped_nodes, remapped_edges

    def _capability_aliases(self, capability: Any) -> set[str]:
        context = self._extract_capability_action_context(capability)
        raw_aliases = [
            str(getattr(capability, "name", "") or ""),
            str(getattr(capability, "description", "") or ""),
            str(context.get("operation_id", "") or ""),
            str(context.get("summary", "") or ""),
            str(context.get("path", "") or ""),
        ]
        aliases: set[str] = set()
        for alias in raw_aliases:
            normalized = self._normalize_lookup_token(alias)
            if normalized:
                aliases.add(normalized)
        return aliases

    def _select_fallback_capabilities(
        self,
        *,
        message: str,
        selected_capabilities: list[SelectedCapability],
        max_items: int = 8,
    ) -> list[SelectedCapability]:
        if len(selected_capabilities) <= max_items:
            return selected_capabilities

        message_lower = (message or "").lower()
        need_users = ("пользоват" in message_lower) or ("user" in message_lower)
        need_hotels = ("отел" in message_lower) or ("hotel" in message_lower)
        need_segment = ("сегмент" in message_lower) or ("segment" in message_lower)
        need_assign = ("назнач" in message_lower) or ("assign" in message_lower)

        def category(item: SelectedCapability) -> str:
            signature = self._fallback_capability_signature(item.capability)
            if "segment" in signature:
                return "segment"
            if "assign" in signature:
                return "assign"
            if "hotel" in signature:
                return "hotel"
            if "user" in signature or "client" in signature:
                return "user"
            return "other"

        by_category: dict[str, list[SelectedCapability]] = {
            "user": [],
            "hotel": [],
            "segment": [],
            "assign": [],
            "other": [],
        }
        for item in selected_capabilities:
            by_category[category(item)].append(item)

        selected: list[SelectedCapability] = []
        seen_ids: set[str] = set()

        def add_first(cat: str) -> None:
            for candidate in by_category.get(cat, []):
                cap_id = str(getattr(candidate.capability, "id", "") or "")
                if cap_id and cap_id in seen_ids:
                    continue
                if cap_id:
                    seen_ids.add(cap_id)
                selected.append(candidate)
                return

        if need_users and need_segment:
            add_first("user")
        if need_hotels and need_segment:
            add_first("hotel")
        if need_segment:
            add_first("segment")
        if need_assign:
            add_first("assign")

        target_len = max(max_items, len(selected))
        for item in selected_capabilities:
            if len(selected) >= target_len:
                break
            cap_id = str(getattr(item.capability, "id", "") or "")
            if cap_id and cap_id in seen_ids:
                continue
            if cap_id:
                seen_ids.add(cap_id)
            selected.append(item)

        return selected[:target_len]

    def _enforce_segment_merge_edges(
        self,
        *,
        message: str,
        nodes: list[dict[str, Any]],
        edges: list[dict[str, Any]],
        selected_capabilities: list[SelectedCapability],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        if len(nodes) < 3:
            return nodes, edges

        normalized_message = self._normalize_lookup_token(message)
        if "segment" not in normalized_message and "сегмент" not in normalized_message:
            return nodes, edges

        capabilities_by_id = {
            str(item.capability.id): item.capability for item in selected_capabilities
        }

        step_to_signature: dict[int, str] = {}
        for node in nodes:
            step = node.get("step")
            if not isinstance(step, int):
                continue
            endpoints = node.get("endpoints")
            if not isinstance(endpoints, list) or not endpoints:
                continue
            endpoint = endpoints[0] if isinstance(endpoints[0], dict) else {}
            cap_id = str(endpoint.get("capability_id", "") or "").strip()
            capability = capabilities_by_id.get(cap_id)
            if capability is None:
                continue
            step_to_signature[step] = self._fallback_capability_signature(capability)

        if not step_to_signature:
            return nodes, edges

        incoming_by_target: dict[int, set[int]] = {}
        seen_edges: set[tuple[int, int, str]] = set()
        normalized_edges: list[dict[str, Any]] = []
        for edge in edges:
            src = edge.get("from_step")
            dst = edge.get("to_step")
            edge_type = str(edge.get("type", "") or "").strip()
            if not isinstance(src, int) or not isinstance(dst, int) or not edge_type:
                continue
            key = (src, dst, edge_type)
            if key in seen_edges:
                continue
            seen_edges.add(key)
            normalized_edges.append(
                {
                    "from_step": src,
                    "to_step": dst,
                    "type": edge_type,
                }
            )
            incoming_by_target.setdefault(dst, set()).add(src)

        segment_steps = [
            step for step, signature in step_to_signature.items() if "segment" in signature
        ]
        if not segment_steps:
            return nodes, normalized_edges

        for segment_step in segment_steps:
            predecessors = [
                step for step in step_to_signature.keys() if step < segment_step
            ]
            if not predecessors:
                continue

            user_step = None
            hotel_step = None
            for step in sorted(predecessors):
                signature = step_to_signature.get(step, "")
                if user_step is None and ("user" in signature or "client" in signature):
                    user_step = step
                if hotel_step is None and "hotel" in signature:
                    hotel_step = step

            if user_step is None or hotel_step is None:
                continue

            current_incoming = incoming_by_target.get(segment_step, set())
            additions: list[tuple[int, str]] = []
            if user_step not in current_incoming:
                additions.append((user_step, "users"))
            if hotel_step not in current_incoming:
                additions.append((hotel_step, "hotels"))

            for src_step, edge_type in additions:
                key = (src_step, segment_step, edge_type)
                if key in seen_edges:
                    continue
                seen_edges.add(key)
                normalized_edges.append(
                    {
                        "from_step": src_step,
                        "to_step": segment_step,
                        "type": edge_type,
                    }
                )
                incoming_by_target.setdefault(segment_step, set()).add(src_step)

        normalized_edges = self._repair_edges_with_data_flow(nodes, normalized_edges)
        self._sync_node_connections(nodes, normalized_edges)
        self._ensure_external_inputs(nodes, normalized_edges)
        return nodes, normalized_edges

    def _enforce_explicit_plan_step_count(
        self,
        *,
        message: str,
        nodes: list[dict[str, Any]],
        edges: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        terms = self._extract_user_plan_terms(message)
        if len(terms) < 2:
            return nodes, edges

        target_steps = len(terms)
        if target_steps <= 0:
            return nodes, edges

        sorted_nodes = sorted(
            [node for node in nodes if isinstance(node.get("step"), int)],
            key=lambda item: item.get("step", 0),
        )
        if not sorted_nodes:
            return nodes, edges

        trimmed_nodes = sorted_nodes[:target_steps]
        allowed_steps = {
            int(node["step"])
            for node in trimmed_nodes
            if isinstance(node.get("step"), int)
        }
        trimmed_edges = [
            edge
            for edge in edges
            if isinstance(edge.get("from_step"), int)
            and isinstance(edge.get("to_step"), int)
            and int(edge.get("from_step")) in allowed_steps
            and int(edge.get("to_step")) in allowed_steps
        ]

        trimmed_nodes, trimmed_edges = self._compact_step_sequence(trimmed_nodes, trimmed_edges)
        self._sync_node_connections(trimmed_nodes, trimmed_edges)
        self._ensure_external_inputs(trimmed_nodes, trimmed_edges)
        return trimmed_nodes, trimmed_edges

    def _prune_unsolicited_send_nodes(
        self,
        *,
        message: str,
        nodes: list[dict[str, Any]],
        edges: list[dict[str, Any]],
        selected_capabilities: list[SelectedCapability],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        if self._message_requests_send_offers(message):
            return nodes, edges

        capabilities_by_id = {
            str(item.capability.id): item.capability for item in selected_capabilities
        }

        send_steps: set[int] = set()
        for node in nodes:
            step = node.get("step")
            if not isinstance(step, int):
                continue
            endpoints = node.get("endpoints")
            if not isinstance(endpoints, list) or not endpoints:
                continue
            endpoint = endpoints[0] if isinstance(endpoints[0], dict) else {}
            cap_id = str(endpoint.get("capability_id", "") or "").strip()
            capability = capabilities_by_id.get(cap_id)
            if capability is None:
                continue
            signature = self._fallback_capability_signature(capability)
            if any(token in signature for token in ("send", "email", "offer")):
                send_steps.add(step)

        if not send_steps:
            return nodes, edges

        pruned_nodes = [
            node
            for node in nodes
            if isinstance(node.get("step"), int) and int(node.get("step")) not in send_steps
        ]
        pruned_edges = [
            edge
            for edge in edges
            if isinstance(edge.get("from_step"), int)
            and isinstance(edge.get("to_step"), int)
            and int(edge.get("from_step")) not in send_steps
            and int(edge.get("to_step")) not in send_steps
        ]

        pruned_nodes, pruned_edges = self._prune_disconnected_nodes(pruned_nodes, pruned_edges)
        pruned_nodes, pruned_edges = self._compact_step_sequence(pruned_nodes, pruned_edges)
        self._sync_node_connections(pruned_nodes, pruned_edges)
        self._ensure_external_inputs(pruned_nodes, pruned_edges)
        return pruned_nodes, pruned_edges

    def _prune_unsolicited_assign_nodes(
        self,
        *,
        message: str,
        nodes: list[dict[str, Any]],
        edges: list[dict[str, Any]],
        selected_capabilities: list[SelectedCapability],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        if self._message_requests_assign_stage(message):
            return nodes, edges

        capabilities_by_id = {
            str(item.capability.id): item.capability for item in selected_capabilities
        }

        assign_steps: set[int] = set()
        for node in nodes:
            step = node.get("step")
            if not isinstance(step, int):
                continue
            endpoints = node.get("endpoints")
            if not isinstance(endpoints, list) or not endpoints:
                continue
            endpoint = endpoints[0] if isinstance(endpoints[0], dict) else {}
            cap_id = str(endpoint.get("capability_id", "") or "").strip()
            capability = capabilities_by_id.get(cap_id)
            if capability is None:
                continue
            signature = self._fallback_capability_signature(capability)
            if "assign" in signature:
                assign_steps.add(step)

        if not assign_steps:
            return nodes, edges

        pruned_nodes = [
            node
            for node in nodes
            if isinstance(node.get("step"), int) and int(node.get("step")) not in assign_steps
        ]
        pruned_edges = [
            edge
            for edge in edges
            if isinstance(edge.get("from_step"), int)
            and isinstance(edge.get("to_step"), int)
            and int(edge.get("from_step")) not in assign_steps
            and int(edge.get("to_step")) not in assign_steps
        ]

        pruned_nodes, pruned_edges = self._prune_disconnected_nodes(pruned_nodes, pruned_edges)
        pruned_nodes, pruned_edges = self._compact_step_sequence(pruned_nodes, pruned_edges)
        self._sync_node_connections(pruned_nodes, pruned_edges)
        self._ensure_external_inputs(pruned_nodes, pruned_edges)
        return pruned_nodes, pruned_edges

    def _selection_is_low_confidence(
        self, selected_capabilities: list[SelectedCapability]
    ) -> bool:
        if not selected_capabilities:
            return False
        return str(selected_capabilities[0].confidence_tier).lower() == "low"

    def _build_low_confidence_question_ru(
        self,
        *,
        question_number: int = 1,
        message: str = "",
        dialog_messages: list[dict[str, Any]] | None = None,
        selected_capabilities: list[SelectedCapability] | None = None,
    ) -> str:
        llm_question = self._generate_clarification_question_ru(
            question_number=question_number,
            message=message,
            dialog_messages=dialog_messages or [],
            selected_capabilities=selected_capabilities or [],
        )
        if llm_question:
            return llm_question

        outcome_question = (
            "Нужно уточнить цель, чтобы построить точный сценарий. "
            "Какой финальный бизнес-результат нужен: сегмент, рассылка, "
            "обновление CRM или отчёт?"
        )
        if question_number <= 1 or not selected_capabilities:
            return outcome_question

        context_tokens = self._collect_user_context_tokens(
            message=message,
            dialog_messages=dialog_messages or [],
        )
        missing_inputs = self._collect_missing_required_inputs(
            selected_capabilities=selected_capabilities,
            context_tokens=context_tokens,
            limit=3,
        )
        if not missing_inputs:
            return (
                "Нужно уточнить входные ограничения для точного графа. "
                "Укажите: кого выбираем, по каким фильтрам, и какие поля обязательны в результате."
            )

        humanized_inputs = ", ".join(self._humanize_input_name(name) for name in missing_inputs)
        return (
            "Нужно уточнить цель, чтобы построить точный сценарий. "
            f"Чтобы собрать исполнимый граф, уточните входные данные: {humanized_inputs}."
        )

    def _generate_clarification_question_ru(
        self,
        *,
        question_number: int,
        message: str,
        dialog_messages: list[dict[str, Any]],
        selected_capabilities: list[SelectedCapability],
    ) -> str | None:
        if not selected_capabilities:
            return None

        user_messages = [
            str(item.get("content", ""))
            for item in dialog_messages[-8:]
            if isinstance(item, dict)
            and str(item.get("role", "")).lower() == "user"
        ]
        previous_questions = [
            self._strip_low_confidence_marker(str(item.get("content", "")))
            for item in dialog_messages[-8:]
            if isinstance(item, dict)
            and str(item.get("role", "")).lower() == "assistant"
            and self._is_low_confidence_question(str(item.get("content", "")))
        ]

        context_tokens = self._collect_user_context_tokens(
            message=message,
            dialog_messages=dialog_messages,
        )
        missing_inputs = self._collect_missing_required_inputs(
            selected_capabilities=selected_capabilities,
            context_tokens=context_tokens,
            limit=5,
        )
        grounding_terms = self._collect_capability_grounding_terms(
            selected_capabilities=selected_capabilities,
            missing_inputs=missing_inputs,
            limit=20,
        )

        capabilities_payload = []
        for item in selected_capabilities[:5]:
            cap = item.capability
            capabilities_payload.append(
                {
                    "name": str(getattr(cap, "name", "") or ""),
                    "description": str(getattr(cap, "description", "") or ""),
                    "required_inputs": self._extract_required_inputs(
                        getattr(cap, "input_schema", None)
                    ),
                    "action_context": self._extract_capability_action_context(cap),
                }
            )

        prompt_payload = {
            "stage": question_number,
            "max_questions": self.LOW_CONFIDENCE_MAX_QUESTIONS,
            "current_user_message": message,
            "recent_user_messages": user_messages[-4:],
            "previous_clarification_questions": previous_questions[-2:],
            "candidate_capabilities": capabilities_payload,
            "missing_required_inputs": missing_inputs,
            "grounding_terms": grounding_terms,
        }

        system_prompt = (
            "Ты продуктовый ассистент, который задаёт один уточняющий вопрос на русском "
            "для построения исполнимого pipeline. "
            "Верни только JSON формата {\"question_ru\": \"...\"}. "
            "Не используй префиксы вроде 'Уточнение 1/2'. "
            "Не перечисляй много пунктов: один конкретный вопрос. "
            "Вопрос должен быть привязан к candidate_capabilities: используй термины из grounding_terms "
            "или missing_required_inputs и не придумывай новые сущности/ручки."
        )
        user_prompt = (
            "Сгенерируй один следующий вопрос для пользователя на основе контекста.\n"
            "Правила:\n"
            "- Если stage=1: спроси про финальный бизнес-результат и критерий успеха.\n"
            "- Если stage=2: спроси про самый важный недостающий вход/ограничение для исполнения.\n"
            "- Вопрос должен быть конкретным и связанным с candidate_capabilities.\n"
            "- Вопрос обязан содержать минимум один термин из grounding_terms.\n"
            "- Если есть missing_required_inputs, приоритетно используй их в формулировке.\n"
            "- Верни только JSON.\n\n"
            f"CONTEXT:\n{json.dumps(prompt_payload, ensure_ascii=False)}"
        )

        try:
            payload = chat_json(system_prompt=system_prompt, user_prompt=user_prompt)
        except Exception:
            return None
        if not isinstance(payload, dict):
            return None

        question = payload.get("question_ru")
        if not isinstance(question, str):
            return None
        normalized = " ".join(question.strip().split())
        normalized = self._strip_low_confidence_marker(normalized)
        normalized = normalized.replace("Уточнение 1/2:", "").replace("Уточнение 2/2:", "").strip()
        if len(normalized) < 12:
            return None
        if not self._is_question_grounded_in_capabilities(
            question=normalized,
            grounding_terms=grounding_terms,
            missing_inputs=missing_inputs,
        ):
            return self._build_grounded_fallback_question_ru(
                question_number=question_number,
                selected_capabilities=selected_capabilities,
                missing_inputs=missing_inputs,
            )
        if not normalized.endswith("?"):
            normalized = f"{normalized}?"
        return normalized

    def _collect_capability_grounding_terms(
        self,
        *,
        selected_capabilities: list[SelectedCapability],
        missing_inputs: list[str],
        limit: int = 20,
    ) -> list[str]:
        terms: list[str] = []
        seen: set[str] = set()

        for item in selected_capabilities[:5]:
            cap = item.capability
            context = self._extract_capability_action_context(cap)
            for value in (
                getattr(cap, "name", None),
                getattr(cap, "description", None),
                context.get("operation_id"),
                context.get("path"),
                context.get("method"),
                context.get("summary"),
            ):
                if not isinstance(value, str):
                    continue
                cleaned = value.strip()
                key = cleaned.lower()
                if not cleaned or key in seen:
                    continue
                seen.add(key)
                terms.append(cleaned)
                if len(terms) >= limit:
                    return terms

            tags = context.get("tags")
            if isinstance(tags, list):
                for tag in tags:
                    if not isinstance(tag, str):
                        continue
                    cleaned = tag.strip()
                    key = cleaned.lower()
                    if not cleaned or key in seen:
                        continue
                    seen.add(key)
                    terms.append(cleaned)
                    if len(terms) >= limit:
                        return terms

            required = context.get("required_inputs")
            if isinstance(required, list):
                for item_value in required:
                    if not isinstance(item_value, str):
                        continue
                    cleaned = item_value.strip()
                    key = cleaned.lower()
                    if not cleaned or key in seen:
                        continue
                    seen.add(key)
                    terms.append(cleaned)
                    if len(terms) >= limit:
                        return terms

        for value in missing_inputs:
            cleaned = str(value).strip()
            key = cleaned.lower()
            if not cleaned or key in seen:
                continue
            seen.add(key)
            terms.append(cleaned)
            if len(terms) >= limit:
                break

        return terms

    def _is_question_grounded_in_capabilities(
        self,
        *,
        question: str,
        grounding_terms: list[str],
        missing_inputs: list[str],
    ) -> bool:
        question_lower = question.lower()
        question_tokens = self._tokenize_text(question)
        if not question_tokens:
            return False

        for term in grounding_terms + missing_inputs:
            term_text = str(term).strip()
            if not term_text:
                continue
            term_lower = term_text.lower()
            if term_lower in question_lower:
                return True
            term_tokens = self._tokenize_field_name(term_text)
            if term_tokens and term_tokens & question_tokens:
                return True

        return False

    def _build_grounded_fallback_question_ru(
        self,
        *,
        question_number: int,
        selected_capabilities: list[SelectedCapability],
        missing_inputs: list[str],
    ) -> str:
        primary_signature = "выбранной capability"
        if selected_capabilities:
            cap = selected_capabilities[0].capability
            context = self._extract_capability_action_context(cap)
            method = context.get("method")
            path = context.get("path")
            if isinstance(method, str) and isinstance(path, str) and method and path:
                primary_signature = f"{method} {path}"
            else:
                cap_name = str(getattr(cap, "name", "") or "").strip()
                if cap_name:
                    primary_signature = cap_name

        if question_number <= 1:
            return (
                f"Какой конечный бизнес-результат нужен для сценария с {primary_signature}, "
                "чтобы я собрал исполнимый pipeline?"
            )

        if missing_inputs:
            first_input = self._humanize_input_name(missing_inputs[0])
            return (
                f"Для шага {primary_signature} какое значение вы передадите в поле "
                f"\"{first_input}\"?"
            )

        return (
            f"Для шага {primary_signature} какие входные параметры обязательны, "
            "чтобы можно было выполнить pipeline без ошибок?"
        )

    def _count_low_confidence_questions(
        self,
        dialog_messages: list[dict[str, Any]],
    ) -> int:
        count = 0
        # Count only the current clarification chain (from the end of dialog),
        # so each new request can start a fresh 2-step clarification when needed.
        for item in reversed(dialog_messages):
            if not isinstance(item, dict):
                continue
            role = str(item.get("role", "")).lower()
            if role == "user":
                continue
            if role != "assistant":
                continue
            content = str(item.get("content", "")).lower()
            if self._is_low_confidence_question(content):
                count += 1
                continue
            break
        return count

    def _is_low_confidence_question(self, content: str) -> bool:
        content = content.strip().lower()
        return (
            self.LOW_CONFIDENCE_DIALOG_MARKER in content
            or self.LOW_CONFIDENCE_QUESTION_MARKER in content
            or "какой финальный бизнес-результат нужен" in content
        )

    def _attach_low_confidence_marker(self, question: str) -> str:
        return f"{question}\n{self.LOW_CONFIDENCE_DIALOG_MARKER}"

    def _strip_low_confidence_marker(self, content: str) -> str:
        return content.replace(self.LOW_CONFIDENCE_DIALOG_MARKER, "").strip()

    def _collect_user_context_tokens(
        self,
        *,
        message: str,
        dialog_messages: list[dict[str, Any]],
    ) -> set[str]:
        tokens = set(self._tokenize_text(message or ""))
        for item in dialog_messages[-6:]:
            if not isinstance(item, dict):
                continue
            if str(item.get("role", "")).lower() != "user":
                continue
            tokens.update(self._tokenize_text(str(item.get("content", ""))))
        return tokens

    def _collect_missing_required_inputs(
        self,
        *,
        selected_capabilities: list[SelectedCapability],
        context_tokens: set[str],
        limit: int = 3,
    ) -> list[str]:
        missing: list[str] = []
        seen: set[str] = set()

        for item in selected_capabilities[:3]:
            required_inputs = self._extract_required_inputs(
                getattr(item.capability, "input_schema", None)
            )
            for required_input in required_inputs:
                key = str(required_input).strip().lower()
                if not key or key in seen:
                    continue
                seen.add(key)

                required_tokens = self._tokenize_field_name(required_input)
                if required_tokens and required_tokens & context_tokens:
                    continue

                missing.append(str(required_input))
                if len(missing) >= limit:
                    return missing

        return missing

    def _tokenize_field_name(self, value: str) -> set[str]:
        normalized = re.sub(r"([a-z])([A-Z])", r"\1 \2", str(value))
        normalized = normalized.replace("_", " ").replace("-", " ")
        tokens = self._tokenize_text(normalized)

        value_lower = str(value).lower()
        if value_lower.endswith("_id") and len(value_lower) > 3:
            tokens.add(value_lower[:-3])
        return tokens

    def _humanize_input_name(self, name: str) -> str:
        label = re.sub(r"([a-z])([A-Z])", r"\1 \2", str(name))
        label = label.replace("_", " ").replace("-", " ").strip()
        return label or str(name)

    def _extract_capability_action_context(self, capability: Any) -> dict[str, Any]:
        llm_payload = getattr(capability, "llm_payload", None)
        if not isinstance(llm_payload, dict):
            return {}

        brief = llm_payload.get("action_context_brief")
        if isinstance(brief, dict):
            return brief

        fallback = llm_payload.get("action_context")
        if not isinstance(fallback, dict):
            return {}

        # Keep prompt payload compact even if full action context is stored.
        compact: dict[str, Any] = {}
        for key in (
            "operation_id",
            "method",
            "path",
            "base_url",
            "summary",
            "description",
            "tags",
            "source_filename",
        ):
            if key in fallback:
                compact[key] = fallback.get(key)
        return compact

    def _build_capability_prompt_payload(self, capability: Any) -> dict[str, Any]:
        payload = {
            "id": str(getattr(capability, "id", "")),
            "action_id": str(getattr(capability, "action_id", ""))
            if getattr(capability, "action_id", None) is not None
            else None,
            "type": self._capability_type_value(capability),
            "name": getattr(capability, "name", None),
            "description": getattr(capability, "description", None),
            "input_type": getattr(capability, "input_schema", None),
            "output_type": getattr(capability, "output_schema", None),
            "required_inputs": self._extract_required_inputs(
                getattr(capability, "input_schema", None)
            ),
            "data_format": getattr(capability, "data_format", None),
            "action_context": self._extract_capability_action_context(capability),
        }
        recipe_summary = self._extract_recipe_summary(capability)
        if recipe_summary is not None:
            payload["recipe_summary"] = recipe_summary
        return payload

    def _extract_recipe_summary(self, capability: Any) -> dict[str, Any] | None:
        llm_payload = getattr(capability, "llm_payload", None)
        if isinstance(llm_payload, dict):
            summary = llm_payload.get("recipe_summary")
            if isinstance(summary, dict):
                return summary

        recipe = getattr(capability, "recipe", None)
        if not isinstance(recipe, dict):
            return None
        steps = recipe.get("steps")
        if not isinstance(steps, list):
            return None
        return {
            "version": recipe.get("version"),
            "steps_count": len(steps),
        }

    def _capability_type_value(self, capability: Any) -> str:
        cap_type = getattr(capability, "type", None)
        if isinstance(cap_type, str):
            return cap_type.upper()
        if hasattr(cap_type, "value"):
            return str(cap_type.value).upper()
        return "ATOMIC"

    def _recipe_is_executable(self, recipe: Any) -> bool:
        if not isinstance(recipe, dict):
            return False
        if recipe.get("version") != 1:
            return False
        steps = recipe.get("steps")
        return isinstance(steps, list) and bool(steps)

    def _normalize_workflow(
        self,
        raw_graph: dict[str, Any],
        selected_capabilities: list[SelectedCapability],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
        capabilities_by_id = {
            str(sc.capability.id): sc.capability for sc in selected_capabilities
        }
        capabilities = [sc.capability for sc in selected_capabilities]
        issues: list[str] = []

        raw_nodes = raw_graph.get("nodes") if isinstance(raw_graph, dict) else None
        if not isinstance(raw_nodes, list):
            raw_nodes = []

        normalized_nodes: list[dict[str, Any]] = []
        step_map: dict[Any, int] = {}
        known_steps: set[int] = set()
        next_step = 1

        for raw_node in raw_nodes:
            if not isinstance(raw_node, dict):
                continue

            raw_step = raw_node.get("step")
            raw_id = raw_node.get("id")
            step = self._resolve_step_reference(raw_step, step_map=step_map, known_steps=known_steps)
            if step is None:
                step = self._resolve_step_reference(raw_id, step_map=step_map, known_steps=known_steps)
            if step is None:
                while next_step in known_steps:
                    next_step += 1
                step = next_step
                next_step += 1

            known_steps.add(step)
            self._register_step_alias(step_map, raw_step, step)
            self._register_step_alias(step_map, raw_id, step)
            self._register_step_alias(step_map, step, step)

            capability_id_raw = raw_node.get("capability_id")
            raw_endpoints = raw_node.get("endpoints")
            has_raw_endpoints = isinstance(raw_endpoints, list) and bool(raw_endpoints)
            cap = self._resolve_capability_for_node(
                raw_node=raw_node,
                capability_id=capability_id_raw,
                capabilities=capabilities,
                capabilities_by_id=capabilities_by_id,
            )
            has_explicit_capability_ref = (
                capability_id_raw is not None and str(capability_id_raw).strip() != ""
            )
            if has_explicit_capability_ref and cap is None:
                issues.append("graph:invalid_capability_ref")
            elif cap is None and len(capabilities) > 1 and not has_raw_endpoints:
                issues.append("graph:missing_capability_ref")

            endpoints_payload = self._resolve_endpoints_for_node(
                raw_node=raw_node,
                fallback_capability=cap,
                capabilities=capabilities,
                capabilities_by_id=capabilities_by_id,
                issues=issues,
            )
            if not endpoints_payload and cap is not None:
                endpoints_payload = [self._build_endpoint_payload(cap)]

            normalized_nodes.append(
                {
                    "step": step,
                    "name": raw_node.get("name")
                    or (cap.name if cap else f"Шаг {step}"),
                    "description": raw_node.get("description"),
                    "input_connected_from": self._normalize_int_list(
                        raw_node.get("input_connected_from")
                    ),
                    "output_connected_to": self._normalize_int_list(
                        raw_node.get("output_connected_to")
                    ),
                    "input_data_type_from_previous": self._normalize_input_data_types(
                        raw_node.get("input_data_type_from_previous")
                    ),
                    "external_inputs": self._normalize_str_list(
                        raw_node.get("external_inputs")
                    ),
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
            from_ref = (
                raw_edge.get("from_step")
                or raw_edge.get("from")
                or raw_edge.get("source")
            )
            to_ref = (
                raw_edge.get("to_step") or raw_edge.get("to") or raw_edge.get("target")
            )
            edge_type = raw_edge.get("type")
            from_step = self._resolve_step_reference(from_ref, step_map=step_map, known_steps=known_steps)
            to_step = self._resolve_step_reference(to_ref, step_map=step_map, known_steps=known_steps)
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

        return normalized_nodes, normalized_edges, sorted(set(issues))

    def _resolve_endpoints_for_node(
        self,
        *,
        raw_node: dict[str, Any],
        fallback_capability: Any | None,
        capabilities: list[Any],
        capabilities_by_id: dict[str, Any],
        issues: list[str],
    ) -> list[dict[str, Any]]:
        raw_endpoints = raw_node.get("endpoints")
        if not isinstance(raw_endpoints, list):
            return []

        resolved_endpoints: list[dict[str, Any]] = []
        fallback_capability_id = raw_node.get("capability_id")
        fallback_capability_str = (
            str(fallback_capability_id).strip()
            if fallback_capability_id is not None
            else ""
        )
        fallback_from_node = (
            capabilities_by_id.get(fallback_capability_str)
            if fallback_capability_str
            else None
        )

        for raw_endpoint in raw_endpoints:
            if not isinstance(raw_endpoint, dict):
                issues.append("graph:invalid_capability_ref")
                continue

            endpoint_capability_id = raw_endpoint.get("capability_id")
            endpoint_capability: Any | None = None
            if endpoint_capability_id is not None and str(endpoint_capability_id).strip():
                endpoint_capability = capabilities_by_id.get(str(endpoint_capability_id).strip())
            elif fallback_from_node is not None:
                endpoint_capability = fallback_from_node
            elif fallback_capability is not None:
                endpoint_capability = fallback_capability
            elif len(capabilities) == 1:
                endpoint_capability = capabilities[0]

            if endpoint_capability is None:
                issues.append("graph:invalid_capability_ref")
                continue

            resolved_endpoints.append(
                self._build_endpoint_payload(endpoint_capability, raw_endpoint=raw_endpoint)
            )

        return resolved_endpoints

    def _build_endpoint_payload(
        self,
        capability: Any,
        *,
        raw_endpoint: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        cap_type = getattr(capability, "type", None)
        if hasattr(cap_type, "value"):
            cap_type_value = cap_type.value
        elif cap_type is None:
            cap_type_value = None
        else:
            cap_type_value = str(cap_type)

        endpoint_name = None
        if isinstance(raw_endpoint, dict):
            raw_name = raw_endpoint.get("name")
            if isinstance(raw_name, str) and raw_name.strip():
                endpoint_name = raw_name.strip()

        return {
            "name": endpoint_name or capability.name,
            "capability_id": str(capability.id),
            "action_id": str(capability.action_id) if capability.action_id is not None else None,
            "type": cap_type_value,
            "input_type": capability.input_schema,
            "output_type": capability.output_schema,
        }

    def _review_graph_with_llm(
        self,
        nodes: list[dict[str, Any]],
        edges: list[dict[str, Any]],
        selected_capabilities: list[SelectedCapability],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        if not nodes:
            return nodes, edges

        capabilities_payload = [
            self._build_capability_prompt_payload(sc.capability)
            for sc in selected_capabilities
        ]

        review_prompt = (
            "ROLE: Graph reviewer for qwen2.5:7b-coder.\n"
            "INPUT: GRAPH and CAPABILITIES only.\n"
            "TASK: Keep executable DAG, remove dead branches, and fix edges.\n"
            "CONSTRAINTS:\n"
            "- Do not invent new steps.\n"
            "- Keep only steps that contribute to final goal.\n"
            "- Return JSON only.\n\n"
            "OUTPUT_FORMAT:\n"
            "{\n"
            "  \"keep_steps\": [1,2,3],\n"
            "  \"edges\": [\n"
            "    {\"from_step\": 1, \"to_step\": 2, \"type\": \"field_name\"}\n"
            "  ]\n"
            "}\n\n"
            f"CAPABILITIES:\n{json.dumps(capabilities_payload, ensure_ascii=False)}\n\n"
            f"GRAPH:\n{json.dumps({'nodes': nodes, 'edges': edges}, ensure_ascii=False)}"
        )
        system_prompt = (
            "You validate workflow graph connectivity for qwen2.5-coder. "
            "Return ONLY one valid JSON object."
        )

        try:
            payload = chat_json(system_prompt=system_prompt, user_prompt=review_prompt)
        except Exception:
            return nodes, edges

        if not isinstance(payload, dict):
            return nodes, edges

        known_steps = {
            step for node in nodes if isinstance((step := node.get("step")), int)
        }
        if not known_steps:
            return nodes, edges

        keep_steps = set(self._normalize_int_list(payload.get("keep_steps")))
        keep_steps = keep_steps & known_steps if keep_steps else set(known_steps)
        if len(nodes) >= 3 and len(keep_steps) < 2:
            # Guardrail: avoid collapsing multi-step workflows to a single step
            # when reviewer output is overly aggressive.
            keep_steps = set(known_steps)

        reviewed_edges = self._normalize_review_edges(payload.get("edges"), keep_steps)
        if not reviewed_edges:
            reviewed_edges = [
                edge
                for edge in edges
                if isinstance(edge.get("from_step"), int)
                and isinstance(edge.get("to_step"), int)
                and edge["from_step"] in keep_steps
                and edge["to_step"] in keep_steps
            ]

        reviewed_nodes = [
            node
            for node in nodes
            if isinstance(node.get("step"), int) and node["step"] in keep_steps
        ]
        return reviewed_nodes, reviewed_edges

    def _normalize_review_edges(
        self,
        raw_edges: Any,
        keep_steps: set[int],
    ) -> list[dict[str, Any]]:
        if not isinstance(raw_edges, list):
            return []

        result: list[dict[str, Any]] = []
        seen: set[tuple[int, int, str]] = set()
        for edge in raw_edges:
            if not isinstance(edge, dict):
                continue
            src = edge.get("from_step")
            dst = edge.get("to_step")
            edge_type = edge.get("type")
            if not isinstance(src, int) or not isinstance(dst, int):
                continue
            if src not in keep_steps or dst not in keep_steps:
                continue
            if not isinstance(edge_type, str) or not edge_type.strip():
                continue
            key = (src, dst, edge_type.strip())
            if key in seen:
                continue
            seen.add(key)
            result.append(
                {
                    "from_step": src,
                    "to_step": dst,
                    "type": edge_type.strip(),
                }
            )
        return result

    def _resolve_capability_for_node(
        self,
        *,
        raw_node: dict[str, Any],
        capability_id: Any,
        capabilities: list[Any],
        capabilities_by_id: dict[str, Any],
    ) -> Any | None:
        _ = raw_node
        has_explicit_capability_ref = capability_id is not None and str(capability_id).strip() != ""
        if has_explicit_capability_ref:
            return capabilities_by_id.get(str(capability_id).strip())

        if len(capabilities) == 1 and not has_explicit_capability_ref:
            return capabilities[0]

        return None

    def _collect_node_capability_hints(self, raw_node: dict[str, Any]) -> list[str]:
        hints: list[str] = []
        for key in ("name", "description", "title", "operation_id", "action_id"):
            value = raw_node.get(key)
            if isinstance(value, str) and value.strip():
                hints.append(value.strip())

        endpoints = raw_node.get("endpoints")
        if isinstance(endpoints, list):
            for endpoint in endpoints:
                if not isinstance(endpoint, dict):
                    continue
                for key in ("name", "description", "summary", "operation_id", "action_id", "path"):
                    value = endpoint.get(key)
                    if isinstance(value, str) and value.strip():
                        hints.append(value.strip())

        return hints

    def _match_capability_by_alias(self, capabilities: list[Any], lookup_value: str) -> Any | None:
        query = str(lookup_value or "").strip()
        if not query:
            return None

        lowered = query.lower()
        normalized_query = self._normalize_lookup_token(query)
        strong_matches: list[Any] = []
        normalized_matches: list[Any] = []
        fuzzy_matches: list[Any] = []

        for capability in capabilities:
            aliases = self._collect_capability_aliases(capability)
            for alias in aliases:
                alias_lower = alias.lower()
                if alias_lower == lowered:
                    strong_matches.append(capability)
                    break

            if capability in strong_matches:
                continue

            for alias in aliases:
                alias_normalized = self._normalize_lookup_token(alias)
                if normalized_query and alias_normalized == normalized_query:
                    normalized_matches.append(capability)
                    break

            if capability in normalized_matches:
                continue

            if len(normalized_query) >= 4:
                for alias in aliases:
                    alias_normalized = self._normalize_lookup_token(alias)
                    if not alias_normalized:
                        continue
                    if normalized_query in alias_normalized or alias_normalized in normalized_query:
                        fuzzy_matches.append(capability)
                        break

        unique_strong = self._single_or_none(strong_matches)
        if unique_strong is not None:
            return unique_strong

        unique_normalized = self._single_or_none(normalized_matches)
        if unique_normalized is not None:
            return unique_normalized

        return self._single_or_none(fuzzy_matches)

    def _collect_capability_aliases(self, capability: Any) -> list[str]:
        aliases: list[str] = []
        seen: set[str] = set()

        def add_alias(raw_value: Any) -> None:
            if raw_value is None:
                return
            value = str(raw_value).strip()
            if not value:
                return
            key = value.lower()
            if key in seen:
                return
            seen.add(key)
            aliases.append(value)

        add_alias(getattr(capability, "id", None))
        add_alias(getattr(capability, "name", None))
        add_alias(getattr(capability, "description", None))
        add_alias(getattr(capability, "action_id", None))

        action_context = self._extract_capability_action_context(capability)
        if isinstance(action_context, dict):
            add_alias(action_context.get("operation_id"))
            add_alias(action_context.get("path"))
            add_alias(action_context.get("summary"))
            add_alias(action_context.get("description"))
            method = action_context.get("method")
            path = action_context.get("path")
            if isinstance(method, str) and isinstance(path, str):
                add_alias(f"{method} {path}")

        return aliases

    def _normalize_lookup_token(self, value: Any) -> str:
        return re.sub(r"[^a-zа-я0-9]+", "", str(value or "").lower())

    def _single_or_none(self, items: list[Any]) -> Any | None:
        if not items:
            return None
        first = items[0]
        if all(candidate is first for candidate in items):
            return first
        return None

    def _repair_edges_with_data_flow(
        self,
        nodes: list[dict[str, Any]],
        edges: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        known_steps = {
            step for node in nodes if isinstance((step := node.get("step")), int)
        }
        sanitized: list[dict[str, Any]] = []
        seen: set[tuple[int, int, str]] = set()
        for edge in edges:
            src = edge.get("from_step")
            dst = edge.get("to_step")
            edge_type = edge.get("type")
            if not isinstance(src, int) or not isinstance(dst, int):
                continue
            if src not in known_steps or dst not in known_steps or src == dst:
                continue
            if not isinstance(edge_type, str) or not edge_type.strip():
                continue
            key = (src, dst, edge_type.strip())
            if key in seen:
                continue
            seen.add(key)
            sanitized.append(
                {
                    "from_step": src,
                    "to_step": dst,
                    "type": edge_type.strip(),
                }
            )
        return sanitized

    def _prune_edges_for_terminal_goal(
        self,
        nodes: list[dict[str, Any]],
        edges: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        known_steps = {
            step for node in nodes if isinstance((step := node.get("step")), int)
        }
        if not known_steps:
            return edges

        out_degree: dict[int, int] = {step: 0 for step in known_steps}
        reverse_adjacency: dict[int, set[int]] = {}
        for edge in edges:
            src = edge.get("from_step")
            dst = edge.get("to_step")
            if isinstance(src, int) and isinstance(dst, int):
                out_degree[src] = out_degree.get(src, 0) + 1
                reverse_adjacency.setdefault(dst, set()).add(src)

        sink_steps = [step for step in known_steps if out_degree.get(step, 0) == 0]
        if not sink_steps:
            return []

        reachable: set[int] = set()
        stack: list[int] = list(sink_steps)
        while stack:
            current = stack.pop()
            if current in reachable:
                continue
            reachable.add(current)
            stack.extend(reverse_adjacency.get(current, set()))

        return [
            edge
            for edge in edges
            if isinstance(edge.get("from_step"), int)
            and isinstance(edge.get("to_step"), int)
            and edge["from_step"] in reachable
            and edge["to_step"] in reachable
        ]

    def _prune_edges_by_required_inputs(
        self,
        nodes: list[dict[str, Any]],
        edges: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        required_by_step: dict[int, set[str]] = {}
        explicit_by_step: dict[int, set[str]] = {}
        for node in nodes:
            step = node.get("step")
            if not isinstance(step, int):
                continue
            required_inputs = self._extract_required_inputs_from_node(node)
            if required_inputs:
                required_by_step[step] = set(required_inputs)
            explicit_types = {
                edge_ref.get("type")
                for edge_ref in self._normalize_input_data_types(
                    node.get("input_data_type_from_previous")
                )
                if isinstance(edge_ref.get("type"), str)
            }
            if explicit_types:
                explicit_by_step[step] = explicit_types

        edges_by_target: dict[int, list[dict[str, Any]]] = {}
        passthrough_edges: list[dict[str, Any]] = []
        for edge in edges:
            to_step = edge.get("to_step")
            if isinstance(to_step, int):
                edges_by_target.setdefault(to_step, []).append(edge)
            else:
                passthrough_edges.append(edge)

        filtered: list[dict[str, Any]] = []
        for to_step, target_edges in edges_by_target.items():
            required_inputs = required_by_step.get(to_step, set())
            explicit_types = explicit_by_step.get(to_step, set())
            if not required_inputs and not explicit_types:
                # Source-like step with no required data should not depend on previous outputs.
                # This preserves parallel branches such as users/hotels collectors.
                continue

            matched_edges = [
                edge
                for edge in target_edges
                if self._edge_matches_expected_inputs(
                    edge_type=edge.get("type"),
                    required_inputs=required_inputs,
                    explicit_types=explicit_types,
                )
            ]
            # Keep original edges if aliases did not match any schema field.
            filtered.extend(matched_edges if matched_edges else target_edges)

        filtered.extend(passthrough_edges)
        return filtered

    def _prune_disconnected_nodes(
        self,
        nodes: list[dict[str, Any]],
        edges: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        if len(nodes) <= 1:
            return nodes, edges

        steps = [n.get("step") for n in nodes if isinstance(n.get("step"), int)]
        if not steps:
            return nodes, edges

        connected_steps: set[int] = set()
        for edge in edges:
            src = edge.get("from_step")
            dst = edge.get("to_step")
            if isinstance(src, int):
                connected_steps.add(src)
            if isinstance(dst, int):
                connected_steps.add(dst)

        if connected_steps:
            keep_steps = connected_steps
        else:
            # No usable data-flow edges: keep all steps so later repair/validation
            # can recover connections instead of collapsing to a single terminal node.
            keep_steps = set(steps)

        if len(steps) >= 4 and len(keep_steps) < 3:
            # Guardrail: do not collapse complex workflows to tiny subgraphs.
            # Keep full node set and let validation surface missing links instead.
            keep_steps = set(steps)

        pruned_nodes = [
            node
            for node in nodes
            if isinstance(node.get("step"), int) and node["step"] in keep_steps
        ]
        pruned_edges = [
            edge
            for edge in edges
            if isinstance(edge.get("from_step"), int)
            and isinstance(edge.get("to_step"), int)
            and edge["from_step"] in keep_steps
            and edge["to_step"] in keep_steps
        ]

        return pruned_nodes, pruned_edges

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
                if self._is_input_satisfied_by_values(required_input, incoming_types):
                    continue
                external_inputs.add(required_input)
            node["external_inputs"] = sorted(external_inputs)

    def _sync_node_connections(
        self,
        nodes: list[dict[str, Any]],
        edges: list[dict[str, Any]],
    ) -> None:
        incoming_by_step: dict[int, set[int]] = {}
        outgoing_by_step: dict[int, set[int]] = {}
        known_steps = {
            step for node in nodes if isinstance((step := node.get("step")), int)
        }

        for step in known_steps:
            incoming_by_step[step] = set()
            outgoing_by_step[step] = set()

        for edge in edges:
            src = edge.get("from_step")
            dst = edge.get("to_step")
            if not isinstance(src, int) or not isinstance(dst, int):
                continue
            if src not in known_steps or dst not in known_steps:
                continue
            outgoing_by_step[src].add(dst)
            incoming_by_step[dst].add(src)

        for node in nodes:
            step = node.get("step")
            if not isinstance(step, int):
                node["input_connected_from"] = []
                node["output_connected_to"] = []
                continue
            node["input_connected_from"] = sorted(incoming_by_step.get(step, set()))
            node["output_connected_to"] = sorted(outgoing_by_step.get(step, set()))

    def _validate_ready_graph(
        self,
        nodes: list[dict[str, Any]],
        edges: list[dict[str, Any]],
    ) -> tuple[bool, list[str]]:
        missing: list[str] = []
        structure_issues = self._collect_graph_structure_issues(nodes, edges)
        # Multiple sink nodes are valid for fan-out scenarios and should not block execution.
        missing.extend(
            issue for issue in structure_issues if issue != "graph:ambiguous_terminal"
        )
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
                if not endpoint.get("capability_id"):
                    missing.append(f"node_{step}: invalid_endpoint")
                    continue
                endpoint_type = str(endpoint.get("type") or "").upper()
                if endpoint_type == "COMPOSITE":
                    continue
                if not endpoint.get("action_id"):
                    missing.append(f"node_{step}: invalid_endpoint")

            required_inputs = self._extract_required_inputs_from_node(node)
            if not required_inputs:
                continue

            external_inputs = set(self._normalize_str_list(node.get("external_inputs")))
            incoming_types = edges_by_target.get(step, set())
            for required_input in required_inputs:
                if self._is_input_satisfied_by_values(required_input, external_inputs):
                    continue
                if self._is_input_satisfied_by_values(required_input, incoming_types):
                    continue
                missing.append(f"node_{step}: missing_input:{required_input}")

        return len(missing) == 0, missing

    def _collect_graph_structure_issues(
        self,
        nodes: list[dict[str, Any]],
        edges: list[dict[str, Any]],
    ) -> list[str]:
        issues: list[str] = []
        valid_steps = {
            step for node in nodes if isinstance((step := node.get("step")), int)
        }
        if not valid_steps:
            return ["graph: empty"]

        valid_edges: list[dict[str, Any]] = []
        for edge in edges:
            src = edge.get("from_step")
            dst = edge.get("to_step")
            edge_type = edge.get("type")
            if not isinstance(src, int) or not isinstance(dst, int):
                issues.append("graph: invalid_edge_reference")
                continue
            if src not in valid_steps or dst not in valid_steps:
                issues.append("graph: edge_to_missing_node")
                continue
            if not isinstance(edge_type, str) or not edge_type.strip():
                issues.append("graph: invalid_edge_type")
                continue
            valid_edges.append(edge)

        if len(valid_steps) > 1 and not valid_edges:
            issues.append("graph: missing_edges")

        if valid_edges and self._graph_has_cycle(valid_steps, valid_edges):
            issues.append("graph: cycle")

        expected_inputs: dict[int, set[int]] = {step: set() for step in valid_steps}
        expected_outputs: dict[int, set[int]] = {step: set() for step in valid_steps}
        reverse_adjacency: dict[int, set[int]] = {step: set() for step in valid_steps}
        for edge in valid_edges:
            src = edge["from_step"]
            dst = edge["to_step"]
            expected_outputs[src].add(dst)
            expected_inputs[dst].add(src)
            reverse_adjacency[dst].add(src)

        if len(valid_steps) > 1 and valid_edges:
            sink_steps = [
                step for step in valid_steps if len(expected_outputs.get(step, set())) == 0
            ]
            if len(sink_steps) > 1:
                issues.append("graph:ambiguous_terminal")
            reachable_to_terminal: set[int] = set()
            stack: list[int] = list(sink_steps)
            while stack:
                current = stack.pop()
                if current in reachable_to_terminal:
                    continue
                reachable_to_terminal.add(current)
                stack.extend(reverse_adjacency.get(current, set()))

            for step in sorted(valid_steps - reachable_to_terminal):
                issues.append(f"graph: unreachable_to_terminal:{step}")

        for node in nodes:
            step = node.get("step")
            if not isinstance(step, int):
                continue
            actual_inputs = set(self._normalize_int_list(node.get("input_connected_from")))
            actual_outputs = set(self._normalize_int_list(node.get("output_connected_to")))
            if actual_inputs != expected_inputs.get(step, set()):
                issues.append(f"graph: node_link_mismatch_input:{step}")
            if actual_outputs != expected_outputs.get(step, set()):
                issues.append(f"graph: node_link_mismatch_output:{step}")

        return issues

    def _graph_has_cycle(
        self,
        valid_steps: set[int],
        edges: list[dict[str, Any]],
    ) -> bool:
        adjacency: dict[int, set[int]] = {step: set() for step in valid_steps}
        for edge in edges:
            adjacency.setdefault(edge["from_step"], set()).add(edge["to_step"])

        visiting: set[int] = set()
        visited: set[int] = set()

        def dfs(step: int) -> bool:
            if step in visiting:
                return True
            if step in visited:
                return False
            visiting.add(step)
            for neighbor in adjacency.get(step, set()):
                if dfs(neighbor):
                    return True
            visiting.remove(step)
            visited.add(step)
            return False

        return any(dfs(step) for step in valid_steps)

    def _build_chat_reply_ru(
        self, nodes: list[dict[str, Any]], edges: list[dict[str, Any]]
    ) -> str:
        if not nodes:
            return "Мне не удалось построить шаги выполнения."

        steps = sorted(nodes, key=lambda n: n.get("step", 0))
        if len(steps) > 1 and not edges:
            return (
                "Мне удалось выделить шаги, но не удалось корректно связать их "
                "в исполнимый сценарий."
            )
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

    def _is_low_quality_message(self, message: str) -> bool:
        normalized = (message or "").strip().lower()
        if not normalized:
            return True

        tokens = self._tokenize_text(normalized)
        if not tokens:
            return True

        explicit_noise = {"писяпопа", "asdf", "qwerty", "лол", "хз", "test", "тест"}
        if any(token in explicit_noise for token in tokens):
            return True

        intent_markers = {
            "сдел",
            "получ",
            "отправ",
            "найд",
            "сегмент",
            "собер",
            "рассыл",
            "обнов",
            "созд",
            "удал",
            "assign",
            "send",
            "segment",
            "build",
            "get",
            "email",
            "user",
            "hotel",
            "pipeline",
        }
        has_intent = any(marker in normalized for marker in intent_markers)
        if len(tokens) == 1 and not has_intent:
            return True
        if len(tokens) <= 2 and not has_intent and len(normalized) < 18:
            return True
        return False

    def _tokenize_text(self, value: str) -> set[str]:
        tokens = set(re.findall(r"[a-zA-Zа-яА-Я0-9]+", value.lower()))
        return {token for token in tokens if len(token) >= 3}

    def _extract_primary_type(self, node: dict[str, Any], field: str) -> str | None:
        endpoints = node.get("endpoints") or []
        if not endpoints:
            return None
        for endpoint in endpoints:
            if not isinstance(endpoint, dict):
                continue
            raw = endpoint.get(field)
            if isinstance(raw, str) and raw.strip():
                return raw
            if isinstance(raw, dict):
                endpoint_type = raw.get("type")
                if isinstance(endpoint_type, str) and endpoint_type.strip():
                    return endpoint_type
        return None

    def _extract_required_inputs(
        self, input_schema: dict[str, Any] | None
    ) -> list[str]:
        if not isinstance(input_schema, dict):
            return []
        required = input_schema.get("required")
        if isinstance(required, list):
            return [str(item) for item in required if item]
        return []

    def _edge_matches_expected_inputs(
        self,
        *,
        edge_type: Any,
        required_inputs: set[str],
        explicit_types: set[str],
    ) -> bool:
        if not isinstance(edge_type, str):
            return False
        if not required_inputs and not explicit_types:
            return True
        for expected in required_inputs | explicit_types:
            if self._field_alias_matches(edge_type=edge_type, expected_input=expected):
                return True
        return False

    def _is_input_satisfied_by_values(
        self,
        required_input: str,
        candidate_values: set[str],
    ) -> bool:
        for candidate in candidate_values:
            if self._field_alias_matches(edge_type=candidate, expected_input=required_input):
                return True
        return False

    def _field_alias_matches(self, *, edge_type: str, expected_input: str) -> bool:
        left = str(edge_type).strip()
        right = str(expected_input).strip()
        if not left or not right:
            return False
        if left == right:
            return True

        left_base = left[:-2] if left.endswith("[]") else left
        right_base = right[:-2] if right.endswith("[]") else right
        if left_base == right_base:
            return True

        left_normalized = self._normalize_lookup_token(left_base)
        right_normalized = self._normalize_lookup_token(right_base)
        if left_normalized and right_normalized and left_normalized == right_normalized:
            return True

        left_tokens = self._tokenize_field_name(left_base)
        right_tokens = self._tokenize_field_name(right_base)
        return bool(left_tokens and right_tokens and left_tokens == right_tokens)

    def _extract_required_inputs_from_node(self, node: dict[str, Any]) -> list[str]:
        endpoints = node.get("endpoints") or []
        if not endpoints:
            return []
        required_inputs: list[str] = []
        seen: set[str] = set()
        for endpoint in endpoints:
            if not isinstance(endpoint, dict):
                continue
            input_type = endpoint.get("input_type")
            if not isinstance(input_type, dict):
                continue
            for input_name in self._extract_required_inputs(input_type):
                normalized = str(input_name).strip()
                if not normalized or normalized in seen:
                    continue
                seen.add(normalized)
                required_inputs.append(normalized)
        return required_inputs

    def _normalize_int_list(self, value: Any) -> list[int]:
        if not isinstance(value, list):
            return []
        result = []
        for item in value:
            if isinstance(item, int):
                result.append(item)
                continue
            if isinstance(item, str):
                stripped = item.strip()
                if stripped.lstrip("-").isdigit():
                    result.append(int(stripped))
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
            normalized_from_step: int | None = None
            if isinstance(from_step, int):
                normalized_from_step = from_step
            elif isinstance(from_step, str):
                stripped = from_step.strip()
                if stripped.lstrip("-").isdigit():
                    normalized_from_step = int(stripped)
            if normalized_from_step is not None and isinstance(edge_type, str):
                result.append({"from_step": normalized_from_step, "type": edge_type})
        return result

    def _register_step_alias(self, step_map: dict[Any, int], alias: Any, step: int) -> None:
        if alias is None:
            return
        step_map[alias] = step
        if isinstance(alias, str):
            stripped = alias.strip()
            if stripped:
                step_map[stripped] = step
                if stripped.lstrip("-").isdigit():
                    step_map[int(stripped)] = step
                step_map[stripped.lower()] = step
        if isinstance(alias, int):
            step_map[str(alias)] = step

    def _resolve_step_reference(
        self,
        raw_ref: Any,
        *,
        step_map: dict[Any, int],
        known_steps: set[int],
    ) -> int | None:
        if isinstance(raw_ref, int):
            if raw_ref in known_steps:
                return raw_ref
            mapped = step_map.get(raw_ref)
            if isinstance(mapped, int):
                return mapped
            return raw_ref

        if isinstance(raw_ref, str):
            stripped = raw_ref.strip()
            if not stripped:
                return None
            mapped = step_map.get(stripped)
            if isinstance(mapped, int):
                return mapped
            mapped = step_map.get(stripped.lower())
            if isinstance(mapped, int):
                return mapped
            if stripped.lstrip("-").isdigit():
                numeric = int(stripped)
                if numeric in known_steps:
                    return numeric
                mapped = step_map.get(numeric)
                if isinstance(mapped, int):
                    return mapped
                return numeric

        mapped = step_map.get(raw_ref)
        if isinstance(mapped, int):
            return mapped
        return None

    def _normalize_str_list(self, value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        return [str(item) for item in value if isinstance(item, (str, int))]

    def _edge_creates_cycle(
        self, edges: list[dict[str, Any]], from_step: int, to_step: int
    ) -> bool:
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

    def _is_linear_chain(
        self, nodes: list[dict[str, Any]], edges: list[dict[str, Any]]
    ) -> bool:
        if not nodes:
            return False
        if len(nodes) == 1:
            return True
        if len(edges) != len(nodes) - 1:
            return False

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
