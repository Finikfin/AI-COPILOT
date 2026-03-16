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
from app.utils.ollama_client import chat_json, reset_model_session


class PipelineServiceError(Exception):
    pass


class PipelineService:
    LOW_CONFIDENCE_MAX_QUESTIONS = 2
    LOW_CONFIDENCE_QUESTION_MARKER = "нужно уточнить цель, чтобы построить точный сценарий"
    LOW_CONFIDENCE_DIALOG_MARKER = "[[low_confidence_question]]"

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
        if self._is_low_quality_message(message):
            return {
                "status": "needs_input",
                "message_ru": "Запрос слишком короткий или не похож на задачу автоматизации.",
                "chat_reply_ru": (
                    "Опишите задачу понятнее: что получить, какие данные использовать "
                    "и какой результат нужен."
                ),
                "nodes": [],
                "edges": [],
                "context_summary": None,
            }

        dialog_messages, dialog_summary = await self.dialog_memory.get_context(
            str(dialog_id)
        )

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
                limit=10,
            )

        previous_pipeline: Pipeline | None = None
        previous_nodes: list[dict[str, Any]] = []
        previous_edges: list[dict[str, Any]] = []
        if previous_pipeline_id is not None:
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

        low_confidence_attempts = self._count_low_confidence_questions(dialog_messages)
        if (
            self._selection_is_low_confidence(selected_capabilities)
            and low_confidence_attempts < self.LOW_CONFIDENCE_MAX_QUESTIONS
        ):
            question = self._build_low_confidence_question_ru(
                question_number=low_confidence_attempts + 1,
                message=message,
                dialog_messages=dialog_messages,
                selected_capabilities=selected_capabilities,
            )
            dialog_question = self._attach_low_confidence_marker(question)
            await self.dialog_memory.append_and_summarize(str(dialog_id), "user", message)
            await self.dialog_memory.append_and_summarize(
                str(dialog_id), "assistant", dialog_question
            )
            return {
                "status": "needs_input",
                "message_ru": "Нужно уточнение цели, чтобы собрать корректный сценарий.",
                "chat_reply_ru": question,
                "nodes": [],
                "edges": [],
                "missing_requirements": ["selection:low_confidence"],
                "context_summary": dialog_summary,
            }

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
            raw_graph = self._build_minimal_raw_graph(selected_capabilities)
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
        chat_reply = self._build_chat_reply_ru(normalized_nodes, normalized_edges)

        if not is_ready:
            fallback_raw_graph = self._build_minimal_raw_graph(selected_capabilities)
            if fallback_raw_graph is not None:
                fallback_nodes, fallback_edges, fallback_ready, fallback_missing = self._prepare_graph(
                    raw_graph=fallback_raw_graph,
                    selected_capabilities=selected_capabilities,
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
            await self.dialog_memory.append_and_summarize(
                str(dialog_id), "user", message
            )
            await self.dialog_memory.append_and_summarize(
                str(dialog_id), "assistant", chat_reply
            )
            return {
                "status": "cannot_build",
                "message_ru": "Сценарий не готов к выполнению. Не хватает входных данных.",
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
        self._sync_node_connections(normalized_nodes, normalized_edges)
        self._ensure_external_inputs(normalized_nodes, normalized_edges)

        reviewed_nodes, reviewed_edges = self._review_graph_with_llm(
            normalized_nodes,
            normalized_edges,
            selected_capabilities,
        )
        reviewed_edges = self._repair_edges_with_data_flow(reviewed_nodes, reviewed_edges)
        reviewed_edges = self._prune_edges_for_terminal_goal(reviewed_nodes, reviewed_edges)
        reviewed_edges = self._prune_edges_by_required_inputs(reviewed_nodes, reviewed_edges)
        reviewed_nodes, reviewed_edges = self._prune_disconnected_nodes(
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

    def _build_minimal_raw_graph(
        self,
        selected_capabilities: list[SelectedCapability],
    ) -> dict[str, Any] | None:
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
            required_inputs = self._extract_required_inputs(
                getattr(cap, "input_schema", None)
            )
            return {
                "nodes": [
                    {
                        "step": 1,
                        "name": str(getattr(cap, "name", "Step 1") or "Step 1"),
                        "description": getattr(cap, "description", None),
                        "capability_id": str(cap_id),
                        "input_connected_from": [],
                        "output_connected_to": [],
                        "input_data_type_from_previous": [],
                        "external_inputs": required_inputs,
                    }
                ],
                "edges": [],
            }
        return None

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
        previous_nodes: list[dict[str, Any]] | None = None,
        previous_edges: list[dict[str, Any]] | None = None,
    ) -> str:
        capabilities_payload = []
        for sc in selected_capabilities:
            cap = sc.capability
            capabilities_payload.append(self._build_capability_prompt_payload(cap))

        context_payload = {
            "summary": dialog_summary,
            "recent_messages": dialog_messages[-6:],
            "previous_graph": {
                "nodes": previous_nodes or [],
                "edges": previous_edges or [],
            },
        }

        instruction = (
            "If PREVIOUS_GRAPH has nodes, modify that graph instead of creating a brand new one.\n"
            "Preserve unchanged steps and connections unless explicitly requested.\n\n"
            "Сгенерируй граф сценария для запроса пользователя.\n"
            "Ответь ТОЛЬКО валидным JSON без markdown и без пояснений.\n\n"
            "Работай в 3 внутренних фазах (не показывай их в ответе):\n"
            "Фаза A — Прямой проход:\n"
            "- Определи цель пользователя.\n"
            "- Разложи процесс на шаги: какие данные производит каждый step и какие данные потребляет.\n"
            "- Построй путь слева направо: источники -> преобразования -> финальный результат.\n\n"
            "Фаза B — Сборка графа:\n"
            "- Создай nodes и edges строго по data-flow.\n"
            "- Для merge-узлов добавь отдельное ребро от каждого источника.\n"
            "- Заполни input_connected_from/output_connected_to строго по edges.\n\n"
            "Фаза C — Обратная проверка (с конца в начало):\n"
            "- Начни с последнего шага и проверь, что каждый его вход имеет источник:\n"
            "  либо edge из предыдущих шагов, либо external_inputs.\n"
            "- Рекурсивно пройди все upstream шаги до начальных источников.\n"
            "- Убери лишние ребра, которые не участвуют в достижении финального шага.\n"
            "- Проверь, что нет циклов и ссылок на несуществующие step.\n\n"
            "Критические правила:\n"
            "1) Граф строго DAG.\n"
            "2) Связи только по данным, не по порядку.\n"
            "3) Для каждого edge from_step->to_step:\n"
            "   - to_step включен в output_connected_to узла from_step\n"
            "   - from_step включен в input_connected_from узла to_step\n"
            "4) Не создавай лишних связей.\n"
            "5) capability_id только из CAPABILITIES.\n"
            "6) COMPOSITE capability — это один шаг, не разворачивай его во внутренние substeps.\n\n"
            "Пример корректного merge-паттерна:\n"
            "- Step 1 produce users\n"
            "- Step 2 produce hotels\n"
            "- Step 3 consumes users and hotels\n"
            "Тогда обязательно:\n"
            '- edges: (1->3, type="users"), (2->3, type="hotels")\n'
            "- node[3].input_connected_from = [1,2]\n"
            "- node[1].output_connected_to содержит 3\n"
            "- node[2].output_connected_to содержит 3"
        )

        return (
            f"{instruction}\n\n"
            f"USER_QUERY:\n{user_query}\n\n"
            f"DIALOG_CONTEXT:\n{json.dumps(context_payload, ensure_ascii=False)}\n\n"
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
            "- Сначала forward-путь, потом backward-валидация от последнего шага.\n"
            "- Если есть рассинхрон edges vs node links — исправь до ответа.\n"
            "- В ответ выводи только итоговый JSON.\n"
        )

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

            capability_id_raw = raw_node.get("capability_id")
            cap = self._resolve_capability_for_node(
                raw_node=raw_node,
                capability_id=capability_id_raw,
                capabilities=capabilities,
                capabilities_by_id=capabilities_by_id,
            )
            if capability_id_raw is not None and cap is None:
                issues.append("graph:invalid_capability_ref")

            if cap is None:
                endpoints_payload = []
            else:
                cap_type = getattr(cap, "type", None)
                if hasattr(cap_type, "value"):
                    cap_type_value = cap_type.value
                elif cap_type is None:
                    cap_type_value = None
                else:
                    cap_type_value = str(cap_type)
                endpoints_payload = [
                    {
                        "name": cap.name,
                        "capability_id": str(cap.id),
                        "action_id": str(cap.action_id) if cap.action_id is not None else None,
                        "type": cap_type_value,
                        "input_type": cap.input_schema,
                        "output_type": cap.output_schema,
                    }
                ]

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
            from_step = (
                raw_edge.get("from_step")
                or raw_edge.get("from")
                or raw_edge.get("source")
            )
            to_step = (
                raw_edge.get("to_step") or raw_edge.get("to") or raw_edge.get("target")
            )
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

        return normalized_nodes, normalized_edges, sorted(set(issues))

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
            "Проверь граф пайплайна и верни только JSON.\n"
            "Не используй внешний контекст. Используй только GRAPH и CAPABILITIES.\n\n"
            "Сделай обратную проверку от финального шага к источникам.\n"
            "Удали висящие узлы и рёбра, которые не участвуют в достижении финального шага.\n"
            "Не придумывай новые шаги. Разрешено только удалить лишнее или исправить связи.\n"
            "Сохрани DAG.\n\n"
            "Ответ строго в формате:\n"
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
            "You validate pipeline graph connectivity and sequencing. "
            "Return ONLY valid JSON."
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
        if capability_id:
            by_id = capabilities_by_id.get(str(capability_id))
            if by_id is not None:
                return by_id
            for cap in capabilities:
                if str(getattr(cap, "name", "")) == str(capability_id):
                    return cap

        # Strict mode: no fuzzy mapping to avoid accidental capability substitutions.
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

        filtered: list[dict[str, Any]] = []
        for edge in edges:
            to_step = edge.get("to_step")
            edge_type = edge.get("type")
            if not isinstance(to_step, int):
                continue
            if not isinstance(edge_type, str):
                continue
            required_inputs = required_by_step.get(to_step, set())
            explicit_types = explicit_by_step.get(to_step, set())
            if not required_inputs and not explicit_types:
                filtered.append(edge)
                continue
            if edge_type in required_inputs or edge_type in explicit_types:
                filtered.append(edge)
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
            # No usable data-flow edges: keep only one primary step
            # to avoid returning multiple hanging nodes.
            keep_steps = {max(steps)}

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
                if required_input in incoming_types:
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
        missing.extend(self._collect_graph_structure_issues(nodes, edges))
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
                if required_input in external_inputs:
                    continue
                if required_input in incoming_types:
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
        endpoint = endpoints[0]
        raw = endpoint.get(field)
        if isinstance(raw, str):
            return raw
        if isinstance(raw, dict):
            return raw.get("type") if isinstance(raw.get("type"), str) else None
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

    def _extract_required_inputs_from_node(self, node: dict[str, Any]) -> list[str]:
        endpoints = node.get("endpoints") or []
        if not endpoints:
            return []
        input_type = endpoints[0].get("input_type")
        return (
            self._extract_required_inputs(input_type)
            if isinstance(input_type, dict)
            else []
        )

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
