from __future__ import annotations

import asyncio
from types import SimpleNamespace
from uuid import uuid4
from unittest.mock import AsyncMock

import pytest

from app.services.pipeline_service import PipelineService, PipelineServiceError
from app.services.semantic_selection import SelectedCapability


def _build_capability() -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid4(),
        action_id=uuid4(),
        name="get_users",
        description="Get users list",
        input_schema={"type": "object", "required": ["token"]},
        output_schema={"type": "object"},
        data_format={"response_schema_types": ["object"]},
    )


def _build_service() -> PipelineService:
    session = SimpleNamespace(
        add=lambda *_: None,
        flush=AsyncMock(),
        refresh=AsyncMock(),
        commit=AsyncMock(),
    )
    return PipelineService(session=session)


def test_generate_raw_graph_returns_payload(monkeypatch):
    service = _build_service()
    capability = _build_capability()
    selected = [SelectedCapability(capability=capability, score=1.0)]

    reset_called = {"value": False}

    def fake_reset():
        reset_called["value"] = True

    monkeypatch.setattr("app.services.pipeline_service.reset_model_session", fake_reset)
    monkeypatch.setattr(
        "app.services.pipeline_service.chat_json",
        lambda **_: {"nodes": [], "edges": []},
    )

    payload = service.generate_raw_graph("build pipeline", selected, "prompt")
    assert payload == {"nodes": [], "edges": []}
    assert reset_called["value"] is True


def test_generate_raw_graph_uses_qwen_coder_system_prompt(monkeypatch):
    service = _build_service()
    capability = _build_capability()
    selected = [SelectedCapability(capability=capability, score=1.0)]
    captured: dict[str, str] = {}

    def fake_chat_json(*, system_prompt, user_prompt):
        captured["system_prompt"] = system_prompt
        captured["user_prompt"] = user_prompt
        return {"nodes": [], "edges": []}

    monkeypatch.setattr("app.services.pipeline_service.reset_model_session", lambda: None)
    monkeypatch.setattr("app.services.pipeline_service.chat_json", fake_chat_json)

    payload = service.generate_raw_graph("build pipeline", selected, "prompt")

    assert payload == {"nodes": [], "edges": []}
    assert "Qwen2.5-Coder (7B)" in captured["system_prompt"]


def test_generate_raw_graph_raises_on_invalid_payload(monkeypatch):
    service = _build_service()
    capability = _build_capability()
    selected = [SelectedCapability(capability=capability, score=1.0)]

    monkeypatch.setattr("app.services.pipeline_service.reset_model_session", lambda: None)
    monkeypatch.setattr("app.services.pipeline_service.chat_json", lambda **_: None)

    with pytest.raises(PipelineServiceError):
        service.generate_raw_graph("build pipeline", selected, "prompt")


def test_generate_llm_failure_falls_back_to_minimal_ready_pipeline():
    service = _build_service()
    capability = _build_capability()

    async def fake_get_capabilities(*, capability_ids):
        _ = capability_ids
        return [capability]

    service.capability_service.get_capabilities = fake_get_capabilities
    service.dialog_memory.get_context = AsyncMock(return_value=([], "ctx summary"))
    service.dialog_memory.append_and_summarize = AsyncMock()
    service.generate_raw_graph = lambda *_: (_ for _ in ()).throw(PipelineServiceError("mocked failure"))

    result = asyncio.run(
        service.generate(
            dialog_id=uuid4(),
            message="Собери сценарий",
            user_id=uuid4(),
            capability_ids=[uuid4()],
        )
    )

    assert result["status"] == "ready"
    assert len(result["nodes"]) == 1
    assert result["nodes"][0]["endpoints"][0]["capability_id"] == str(capability.id)
    assert result["nodes"][0]["endpoints"][0]["action_id"] == str(capability.action_id)
    assert result["edges"] == []
    assert result["context_summary"] == "ctx summary"


def test_sync_node_connections_from_edges_overrides_stale_links():
    service = _build_service()

    nodes = [
        {"step": 1, "input_connected_from": [], "output_connected_to": [2], "endpoints": []},
        {"step": 2, "input_connected_from": [1], "output_connected_to": [], "endpoints": []},
        {"step": 3, "input_connected_from": [1, 2], "output_connected_to": [], "endpoints": []},
    ]
    edges = [
        {"from_step": 1, "to_step": 3, "type": "users"},
        {"from_step": 2, "to_step": 3, "type": "hotels"},
    ]

    service._sync_node_connections(nodes, edges)

    assert nodes[0]["output_connected_to"] == [3]
    assert nodes[1]["output_connected_to"] == [3]
    assert nodes[2]["input_connected_from"] == [1, 2]


def test_compact_step_sequence_remaps_gapped_steps_and_refs():
    service = _build_service()

    nodes = [
        {
            "step": 1,
            "input_connected_from": [],
            "output_connected_to": [3],
            "input_data_type_from_previous": [],
            "external_inputs": [],
            "endpoints": [],
        },
        {
            "step": 3,
            "input_connected_from": [1],
            "output_connected_to": [5],
            "input_data_type_from_previous": [{"from_step": 1, "type": "users"}],
            "external_inputs": [],
            "endpoints": [],
        },
        {
            "step": 5,
            "input_connected_from": [3],
            "output_connected_to": [],
            "input_data_type_from_previous": [{"from_step": 3, "type": "segments"}],
            "external_inputs": [],
            "endpoints": [],
        },
    ]
    edges = [
        {"from_step": 1, "to_step": 3, "type": "users"},
        {"from_step": 3, "to_step": 5, "type": "segments"},
    ]

    compact_nodes, compact_edges = service._compact_step_sequence(nodes, edges)

    assert [item["step"] for item in compact_nodes] == [1, 2, 3]
    assert compact_edges == [
        {"from_step": 1, "to_step": 2, "type": "users"},
        {"from_step": 2, "to_step": 3, "type": "segments"},
    ]
    assert compact_nodes[1]["input_connected_from"] == [1]
    assert compact_nodes[1]["output_connected_to"] == [3]
    assert compact_nodes[1]["input_data_type_from_previous"] == [
        {"from_step": 1, "type": "users"}
    ]
    assert compact_nodes[2]["input_connected_from"] == [2]
    assert compact_nodes[2]["input_data_type_from_previous"] == [
        {"from_step": 2, "type": "segments"}
    ]


def test_build_generation_prompt_contains_qwen_coder_contract():
    service = _build_service()
    capability = _build_capability()
    selected = [SelectedCapability(capability=capability, score=1.0)]

    prompt = service._build_generation_prompt(
        user_query="Собери travel-цепочку",
        selected_capabilities=selected,
        dialog_messages=[],
        dialog_summary=None,
    )

    assert "MODEL_PROFILE: qwen2.5:7b-coder" in prompt
    assert "HARD_RULES:" in prompt
    assert "ALLOWED_CAPABILITY_IDS" in prompt
    assert str(capability.id) in prompt
    assert "MERGE_PATTERN_EXAMPLE" in prompt
    assert "SELF-CHECK (INTERNAL ONLY)" in prompt
    assert "ALLOWED_CAPABILITY_IDS" in prompt
    assert str(capability.id) in prompt
    assert "Никогда не подменяй capability_id" in prompt
    assert "Step 1 produce users" in prompt
    assert "Step 2 produce hotels" in prompt
    assert "Step 3 consumes users and hotels" in prompt


def test_prune_edges_by_required_inputs_removes_extra_edge():
    service = _build_service()
    nodes = [
        {"step": 1, "endpoints": [{"input_type": {"type": "object", "required": []}}]},
        {"step": 3, "endpoints": [{"input_type": {"type": "object", "required": []}}]},
        {"step": 4, "endpoints": [{"input_type": {"type": "object", "required": ["segments"]}}]},
    ]
    edges = [
        {"from_step": 1, "to_step": 4, "type": "users"},
        {"from_step": 3, "to_step": 4, "type": "segments"},
    ]

    pruned = service._prune_edges_by_required_inputs(nodes, edges)

    assert pruned == [{"from_step": 3, "to_step": 4, "type": "segments"}]


def test_ensure_external_inputs_adds_missing_for_final_step():
    service = _build_service()
    nodes = [
        {
            "step": 4,
            "external_inputs": [],
            "endpoints": [{"input_type": {"type": "object", "required": ["segments"]}}],
        }
    ]
    edges: list[dict[str, str | int]] = []

    service._ensure_external_inputs(nodes, edges)

    assert nodes[0]["external_inputs"] == ["segments"]


def test_validate_ready_graph_rejects_multiple_nodes_without_edges():
    service = _build_service()
    nodes = [
        {
            "step": 3,
            "name": "Segment users",
            "input_connected_from": [],
            "output_connected_to": [],
            "external_inputs": [],
            "endpoints": [{"capability_id": "a", "action_id": "b", "input_type": None}],
        },
        {
            "step": 4,
            "name": "Send offers",
            "input_connected_from": [],
            "output_connected_to": [],
            "external_inputs": [],
            "endpoints": [{"capability_id": "c", "action_id": "d", "input_type": None}],
        },
    ]

    is_ready, missing = service._validate_ready_graph(nodes, [])

    assert is_ready is False
    assert "graph: missing_edges" in missing


def test_build_chat_reply_reports_unconnected_steps():
    service = _build_service()
    nodes = [
        {"step": 3, "name": "Segment users"},
        {"step": 4, "name": "Send offers"},
    ]

    reply = service._build_chat_reply_ru(nodes, [])

    assert "не удалось корректно связать" in reply


def test_is_linear_chain_requires_real_edges():
    service = _build_service()
    nodes = [
        {"step": 1},
        {"step": 2},
    ]

    assert service._is_linear_chain(nodes, []) is False


def test_review_graph_prompt_uses_capabilities_context(monkeypatch):
    service = _build_service()
    capability = _build_capability()
    selected = [SelectedCapability(capability=capability, score=1.0)]
    captured: dict[str, str] = {}

    def fake_chat_json(*, system_prompt, user_prompt):
        captured["system_prompt"] = system_prompt
        captured["user_prompt"] = user_prompt
        return {"keep_steps": [1], "edges": []}

    monkeypatch.setattr("app.services.pipeline_service.chat_json", fake_chat_json)

    service._review_graph_with_llm(
        nodes=[{"step": 1, "name": "Get users"}],
        edges=[],
        selected_capabilities=selected,
    )

    assert "CAPABILITIES:" in captured["user_prompt"]
    assert capability.name in captured["user_prompt"]


def test_generate_returns_needs_input_on_low_confidence_selection():
    service = _build_service()
    capability = _build_capability()
    selected = [
        SelectedCapability(capability=capability, score=0.2, confidence_tier="low")
    ]

    service.dialog_memory.get_context = AsyncMock(return_value=([], "ctx summary"))
    service.dialog_memory.append_and_summarize = AsyncMock()
    service.semantic_selector.select_capabilities = AsyncMock(return_value=selected)
    service.generate_raw_graph = lambda *_: (_ for _ in ()).throw(
        AssertionError("generate_raw_graph must not be called on low confidence")
    )

    result = asyncio.run(
        service.generate(
            dialog_id=uuid4(),
            message="Собери CRM-сценарий для кампании и сегментации",
            user_id=uuid4(),
        )
    )

    assert result["status"] == "needs_input"
    assert isinstance(result["chat_reply_ru"], str)
    assert len(result["chat_reply_ru"].strip()) > 8
    assert "уточнение" not in result["chat_reply_ru"].lower()
    assert "selection:low_confidence" in result["missing_requirements"]


def test_second_clarification_question_mentions_missing_required_inputs():
    service = _build_service()
    capability = _build_capability()
    selected = [
        SelectedCapability(capability=capability, score=0.2, confidence_tier="low")
    ]
    service._generate_clarification_question_ru = lambda **_: None

    question = service._build_low_confidence_question_ru(
        question_number=2,
        message="Собери сценарий рассылки по пользователям",
        dialog_messages=[{"role": "user", "content": "Сделай рассылку"}],
        selected_capabilities=selected,
    )

    assert "уточнение" not in question.lower()
    assert "token" in question.lower()


def test_low_confidence_attempts_1_2_then_build_on_3rd():
    service = _build_service()
    capability = _build_capability()
    selected = [
        SelectedCapability(capability=capability, score=0.2, confidence_tier="low")
    ]
    graph_called = {"value": False}
    dialog_messages: list[dict[str, str]] = []

    async def fake_get_context(_dialog_id):
        return list(dialog_messages), "ctx summary"

    async def fake_append_and_summarize(_dialog_id, role, content):
        dialog_messages.append({"role": role, "content": content})

    service.dialog_memory.get_context = fake_get_context
    service.dialog_memory.append_and_summarize = fake_append_and_summarize
    service.semantic_selector.select_capabilities = AsyncMock(return_value=selected)

    def fake_generate_raw_graph(*_args, **_kwargs):
        graph_called["value"] = True
        return {
            "nodes": [
                {
                    "step": 1,
                    "name": "Fetch users",
                    "capability_id": str(capability.id),
                }
            ],
            "edges": [],
        }

    service.generate_raw_graph = fake_generate_raw_graph

    dialog_id = uuid4()
    user_id = uuid4()
    responses = []
    for _ in range(2):
        result = asyncio.run(
            service.generate(
                dialog_id=dialog_id,
                message="Собери CRM-сценарий",
                user_id=user_id,
            )
        )
        responses.append(result)
        assert result["status"] == "needs_input"
        assert graph_called["value"] is False

    assert "уточнение" not in responses[0]["chat_reply_ru"].lower()
    assert "уточнение" not in responses[1]["chat_reply_ru"].lower()

    result = asyncio.run(
        service.generate(
            dialog_id=dialog_id,
            message="Собери CRM-сценарий",
            user_id=user_id,
        )
    )
    assert graph_called["value"] is True
    assert result["status"] != "needs_input"


def test_generate_invalid_graph_with_unknown_capability_is_rejected():
    service = _build_service()
    capability = _build_capability()
    service.dialog_memory.get_context = AsyncMock(return_value=([], "ctx summary"))
    service.dialog_memory.append_and_summarize = AsyncMock()
    service.semantic_selector.select_capabilities = AsyncMock(
        return_value=[SelectedCapability(capability=capability, score=1.0, confidence_tier="high")]
    )

    def fake_generate_raw_graph(*_args, **_kwargs):
        # Non-executable raw graph: unresolved capability reference.
        return {
            "nodes": [
                {
                    "step": 1,
                    "name": "Broken step",
                    "capability_id": str(uuid4()),
                }
            ],
            "edges": [],
        }

    service.generate_raw_graph = fake_generate_raw_graph

    result = asyncio.run(
        service.generate(
            dialog_id=uuid4(),
            message="Собери что-нибудь",
            user_id=uuid4(),
        )
    )

    assert result["status"] == "cannot_build"
    assert len(result["nodes"]) == 1
    assert result["nodes"][0]["endpoints"] == []
    assert "graph:invalid_capability_ref" in result["missing_requirements"]
    assert "неподтвержден" in result["message_ru"].lower()


def test_no_capabilities_returns_needs_input_with_import_instruction():
    service = _build_service()
    service.dialog_memory.get_context = AsyncMock(return_value=([], "ctx summary"))
    service.dialog_memory.append_and_summarize = AsyncMock()
    service.semantic_selector.select_capabilities = AsyncMock(return_value=[])

    result = asyncio.run(
        service.generate(
            dialog_id=uuid4(),
            message="Собери pipeline",
            user_id=uuid4(),
        )
    )

    assert result["status"] == "needs_input"
    assert "openapi/swagger" in result["chat_reply_ru"].lower()
    assert "selection:no_matches" in result["missing_requirements"]


def test_user_scope_enforced_for_semantic_selection():
    service = _build_service()
    service.dialog_memory.get_context = AsyncMock(return_value=([], "ctx summary"))
    service.dialog_memory.append_and_summarize = AsyncMock()
    service.semantic_selector.select_capabilities = AsyncMock(return_value=[])

    user_id = uuid4()
    asyncio.run(
        service.generate(
            dialog_id=uuid4(),
            message="Собери pipeline",
            user_id=user_id,
        )
    )

    assert service.semantic_selector.select_capabilities.await_count == 1
    call_args = service.semantic_selector.select_capabilities.await_args
    assert call_args.kwargs["owner_user_id"] == user_id


def test_normalize_workflow_marks_invalid_capability_reference():
    service = _build_service()
    capability = _build_capability()
    selected = [SelectedCapability(capability=capability, score=1.0)]
    unknown_capability_id = str(uuid4())

    nodes, edges, issues = service._normalize_workflow(
        {
            "nodes": [
                {
                    "step": 1,
                    "name": "Unknown capability node",
                    "capability_id": unknown_capability_id,
                }
            ],
            "edges": [],
        },
        selected,
    )

    assert len(nodes) == 1
    assert edges == []
    assert nodes[0]["endpoints"] == []
    assert "graph:invalid_capability_ref" in issues


def test_normalize_workflow_rejects_name_as_capability_id():
    service = _build_service()
    capability = _build_capability()
    selected = [SelectedCapability(capability=capability, score=1.0)]

    nodes, edges, issues = service._normalize_workflow(
        {
            "nodes": [
                {
                    "step": 1,
                    "name": "Name instead of id",
                    "capability_id": capability.name,
                }
            ],
            "edges": [],
        },
        selected,
    )

    assert len(nodes) == 1
    assert edges == []
    assert nodes[0]["endpoints"] == []
    assert "graph:invalid_capability_ref" in issues


def test_normalize_workflow_marks_missing_capability_reference_for_multi_cap_set():
    service = _build_service()
    capability_1 = _build_capability()
    capability_2 = _build_capability()
    selected = [
        SelectedCapability(capability=capability_1, score=1.0),
        SelectedCapability(capability=capability_2, score=0.9),
    ]

    nodes, edges, issues = service._normalize_workflow(
        {
            "nodes": [
                {
                    "step": 1,
                    "name": "Missing capability id",
                }
            ],
            "edges": [],
        },
        selected,
    )

    assert len(nodes) == 1
    assert edges == []
    assert nodes[0]["endpoints"] == []
    assert "graph:missing_capability_ref" in issues


def test_repair_edges_with_data_flow_does_not_autolink_object_types():
    service = _build_service()
    nodes = [
        {"step": 1, "endpoints": [{"output_type": {"type": "object"}}]},
        {"step": 2, "endpoints": [{"input_type": {"type": "object"}}]},
    ]
    repaired = service._repair_edges_with_data_flow(nodes, [])
    assert repaired == []


def test_prune_edges_for_terminal_goal_keeps_all_sink_branches():
    service = _build_service()
    nodes = [{"step": 1}, {"step": 2}, {"step": 3}]
    edges = [
        {"from_step": 1, "to_step": 2, "type": "audience"},
        {"from_step": 1, "to_step": 3, "type": "report"},
    ]

    pruned = service._prune_edges_for_terminal_goal(nodes, edges)

    assert pruned == edges


def test_prune_edges_by_required_inputs_allows_explicit_input_type_reference():
    service = _build_service()
    nodes = [
        {"step": 1, "endpoints": [{"output_type": {"type": "object"}}]},
        {"step": 2, "endpoints": [{"output_type": {"type": "object"}}]},
        {
            "step": 3,
            "input_data_type_from_previous": [{"from_step": 1, "type": "audience"}],
            "endpoints": [{"input_type": {"type": "object", "required": []}}],
        },
    ]
    edges = [
        {"from_step": 1, "to_step": 3, "type": "audience"},
        {"from_step": 2, "to_step": 3, "type": "hotels"},
    ]

    pruned = service._prune_edges_by_required_inputs(nodes, edges)

    assert pruned == [{"from_step": 1, "to_step": 3, "type": "audience"}]


def test_collect_graph_structure_issues_reports_ambiguous_terminal():
    service = _build_service()
    nodes = [{"step": 1}, {"step": 2}, {"step": 3}]
    edges = [
        {"from_step": 1, "to_step": 2, "type": "audience"},
        {"from_step": 1, "to_step": 3, "type": "report"},
    ]

    issues = service._collect_graph_structure_issues(nodes, edges)

    assert "graph:ambiguous_terminal" in issues


def test_crm_prompt_regression_prefers_needs_input_over_cannot_build():
    service = _build_service()
    capability = _build_capability()
    selected = [
        SelectedCapability(capability=capability, score=0.2, confidence_tier="low")
    ]

    service.dialog_memory.get_context = AsyncMock(return_value=([], "ctx summary"))
    service.dialog_memory.append_and_summarize = AsyncMock()
    service.semantic_selector.select_capabilities = AsyncMock(return_value=selected)

    prompts = [
        "Сегментируй лидов для кампании",
        "Сделай рассылку по новой аудитории",
        "Обнови CRM после кампании",
        "Построй отчёт по ретеншну",
        "Сегмент и пуш для новых клиентов",
        "Запусти маркетинговую кампанию по лидам",
        "Нужен отчёт по конверсии по сегментам",
        "Сделай CRM обновление по результату рассылки",
        "Аудитория для оффера и отчёт по продажам",
        "Ретеншн сценарий по лидам и email",
    ]

    statuses = []
    for prompt in prompts:
        result = asyncio.run(
            service.generate(
                dialog_id=uuid4(),
                message=prompt,
                user_id=uuid4(),
            )
        )
        statuses.append(result["status"])

    assert statuses.count("needs_input") == len(prompts)
    assert "cannot_build" not in statuses
