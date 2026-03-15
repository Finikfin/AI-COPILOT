from __future__ import annotations

import asyncio
from types import SimpleNamespace
from uuid import uuid4
from unittest.mock import AsyncMock

import pytest

from app.services.pipeline_generation import PipelineGenerationError, PipelineGenerationService
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


def _build_service() -> PipelineGenerationService:
    session = SimpleNamespace(
        add=lambda *_: None,
        flush=AsyncMock(),
        refresh=AsyncMock(),
        commit=AsyncMock(),
    )
    return PipelineGenerationService(session=session)


def test_generate_raw_graph_returns_payload(monkeypatch):
    service = _build_service()
    capability = _build_capability()
    selected = [SelectedCapability(capability=capability, score=1.0)]

    reset_called = {"value": False}

    def fake_reset():
        reset_called["value"] = True

    monkeypatch.setattr("app.services.pipeline_generation.reset_model_session", fake_reset)
    monkeypatch.setattr(
        "app.services.pipeline_generation.chat_json",
        lambda **_: {"nodes": [], "edges": []},
    )

    payload = service.generate_raw_graph("build pipeline", selected, "prompt")
    assert payload == {"nodes": [], "edges": []}
    assert reset_called["value"] is True


def test_generate_raw_graph_raises_on_invalid_payload(monkeypatch):
    service = _build_service()
    capability = _build_capability()
    selected = [SelectedCapability(capability=capability, score=1.0)]

    monkeypatch.setattr("app.services.pipeline_generation.reset_model_session", lambda: None)
    monkeypatch.setattr("app.services.pipeline_generation.chat_json", lambda **_: None)

    with pytest.raises(PipelineGenerationError):
        service.generate_raw_graph("build pipeline", selected, "prompt")


def test_generate_returns_cannot_build_when_graph_generation_fails():
    service = _build_service()
    capability = _build_capability()

    async def fake_get_capabilities(*, capability_ids):
        _ = capability_ids
        return [capability]

    service.capability_service.get_capabilities = fake_get_capabilities
    service.dialog_memory.get_context = AsyncMock(return_value=([], "ctx summary"))
    service.dialog_memory.append_and_summarize = AsyncMock()
    service.generate_raw_graph = lambda *_: (_ for _ in ()).throw(PipelineGenerationError("mocked failure"))

    result = asyncio.run(
        service.generate(
            dialog_id=uuid4(),
            message="Собери сценарий",
            user_id=uuid4(),
            capability_ids=[uuid4()],
        )
    )

    assert result["status"] == "cannot_build"
    assert result["message_ru"] == "mocked failure"
    assert result["nodes"] == []
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


def test_build_generation_prompt_contains_forward_backward_self_check():
    service = _build_service()
    capability = _build_capability()
    selected = [SelectedCapability(capability=capability, score=1.0)]

    prompt = service._build_generation_prompt(
        user_query="Собери travel-цепочку",
        selected_capabilities=selected,
        dialog_messages=[],
        dialog_summary=None,
    )

    assert "Фаза A — Прямой проход" in prompt
    assert "Фаза C — Обратная проверка (с конца в начало)" in prompt
    assert "SELF-CHECK (INTERNAL ONLY)" in prompt
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
