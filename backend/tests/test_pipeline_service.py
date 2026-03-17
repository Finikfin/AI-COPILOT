from __future__ import annotations

from uuid import uuid4

from app.models.capability import Capability, CapabilityType
from app.services.pipeline_service import PipelineService
from app.services.semantic_selection import SelectedCapability


def _build_capability(*, name: str, required_inputs: list[str] | None = None) -> Capability:
    cap_id = uuid4()
    action_id = uuid4()
    input_schema = None
    if required_inputs is not None:
        input_schema = {
            "type": "object",
            "required": required_inputs,
            "properties": {
                input_name: {"type": "string"}
                for input_name in required_inputs
            },
        }
    return Capability(
        id=cap_id,
        action_id=action_id,
        type=CapabilityType.ATOMIC,
        name=name,
        input_schema=input_schema,
        output_schema={"type": "object"},
    )


def _select(capability: Capability) -> SelectedCapability:
    return SelectedCapability(capability=capability, score=1.0, confidence_tier="high")


def test_extract_required_inputs_from_node_merges_all_endpoints():
    service = PipelineService(session=None)  # type: ignore[arg-type]
    node = {
        "step": 1,
        "endpoints": [
            {
                "input_type": {
                    "type": "object",
                    "required": ["users", "campaignId"],
                }
            },
            {
                "input_type": {
                    "type": "object",
                    "required": ["segments", "users"],
                }
            },
        ],
    }

    required = service._extract_required_inputs_from_node(node)

    assert required == ["users", "campaignId", "segments"]


def test_normalize_workflow_preserves_multi_endpoint_nodes():
    capability_a = _build_capability(name="Get users", required_inputs=["users"])
    capability_b = _build_capability(name="Build segments", required_inputs=["users"])
    selected = [_select(capability_a), _select(capability_b)]
    service = PipelineService(session=None)  # type: ignore[arg-type]

    raw_graph = {
        "nodes": [
            {
                "step": 1,
                "name": "Composite-like node",
                "endpoints": [
                    {
                        "capability_id": str(capability_a.id),
                    },
                    {
                        "capability_id": str(capability_b.id),
                    },
                ],
            }
        ],
        "edges": [],
    }

    nodes, edges, issues = service._normalize_workflow(raw_graph, selected)

    assert issues == []
    assert edges == []
    assert len(nodes) == 1
    endpoints = nodes[0]["endpoints"]
    assert len(endpoints) == 2
    assert endpoints[0]["capability_id"] == str(capability_a.id)
    assert endpoints[1]["capability_id"] == str(capability_b.id)
    assert endpoints[0]["action_id"] == str(capability_a.action_id)
    assert endpoints[1]["action_id"] == str(capability_b.action_id)


def test_normalize_workflow_flags_invalid_endpoint_capability_refs():
    capability = _build_capability(name="Get users", required_inputs=["users"])
    selected = [_select(capability)]
    service = PipelineService(session=None)  # type: ignore[arg-type]

    raw_graph = {
        "nodes": [
            {
                "step": 1,
                "name": "Node with invalid endpoint",
                "endpoints": [
                    {"capability_id": str(uuid4())},
                    {"capability_id": str(capability.id)},
                ],
            }
        ],
        "edges": [],
    }

    nodes, _edges, issues = service._normalize_workflow(raw_graph, selected)

    assert "graph:invalid_capability_ref" in issues
    assert len(nodes) == 1
    assert len(nodes[0]["endpoints"]) == 1
    assert nodes[0]["endpoints"][0]["capability_id"] == str(capability.id)
