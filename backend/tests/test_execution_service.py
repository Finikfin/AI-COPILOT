from __future__ import annotations

import copy
from typing import Any
from uuid import uuid4

import pytest

from app.models import Action, HttpMethod
from app.models.capability import Capability
from app.models.execution import (
    ExecutionRun,
    ExecutionRunStatus,
    ExecutionStepRun,
    ExecutionStepStatus,
)
from app.models.pipeline import Pipeline, PipelineStatus
from app.services.execution_service import ExecutionService, StepExecutionError


class FakeSession:
    def __init__(self, initial: dict[tuple[type[Any], Any], Any] | None = None) -> None:
        self._store = initial or {}
        self.step_runs_by_step: dict[int, ExecutionStepRun] = {}
        self.commit_calls = 0

    async def get(self, model: type[Any], key: Any) -> Any:
        return self._store.get((model, key))

    def add(self, obj: Any) -> None:
        if isinstance(obj, ExecutionStepRun):
            self.step_runs_by_step[obj.step] = obj

    def add_all(self, items: list[Any]) -> None:
        for item in items:
            self.add(item)

    async def commit(self) -> None:
        self.commit_calls += 1

    async def refresh(self, _obj: Any) -> None:
        return None


class FakeContextStore:
    def __init__(self, initial: Any = None) -> None:
        self._context = initial
        self.saved_contexts: list[dict[str, Any]] = []

    async def load_context(self, _run_id) -> dict[str, Any]:
        if isinstance(self._context, dict):
            return copy.deepcopy(self._context)
        return {}

    async def save_context(self, _run_id, context: dict[str, Any]) -> None:
        normalized = copy.deepcopy(context)
        self._context = normalized
        self.saved_contexts.append(normalized)


def _build_action(action_id) -> Action:
    return Action(
        id=action_id,
        method=HttpMethod.GET,
        path="/resource",
        base_url="https://api.example.com",
    )


def _build_capability(capability_id, action_id) -> Capability:
    return Capability(
        id=capability_id,
        action_id=action_id,
        name=f"cap_{capability_id.hex[:8]}",
    )


def _build_node(step: int, capability_id, action_id, *, external_inputs: list[str] | None = None) -> dict[str, Any]:
    return {
        "step": step,
        "name": f"Step {step}",
        "external_inputs": external_inputs or [],
        "endpoints": [
            {
                "capability_id": str(capability_id),
                "action_id": str(action_id),
            }
        ],
    }


def test_topological_sort_linear_graph():
    ordered = ExecutionService._topological_sort(
        steps=[1, 2, 3],
        edges=[
            {"from_step": 1, "to_step": 2, "type": "users"},
            {"from_step": 2, "to_step": 3, "type": "segments"},
        ],
    )
    assert ordered == [1, 2, 3]


def test_extract_value_from_output_by_edge_type():
    output = {"users": [{"id": 1}]}
    value = ExecutionService._extract_value_from_output(output, "users")
    assert value == [{"id": 1}]


def test_build_request_payload_uses_path_params_and_defaults():
    action = Action(
        method=HttpMethod.GET,
        path="/users/{user_id}",
        base_url="https://api.example.com",
        parameters_schema={
            "type": "object",
            "properties": {
                "user_id": {
                    "type": "string",
                    "x-parameter-location": "path",
                },
                "limit": {
                    "type": "integer",
                    "x-parameter-location": "query",
                    "default": 10,
                },
            },
            "required": ["user_id"],
        },
    )

    service = ExecutionService(session=None)  # type: ignore[arg-type]
    payload = service._build_request_payload(
        action=action,
        resolved_inputs={"user_id": "abc"},
    )

    assert payload["url"] == "https://api.example.com/users/abc"
    assert payload["query_params"] == {"limit": 10}
    assert payload["missing_required"] == []


@pytest.mark.asyncio
async def test_get_action_from_node_uses_capability_action_id():
    primary_action_id = uuid4()
    stale_action_id = uuid4()
    capability_id = uuid4()

    action = _build_action(primary_action_id)
    capability = _build_capability(capability_id, primary_action_id)
    session = FakeSession(
        {
            (Capability, capability_id): capability,
            (Action, primary_action_id): action,
        }
    )

    service = ExecutionService(session=session)  # type: ignore[arg-type]
    node = _build_node(step=1, capability_id=capability_id, action_id=stale_action_id)
    resolved_capability_id, resolved_action = await service._get_action_from_node(node)

    assert resolved_capability_id == capability_id
    assert resolved_action.id == primary_action_id


@pytest.mark.asyncio
async def test_get_action_from_node_raises_for_invalid_or_missing_bindings():
    service = ExecutionService(session=FakeSession())  # type: ignore[arg-type]

    with pytest.raises(StepExecutionError, match="valid capability_id"):
        await service._get_action_from_node(
            {"step": 1, "endpoints": [{"capability_id": "invalid"}]}
        )

    missing_capability_id = uuid4()
    with pytest.raises(StepExecutionError, match=f"Capability not found: {missing_capability_id}"):
        await service._get_action_from_node(
            {
                "step": 1,
                "endpoints": [{"capability_id": str(missing_capability_id)}],
            }
        )

    capability_id = uuid4()
    capability_without_action = _build_capability(capability_id, None)
    session = FakeSession({(Capability, capability_id): capability_without_action})
    service = ExecutionService(session=session)  # type: ignore[arg-type]
    with pytest.raises(StepExecutionError, match=f"Capability does not have action_id: {capability_id}"):
        await service._get_action_from_node(
            {"step": 1, "endpoints": [{"capability_id": str(capability_id)}]}
        )

    missing_action_id = uuid4()
    capability_with_missing_action = _build_capability(capability_id, missing_action_id)
    session = FakeSession({(Capability, capability_id): capability_with_missing_action})
    service = ExecutionService(session=session)  # type: ignore[arg-type]
    with pytest.raises(StepExecutionError, match=f"Action not found for capability {capability_id}: {missing_action_id}"):
        await service._get_action_from_node(
            {"step": 1, "endpoints": [{"capability_id": str(capability_id)}]}
        )


def test_resolve_node_inputs_prefers_edge_values_over_step_outputs():
    service = ExecutionService(session=None)  # type: ignore[arg-type]
    resolved, missing = service._resolve_node_inputs(
        node={"step": 2, "external_inputs": []},
        incoming_edges=[{"from_step": 1, "to_step": 2, "type": "users"}],
        step_outputs={"1": {"users": [{"id": 1}]}},
        edge_values={"1:2:users": [{"id": 42}]},
        run_inputs={},
    )

    assert resolved == {"users": [{"id": 42}]}
    assert missing == []


def test_resolve_node_inputs_normalizes_array_suffix_edge_types():
    service = ExecutionService(session=None)  # type: ignore[arg-type]
    resolved, missing = service._resolve_node_inputs(
        node={"step": 3, "external_inputs": []},
        incoming_edges=[{"from_step": 1, "to_step": 3, "type": "users[]"}],
        step_outputs={"1": {"users": [{"id": 1}]}},
        edge_values={},
        run_inputs={},
    )

    assert resolved["users[]"] == [{"id": 1}]
    assert resolved["users"] == [{"id": 1}]
    assert missing == []


@pytest.mark.asyncio
async def test_execute_run_linear_pipeline_succeeds_and_persists_context():
    run_id = uuid4()
    pipeline_id = uuid4()
    action_1_id = uuid4()
    action_2_id = uuid4()
    capability_1_id = uuid4()
    capability_2_id = uuid4()

    action_1 = _build_action(action_1_id)
    action_2 = _build_action(action_2_id)
    capability_1 = _build_capability(capability_1_id, action_1_id)
    capability_2 = _build_capability(capability_2_id, action_2_id)

    pipeline = Pipeline(
        id=pipeline_id,
        name="Linear pipeline",
        nodes=[
            _build_node(1, capability_1_id, action_1_id, external_inputs=["seed"]),
            _build_node(2, capability_2_id, action_2_id),
        ],
        edges=[{"from_step": 1, "to_step": 2, "type": "users"}],
        status=PipelineStatus.READY,
    )
    run = ExecutionRun(
        id=run_id,
        pipeline_id=pipeline_id,
        status=ExecutionRunStatus.QUEUED,
        inputs={"seed": "abc"},
    )

    session = FakeSession(
        {
            (ExecutionRun, run_id): run,
            (Pipeline, pipeline_id): pipeline,
            (Capability, capability_1_id): capability_1,
            (Capability, capability_2_id): capability_2,
            (Action, action_1_id): action_1,
            (Action, action_2_id): action_2,
        }
    )
    context_store = FakeContextStore(initial={"step_outputs": "bad", "edge_values": []})
    service = ExecutionService(session=session, context_store=context_store)  # type: ignore[arg-type]

    async def fake_call_action(action: Action, request_payload: dict[str, Any]):
        if action.id == action_1_id:
            assert request_payload["resolved_inputs"]["seed"] == "abc"
            return {"status_code": 200, "body": {"users": [{"id": 1}]}}, {"users": [{"id": 1}]}
        return {"status_code": 200, "body": {"ok": True}}, {"ok": True}

    service._call_action = fake_call_action  # type: ignore[method-assign]

    await service.execute_run(run_id)

    assert run.status == ExecutionRunStatus.SUCCEEDED
    assert run.summary is not None
    assert run.summary["total_steps"] == 2
    assert run.summary["succeeded_steps"] == 2
    assert run.summary["failed_steps"] == 0
    assert run.summary["skipped_steps"] == 0
    assert run.summary["final_output_step"] == 2
    assert run.summary["final_output"] == {"ok": True}
    assert session.step_runs_by_step[1].status == ExecutionStepStatus.SUCCEEDED
    assert session.step_runs_by_step[2].status == ExecutionStepStatus.SUCCEEDED
    assert context_store.saved_contexts[-1]["edge_values"]["1:2:users"] == [{"id": 1}]
    assert context_store.saved_contexts[-1]["step_outputs"]["1"] == {"users": [{"id": 1}]}


@pytest.mark.asyncio
async def test_execute_run_is_fail_fast_and_marks_remaining_as_skipped():
    run_id = uuid4()
    pipeline_id = uuid4()
    action_1_id = uuid4()
    action_2_id = uuid4()
    action_3_id = uuid4()
    capability_1_id = uuid4()
    capability_2_id = uuid4()
    capability_3_id = uuid4()

    action_1 = _build_action(action_1_id)
    action_2 = _build_action(action_2_id)
    action_3 = _build_action(action_3_id)
    capability_1 = _build_capability(capability_1_id, action_1_id)
    capability_2 = _build_capability(capability_2_id, action_2_id)
    capability_3 = _build_capability(capability_3_id, action_3_id)

    pipeline = Pipeline(
        id=pipeline_id,
        name="Fail fast pipeline",
        nodes=[
            _build_node(1, capability_1_id, action_1_id),
            _build_node(2, capability_2_id, action_2_id),
            _build_node(3, capability_3_id, action_3_id),
        ],
        edges=[
            {"from_step": 1, "to_step": 2, "type": "users"},
            {"from_step": 2, "to_step": 3, "type": "segments"},
        ],
        status=PipelineStatus.READY,
    )
    run = ExecutionRun(
        id=run_id,
        pipeline_id=pipeline_id,
        status=ExecutionRunStatus.QUEUED,
        inputs={},
    )

    session = FakeSession(
        {
            (ExecutionRun, run_id): run,
            (Pipeline, pipeline_id): pipeline,
            (Capability, capability_1_id): capability_1,
            (Capability, capability_2_id): capability_2,
            (Capability, capability_3_id): capability_3,
            (Action, action_1_id): action_1,
            (Action, action_2_id): action_2,
            (Action, action_3_id): action_3,
        }
    )
    service = ExecutionService(
        session=session,  # type: ignore[arg-type]
        context_store=FakeContextStore(initial={"step_outputs": {}, "edge_values": {}}),
    )

    async def fake_call_action(action: Action, _request_payload: dict[str, Any]):
        if action.id == action_2_id:
            raise StepExecutionError("boom")
        return {"status_code": 200}, {"users": [1]}

    service._call_action = fake_call_action  # type: ignore[method-assign]

    await service.execute_run(run_id)

    assert run.status == ExecutionRunStatus.PARTIAL_FAILED
    assert run.summary is not None
    assert run.summary["total_steps"] == 3
    assert run.summary["succeeded_steps"] == 1
    assert run.summary["failed_steps"] == 1
    assert run.summary["skipped_steps"] == 1
    assert run.summary["final_output_step"] == 1
    assert run.summary["final_output"] == {"users": [1]}
    assert session.step_runs_by_step[1].status == ExecutionStepStatus.SUCCEEDED
    assert session.step_runs_by_step[2].status == ExecutionStepStatus.FAILED
    assert session.step_runs_by_step[3].status == ExecutionStepStatus.SKIPPED


@pytest.mark.asyncio
async def test_execute_run_multi_endpoint_node_executes_sequential_chain():
    run_id = uuid4()
    pipeline_id = uuid4()
    action_1_id = uuid4()
    action_2_id = uuid4()
    capability_1_id = uuid4()
    capability_2_id = uuid4()

    action_1 = Action(
        id=action_1_id,
        method=HttpMethod.GET,
        path="/users/recent",
        base_url="https://api.example.com",
    )
    action_2 = Action(
        id=action_2_id,
        method=HttpMethod.GET,
        path="/segments/build",
        base_url="https://api.example.com",
        parameters_schema={
            "type": "object",
            "required": ["usersList"],
            "properties": {
                "usersList": {
                    "type": "array",
                    "x-parameter-location": "query",
                }
            },
        },
    )

    capability_1 = _build_capability(capability_1_id, action_1_id)
    capability_2 = _build_capability(capability_2_id, action_2_id)

    multi_endpoint_node = {
        "step": 1,
        "name": "Multi endpoint node",
        "external_inputs": [],
        "endpoints": [
            {
                "capability_id": str(capability_1_id),
                "action_id": str(action_1_id),
            },
            {
                "capability_id": str(capability_2_id),
                "action_id": str(action_2_id),
            },
        ],
    }

    pipeline = Pipeline(
        id=pipeline_id,
        name="Multi endpoint chain",
        nodes=[multi_endpoint_node],
        edges=[],
        status=PipelineStatus.READY,
    )
    run = ExecutionRun(
        id=run_id,
        pipeline_id=pipeline_id,
        status=ExecutionRunStatus.QUEUED,
        inputs={},
    )

    session = FakeSession(
        {
            (ExecutionRun, run_id): run,
            (Pipeline, pipeline_id): pipeline,
            (Capability, capability_1_id): capability_1,
            (Capability, capability_2_id): capability_2,
            (Action, action_1_id): action_1,
            (Action, action_2_id): action_2,
        }
    )
    service = ExecutionService(
        session=session,  # type: ignore[arg-type]
        context_store=FakeContextStore(initial={"step_outputs": {}, "edge_values": {}}),
    )

    call_order: list[Any] = []

    async def fake_call_action(action: Action, request_payload: dict[str, Any]):
        call_order.append(action.id)
        if action.id == action_1_id:
            return {"status_code": 200, "body": {"users_list": [{"id": 1}]}}, {"users_list": [{"id": 1}]}
        assert request_payload["resolved_inputs"]["usersList"] == [{"id": 1}]
        return {"status_code": 200, "body": {"segments": [1]}}, {"segments": [1]}

    service._call_action = fake_call_action  # type: ignore[method-assign]

    await service.execute_run(run_id)

    assert run.status == ExecutionRunStatus.SUCCEEDED
    assert run.summary is not None
    assert run.summary["final_output"] == {"segments": [1]}
    assert call_order == [action_1_id, action_2_id]
    assert session.step_runs_by_step[1].capability_id == capability_1_id
    assert session.step_runs_by_step[1].action_id == action_1_id
    trace = session.step_runs_by_step[1].response_snapshot["endpoints_trace"]  # type: ignore[index]
    assert len(trace) == 2
    assert trace[0]["status"] == "succeeded"
    assert trace[1]["status"] == "succeeded"


@pytest.mark.asyncio
async def test_execute_run_multi_endpoint_failure_stops_pipeline():
    run_id = uuid4()
    pipeline_id = uuid4()
    action_1_id = uuid4()
    action_2_id = uuid4()
    action_3_id = uuid4()
    capability_1_id = uuid4()
    capability_2_id = uuid4()
    capability_3_id = uuid4()

    action_1 = _build_action(action_1_id)
    action_2 = _build_action(action_2_id)
    action_3 = _build_action(action_3_id)
    capability_1 = _build_capability(capability_1_id, action_1_id)
    capability_2 = _build_capability(capability_2_id, action_2_id)
    capability_3 = _build_capability(capability_3_id, action_3_id)

    multi_endpoint_node = {
        "step": 1,
        "name": "Fail on second endpoint",
        "external_inputs": [],
        "endpoints": [
            {"capability_id": str(capability_1_id), "action_id": str(action_1_id)},
            {"capability_id": str(capability_2_id), "action_id": str(action_2_id)},
        ],
    }

    pipeline = Pipeline(
        id=pipeline_id,
        name="Failing multi-endpoint pipeline",
        nodes=[
            multi_endpoint_node,
            _build_node(2, capability_3_id, action_3_id),
        ],
        edges=[{"from_step": 1, "to_step": 2, "type": "segments"}],
        status=PipelineStatus.READY,
    )
    run = ExecutionRun(
        id=run_id,
        pipeline_id=pipeline_id,
        status=ExecutionRunStatus.QUEUED,
        inputs={},
    )

    session = FakeSession(
        {
            (ExecutionRun, run_id): run,
            (Pipeline, pipeline_id): pipeline,
            (Capability, capability_1_id): capability_1,
            (Capability, capability_2_id): capability_2,
            (Capability, capability_3_id): capability_3,
            (Action, action_1_id): action_1,
            (Action, action_2_id): action_2,
            (Action, action_3_id): action_3,
        }
    )
    service = ExecutionService(
        session=session,  # type: ignore[arg-type]
        context_store=FakeContextStore(initial={"step_outputs": {}, "edge_values": {}}),
    )

    async def fake_call_action(action: Action, _request_payload: dict[str, Any]):
        if action.id == action_2_id:
            raise StepExecutionError("boom")
        return {"status_code": 200, "body": {"segments": [1]}}, {"segments": [1]}

    service._call_action = fake_call_action  # type: ignore[method-assign]

    await service.execute_run(run_id)

    assert run.status == ExecutionRunStatus.FAILED
    assert run.summary is not None
    assert run.summary["succeeded_steps"] == 0
    assert run.summary["failed_steps"] == 1
    assert run.summary["skipped_steps"] == 1
    assert session.step_runs_by_step[1].status == ExecutionStepStatus.FAILED
    assert session.step_runs_by_step[2].status == ExecutionStepStatus.SKIPPED
    failed_trace = session.step_runs_by_step[1].response_snapshot["endpoints_trace"]  # type: ignore[index]
    assert len(failed_trace) == 2
    assert failed_trace[0]["status"] == "succeeded"
    assert failed_trace[1]["status"] == "failed"


@pytest.mark.asyncio
async def test_execute_run_multi_endpoint_chain_supports_composite_endpoint():
    run_id = uuid4()
    pipeline_id = uuid4()
    action_1_id = uuid4()
    atomic_capability_id = uuid4()
    composite_capability_id = uuid4()

    action_1 = Action(
        id=action_1_id,
        method=HttpMethod.GET,
        path="/users/recent",
        base_url="https://api.example.com",
    )
    atomic_capability = _build_capability(atomic_capability_id, action_1_id)
    composite_capability = Capability(
        id=composite_capability_id,
        action_id=None,
        type="COMPOSITE",
        name="composite_cap",
        input_schema={
            "type": "object",
            "required": ["users"],
            "properties": {
                "users": {"type": "array"},
            },
        },
        recipe={"version": 1, "steps": [{"step": 1, "capability_id": str(atomic_capability_id), "inputs": {}}]},
    )

    node = {
        "step": 1,
        "name": "Atomic then composite",
        "external_inputs": [],
        "endpoints": [
            {"capability_id": str(atomic_capability_id), "action_id": str(action_1_id)},
            {"capability_id": str(composite_capability_id), "action_id": None},
        ],
    }
    pipeline = Pipeline(
        id=pipeline_id,
        name="mixed chain pipeline",
        nodes=[node],
        edges=[],
        status=PipelineStatus.READY,
    )
    run = ExecutionRun(
        id=run_id,
        pipeline_id=pipeline_id,
        status=ExecutionRunStatus.QUEUED,
        inputs={},
    )

    session = FakeSession(
        {
            (ExecutionRun, run_id): run,
            (Pipeline, pipeline_id): pipeline,
            (Capability, atomic_capability_id): atomic_capability,
            (Capability, composite_capability_id): composite_capability,
            (Action, action_1_id): action_1,
        }
    )
    service = ExecutionService(
        session=session,  # type: ignore[arg-type]
        context_store=FakeContextStore(initial={"step_outputs": {}, "edge_values": {}}),
    )

    async def fake_call_action(action: Action, _request_payload: dict[str, Any]):
        assert action.id == action_1_id
        return {"status_code": 200, "body": {"users": [{"id": 1}]}}, {"users": [{"id": 1}]}

    async def fake_execute_composite_capability(
        *,
        capability: Capability,
        resolved_inputs: dict[str, Any],
        run_inputs: dict[str, Any],
    ):
        assert capability.id == composite_capability_id
        assert resolved_inputs["users"] == [{"id": 1}]
        assert run_inputs == {}
        return {"capability_type": "COMPOSITE", "status_code": 200}, {"segments": [1]}

    service._call_action = fake_call_action  # type: ignore[method-assign]
    service._execute_composite_capability = fake_execute_composite_capability  # type: ignore[method-assign]

    await service.execute_run(run_id)

    assert run.status == ExecutionRunStatus.SUCCEEDED
    assert run.summary is not None
    assert run.summary["final_output"] == {"segments": [1]}
    trace = session.step_runs_by_step[1].response_snapshot["endpoints_trace"]  # type: ignore[index]
    assert len(trace) == 2
    assert trace[0]["capability_id"] == str(atomic_capability_id)
    assert trace[1]["capability_id"] == str(composite_capability_id)
    assert trace[1]["capability_type"] == "COMPOSITE"
