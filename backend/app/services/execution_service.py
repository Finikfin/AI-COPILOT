from __future__ import annotations

import asyncio
import json
import os
import re
from urllib.parse import urlparse
import uuid
from datetime import datetime, timezone
from typing import Any

import httpx
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database.session import SessionLocal
from app.models import (
    Action,
    Capability,
    ExecutionRun,
    ExecutionRunStatus,
    ExecutionStepRun,
    ExecutionStepStatus,
    HttpMethod,
    Pipeline,
    PipelineStatus,
)
from app.models.capability import CapabilityType
from app.utils.business_logger import log_business_event


class ExecutionServiceError(Exception):
    pass


class StepExecutionError(ExecutionServiceError):
    def __init__(self, message: str, response_snapshot: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.response_snapshot = response_snapshot


class RunContextStore:
    _memory_fallback: dict[str, dict[str, Any]] = {}

    def __init__(self, redis_url: str | None = None, *, ttl_seconds: int = 24 * 60 * 60) -> None:
        self.redis_url = redis_url or os.getenv("REDIS_URL")
        self.ttl_seconds = ttl_seconds
        self._redis: Redis | None = None
        self._redis_disabled = False

    async def load_context(self, run_id: uuid.UUID) -> dict[str, Any]:
        key = self._build_key(run_id)
        redis = await self._get_redis()
        if redis is not None:
            raw = await redis.get(key)
            if isinstance(raw, str) and raw.strip():
                try:
                    payload = json.loads(raw)
                    if isinstance(payload, dict):
                        return payload
                except json.JSONDecodeError:
                    pass

        cached = self._memory_fallback.get(key)
        if isinstance(cached, dict):
            return cached

        return {}

    async def save_context(self, run_id: uuid.UUID, context: dict[str, Any]) -> None:
        key = self._build_key(run_id)
        redis = await self._get_redis()
        if redis is not None:
            await redis.set(key, json.dumps(context, ensure_ascii=False, default=str), ex=self.ttl_seconds)
        self._memory_fallback[key] = context

    def _build_key(self, run_id: uuid.UUID) -> str:
        return f"execution:{run_id}:context"

    async def _get_redis(self) -> Redis | None:
        if self._redis_disabled or not self.redis_url:
            return None
        if self._redis is not None:
            return self._redis

        try:
            self._redis = Redis.from_url(self.redis_url, encoding="utf-8", decode_responses=True)
            await self._redis.ping()
            return self._redis
        except Exception:
            self._redis_disabled = True
            self._redis = None
            return None


class ExecutionService:
    ACTIVE_TASKS: set[asyncio.Task[Any]] = set()

    def __init__(self, session: AsyncSession, *, context_store: RunContextStore | None = None) -> None:
        self.session = session
        self.context_store = context_store or RunContextStore()

    async def create_run(
        self,
        *,
        pipeline_id: uuid.UUID,
        inputs: dict[str, Any] | None = None,
        initiated_by: uuid.UUID | None = None,
    ) -> ExecutionRun:
        pipeline = await self.session.get(Pipeline, pipeline_id)
        if pipeline is None:
            raise ExecutionServiceError("Pipeline not found")
        if pipeline.status != PipelineStatus.READY:
            raise ExecutionServiceError("Pipeline is not ready for execution")

        run = ExecutionRun(
            pipeline_id=pipeline_id,
            initiated_by=initiated_by,
            status=ExecutionRunStatus.QUEUED,
            inputs=inputs or {},
        )
        self.session.add(run)
        await self.session.commit()
        await self.session.refresh(run)

        await self.context_store.save_context(run.id, self._build_empty_context())
        log_business_event(
            "execution_run_queued",
            run_id=str(run.id),
            pipeline_id=str(run.pipeline_id),
            user_id=str(initiated_by) if initiated_by is not None else None,
            inputs_count=len(run.inputs or {}),
        )
        return run

    @classmethod
    def start_background_execution(cls, run_id: uuid.UUID) -> None:
        task = asyncio.create_task(cls._run_in_background(run_id))
        cls.ACTIVE_TASKS.add(task)
        task.add_done_callback(cls.ACTIVE_TASKS.discard)

    @classmethod
    async def _run_in_background(cls, run_id: uuid.UUID) -> None:
        async with SessionLocal() as session:
            service = cls(session)
            await service.execute_run(run_id)

    async def execute_run(self, run_id: uuid.UUID) -> None:
        run = await self.session.get(ExecutionRun, run_id)
        if run is None:
            log_business_event(
                "execution_run_rejected",
                run_id=str(run_id),
                reason="run_not_found",
            )
            raise ExecutionServiceError("Execution run not found")

        pipeline = await self.session.get(Pipeline, run.pipeline_id)
        if pipeline is None:
            run.status = ExecutionRunStatus.FAILED
            run.error = "Pipeline not found"
            run.finished_at = self._now_utc()
            await self.session.commit()
            log_business_event(
                "execution_run_failed",
                run_id=str(run.id),
                pipeline_id=str(run.pipeline_id),
                reason="pipeline_not_found",
            )
            return

        try:
            node_by_step, edges, edges_by_target, edges_by_source = self._normalize_graph(pipeline.nodes, pipeline.edges)
            ordered_steps = self._topological_sort(list(node_by_step.keys()), edges)
            if not ordered_steps:
                raise ExecutionServiceError("Pipeline graph has no executable steps")
        except Exception as exc:
            run.status = ExecutionRunStatus.FAILED
            run.error = f"Invalid pipeline graph: {exc}"
            run.finished_at = self._now_utc()
            await self.session.commit()
            log_business_event(
                "execution_run_failed",
                run_id=str(run.id),
                pipeline_id=str(run.pipeline_id),
                reason="invalid_pipeline_graph",
                details=str(exc),
            )
            return

        run.status = ExecutionRunStatus.RUNNING
        run.started_at = self._now_utc()
        run.error = None
        run.summary = None
        await self.session.commit()
        log_business_event(
            "execution_run_started",
            run_id=str(run.id),
            pipeline_id=str(run.pipeline_id),
            user_id=str(run.initiated_by) if run.initiated_by is not None else None,
            total_steps=len(ordered_steps),
        )

        context = await self.context_store.load_context(run.id)
        context = self._normalize_context(context)
        step_outputs = context["step_outputs"]
        edge_values = context["edge_values"]
        await self.context_store.save_context(run.id, context)

        status_by_step: dict[int, ExecutionStepStatus] = {}
        succeeded_count = 0
        failed_count = 0
        skipped_count = 0

        for index, step in enumerate(ordered_steps):
            node = node_by_step.get(step)
            if node is None:
                continue
            request_payload: dict[str, Any] = {}
            incoming = edges_by_target.get(step, [])

            step_run = self._create_step_run_from_node(run.id, node, status=ExecutionStepStatus.RUNNING)
            step_run.started_at = self._now_utc()
            self.session.add(step_run)
            await self.session.commit()

            try:
                resolved_inputs, _missing_external = self._resolve_node_inputs(
                    node=node,
                    incoming_edges=incoming,
                    step_outputs=step_outputs,
                    edge_values=edge_values,
                    run_inputs=run.inputs or {},
                )
                request_payload = {
                    "resolved_inputs": dict(resolved_inputs),
                    "request_snapshot": {
                        "chain_mode": "sequential_endpoints",
                        "endpoints_trace": [],
                    },
                }
                try:
                    (
                        request_payload,
                        response_snapshot,
                        output_payload,
                        primary_capability_id,
                        primary_action_id,
                    ) = await self._execute_node_endpoint_chain(
                        node=node,
                        resolved_inputs=resolved_inputs,
                        run_inputs=run.inputs or {},
                    )
                except StepExecutionError as chain_exc:
                    chain_snapshot = (
                        chain_exc.response_snapshot
                        if isinstance(chain_exc.response_snapshot, dict)
                        else {}
                    )
                    chain_trace = chain_snapshot.get("endpoints_trace")
                    if isinstance(chain_trace, list):
                        request_payload["request_snapshot"]["endpoints_trace"] = chain_trace
                    raise

                step_run.capability_id = primary_capability_id
                step_run.action_id = primary_action_id

                step_outputs[str(step)] = output_payload
                context["step_outputs"] = step_outputs
                context["final_output_step"] = step
                context["final_output"] = output_payload
                for edge in edges_by_source.get(step, []):
                    edge_type = edge.get("type")
                    to_step = edge.get("to_step")
                    if not isinstance(edge_type, str) or not isinstance(to_step, int):
                        continue
                    value = self._extract_value_from_output(output_payload, edge_type)
                    if value is not None:
                        edge_values[self._build_edge_value_key(step, to_step, edge_type)] = value
                context["edge_values"] = edge_values
                await self.context_store.save_context(run.id, context)

                await self._finalize_step_run(
                    step_run=step_run,
                    status=ExecutionStepStatus.SUCCEEDED,
                    request_payload=request_payload,
                    response_snapshot=response_snapshot,
                    error=None,
                )

                status_by_step[step] = ExecutionStepStatus.SUCCEEDED
                succeeded_count += 1
            except StepExecutionError as exc:
                await self._finalize_step_run(
                    step_run=step_run,
                    status=ExecutionStepStatus.FAILED,
                    request_payload=request_payload,
                    response_snapshot=exc.response_snapshot,
                    error=str(exc),
                )
                log_business_event(
                    "execution_step_failed",
                    run_id=str(run.id),
                    pipeline_id=str(run.pipeline_id),
                    step=step,
                    reason=str(exc),
                )

                status_by_step[step] = ExecutionStepStatus.FAILED
                failed_count += 1
                skipped_count += await self._mark_remaining_steps_as_skipped(
                    run_id=run.id,
                    node_by_step=node_by_step,
                    remaining_steps=ordered_steps[index + 1:],
                    status_by_step=status_by_step,
                    reason=f"Skipped: run stopped after failure at step {step}",
                )
                break
            except Exception as exc:
                await self._finalize_step_run(
                    step_run=step_run,
                    status=ExecutionStepStatus.FAILED,
                    request_payload=request_payload,
                    response_snapshot={
                        "error_type": type(exc).__name__,
                    },
                    error=f"Unhandled step error: {exc}",
                )
                log_business_event(
                    "execution_step_failed",
                    run_id=str(run.id),
                    pipeline_id=str(run.pipeline_id),
                    step=step,
                    reason=f"Unhandled step error: {exc}",
                )

                status_by_step[step] = ExecutionStepStatus.FAILED
                failed_count += 1
                skipped_count += await self._mark_remaining_steps_as_skipped(
                    run_id=run.id,
                    node_by_step=node_by_step,
                    remaining_steps=ordered_steps[index + 1:],
                    status_by_step=status_by_step,
                    reason=f"Skipped: run stopped after failure at step {step}",
                )
                break

        run.finished_at = self._now_utc()
        run.summary = {
            "total_steps": len(ordered_steps),
            "succeeded_steps": succeeded_count,
            "failed_steps": failed_count,
            "skipped_steps": skipped_count,
            "final_output_step": context.get("final_output_step"),
            "final_output": context.get("final_output"),
        }

        if failed_count == 0 and skipped_count == 0:
            run.status = ExecutionRunStatus.SUCCEEDED
            run.error = None
        elif succeeded_count > 0:
            run.status = ExecutionRunStatus.PARTIAL_FAILED
            run.error = "Execution finished with failed/skipped steps"
        else:
            run.status = ExecutionRunStatus.FAILED
            run.error = "Execution failed"

        await self.session.commit()
        log_business_event(
            "execution_run_finished",
            run_id=str(run.id),
            pipeline_id=str(run.pipeline_id),
            user_id=str(run.initiated_by) if run.initiated_by is not None else None,
            result_status=run.status.value,
            total_steps=len(ordered_steps),
            succeeded_steps=succeeded_count,
            failed_steps=failed_count,
            skipped_steps=skipped_count,
        )

    async def _finalize_step_run(
        self,
        *,
        step_run: ExecutionStepRun,
        status: ExecutionStepStatus,
        request_payload: dict[str, Any],
        response_snapshot: dict[str, Any] | None,
        error: str | None,
    ) -> None:
        step_run.status = status
        step_run.resolved_inputs = request_payload.get("resolved_inputs")
        step_run.request_snapshot = request_payload.get("request_snapshot")
        step_run.response_snapshot = response_snapshot
        step_run.error = error
        step_run.finished_at = self._now_utc()
        step_run.duration_ms = self._duration_ms(step_run.started_at, step_run.finished_at)
        self.session.add(step_run)
        await self.session.commit()

    @staticmethod
    def _build_empty_context() -> dict[str, Any]:
        return {
            "step_outputs": {},
            "edge_values": {},
            "final_output_step": None,
            "final_output": None,
        }

    def _normalize_context(self, raw_context: Any) -> dict[str, Any]:
        context = dict(raw_context) if isinstance(raw_context, dict) else {}
        step_outputs = context.get("step_outputs")
        if not isinstance(step_outputs, dict):
            step_outputs = {}
        edge_values = context.get("edge_values")
        if not isinstance(edge_values, dict):
            edge_values = {}
        final_output_step = context.get("final_output_step")
        if not isinstance(final_output_step, int):
            final_output_step = None
        final_output = context.get("final_output")
        return {
            "step_outputs": step_outputs,
            "edge_values": edge_values,
            "final_output_step": final_output_step,
            "final_output": final_output,
        }

    @staticmethod
    def _build_edge_value_key(from_step: int, to_step: int, edge_type: str) -> str:
        return f"{from_step}:{to_step}:{edge_type}"

    async def _execute_node_endpoint_chain(
        self,
        *,
        node: dict[str, Any],
        resolved_inputs: dict[str, Any],
        run_inputs: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any], Any, uuid.UUID | None, uuid.UUID | None]:
        endpoints = self._get_node_endpoints(node)
        if not endpoints:
            raise StepExecutionError("Node endpoint does not have a valid capability_id")

        endpoints_trace: list[dict[str, Any]] = []
        chain_scope = dict(resolved_inputs)
        protected_inputs = set(resolved_inputs.keys())
        previous_output: Any = None
        final_output: Any = None
        final_request_snapshot: dict[str, Any] | None = None
        final_response_snapshot: dict[str, Any] | None = None
        primary_capability_id: uuid.UUID | None = None
        primary_action_id: uuid.UUID | None = None

        for endpoint_index, endpoint in enumerate(endpoints, start=1):
            capability_uuid, capability = await self._get_capability_from_endpoint(endpoint)
            if primary_capability_id is None:
                primary_capability_id = capability_uuid

            capability_type = self._capability_type_value(capability)
            trace_item: dict[str, Any] = {
                "endpoint_index": endpoint_index,
                "capability_id": str(capability_uuid),
                "capability_type": capability_type,
            }

            if capability_type == CapabilityType.COMPOSITE.value:
                expected_inputs = self._collect_expected_input_names(capability=capability)
                endpoint_inputs = self._apply_chained_output_inputs(
                    base_scope=chain_scope,
                    previous_output=previous_output,
                    expected_inputs=expected_inputs,
                    protected_inputs=protected_inputs,
                )
                composite_request_snapshot = {
                    "capability_type": capability_type,
                    "recipe_version": (
                        capability.recipe.get("version")
                        if isinstance(capability.recipe, dict)
                        else None
                    ),
                }
                trace_item["resolved_inputs"] = endpoint_inputs
                trace_item["request_snapshot"] = composite_request_snapshot
                try:
                    endpoint_response, endpoint_output = await self._execute_composite_capability(
                        capability=capability,
                        resolved_inputs=endpoint_inputs,
                        run_inputs=run_inputs,
                    )
                except StepExecutionError as exc:
                    trace_item["status"] = "failed"
                    trace_item["response_snapshot"] = exc.response_snapshot
                    trace_item["error"] = str(exc)
                    endpoints_trace.append(trace_item)
                    raise StepExecutionError(
                        f"Endpoint {endpoint_index} failed: {exc}",
                        response_snapshot={"endpoints_trace": endpoints_trace},
                    ) from exc

                trace_item["status"] = "succeeded"
                trace_item["response_snapshot"] = endpoint_response
                endpoints_trace.append(trace_item)
                final_request_snapshot = composite_request_snapshot
                final_response_snapshot = endpoint_response
                final_output = endpoint_output
                previous_output = endpoint_output
                chain_scope = endpoint_inputs
                continue

            action = await self._get_action_from_capability(capability_uuid, capability)
            if endpoint_index == 1 and primary_action_id is None:
                primary_action_id = action.id

            expected_inputs = self._collect_expected_input_names(
                capability=capability,
                action=action,
            )
            endpoint_inputs = self._apply_chained_output_inputs(
                base_scope=chain_scope,
                previous_output=previous_output,
                expected_inputs=expected_inputs,
                protected_inputs=protected_inputs,
            )
            step_request = self._build_request_payload(
                action=action,
                resolved_inputs=endpoint_inputs,
            )
            missing_required = sorted(set(step_request["missing_required"]))
            trace_item["action_id"] = str(action.id)
            trace_item["resolved_inputs"] = endpoint_inputs
            trace_item["request_snapshot"] = step_request.get("request_snapshot")

            if missing_required:
                trace_item["status"] = "failed"
                trace_item["missing_required"] = missing_required
                endpoints_trace.append(trace_item)
                raise StepExecutionError(
                    f"Endpoint {endpoint_index} missing required inputs: {missing_required}",
                    response_snapshot={"endpoints_trace": endpoints_trace},
                )

            try:
                endpoint_response, endpoint_output = await self._call_action(action, step_request)
            except StepExecutionError as exc:
                trace_item["status"] = "failed"
                trace_item["response_snapshot"] = exc.response_snapshot
                trace_item["error"] = str(exc)
                endpoints_trace.append(trace_item)
                raise StepExecutionError(
                    f"Endpoint {endpoint_index} failed: {exc}",
                    response_snapshot={"endpoints_trace": endpoints_trace},
                ) from exc

            trace_item["status"] = "succeeded"
            trace_item["response_snapshot"] = endpoint_response
            endpoints_trace.append(trace_item)
            final_request_snapshot = step_request.get("request_snapshot")
            final_response_snapshot = endpoint_response
            final_output = endpoint_output
            previous_output = endpoint_output
            chain_scope = endpoint_inputs

        request_snapshot = (
            dict(final_request_snapshot)
            if isinstance(final_request_snapshot, dict)
            else {}
        )
        request_snapshot["chain_mode"] = "sequential_endpoints"
        request_snapshot["endpoints_trace"] = endpoints_trace

        response_snapshot = (
            dict(final_response_snapshot)
            if isinstance(final_response_snapshot, dict)
            else {}
        )
        response_snapshot["endpoints_trace"] = endpoints_trace
        if "body" not in response_snapshot:
            response_snapshot["body"] = final_output

        request_payload = {
            "resolved_inputs": dict(resolved_inputs),
            "request_snapshot": request_snapshot,
        }
        return (
            request_payload,
            response_snapshot,
            final_output,
            primary_capability_id,
            primary_action_id,
        )

    def _apply_chained_output_inputs(
        self,
        *,
        base_scope: dict[str, Any],
        previous_output: Any,
        expected_inputs: list[str],
        protected_inputs: set[str] | None = None,
    ) -> dict[str, Any]:
        merged = dict(base_scope)
        protected = protected_inputs or set()
        if previous_output is None:
            return merged

        for expected_input in expected_inputs:
            if expected_input in merged and expected_input in protected:
                continue
            resolved = self._resolve_expected_input_from_output(
                output=previous_output,
                expected_input=expected_input,
            )
            if resolved is not None:
                merged[expected_input] = resolved
        return merged

    def _collect_expected_input_names(
        self,
        *,
        capability: Capability | None = None,
        action: Action | None = None,
    ) -> list[str]:
        names: list[str] = []
        seen: set[str] = set()

        def add_name(raw_name: Any) -> None:
            if not isinstance(raw_name, (str, int)):
                return
            name = str(raw_name).strip()
            if not name or name in seen:
                return
            names.append(name)
            seen.add(name)

        if capability is not None and isinstance(capability.input_schema, dict):
            for name in self._collect_schema_input_names(capability.input_schema):
                add_name(name)

        if action is not None:
            for schema in (action.parameters_schema, action.request_body_schema):
                if not isinstance(schema, dict):
                    continue
                for name in self._collect_schema_input_names(schema):
                    add_name(name)

        return names

    @staticmethod
    def _collect_schema_input_names(schema: dict[str, Any]) -> list[str]:
        names: list[str] = []
        required = schema.get("required")
        if isinstance(required, list):
            names.extend(str(item) for item in required if isinstance(item, (str, int)))
        properties = schema.get("properties")
        if isinstance(properties, dict):
            names.extend(
                str(name) for name in properties.keys() if isinstance(name, (str, int))
            )

        deduplicated: list[str] = []
        seen: set[str] = set()
        for name in names:
            normalized = str(name).strip()
            if not normalized or normalized in seen:
                continue
            deduplicated.append(normalized)
            seen.add(normalized)
        return deduplicated

    def _resolve_expected_input_from_output(self, *, output: Any, expected_input: str) -> Any:
        if isinstance(output, dict):
            if expected_input in output:
                return output[expected_input]
            expected_base = expected_input[:-2] if expected_input.endswith("[]") else expected_input
            if expected_base in output:
                return output[expected_base]
            for field_name, field_value in output.items():
                if not isinstance(field_name, str):
                    continue
                if self._field_alias_matches(
                    field_name=field_name,
                    expected_input=expected_input,
                ):
                    return field_value

        fallback = self._extract_value_from_output(output, expected_input)
        return fallback

    def _field_alias_matches(self, *, field_name: str, expected_input: str) -> bool:
        left = str(field_name).strip()
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

    @staticmethod
    def _tokenize_field_name(value: str) -> set[str]:
        normalized = re.sub(r"([a-z])([A-Z])", r"\1 \2", str(value))
        normalized = normalized.replace("_", " ").replace("-", " ")
        tokens = {
            token
            for token in re.findall(r"[a-zA-Z0-9]+", normalized.lower())
            if token
        }
        singularized = {
            token[:-1]
            for token in tokens
            if token.endswith("s") and len(token) > 3
        }
        return tokens | singularized

    @staticmethod
    def _normalize_lookup_token(value: str) -> str:
        return re.sub(r"[^a-zA-Z0-9]+", "", str(value).lower())

    async def _get_capability_from_endpoint(
        self,
        endpoint: dict[str, Any],
    ) -> tuple[uuid.UUID, Capability]:
        capability_id = endpoint.get("capability_id") if isinstance(endpoint, dict) else None
        capability_uuid = self._to_uuid(capability_id)
        if capability_uuid is None:
            raise StepExecutionError("Node endpoint does not have a valid capability_id")

        capability = await self.session.get(Capability, capability_uuid)
        if capability is None:
            raise StepExecutionError(f"Capability not found: {capability_uuid}")
        return capability_uuid, capability

    async def _get_capability_from_node(self, node: dict[str, Any]) -> tuple[uuid.UUID, Capability]:
        endpoints = self._get_node_endpoints(node)
        if not endpoints:
            raise StepExecutionError("Node endpoint does not have a valid capability_id")
        return await self._get_capability_from_endpoint(endpoints[0])

    async def _get_action_from_capability(
        self,
        capability_uuid: uuid.UUID,
        capability: Capability,
    ) -> Action:
        action_uuid = capability.action_id
        if action_uuid is None:
            raise StepExecutionError(
                f"Capability does not have action_id: {capability_uuid}"
            )

        action = await self.session.get(Action, action_uuid)
        if action is None:
            raise StepExecutionError(
                f"Action not found for capability {capability_uuid}: {action_uuid}"
            )
        return action

    async def _get_action_from_node(self, node: dict[str, Any]) -> tuple[uuid.UUID, Action]:
        capability_uuid, capability = await self._get_capability_from_node(node)
        action = await self._get_action_from_capability(capability_uuid, capability)
        return capability_uuid, action

    def _create_step_run_from_node(
        self,
        run_id: uuid.UUID,
        node: dict[str, Any],
        *,
        status: ExecutionStepStatus,
    ) -> ExecutionStepRun:
        endpoints = self._get_node_endpoints(node)
        endpoint = endpoints[0] if endpoints else {}
        capability_id = self._to_uuid(endpoint.get("capability_id"))
        action_id = self._to_uuid(endpoint.get("action_id"))
        return ExecutionStepRun(
            run_id=run_id,
            step=self._safe_int(node.get("step"), fallback=0),
            name=str(node.get("name")) if node.get("name") is not None else None,
            capability_id=capability_id,
            action_id=action_id,
            status=status,
        )

    def _resolve_node_inputs(
        self,
        *,
        node: dict[str, Any],
        incoming_edges: list[dict[str, Any]],
        step_outputs: dict[str, Any],
        edge_values: dict[str, Any],
        run_inputs: dict[str, Any],
    ) -> tuple[dict[str, Any], list[str]]:
        resolved: dict[str, Any] = {}

        def add_resolved(key: str, value: Any) -> None:
            resolved[key] = value
            # Normalize common edge notation (users[] -> users) so request/body
            # keys and composite bindings can resolve expected field names.
            if key.endswith("[]"):
                normalized = key[:-2]
                if normalized and normalized not in resolved:
                    resolved[normalized] = value

        for edge in incoming_edges:
            src = edge.get("from_step")
            dst = edge.get("to_step")
            edge_type = edge.get("type")
            if not isinstance(src, int) or not isinstance(dst, int) or not isinstance(edge_type, str):
                continue
            edge_key = self._build_edge_value_key(src, dst, edge_type)
            if edge_key in edge_values:
                add_resolved(edge_type, edge_values[edge_key])
                continue
            source_output = step_outputs.get(str(src))
            if source_output is None:
                continue
            value = self._extract_value_from_output(source_output, edge_type)
            if value is not None:
                add_resolved(edge_type, value)

        external_inputs = self._normalize_str_list(node.get("external_inputs"))
        for input_name in external_inputs:
            if input_name in run_inputs:
                resolved[input_name] = run_inputs[input_name]

        # One-click execution: external inputs are optional and do not block run start.
        # Required fields are validated against action schema in _build_request_payload.
        missing_external: list[str] = []
        return resolved, missing_external

    def _build_request_payload(self, *, action: Action, resolved_inputs: dict[str, Any]) -> dict[str, Any]:
        params_schema = action.parameters_schema if isinstance(action.parameters_schema, dict) else {}
        params_properties = params_schema.get("properties", {}) if isinstance(params_schema.get("properties"), dict) else {}
        params_required = [
            str(name)
            for name in params_schema.get("required", [])
            if isinstance(name, (str, int))
        ]

        body_schema = action.request_body_schema if isinstance(action.request_body_schema, dict) else {}
        body_type = body_schema.get("type") if isinstance(body_schema.get("type"), str) else None
        body_properties = body_schema.get("properties", {}) if isinstance(body_schema.get("properties"), dict) else {}
        body_required = [
            str(name)
            for name in body_schema.get("required", [])
            if isinstance(name, (str, int))
        ]

        path_params: dict[str, Any] = {}
        query_params: dict[str, Any] = {}
        headers: dict[str, Any] = {}
        cookies: dict[str, Any] = {}
        body: Any = {} if body_type == "object" else None
        unresolved: dict[str, Any] = {}

        for key, value in resolved_inputs.items():
            property_schema = params_properties.get(key)
            if isinstance(property_schema, dict):
                location = property_schema.get("x-parameter-location", "query")
                if location == "path":
                    path_params[key] = value
                elif location == "header":
                    headers[key] = value
                elif location == "cookie":
                    cookies[key] = value
                else:
                    query_params[key] = value
                continue

            if body_type == "object" and (not body_properties or key in body_properties):
                if not isinstance(body, dict):
                    body = {}
                body[key] = value
                continue

            unresolved[key] = value

        self._apply_schema_defaults(params_properties, path_params, query_params, headers, cookies)
        if body_type == "object":
            if not isinstance(body, dict):
                body = {}
            for field_name, field_schema in body_properties.items():
                if field_name in body:
                    continue
                fallback = self._schema_default_or_example(field_schema)
                if fallback is not None:
                    body[field_name] = fallback
            if not body and isinstance(body_schema.get("example"), dict):
                body = dict(body_schema["example"])

        if unresolved:
            if action.method in {HttpMethod.GET, HttpMethod.DELETE, HttpMethod.HEAD, HttpMethod.OPTIONS}:
                query_params.update({key: value for key, value in unresolved.items() if key not in query_params})
            else:
                if body is None:
                    body = {}
                if isinstance(body, dict):
                    for key, value in unresolved.items():
                        body.setdefault(key, value)
                else:
                    body = unresolved

        missing_required: list[str] = []
        for field_name in params_required:
            if field_name in path_params or field_name in query_params or field_name in headers or field_name in cookies:
                continue
            missing_required.append(field_name)

        if body_type == "object":
            body_dict = body if isinstance(body, dict) else {}
            for field_name in body_required:
                if field_name not in body_dict:
                    missing_required.append(field_name)
        elif body_schema.get("x-required") and body in (None, "", {}, []):
            missing_required.append("__request_body__")

        path = action.path or ""
        for path_param in re.findall(r"{([^{}]+)}", path):
            if path_param in path_params:
                path = path.replace(f"{{{path_param}}}", str(path_params[path_param]))
            else:
                missing_required.append(path_param)

        path_is_absolute_url = self._is_absolute_url(path)
        base_url = self._resolve_action_base_url(action)
        if not path_is_absolute_url and not base_url:
            missing_required.append("__base_url__")
        if path_is_absolute_url:
            url = path
        else:
            url = self._join_url(base_url or "", path)

        content_type = body_schema.get("x-content-type")
        if isinstance(content_type, str) and body is not None:
            headers.setdefault("Content-Type", content_type)

        return {
            "url": url,
            "query_params": query_params,
            "headers": headers,
            "cookies": cookies,
            "json_body": body,
            "missing_required": sorted(set(missing_required)),
            "resolved_inputs": resolved_inputs,
            "request_snapshot": {
                "method": action.method.value,
                "url": url,
                "path_params": path_params,
                "query_params": query_params,
                "headers": headers,
                "cookies": cookies,
                "json_body": body,
            },
        }

    def _resolve_action_base_url(self, action: Action) -> str | None:
        fallback_base_url = os.getenv("EXECUTION_DEFAULT_BASE_URL")
        fallback_normalized = self._normalize_base_url(fallback_base_url)

        base_url = self._normalize_base_url(getattr(action, "base_url", None))
        resolved_base_url = self._resolve_base_url_with_fallback(
            candidate=base_url,
            fallback=fallback_normalized,
        )
        if resolved_base_url:
            return resolved_base_url

        raw_spec = getattr(action, "raw_spec", None)
        if isinstance(raw_spec, dict):
            servers = raw_spec.get("servers")
            if isinstance(servers, list):
                for server in servers:
                    candidate = self._resolve_server_url(server)
                    resolved = self._resolve_base_url_with_fallback(
                        candidate=candidate,
                        fallback=fallback_normalized,
                    )
                    if resolved:
                        return resolved

        if fallback_normalized:
            return fallback_normalized

        return None

    def _resolve_server_url(self, server: Any) -> str | None:
        if not isinstance(server, dict):
            return None
        raw_url = server.get("url")
        if not isinstance(raw_url, str):
            return None
        url = raw_url.strip()
        if not url:
            return None

        variables = server.get("variables")
        if isinstance(variables, dict):
            for variable_name, variable_payload in variables.items():
                placeholder = f"{{{variable_name}}}"
                if placeholder not in url:
                    continue
                default_value: str | None = None
                if isinstance(variable_payload, dict):
                    raw_default = variable_payload.get("default")
                    if isinstance(raw_default, str):
                        default_value = raw_default.strip()
                if default_value:
                    url = url.replace(placeholder, default_value)

        return self._normalize_base_url(url)

    def _resolve_base_url_with_fallback(
        self,
        *,
        candidate: str | None,
        fallback: str | None,
    ) -> str | None:
        if not candidate:
            return fallback
        if self._is_absolute_url(candidate):
            return candidate
        if fallback:
            return self._join_url(fallback, candidate)
        return None

    @staticmethod
    def _normalize_base_url(value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        normalized = value.strip()
        return normalized or None

    @staticmethod
    def _is_absolute_url(value: str) -> bool:
        parsed = urlparse(str(value or ""))
        return bool(parsed.scheme and parsed.netloc)

    async def _call_action(
        self,
        action: Action,
        request_payload: dict[str, Any],
    ) -> tuple[dict[str, Any], Any]:
        timeout_seconds = float(os.getenv("EXECUTION_STEP_TIMEOUT_SECONDS", "30"))
        async with httpx.AsyncClient(timeout=timeout_seconds, follow_redirects=True) as client:
            try:
                response = await client.request(
                    method=action.method.value,
                    url=request_payload["url"],
                    params=request_payload["query_params"] or None,
                    headers=request_payload["headers"] or None,
                    cookies=request_payload["cookies"] or None,
                    json=request_payload["json_body"],
                )
            except httpx.TimeoutException as exc:
                raise StepExecutionError(f"Timeout while calling endpoint: {exc}") from exc
            except httpx.RequestError as exc:
                raise StepExecutionError(f"Request error while calling endpoint: {exc}") from exc

        response_body = self._extract_response_body(response)
        response_snapshot = {
            "status_code": response.status_code,
            "content_type": response.headers.get("content-type"),
            "body": response_body,
        }
        if response.status_code >= 400:
            raise StepExecutionError(f"Upstream endpoint returned HTTP {response.status_code}", response_snapshot=response_snapshot)

        return response_snapshot, response_body

    async def _execute_composite_capability(
        self,
        *,
        capability: Capability,
        resolved_inputs: dict[str, Any],
        run_inputs: dict[str, Any],
    ) -> tuple[dict[str, Any], Any]:
        recipe = capability.recipe if isinstance(capability.recipe, dict) else None
        if recipe is None:
            raise StepExecutionError(
                f"Composite capability does not have a valid recipe: {capability.id}"
            )
        steps = recipe.get("steps")
        if not isinstance(steps, list) or not steps:
            raise StepExecutionError(
                f"Composite capability recipe has no steps: {capability.id}"
            )

        ordered_steps = sorted(
            [step for step in steps if isinstance(step, dict)],
            key=lambda item: self._safe_int(item.get("step"), fallback=0),
        )
        composite_run_scope = dict(run_inputs)
        composite_run_scope.update(resolved_inputs)
        nested_outputs: dict[int, Any] = {}
        nested_trace: list[dict[str, Any]] = []

        for raw_step in ordered_steps:
            step_number = self._safe_int(raw_step.get("step"), fallback=0)
            if step_number <= 0:
                raise StepExecutionError("Composite recipe has invalid step number")

            step_capability_uuid = self._to_uuid(raw_step.get("capability_id"))
            if step_capability_uuid is None:
                raise StepExecutionError(
                    f"Composite recipe step {step_number} has invalid capability_id"
                )
            step_capability = await self.session.get(Capability, step_capability_uuid)
            if step_capability is None:
                raise StepExecutionError(
                    f"Composite recipe step {step_number} capability not found: {step_capability_uuid}"
                )
            if self._capability_type_value(step_capability) != CapabilityType.ATOMIC.value:
                raise StepExecutionError(
                    f"Composite recipe step {step_number} must reference ATOMIC capability: {step_capability_uuid}"
                )

            step_action = await self._get_action_from_capability(
                step_capability_uuid,
                step_capability,
            )

            raw_inputs = raw_step.get("inputs")
            normalized_inputs: dict[str, Any] = {}
            if isinstance(raw_inputs, dict):
                for input_name, binding_expr in raw_inputs.items():
                    if not isinstance(input_name, str):
                        continue
                    if not isinstance(binding_expr, str):
                        continue
                    value = self._resolve_composite_binding(
                        binding_expr=binding_expr.strip(),
                        run_scope=composite_run_scope,
                        step_outputs=nested_outputs,
                    )
                    if value is not None:
                        normalized_inputs[input_name] = value

            step_request = self._build_request_payload(
                action=step_action,
                resolved_inputs=normalized_inputs,
            )
            missing_required = sorted(set(step_request["missing_required"]))
            if missing_required:
                nested_trace.append(
                    {
                        "step": step_number,
                        "capability_id": str(step_capability_uuid),
                        "action_id": str(step_action.id),
                        "status": "failed",
                        "resolved_inputs": normalized_inputs,
                        "missing_required": missing_required,
                    }
                )
                raise StepExecutionError(
                    f"Composite step {step_number} missing required inputs: {missing_required}",
                    response_snapshot={"nested_trace": nested_trace},
                )

            try:
                action_response, action_output = await self._call_action(step_action, step_request)
            except StepExecutionError as exc:
                nested_trace.append(
                    {
                        "step": step_number,
                        "capability_id": str(step_capability_uuid),
                        "action_id": str(step_action.id),
                        "status": "failed",
                        "resolved_inputs": normalized_inputs,
                        "request_snapshot": step_request.get("request_snapshot"),
                        "response_snapshot": exc.response_snapshot,
                        "error": str(exc),
                    }
                )
                raise StepExecutionError(
                    f"Composite step {step_number} failed: {exc}",
                    response_snapshot={"nested_trace": nested_trace},
                ) from exc

            nested_outputs[step_number] = action_output
            nested_trace.append(
                {
                    "step": step_number,
                    "capability_id": str(step_capability_uuid),
                    "action_id": str(step_action.id),
                    "status": "succeeded",
                    "resolved_inputs": normalized_inputs,
                    "request_snapshot": step_request.get("request_snapshot"),
                    "response_snapshot": action_response,
                }
            )

        if not nested_outputs:
            raise StepExecutionError(
                f"Composite capability recipe has no executable steps: {capability.id}"
            )

        final_step = max(nested_outputs.keys())
        final_output = nested_outputs[final_step]
        composite_response = {
            "capability_type": CapabilityType.COMPOSITE.value,
            "recipe_version": recipe.get("version"),
            "steps_executed": len(nested_outputs),
            "nested_trace": nested_trace,
        }
        return composite_response, final_output

    def _resolve_composite_binding(
        self,
        *,
        binding_expr: str,
        run_scope: dict[str, Any],
        step_outputs: dict[int, Any],
    ) -> Any:
        if binding_expr.startswith("$run."):
            path = binding_expr[len("$run.") :]
            return self._resolve_dot_path(run_scope, path)
        if binding_expr.startswith("$step."):
            match = re.fullmatch(r"\$step\.(\d+)\.(.+)", binding_expr)
            if not match:
                return None
            source_step = int(match.group(1))
            path = match.group(2)
            source_payload = step_outputs.get(source_step)
            return self._resolve_dot_path(source_payload, path)
        return None

    def _resolve_dot_path(self, payload: Any, path: str) -> Any:
        if payload is None:
            return None
        current: Any = payload
        for part in [chunk for chunk in str(path).split(".") if chunk]:
            if isinstance(current, dict):
                if part not in current:
                    return None
                current = current.get(part)
                continue
            if isinstance(current, list):
                if not part.isdigit():
                    return None
                index = int(part)
                if index < 0 or index >= len(current):
                    return None
                current = current[index]
                continue
            return None
        return current

    def _capability_type_value(self, capability: Capability) -> str:
        raw = getattr(capability, "type", None)
        if isinstance(raw, CapabilityType):
            return raw.value
        if isinstance(raw, str):
            return raw
        if hasattr(raw, "value"):
            return str(raw.value)
        return CapabilityType.ATOMIC.value

    @staticmethod
    def _extract_response_body(response: httpx.Response) -> Any:
        content_type = response.headers.get("content-type", "")
        if "json" in content_type.lower():
            try:
                return response.json()
            except ValueError:
                pass

        text_body = response.text
        if len(text_body) > 20000:
            return text_body[:20000] + "...(truncated)"
        return text_body

    @staticmethod
    def _extract_value_from_output(output: Any, edge_type: str) -> Any:
        if isinstance(output, dict):
            if edge_type in output:
                return output[edge_type]
            normalized = edge_type[:-2] if edge_type.endswith("[]") else edge_type
            if normalized in output:
                return output[normalized]
            if len(output) == 1:
                return next(iter(output.values()))
        if isinstance(output, list):
            return output
        return output

    @staticmethod
    def _normalize_graph(
        raw_nodes: Any,
        raw_edges: Any,
    ) -> tuple[dict[int, dict[str, Any]], list[dict[str, Any]], dict[int, list[dict[str, Any]]], dict[int, list[dict[str, Any]]]]:
        node_by_step: dict[int, dict[str, Any]] = {}
        if isinstance(raw_nodes, list):
            for node in raw_nodes:
                if not isinstance(node, dict):
                    continue
                step = node.get("step")
                if isinstance(step, int):
                    node_by_step[step] = node

        edges: list[dict[str, Any]] = []
        edges_by_target: dict[int, list[dict[str, Any]]] = {}
        edges_by_source: dict[int, list[dict[str, Any]]] = {}
        if isinstance(raw_edges, list):
            for edge in raw_edges:
                if not isinstance(edge, dict):
                    continue
                src = edge.get("from_step")
                dst = edge.get("to_step")
                edge_type = edge.get("type")
                if not isinstance(src, int) or not isinstance(dst, int) or not isinstance(edge_type, str):
                    continue
                if src not in node_by_step or dst not in node_by_step:
                    continue
                normalized_edge = {"from_step": src, "to_step": dst, "type": edge_type}
                edges.append(normalized_edge)
                edges_by_target.setdefault(dst, []).append(normalized_edge)
                edges_by_source.setdefault(src, []).append(normalized_edge)

        return node_by_step, edges, edges_by_target, edges_by_source

    @staticmethod
    def _topological_sort(steps: list[int], edges: list[dict[str, Any]]) -> list[int]:
        if not steps:
            return []

        in_degree: dict[int, int] = {step: 0 for step in steps}
        adjacency: dict[int, set[int]] = {step: set() for step in steps}

        for edge in edges:
            src = edge["from_step"]
            dst = edge["to_step"]
            if dst not in in_degree or src not in adjacency:
                continue
            if dst in adjacency[src]:
                continue
            adjacency[src].add(dst)
            in_degree[dst] += 1

        queue = sorted([step for step, degree in in_degree.items() if degree == 0])
        ordered: list[int] = []

        while queue:
            current = queue.pop(0)
            ordered.append(current)
            for neighbor in sorted(adjacency[current]):
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
            queue.sort()

        if len(ordered) != len(steps):
            raise ExecutionServiceError("Graph contains a cycle")
        return ordered

    @staticmethod
    def _get_node_endpoints(node: dict[str, Any]) -> list[dict[str, Any]]:
        endpoints = node.get("endpoints")
        if not isinstance(endpoints, list):
            return []
        return [item for item in endpoints if isinstance(item, dict)]

    @staticmethod
    def _normalize_str_list(value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        return [str(item) for item in value if isinstance(item, (str, int))]

    @staticmethod
    def _to_uuid(value: Any) -> uuid.UUID | None:
        if value is None:
            return None
        try:
            return uuid.UUID(str(value))
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _safe_int(value: Any, *, fallback: int) -> int:
        if isinstance(value, int):
            return value
        try:
            return int(value)
        except (TypeError, ValueError):
            return fallback

    @staticmethod
    def _join_url(base_url: str, path: str) -> str:
        if ExecutionService._is_absolute_url(path):
            return path
        if not base_url:
            return path
        base = base_url.rstrip("/")
        suffix = path if path.startswith("/") else f"/{path}"
        return f"{base}{suffix}"

    @staticmethod
    def _now_utc() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _duration_ms(started_at: datetime | None, finished_at: datetime | None) -> int | None:
        if started_at is None or finished_at is None:
            return None
        return max(0, int((finished_at - started_at).total_seconds() * 1000))

    @staticmethod
    def _schema_default_or_example(schema: Any) -> Any:
        if not isinstance(schema, dict):
            return None
        if "default" in schema:
            return schema.get("default")
        if "example" in schema:
            return schema.get("example")
        examples = schema.get("examples")
        if isinstance(examples, dict):
            for example_payload in examples.values():
                if isinstance(example_payload, dict) and "value" in example_payload:
                    return example_payload["value"]
                if example_payload is not None:
                    return example_payload
        return None

    def _apply_schema_defaults(
        self,
        parameter_properties: dict[str, Any],
        path_params: dict[str, Any],
        query_params: dict[str, Any],
        headers: dict[str, Any],
        cookies: dict[str, Any],
    ) -> None:
        for parameter_name, parameter_schema in parameter_properties.items():
            if not isinstance(parameter_schema, dict):
                continue
            if parameter_name in path_params or parameter_name in query_params or parameter_name in headers or parameter_name in cookies:
                continue
            fallback = self._schema_default_or_example(parameter_schema)
            if fallback is None:
                continue
            location = parameter_schema.get("x-parameter-location", "query")
            if location == "path":
                path_params[parameter_name] = fallback
            elif location == "header":
                headers[parameter_name] = fallback
            elif location == "cookie":
                cookies[parameter_name] = fallback
            else:
                query_params[parameter_name] = fallback

    async def _mark_remaining_steps_as_skipped(
        self,
        *,
        run_id: uuid.UUID,
        node_by_step: dict[int, dict[str, Any]],
        remaining_steps: list[int],
        status_by_step: dict[int, ExecutionStepStatus],
        reason: str,
    ) -> int:
        if not remaining_steps:
            return 0

        now = self._now_utc()
        skipped_items: list[ExecutionStepRun] = []
        for step in remaining_steps:
            node = node_by_step.get(step)
            if node is None:
                continue
            step_run = self._create_step_run_from_node(
                run_id,
                node,
                status=ExecutionStepStatus.SKIPPED,
            )
            step_run.error = reason
            step_run.started_at = now
            step_run.finished_at = now
            step_run.duration_ms = 0
            skipped_items.append(step_run)
            status_by_step[step] = ExecutionStepStatus.SKIPPED

        if skipped_items:
            self.session.add_all(skipped_items)
            await self.session.commit()

        return len(skipped_items)
