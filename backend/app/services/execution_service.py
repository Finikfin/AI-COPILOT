from __future__ import annotations

import asyncio
import json
import os
import re
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
            raise ExecutionServiceError("Execution run not found")

        pipeline = await self.session.get(Pipeline, run.pipeline_id)
        if pipeline is None:
            run.status = ExecutionRunStatus.FAILED
            run.error = "Pipeline not found"
            run.finished_at = self._now_utc()
            await self.session.commit()
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
            return

        run.status = ExecutionRunStatus.RUNNING
        run.started_at = self._now_utc()
        run.error = None
        run.summary = None
        await self.session.commit()

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
                capability_id, capability = await self._get_capability_from_node(node)
                capability_type = self._capability_type_value(capability)
                step_run.capability_id = capability_id
                resolved_inputs, _missing_external = self._resolve_node_inputs(
                    node=node,
                    incoming_edges=incoming,
                    step_outputs=step_outputs,
                    edge_values=edge_values,
                    run_inputs=run.inputs or {},
                )
                if capability_type == CapabilityType.COMPOSITE.value:
                    request_payload = {
                        "resolved_inputs": resolved_inputs,
                        "request_snapshot": {
                            "capability_type": capability_type,
                            "recipe_version": (
                                capability.recipe.get("version")
                                if isinstance(capability.recipe, dict)
                                else None
                            ),
                        },
                    }
                    response_snapshot, output_payload = await self._execute_composite_capability(
                        capability=capability,
                        resolved_inputs=resolved_inputs,
                        run_inputs=run.inputs or {},
                    )
                    step_run.action_id = None
                else:
                    action = await self._get_action_from_capability(capability_id, capability)
                    step_run.action_id = action.id
                    request_payload = self._build_request_payload(
                        action=action,
                        resolved_inputs=resolved_inputs,
                    )
                    missing_required = sorted(set(request_payload["missing_required"]))
                    if missing_required:
                        raise StepExecutionError(f"Missing inputs: {missing_required}")

                    response_snapshot, output_payload = await self._call_action(action, request_payload)

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

    async def _get_capability_from_node(self, node: dict[str, Any]) -> tuple[uuid.UUID, Capability]:
        endpoint = self._get_primary_endpoint(node)
        capability_id = endpoint.get("capability_id") if isinstance(endpoint, dict) else None
        capability_uuid = self._to_uuid(capability_id)
        if capability_uuid is None:
            raise StepExecutionError("Node endpoint does not have a valid capability_id")

        capability = await self.session.get(Capability, capability_uuid)
        if capability is None:
            raise StepExecutionError(f"Capability not found: {capability_uuid}")
        return capability_uuid, capability

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
        endpoint = self._get_primary_endpoint(node)
        capability_id = self._to_uuid(endpoint.get("capability_id")) if isinstance(endpoint, dict) else None
        action_id = self._to_uuid(endpoint.get("action_id")) if isinstance(endpoint, dict) else None
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

        for edge in incoming_edges:
            src = edge.get("from_step")
            dst = edge.get("to_step")
            edge_type = edge.get("type")
            if not isinstance(src, int) or not isinstance(dst, int) or not isinstance(edge_type, str):
                continue
            edge_key = self._build_edge_value_key(src, dst, edge_type)
            if edge_key in edge_values:
                resolved[edge_type] = edge_values[edge_key]
                continue
            source_output = step_outputs.get(str(src))
            if source_output is None:
                continue
            value = self._extract_value_from_output(source_output, edge_type)
            if value is not None:
                resolved[edge_type] = value

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

        if not action.base_url:
            missing_required.append("__base_url__")
        url = self._join_url(action.base_url or "", path)

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
    def _get_primary_endpoint(node: dict[str, Any]) -> dict[str, Any]:
        endpoints = node.get("endpoints")
        if isinstance(endpoints, list) and endpoints and isinstance(endpoints[0], dict):
            return endpoints[0]
        return {}

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
