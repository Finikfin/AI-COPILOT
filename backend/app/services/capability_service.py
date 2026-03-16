from __future__ import annotations

import re
from typing import Any
from uuid import UUID

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Action, Capability
from app.models.capability import CapabilityType


class CompositeRecipeValidationError(ValueError):
    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        super().__init__("; ".join(errors))


class CapabilityService:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    @staticmethod
    def build_from_actions(
        actions: list[Action],
        *,
        owner_user_id: UUID,
    ) -> list[Capability]:
        capabilities: list[Capability] = []
        for action in actions:
            capability_payload = CapabilityService._build_capability_payload(action)
            capabilities.append(
                Capability(
                    user_id=owner_user_id,
                    action_id=action.id,
                    type=CapabilityType.ATOMIC,
                    name=capability_payload["name"],
                    description=capability_payload.get("description"),
                    input_schema=capability_payload.get("input_schema"),
                    output_schema=capability_payload.get("output_schema"),
                    data_format=capability_payload.get("data_format"),
                    llm_payload=capability_payload.get("llm_payload"),
                )
            )

        return capabilities

    async def create_composite_capability(
        self,
        *,
        owner_user_id: UUID,
        name: str,
        description: str | None = None,
        input_schema: dict[str, Any] | None = None,
        output_schema: dict[str, Any] | None = None,
        recipe: dict[str, Any],
        llm_payload: dict[str, Any] | None = None,
        data_format: dict[str, Any] | None = None,
    ) -> Capability:
        capability = Capability(
            user_id=owner_user_id,
            type=CapabilityType.COMPOSITE,
            name=name,
            description=description,
            input_schema=input_schema,
            output_schema=output_schema,
            recipe=recipe,
            llm_payload=llm_payload,
            data_format=data_format,
        )
        self.session.add(capability)
        await self.session.flush()
        await self.session.refresh(capability)
        return capability

    async def create_validated_composite_capability(
        self,
        *,
        owner_user_id: UUID,
        name: str,
        description: str | None = None,
        input_schema: dict[str, Any] | None = None,
        output_schema: dict[str, Any] | None = None,
        recipe: dict[str, Any],
        include_all: bool = False,
    ) -> Capability:
        normalized_recipe, step_capabilities = await self.validate_composite_recipe(
            recipe=recipe,
            owner_user_id=owner_user_id,
            include_all=include_all,
        )
        llm_payload = self._build_composite_llm_payload(step_capabilities)
        data_format = {
            "request_schema_type": input_schema.get("type")
            if isinstance(input_schema, dict)
            else None,
            "response_schema_types": [output_schema.get("type")]
            if isinstance(output_schema, dict)
            and isinstance(output_schema.get("type"), str)
            else [],
            "composite": {
                "version": normalized_recipe.get("version"),
                "steps_count": len(normalized_recipe.get("steps", [])),
                "step_capability_names": [
                    str(getattr(capability, "name", ""))
                    for capability in step_capabilities
                ],
            },
        }
        return await self.create_composite_capability(
            owner_user_id=owner_user_id,
            name=name,
            description=description,
            input_schema=input_schema,
            output_schema=output_schema,
            recipe=normalized_recipe,
            llm_payload=llm_payload,
            data_format=data_format,
        )

    async def validate_composite_recipe(
        self,
        *,
        recipe: dict[str, Any],
        owner_user_id: UUID,
        include_all: bool = False,
    ) -> tuple[dict[str, Any], list[Capability]]:
        errors: list[str] = []
        if not isinstance(recipe, dict):
            raise CompositeRecipeValidationError(["recipe must be an object"])

        version = recipe.get("version")
        if version != 1:
            errors.append("recipe.version must be 1")

        raw_steps = recipe.get("steps")
        if not isinstance(raw_steps, list) or not raw_steps:
            errors.append("recipe.steps must be a non-empty list")
            raise CompositeRecipeValidationError(errors)

        normalized_steps: list[dict[str, Any]] = []
        seen_step_numbers: set[int] = set()
        for index, raw_step in enumerate(raw_steps):
            if not isinstance(raw_step, dict):
                errors.append(f"recipe.steps[{index}] must be an object")
                continue

            step_number = raw_step.get("step")
            if not isinstance(step_number, int) or step_number < 1:
                errors.append(f"recipe.steps[{index}].step must be positive integer")
                continue

            if step_number in seen_step_numbers:
                errors.append(f"recipe.steps[{index}].step duplicates step {step_number}")
            seen_step_numbers.add(step_number)

            capability_uuid = self._to_uuid(raw_step.get("capability_id"))
            if capability_uuid is None:
                errors.append(f"recipe.steps[{index}].capability_id must be UUID")
                continue

            raw_inputs = raw_step.get("inputs", {})
            if raw_inputs is None:
                raw_inputs = {}
            if not isinstance(raw_inputs, dict):
                errors.append(f"recipe.steps[{index}].inputs must be an object")
                raw_inputs = {}

            normalized_inputs: dict[str, str] = {}
            for input_name, binding in raw_inputs.items():
                if not isinstance(input_name, str) or not input_name.strip():
                    errors.append(f"recipe.steps[{index}].inputs has invalid key")
                    continue
                if not isinstance(binding, str):
                    errors.append(
                        f"recipe.steps[{index}].inputs.{input_name} must be string binding"
                    )
                    continue
                normalized_binding = binding.strip()
                if not normalized_binding:
                    errors.append(
                        f"recipe.steps[{index}].inputs.{input_name} must be non-empty binding"
                    )
                    continue
                if not self._is_supported_binding_expression(normalized_binding):
                    errors.append(
                        f"recipe.steps[{index}].inputs.{input_name} has unsupported binding '{normalized_binding}'"
                    )
                    continue
                normalized_inputs[input_name] = normalized_binding

            normalized_steps.append(
                {
                    "step": step_number,
                    "capability_id": str(capability_uuid),
                    "inputs": normalized_inputs,
                }
            )

        if errors:
            raise CompositeRecipeValidationError(errors)

        normalized_steps.sort(key=lambda item: item["step"])
        for idx in range(1, len(normalized_steps)):
            if normalized_steps[idx]["step"] <= normalized_steps[idx - 1]["step"]:
                errors.append("recipe.steps must be strictly increasing by step")
                break

        known_steps = {item["step"] for item in normalized_steps}
        for item in normalized_steps:
            for binding in item["inputs"].values():
                if not binding.startswith("$step."):
                    continue
                source_step = self._extract_binding_source_step(binding)
                if source_step is None:
                    errors.append(
                        f"step {item['step']}: invalid step binding '{binding}'"
                    )
                    continue
                if source_step not in known_steps:
                    errors.append(
                        f"step {item['step']}: binding references missing step {source_step}"
                    )
                    continue
                if source_step >= item["step"]:
                    errors.append(
                        f"step {item['step']}: binding references non-previous step {source_step}"
                    )

        capability_ids = [UUID(item["capability_id"]) for item in normalized_steps]
        capabilities = await self.get_capabilities(
            capability_ids=capability_ids,
            owner_user_id=owner_user_id,
            include_all=include_all,
        )
        capabilities_by_id = {str(item.id): item for item in capabilities}
        for item in normalized_steps:
            capability = capabilities_by_id.get(item["capability_id"])
            if capability is None:
                errors.append(
                    f"step {item['step']}: capability {item['capability_id']} not found or not accessible"
                )
                continue

            capability_type = self._capability_type_value(capability)
            if capability_type != CapabilityType.ATOMIC.value:
                errors.append(
                    f"step {item['step']}: nested composite is not allowed ({item['capability_id']})"
                )
                continue
            if getattr(capability, "action_id", None) is None:
                errors.append(
                    f"step {item['step']}: atomic capability {item['capability_id']} has no action_id"
                )

        if errors:
            raise CompositeRecipeValidationError(errors)

        normalized_recipe = {
            "version": 1,
            "steps": normalized_steps,
        }
        ordered_caps = [
            capabilities_by_id[item["capability_id"]]
            for item in normalized_steps
            if item["capability_id"] in capabilities_by_id
        ]
        return normalized_recipe, ordered_caps

    async def create_from_actions(
        self,
        actions: list[Action],
        *,
        owner_user_id: UUID,
        refresh: bool = True,
    ) -> list[Capability]:
        capabilities = self.build_from_actions(actions, owner_user_id=owner_user_id)
        if not capabilities:
            return []

        self.session.add_all(capabilities)
        await self.session.flush()

        if refresh:
            for capability in capabilities:
                await self.session.refresh(capability)

        return capabilities

    async def get_capabilities(
        self,
        *,
        capability_ids: list[UUID] | None = None,
        action_ids: list[UUID] | None = None,
        owner_user_id: UUID | None = None,
        include_all: bool = False,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[Capability]:
        query = select(Capability).order_by(Capability.created_at.asc())

        if not include_all and owner_user_id is not None:
            # Legacy compatibility: some old rows may have user_id=NULL while action is user-owned.
            query = query.outerjoin(Action, Capability.action_id == Action.id).where(
                or_(
                    Capability.user_id == owner_user_id,
                    and_(
                        Capability.user_id.is_(None),
                        Action.user_id == owner_user_id,
                    ),
                )
            )

        if capability_ids:
            query = query.where(Capability.id.in_(capability_ids))

        if action_ids:
            query = query.where(Capability.action_id.in_(action_ids))

        if offset:
            query = query.offset(offset)

        if limit is not None:
            query = query.limit(limit)

        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def get_capability(
        self,
        capability_id: UUID,
        *,
        owner_user_id: UUID | None = None,
        include_all: bool = False,
    ) -> Capability | None:
        query = select(Capability).where(Capability.id == capability_id)
        if not include_all and owner_user_id is not None:
            query = query.outerjoin(Action, Capability.action_id == Action.id).where(
                or_(
                    Capability.user_id == owner_user_id,
                    and_(
                        Capability.user_id.is_(None),
                        Action.user_id == owner_user_id,
                    ),
                )
            )
        result = await self.session.execute(query)
        return result.scalar_one_or_none()

    @staticmethod
    def _is_supported_binding_expression(value: str) -> bool:
        if re.fullmatch(r"\$run\.[A-Za-z0-9_][A-Za-z0-9_\.]*", value):
            return True
        if re.fullmatch(r"\$step\.\d+\.[A-Za-z0-9_][A-Za-z0-9_\.]*", value):
            return True
        return False

    @staticmethod
    def _extract_binding_source_step(value: str) -> int | None:
        match = re.fullmatch(r"\$step\.(\d+)\.[A-Za-z0-9_][A-Za-z0-9_\.]*", value)
        if not match:
            return None
        return int(match.group(1))

    @staticmethod
    def _to_uuid(value: Any) -> UUID | None:
        try:
            return UUID(str(value))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _capability_type_value(capability: Capability) -> str:
        cap_type = getattr(capability, "type", None)
        if isinstance(cap_type, CapabilityType):
            return cap_type.value
        if isinstance(cap_type, str):
            return cap_type
        if hasattr(cap_type, "value"):
            return str(cap_type.value)
        return CapabilityType.ATOMIC.value

    @staticmethod
    def _build_composite_llm_payload(step_capabilities: list[Capability]) -> dict[str, Any]:
        step_names = [
            str(getattr(capability, "name", "") or "")
            for capability in step_capabilities
            if str(getattr(capability, "name", "") or "").strip()
        ]
        return {
            "source": "composite",
            "recipe_summary": {
                "steps_count": len(step_capabilities),
                "step_names": step_names,
            },
        }

    @staticmethod
    def _build_capability_payload(action: Action) -> dict[str, Any]:
        input_schema = CapabilityService._build_input_schema(action)
        output_schema = getattr(action, "response_schema", None)
        data_format = CapabilityService._build_data_format(action)
        action_context = CapabilityService._build_action_context(
            action=action,
            input_schema=input_schema,
            output_schema=output_schema,
            data_format=data_format,
        )
        openapi_hints = CapabilityService._build_openapi_hints(
            action=action,
            input_schema=input_schema,
            output_schema=output_schema,
        )
        return {
            "name": CapabilityService._build_capability_name(action),
            "description": CapabilityService._build_capability_description(action),
            "input_schema": input_schema,
            "output_schema": output_schema,
            "data_format": data_format,
            "llm_payload": {
                "source": "deterministic",
                "action_context_version": "v2",
                "action_context": action_context,
                "action_context_brief": CapabilityService._build_action_context_brief(
                    action_context=action_context,
                    openapi_hints=openapi_hints,
                ),
                "openapi_hints": openapi_hints,
            },
        }

    @staticmethod
    def _build_action_context(
        *,
        action: Action,
        input_schema: dict[str, Any] | None,
        output_schema: dict[str, Any] | None,
        data_format: dict[str, Any] | None,
    ) -> dict[str, Any]:
        method = getattr(action, "method", None)
        method_value = method.value if hasattr(method, "value") else str(method or "")
        parameter_names = CapabilityService._extract_parameter_names_by_location(
            getattr(action, "parameters_schema", None)
        )
        request_property_names = CapabilityService._extract_schema_property_names(
            getattr(action, "request_body_schema", None)
        )
        response_property_names = CapabilityService._extract_schema_property_names(
            getattr(action, "response_schema", None)
        )

        return {
            "action_id": str(getattr(action, "id", "")),
            "operation_id": getattr(action, "operation_id", None),
            "method": method_value,
            "path": getattr(action, "path", None),
            "base_url": getattr(action, "base_url", None),
            "summary": getattr(action, "summary", None),
            "description": getattr(action, "description", None),
            "tags": getattr(action, "tags", None) or [],
            "source_filename": getattr(action, "source_filename", None),
            "input_schema": input_schema,
            "output_schema": output_schema,
            "parameters_schema": getattr(action, "parameters_schema", None),
            "request_body_schema": getattr(action, "request_body_schema", None),
            "response_schema": getattr(action, "response_schema", None),
            "raw_spec": getattr(action, "raw_spec", None),
            "data_format": data_format,
            "input_signals": {
                "required_inputs": CapabilityService._extract_required_inputs(input_schema),
                "parameter_names_by_location": parameter_names,
                "request_property_names": request_property_names,
            },
            "output_signals": {
                "response_property_names": response_property_names,
            },
        }

    @staticmethod
    def _build_openapi_hints(
        *,
        action: Action,
        input_schema: dict[str, Any] | None,
        output_schema: dict[str, Any] | None,
    ) -> dict[str, Any]:
        raw_spec = getattr(action, "raw_spec", None)
        if not isinstance(raw_spec, dict):
            raw_spec = {}

        request_content_types = CapabilityService._extract_content_types_from_request(raw_spec)
        response_status_codes, response_content_types = (
            CapabilityService._extract_response_hints(raw_spec)
        )
        security_requirements = (
            raw_spec.get("security") if isinstance(raw_spec.get("security"), list) else []
        )
        parameter_names = CapabilityService._extract_parameter_names_by_location(
            getattr(action, "parameters_schema", None)
        )
        vendor_extensions = {
            key: value
            for key, value in raw_spec.items()
            if isinstance(key, str) and key.startswith("x-")
        }
        path_value = str(getattr(action, "path", "") or "")
        path_segments = [
            segment
            for segment in path_value.strip("/").split("/")
            if segment and not segment.startswith("{")
        ]

        return {
            "deprecated": bool(raw_spec.get("deprecated")),
            "security_requirements": security_requirements,
            "request_content_types": request_content_types,
            "response_content_types": response_content_types,
            "response_status_codes": response_status_codes,
            "has_request_body": bool(getattr(action, "request_body_schema", None)),
            "has_response_body": bool(output_schema),
            "required_inputs": CapabilityService._extract_required_inputs(input_schema),
            "parameter_names_by_location": parameter_names,
            "path_segments": path_segments,
            "tags": getattr(action, "tags", None) or [],
            "vendor_extensions": vendor_extensions,
        }

    @staticmethod
    def _build_action_context_brief(
        *,
        action_context: dict[str, Any],
        openapi_hints: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "operation_id": action_context.get("operation_id"),
            "method": action_context.get("method"),
            "path": action_context.get("path"),
            "base_url": action_context.get("base_url"),
            "summary": action_context.get("summary"),
            "description": action_context.get("description"),
            "tags": action_context.get("tags") or [],
            "required_inputs": (action_context.get("input_signals") or {}).get("required_inputs") or [],
            "parameter_names_by_location": (action_context.get("input_signals") or {}).get(
                "parameter_names_by_location"
            )
            or {},
            "request_content_types": openapi_hints.get("request_content_types") or [],
            "response_content_types": openapi_hints.get("response_content_types") or [],
            "response_status_codes": openapi_hints.get("response_status_codes") or [],
            "security_requirements": openapi_hints.get("security_requirements") or [],
        }

    @staticmethod
    def _build_capability_name(action: Action) -> str:
        operation_id = getattr(action, "operation_id", None)
        if operation_id:
            return str(operation_id)

        method = getattr(action, "method", None)
        method_value = method.value.lower() if method is not None else "call"
        path = getattr(action, "path", "") or ""
        normalized_path = re.sub(r"[{}]", "", path).strip("/")
        normalized_path = re.sub(r"[^a-zA-Z0-9/]+", "_", normalized_path)
        normalized_path = normalized_path.replace("/", "_") or "root"
        return f"{method_value}_{normalized_path.lower()}"

    @staticmethod
    def _build_capability_description(action: Action) -> str:
        summary = getattr(action, "summary", None)
        description = getattr(action, "description", None)
        operation_id = getattr(action, "operation_id", None)
        return str(
            summary
            or description
            or operation_id
            or CapabilityService._build_capability_name(action)
        )

    @staticmethod
    def _build_input_schema(action: Action) -> dict[str, Any] | None:
        parameters_schema = getattr(action, "parameters_schema", None)
        request_body_schema = getattr(action, "request_body_schema", None)

        if parameters_schema and request_body_schema:
            return {
                "type": "object",
                "properties": {
                    "parameters": parameters_schema,
                    "request_body": request_body_schema,
                },
            }
        if parameters_schema:
            return parameters_schema
        if request_body_schema:
            return request_body_schema
        return None

    @staticmethod
    def _build_data_format(action: Action) -> dict[str, Any]:
        parameters_schema = getattr(action, "parameters_schema", None) or {}
        request_body_schema = getattr(action, "request_body_schema", None) or {}
        response_schema = getattr(action, "response_schema", None) or {}

        parameter_locations: list[str] = []
        if isinstance(parameters_schema, dict):
            properties = parameters_schema.get("properties", {})
            if isinstance(properties, dict):
                for property_schema in properties.values():
                    if not isinstance(property_schema, dict):
                        continue
                    location = property_schema.get("x-parameter-location")
                    if isinstance(location, str) and location not in parameter_locations:
                        parameter_locations.append(location)

        request_content_type = (
            request_body_schema.get("x-content-type")
            if isinstance(request_body_schema, dict)
            else None
        )
        response_content_type = (
            response_schema.get("x-content-type")
            if isinstance(response_schema, dict)
            else None
        )

        return {
            "parameter_locations": parameter_locations,
            "request_content_types": [request_content_type]
            if isinstance(request_content_type, str)
            else [],
            "request_schema_type": request_body_schema.get("type")
            if isinstance(request_body_schema, dict)
            else None,
            "response_content_types": [response_content_type]
            if isinstance(response_content_type, str)
            else [],
            "response_schema_types": [response_schema.get("type")]
            if isinstance(response_schema, dict)
            and isinstance(response_schema.get("type"), str)
            else [],
        }

    @staticmethod
    def _extract_required_inputs(input_schema: dict[str, Any] | None) -> list[str]:
        if not isinstance(input_schema, dict):
            return []

        required = input_schema.get("required")
        if isinstance(required, list):
            return [str(item) for item in required if isinstance(item, str) and item]

        # Nested schemas: {"properties":{"parameters":{"required":[...]}, "request_body":{"required":[...]}}}
        nested_required: list[str] = []
        properties = input_schema.get("properties")
        if isinstance(properties, dict):
            for nested_name in ("parameters", "request_body"):
                nested_schema = properties.get(nested_name)
                if not isinstance(nested_schema, dict):
                    continue
                nested = nested_schema.get("required")
                if isinstance(nested, list):
                    for value in nested:
                        if isinstance(value, str) and value and value not in nested_required:
                            nested_required.append(value)
        return nested_required

    @staticmethod
    def _extract_parameter_names_by_location(
        parameters_schema: dict[str, Any] | None,
    ) -> dict[str, list[str]]:
        names_by_location: dict[str, list[str]] = {
            "path": [],
            "query": [],
            "header": [],
            "cookie": [],
        }
        if not isinstance(parameters_schema, dict):
            return names_by_location

        properties = parameters_schema.get("properties")
        if not isinstance(properties, dict):
            return names_by_location

        for name, schema in properties.items():
            if not isinstance(name, str):
                continue
            location = "query"
            if isinstance(schema, dict):
                location_raw = schema.get("x-parameter-location")
                if isinstance(location_raw, str) and location_raw in names_by_location:
                    location = location_raw
            if name not in names_by_location[location]:
                names_by_location[location].append(name)
        return names_by_location

    @staticmethod
    def _extract_schema_property_names(
        schema: dict[str, Any] | None,
        *,
        limit: int = 64,
    ) -> list[str]:
        if not isinstance(schema, dict):
            return []

        result: list[str] = []
        queue: list[dict[str, Any]] = [schema]
        seen: set[str] = set()

        while queue and len(result) < limit:
            current = queue.pop(0)
            properties = current.get("properties")
            if isinstance(properties, dict):
                for key, value in properties.items():
                    if isinstance(key, str) and key not in seen:
                        seen.add(key)
                        result.append(key)
                        if len(result) >= limit:
                            break
                    if isinstance(value, dict):
                        queue.append(value)
            items = current.get("items")
            if isinstance(items, dict):
                queue.append(items)

        return result

    @staticmethod
    def _extract_content_types_from_request(raw_spec: dict[str, Any]) -> list[str]:
        request_body = raw_spec.get("requestBody")
        if not isinstance(request_body, dict):
            return []
        content = request_body.get("content")
        if not isinstance(content, dict):
            return []
        return [str(content_type) for content_type in content.keys() if isinstance(content_type, str)]

    @staticmethod
    def _extract_response_hints(raw_spec: dict[str, Any]) -> tuple[list[str], list[str]]:
        responses = raw_spec.get("responses")
        if not isinstance(responses, dict):
            return [], []

        response_status_codes: list[str] = []
        response_content_types: list[str] = []
        for status_code, response_payload in responses.items():
            status_value = str(status_code)
            if status_value not in response_status_codes:
                response_status_codes.append(status_value)
            if not isinstance(response_payload, dict):
                continue
            content = response_payload.get("content")
            if not isinstance(content, dict):
                continue
            for content_type in content.keys():
                if isinstance(content_type, str) and content_type not in response_content_types:
                    response_content_types.append(content_type)

        return response_status_codes, response_content_types
