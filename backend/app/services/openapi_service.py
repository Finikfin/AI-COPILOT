from __future__ import annotations

import re
from typing import Any

import yaml

from app.models import ActionIngestStatus, HttpMethod


class OpenAPIService:
    SUPPORTED_METHODS = {method.value.lower(): method for method in HttpMethod}
    JSON_CONTENT_TYPES = ("application/json", "application/*+json")

    @staticmethod
    def load_document(raw_bytes: bytes) -> dict[str, Any]:
        if not raw_bytes:
            raise ValueError("OpenAPI file is empty")

        try:
            document = yaml.safe_load(raw_bytes.decode("utf-8"))
        except UnicodeDecodeError as exc:
            raise ValueError("OpenAPI file must be UTF-8 encoded") from exc
        except yaml.YAMLError as exc:
            raise ValueError("OpenAPI file is not valid YAML or JSON") from exc

        if not isinstance(document, dict):
            raise ValueError("OpenAPI root must be an object")

        openapi_version = document.get("openapi")
        if not isinstance(openapi_version, str) or not openapi_version.startswith("3."):
            raise ValueError("Only OpenAPI 3.x documents are supported")

        if not isinstance(document.get("paths"), dict) or not document["paths"]:
            raise ValueError("OpenAPI file must contain a non-empty paths section")

        return document

    @classmethod
    def extract_actions(
        cls,
        document: dict[str, Any],
        *,
        source_filename: str | None = None,
    ) -> list[dict[str, Any]]:
        return cls.extract_actions_with_failures(document, source_filename=source_filename)["succeeded"]

    @classmethod
    def extract_actions_with_failures(
        cls,
        document: dict[str, Any],
        *,
        source_filename: str | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        base_url = cls._extract_base_url(document)
        succeeded_actions: list[dict[str, Any]] = []
        failed_actions: list[dict[str, Any]] = []

        for path, path_item in document.get("paths", {}).items():
            if not isinstance(path_item, dict):
                continue

            shared_parameters = path_item.get("parameters", [])

            for method_name, operation in path_item.items():
                if method_name not in cls.SUPPORTED_METHODS:
                    continue
                if not isinstance(operation, dict):
                    failed_actions.append(
                        cls._build_failed_action_payload(
                            method_name=method_name,
                            path=path,
                            base_url=base_url,
                            source_filename=source_filename,
                            raw_spec=operation,
                            error_message="Operation definition must be an object",
                        )
                    )
                    continue

                try:
                    succeeded_actions.append(
                        cls._build_succeeded_action_payload(
                            method_name=method_name,
                            path=path,
                            operation=operation,
                            shared_parameters=shared_parameters,
                            document=document,
                            base_url=base_url,
                            source_filename=source_filename,
                        )
                    )
                except ValueError as exc:
                    failed_actions.append(
                        cls._build_failed_action_payload(
                            method_name=method_name,
                            path=path,
                            base_url=base_url,
                            source_filename=source_filename,
                            raw_spec=operation,
                            error_message=str(exc),
                        )
                    )

        return {
            "succeeded": succeeded_actions,
            "failed": failed_actions,
        }

    @classmethod
    def _build_succeeded_action_payload(
        cls,
        *,
        method_name: str,
        path: str,
        operation: dict[str, Any],
        shared_parameters: list[Any] | None,
        document: dict[str, Any],
        base_url: str | None,
        source_filename: str | None,
    ) -> dict[str, Any]:
        normalized_operation = cls._dereference(operation, document)
        parameters = cls._merge_parameters(shared_parameters, normalized_operation.get("parameters", []), document)

        return {
            "operation_id": normalized_operation.get("operationId") or cls._build_operation_id(method_name, path),
            "method": cls.SUPPORTED_METHODS[method_name],
            "path": path,
            "base_url": base_url,
            "summary": normalized_operation.get("summary"),
            "description": normalized_operation.get("description"),
            "tags": normalized_operation.get("tags"),
            "parameters_schema": cls._build_parameters_schema(parameters, document),
            "request_body_schema": cls._extract_request_body_schema(normalized_operation, document),
            "response_schema": cls._extract_response_schema(normalized_operation, document),
            "source_filename": source_filename,
            "raw_spec": normalized_operation,
            "ingest_status": ActionIngestStatus.SUCCEEDED,
            "ingest_error": None,
        }

    @classmethod
    def _build_failed_action_payload(
        cls,
        *,
        method_name: str,
        path: str,
        base_url: str | None,
        source_filename: str | None,
        raw_spec: Any,
        error_message: str,
    ) -> dict[str, Any]:
        operation = raw_spec if isinstance(raw_spec, dict) else {}

        return {
            "operation_id": operation.get("operationId") or cls._build_operation_id(method_name, path),
            "method": cls.SUPPORTED_METHODS[method_name],
            "path": path,
            "base_url": base_url,
            "summary": operation.get("summary"),
            "description": operation.get("description"),
            "tags": operation.get("tags"),
            "parameters_schema": None,
            "request_body_schema": None,
            "response_schema": None,
            "source_filename": source_filename,
            "raw_spec": operation or None,
            "ingest_status": ActionIngestStatus.FAILED,
            "ingest_error": error_message,
        }

    @staticmethod
    def _extract_base_url(document: dict[str, Any]) -> str | None:
        servers = document.get("servers")
        if isinstance(servers, list) and servers:
            first_server = servers[0]
            if isinstance(first_server, dict):
                return first_server.get("url")
        return None

    @classmethod
    def _merge_parameters(
        cls,
        path_parameters: list[Any] | None,
        operation_parameters: list[Any] | None,
        document: dict[str, Any],
    ) -> list[dict[str, Any]]:
        merged: dict[tuple[str | None, str | None], dict[str, Any]] = {}

        for raw_parameter in (path_parameters or []) + (operation_parameters or []):
            parameter = cls._dereference(raw_parameter, document)
            if not isinstance(parameter, dict):
                continue
            key = (parameter.get("name"), parameter.get("in"))
            merged[key] = parameter

        return list(merged.values())

    @classmethod
    def _build_parameters_schema(
        cls,
        parameters: list[dict[str, Any]],
        document: dict[str, Any],
    ) -> dict[str, Any] | None:
        if not parameters:
            return None

        properties: dict[str, Any] = {}
        required: list[str] = []

        for parameter in parameters:
            name = parameter.get("name")
            if not name:
                continue
            if parameter.get("in") not in {"query", "path", "header", "cookie"}:
                continue

            schema = parameter.get("schema")
            if schema is None:
                schema = cls._extract_schema_from_content(parameter.get("content"), document)
            else:
                schema = cls._dereference(schema, document)

            property_schema = schema if isinstance(schema, dict) else {"type": "string"}
            property_schema = {
                **property_schema,
                "x-parameter-location": parameter.get("in"),
            }

            if parameter.get("description"):
                property_schema["description"] = parameter["description"]

            properties[name] = property_schema

            if parameter.get("required"):
                required.append(name)

        if not properties:
            return None

        schema: dict[str, Any] = {
            "type": "object",
            "properties": properties,
        }
        if required:
            schema["required"] = required

        return schema

    @classmethod
    def _extract_request_body_schema(
        cls,
        operation: dict[str, Any],
        document: dict[str, Any],
    ) -> dict[str, Any] | None:
        request_body = operation.get("requestBody")
        if not isinstance(request_body, dict):
            return None
        request_body = cls._dereference(request_body, document)
        schema = cls._extract_schema_from_content(request_body.get("content"), document)
        if not isinstance(schema, dict):
            return None

        if request_body.get("required"):
            schema = {**schema, "x-required": True}

        return schema

    @classmethod
    def _extract_response_schema(
        cls,
        operation: dict[str, Any],
        document: dict[str, Any],
    ) -> dict[str, Any] | None:
        responses = operation.get("responses")
        if not isinstance(responses, dict):
            return None

        for status_code, response in responses.items():
            if not str(status_code).startswith("2"):
                continue

            normalized_response = cls._dereference(response, document)
            if not isinstance(normalized_response, dict):
                continue

            schema = cls._extract_schema_from_content(normalized_response.get("content"), document)
            if isinstance(schema, dict):
                return schema

            if normalized_response.get("description"):
                return {"description": normalized_response["description"]}

        return None

    @classmethod
    def _extract_schema_from_content(cls, content: Any, document: dict[str, Any]) -> dict[str, Any] | None:
        if not isinstance(content, dict):
            return None

        preferred_content_type = next((content_type for content_type in cls.JSON_CONTENT_TYPES if content_type in content), None)
        items = []
        if preferred_content_type:
            items.append((preferred_content_type, content[preferred_content_type]))
        items.extend((content_type, value) for content_type, value in content.items() if content_type != preferred_content_type)

        for content_type, value in items:
            if not isinstance(value, dict):
                continue
            schema = value.get("schema")
            if not isinstance(schema, dict):
                continue

            normalized_schema = cls._dereference(schema, document)
            if isinstance(normalized_schema, dict):
                return {
                    **normalized_schema,
                    "x-content-type": content_type,
                }

        return None

    @classmethod
    def _dereference(cls, value: Any, document: dict[str, Any]) -> Any:
        if isinstance(value, list):
            return [cls._dereference(item, document) for item in value]

        if not isinstance(value, dict):
            return value

        if "$ref" in value:
            resolved = cls._resolve_ref(value["$ref"], document)
            merged = cls._dereference(resolved, document)
            if not isinstance(merged, dict):
                return merged

            sibling_fields = {key: cls._dereference(item, document) for key, item in value.items() if key != "$ref"}
            return {**merged, **sibling_fields}

        return {key: cls._dereference(item, document) for key, item in value.items()}

    @staticmethod
    def _resolve_ref(ref: str, document: dict[str, Any]) -> Any:
        if not ref.startswith("#/"):
            raise ValueError(f"Only local $ref values are supported, got: {ref}")

        current: Any = document
        for part in ref[2:].split("/"):
            token = part.replace("~1", "/").replace("~0", "~")
            if not isinstance(current, dict) or token not in current:
                raise ValueError(f"Could not resolve OpenAPI reference: {ref}")
            current = current[token]

        return current

    @staticmethod
    def _build_operation_id(method_name: str, path: str) -> str:
        normalized_path = re.sub(r"[{}]", "", path).strip("/")
        normalized_path = re.sub(r"[^a-zA-Z0-9/]+", "_", normalized_path)
        normalized_path = normalized_path.replace("/", "_") or "root"
        return f"{method_name.lower()}_{normalized_path.lower()}"
