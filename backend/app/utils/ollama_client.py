from __future__ import annotations

import json
import os
import re
from typing import Any

import httpx


def build_capability_from_action(action: Any) -> dict[str, Any]:
    llm_result = _call_ollama_json(
        system_prompt=(
            "You convert one API action into one capability. "
            "Return only valid JSON with keys: "
            "name, description, input_schema, output_schema, data_format."
        ),
        user_prompt=_build_prompt(action),
    )
    if llm_result is not None:
        normalized = _normalize_capability_payload(llm_result, action)
        normalized["llm_payload"] = llm_result
        return normalized

    fallback = _build_fallback_capability(action)
    fallback["llm_payload"] = {
        "source": "fallback",
        "reason": "ollama_unavailable_or_invalid_response",
    }
    return fallback


def chat_json(system_prompt: str, user_prompt: str) -> dict[str, Any] | None:
    return _call_ollama_json(system_prompt=system_prompt, user_prompt=user_prompt)


def reset_model_session() -> None:
    if _resolve_provider() != "ollama":
        return None

    host = os.getenv("OLLAMA_HOST", "http://178.154.193.191:8067").strip()
    model = os.getenv("OLLAMA_MODEL", "qwen2.5-coder:7b")
    headers = _load_ollama_headers()

    try:
        from ollama import Client
    except Exception:
        return None

    try:
        client = Client(host=host, headers=headers or None)
        _reset_model_session(client=client, model=model)
    except Exception:
        return None


async def summarize_dialog_text(messages: list[dict[str, Any]]) -> str | None:
    prompt = (
        "Кратко сожми историю диалога на русском. "
        "Сохрани цель пользователя, ограничения, недостающие данные и важные решения. "
        "Ответь только текстом без markdown.\n\n"
        f"История:\n{json.dumps(messages, ensure_ascii=False)}"
    )
    payload = _call_ollama_json(
        system_prompt="Ты помощник, который сжимает диалоговый контекст для дальнейшего планирования.",
        user_prompt=prompt,
    )
    if isinstance(payload, dict):
        summary = payload.get("summary")
        if isinstance(summary, str) and summary.strip():
            return summary.strip()
    return None


def _call_ollama_json(system_prompt: str, user_prompt: str) -> dict[str, Any] | None:
    provider = _resolve_provider()
    if provider == "openai":
        return _call_openai_compatible_json(system_prompt=system_prompt, user_prompt=user_prompt)
    if provider == "yandex":
        return _call_yandex_json(system_prompt=system_prompt, user_prompt=user_prompt)

    host = os.getenv("OLLAMA_HOST", "http://178.154.193.191:8067").strip()
    model = os.getenv("OLLAMA_MODEL", "qwen2.5-coder:7b")
    headers = _load_ollama_headers()

    try:
        from ollama import Client
    except Exception:
        return None

    try:
        client = Client(host=host, headers=headers or None)
        response = client.chat(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": system_prompt,
                },
                {
                    "role": "user",
                    "content": user_prompt,
                },
            ],
            options={"temperature": 0},
        )
    except Exception:
        return None

    content = _extract_message_content(response)
    if not content:
        return None
    payload = _parse_json_payload(content)
    if not isinstance(payload, dict):
        return None
    return payload


def _call_openai_compatible_json(system_prompt: str, user_prompt: str) -> dict[str, Any] | None:
    api_key = os.getenv("LLM_API_KEY", "").strip()
    if not api_key:
        return None

    model = os.getenv("LLM_MODEL", "gpt-4.1")
    base_url = os.getenv("LLM_BASE_URL", "https://api.openai.com/v1").strip().rstrip("/")
    timeout_seconds = _load_timeout_seconds()

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    headers.update(_load_openai_headers())

    payload = {
        "model": model,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    }

    try:
        response = httpx.post(
            f"{base_url}/chat/completions",
            headers=headers,
            json=payload,
            timeout=timeout_seconds,
        )
        response.raise_for_status()
        data = response.json()
    except Exception:
        return None

    content = _extract_openai_message_content(data)
    if not content:
        return None

    parsed = _parse_json_payload(content)
    if not isinstance(parsed, dict):
        return None
    return parsed


def _call_yandex_json(system_prompt: str, user_prompt: str) -> dict[str, Any] | None:
    api_key = os.getenv("YANDEX_API_KEY", "").strip()
    iam_token = os.getenv("YANDEX_IAM_TOKEN", "").strip()
    folder_id = os.getenv("YANDEX_FOLDER_ID", "").strip()
    endpoint = os.getenv(
        "YANDEX_API_URL",
        "https://llm.api.cloud.yandex.net/foundationModels/v1/completion",
    ).strip()

    if not endpoint:
        return None
    if not api_key and not iam_token:
        return None

    model_uri = os.getenv("YANDEX_MODEL_URI", "").strip()
    if not model_uri:
        model_name = os.getenv("YANDEX_MODEL_NAME", "yandexgpt-lite/latest").strip()
        if not folder_id:
            return None
        model_uri = f"gpt://{folder_id}/{model_name}"

    headers = {
        "Content-Type": "application/json",
    }
    if api_key:
        headers["Authorization"] = f"Api-Key {api_key}"
    else:
        headers["Authorization"] = f"Bearer {iam_token}"
    if folder_id:
        headers["x-folder-id"] = folder_id
    headers.update(_load_yandex_headers())

    payload = {
        "modelUri": model_uri,
        "completionOptions": {
            "stream": False,
            "temperature": 0,
            "maxTokens": "4000",
        },
        "messages": [
            {"role": "system", "text": system_prompt},
            {"role": "user", "text": user_prompt},
        ],
    }

    try:
        response = httpx.post(
            endpoint,
            headers=headers,
            json=payload,
            timeout=_load_timeout_seconds(),
        )
        response.raise_for_status()
        data = response.json()
    except Exception:
        return None

    content = _extract_yandex_message_text(data)
    if not content:
        return None
    parsed = _parse_json_payload(content)
    if not isinstance(parsed, dict):
        return None
    return parsed


def _build_prompt(action: Any) -> str:
    payload = {
        "operation_id": getattr(action, "operation_id", None),
        "method": getattr(action, "method", None).value if getattr(action, "method", None) else None,
        "path": getattr(action, "path", None),
        "base_url": getattr(action, "base_url", None),
        "summary": getattr(action, "summary", None),
        "description": getattr(action, "description", None),
        "tags": getattr(action, "tags", None),
        "parameters_schema": getattr(action, "parameters_schema", None),
        "request_body_schema": getattr(action, "request_body_schema", None),
        "response_schema": getattr(action, "response_schema", None),
    }
    return json.dumps(payload, ensure_ascii=True, indent=2)


def _extract_message_content(response: Any) -> str | None:
    if isinstance(response, dict):
        message = response.get("message")
        if isinstance(message, dict):
            content = message.get("content")
            if isinstance(content, str):
                return content
        content = response.get("content")
        if isinstance(content, str):
            return content
        return None

    message = getattr(response, "message", None)
    if message is not None:
        content = getattr(message, "content", None)
        if isinstance(content, str):
            return content

    content = getattr(response, "content", None)
    if isinstance(content, str):
        return content
    return None


def _extract_openai_message_content(response: Any) -> str | None:
    if not isinstance(response, dict):
        return None

    choices = response.get("choices")
    if not isinstance(choices, list) or not choices:
        return None

    first = choices[0]
    if not isinstance(first, dict):
        return None

    message = first.get("message")
    if not isinstance(message, dict):
        return None

    content = message.get("content")
    if isinstance(content, str):
        return content

    if isinstance(content, list):
        text_parts: list[str] = []
        for item in content:
            if not isinstance(item, dict):
                continue
            text = item.get("text")
            if isinstance(text, str):
                text_parts.append(text)
        if text_parts:
            return "\n".join(text_parts)
    return None


def _extract_yandex_message_text(response: Any) -> str | None:
    if not isinstance(response, dict):
        return None

    result = response.get("result")
    if not isinstance(result, dict):
        return None

    alternatives = result.get("alternatives")
    if not isinstance(alternatives, list) or not alternatives:
        return None

    first = alternatives[0]
    if not isinstance(first, dict):
        return None

    message = first.get("message")
    if not isinstance(message, dict):
        return None

    text = message.get("text")
    if isinstance(text, str) and text.strip():
        return text
    return None


def _parse_json_payload(content: str) -> dict[str, Any] | None:
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", content, re.DOTALL)
        if not match:
            return None
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            return None


def _normalize_capability_payload(payload: dict[str, Any], action: Any) -> dict[str, Any]:
    fallback = _build_fallback_capability(action)
    return {
        "name": str(payload.get("name") or fallback["name"]),
        "description": str(payload.get("description") or fallback["description"]),
        "input_schema": _normalize_schema(payload.get("input_schema")) or fallback["input_schema"],
        "output_schema": _normalize_schema(payload.get("output_schema")) or fallback["output_schema"],
        "data_format": _normalize_data_format(payload.get("data_format")) or fallback["data_format"],
    }


def _build_fallback_capability(action: Any) -> dict[str, Any]:
    return {
        "name": _build_capability_name(action),
        "description": _build_capability_description(action),
        "input_schema": _build_input_schema(action),
        "output_schema": getattr(action, "response_schema", None),
        "data_format": _build_data_format(action),
    }


def _build_capability_name(action: Any) -> str:
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


def _build_capability_description(action: Any) -> str:
    summary = getattr(action, "summary", None)
    description = getattr(action, "description", None)
    operation_id = getattr(action, "operation_id", None)
    return str(summary or description or operation_id or _build_capability_name(action))


def _build_input_schema(action: Any) -> dict[str, Any] | None:
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


def _build_data_format(action: Any) -> dict[str, Any]:
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

    request_content_type = request_body_schema.get("x-content-type") if isinstance(request_body_schema, dict) else None
    response_content_type = response_schema.get("x-content-type") if isinstance(response_schema, dict) else None

    return {
        "parameter_locations": parameter_locations,
        "request_content_types": [request_content_type] if isinstance(request_content_type, str) else [],
        "request_schema_type": request_body_schema.get("type") if isinstance(request_body_schema, dict) else None,
        "response_content_types": [response_content_type] if isinstance(response_content_type, str) else [],
        "response_schema_types": [response_schema.get("type")] if isinstance(response_schema, dict) and isinstance(response_schema.get("type"), str) else [],
    }


def _normalize_schema(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return value
    return None


def _normalize_data_format(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None

    return {
        "parameter_locations": _normalize_string_list(value.get("parameter_locations")),
        "request_content_types": _normalize_string_list(value.get("request_content_types")),
        "request_schema_type": value.get("request_schema_type"),
        "response_content_types": _normalize_string_list(value.get("response_content_types")),
        "response_schema_types": _normalize_string_list(value.get("response_schema_types")),
    }


def _normalize_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if item is not None]
    return [str(value)]


def _load_ollama_headers() -> dict[str, str]:
    headers_payload = os.getenv("OLLAMA_HEADERS_JSON")
    if not headers_payload:
        return {}
    try:
        parsed = json.loads(headers_payload)
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed, dict):
        return {}
    return {str(key): str(value) for key, value in parsed.items()}


def _load_openai_headers() -> dict[str, str]:
    headers_payload = os.getenv("LLM_HEADERS_JSON")
    if not headers_payload:
        return {}
    try:
        parsed = json.loads(headers_payload)
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed, dict):
        return {}
    return {str(key): str(value) for key, value in parsed.items()}


def _load_yandex_headers() -> dict[str, str]:
    headers_payload = os.getenv("YANDEX_HEADERS_JSON")
    if not headers_payload:
        return {}
    try:
        parsed = json.loads(headers_payload)
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed, dict):
        return {}
    return {str(key): str(value) for key, value in parsed.items()}


def _resolve_provider() -> str:
    provider = os.getenv("LLM_PROVIDER", "auto").strip().lower()
    if provider in {"openai", "ollama", "yandex"}:
        return provider

    # Auto mode: prefer remote API when credentials are present.
    if os.getenv("YANDEX_API_KEY", "").strip() or os.getenv("YANDEX_IAM_TOKEN", "").strip():
        return "yandex"
    if os.getenv("LLM_API_KEY", "").strip():
        return "openai"
    return "ollama"


def _load_timeout_seconds() -> float:
    raw = os.getenv("LLM_TIMEOUT_SECONDS", "60").strip()
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return 60.0
    if value <= 0:
        return 60.0
    return value
