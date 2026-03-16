from __future__ import annotations

import logging
from typing import Any


business_logger = logging.getLogger("app.business")


def _derive_event_group(event: str) -> tuple[str, str | None]:
    normalized = (event or "").strip().lower()

    if normalized.startswith("auth_"):
        return "auth", None

    if normalized.startswith("action_") or normalized.startswith("actions_"):
        return "actions", None

    if (
        normalized.startswith("capability_")
        or normalized.startswith("capabilities_")
        or normalized.startswith("composite_capability_")
    ):
        return "capabilities", None

    if normalized.startswith("pipeline_prompt_"):
        return "pipelines", "prompt"
    if normalized.startswith("pipeline_run_"):
        return "pipelines", "run"
    if normalized.startswith("pipeline_dialog_"):
        return "pipelines", "dialog"
    if normalized.startswith("pipeline_") or normalized.startswith("pipelines_"):
        return "pipelines", None

    if normalized.startswith("execution_run_"):
        return "executions", "run"
    if normalized.startswith("execution_step_"):
        return "executions", "step"
    if normalized.startswith("execution_") or normalized.startswith("executions_"):
        return "executions", None

    if normalized.startswith("user_") or normalized.startswith("users_"):
        return "users", None

    return "other", None


def log_business_event(event: str, **fields: Any) -> None:
    safe_fields: dict[str, Any] = {"event": event}
    event_group, event_subgroup = _derive_event_group(event)

    if "event_group" not in fields:
        safe_fields["event_group"] = event_group
    if event_subgroup is not None and "event_subgroup" not in fields:
        safe_fields["event_subgroup"] = event_subgroup

    for key, value in fields.items():
        if isinstance(value, (str, int, float, bool)) or value is None:
            safe_fields[key] = value
        else:
            safe_fields[key] = str(value)

    business_logger.info(event, extra=safe_fields)
