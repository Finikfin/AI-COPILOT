from __future__ import annotations

import logging
from typing import Any


business_logger = logging.getLogger("app.business")


def log_business_event(event: str, **fields: Any) -> None:
    safe_fields: dict[str, Any] = {"event": event}
    for key, value in fields.items():
        if isinstance(value, (str, int, float, bool)) or value is None:
            safe_fields[key] = value
        else:
            safe_fields[key] = str(value)

    business_logger.info(event, extra=safe_fields)
