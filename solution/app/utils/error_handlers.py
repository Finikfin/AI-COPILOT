from datetime import datetime, timezone
from typing import Any
import uuid

from fastapi import Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    trace_id = getattr(request.state, "traceId", str(uuid.uuid4()))
    is_json_error = any(e.get("type") in ("json_invalid", "json_decode", "value_error.jsondecode") for e in exc.errors())
    
    if is_json_error:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "code": "BAD_REQUEST",
                "message": "Невалидный JSON",
                "traceId": trace_id,
                "timestamp": now_iso(),
                "path": request.url.path,
                "details": {"hint": "Проверьте запятые/кавычки"},
            },
        )

    field_errors: list[dict[str, Any]] = []
    for err in exc.errors():
        loc = [str(x) for x in err.get("loc", []) if x != "body"]
        field_name = ".".join(loc) if loc else "unknown"
        
        msg = err.get("msg", "invalid")
        if msg.startswith("Value error, "):
            msg = msg.replace("Value error, ", "")

        field_errors.append({
            "field": field_name,
            "issue": msg,
            "rejectedValue": err.get("input", None), 
        })

    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "code": "VALIDATION_FAILED",
            "message": "Некоторые поля не прошли валидацию",
            "traceId": trace_id,
            "timestamp": now_iso(),
            "path": request.url.path,
            "fieldErrors": field_errors,
        },
    )


async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    trace_id = getattr(request.state, "traceId", str(uuid.uuid4()))

    message = str(exc.detail)
    details = None
    
    if isinstance(exc.detail, dict):
        message = exc.detail.get("message", str(exc.detail))
        details_data = {k: v for k, v in exc.detail.items() if k != "message"}
        if details_data:
            details = details_data

    code = "HTTP_ERROR"
    if exc.status_code == status.HTTP_409_CONFLICT:
        code = "EMAIL_ALREADY_EXISTS" if "email" in message.lower() else "CONFLICT"
    elif exc.status_code == status.HTTP_400_BAD_REQUEST:
        code = "BAD_REQUEST"
    elif exc.status_code == status.HTTP_401_UNAUTHORIZED:
        code = "UNAUTHORIZED"
    elif exc.status_code == status.HTTP_423_LOCKED:
        code = "USER_INACTIVE"
    elif exc.status_code == status.HTTP_403_FORBIDDEN:
        code = "FORBIDDEN"
    elif exc.status_code == status.HTTP_404_NOT_FOUND:
        code = "NOT_FOUND"
        if message == "Not Found": message = "Ресурс не найден"
    elif exc.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY:
        code = "VALIDATION_FAILED"

    content = {
        "code": code,
        "message": message,
        "traceId": trace_id,
        "timestamp": now_iso(),
        "path": request.url.path,
    }
    
    if details:
        content["details"] = details

    return JSONResponse(
        status_code=exc.status_code,
        content=content,
    )