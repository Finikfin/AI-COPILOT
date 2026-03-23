from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Request, UploadFile, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database.session import get_session
from app.models import Action, ActionIngestStatus, User
from app.schemas.capability_sch import ActionIngestWithCapabilitiesResponse
from app.services.capability_service import CapabilityService
from app.services.dialog_memory import DialogMemoryService
from app.services.openapi_service import OpenAPIService
from app.utils.business_logger import log_business_event
from app.utils.token_manager import get_current_user


router = APIRouter(tags=["Actions"])


@router.post("/ingest", response_model=ActionIngestWithCapabilitiesResponse, status_code=status.HTTP_201_CREATED)
async def ingest_actions(
    request: Request,
    file: UploadFile = File(...),
    dialog_id_form: UUID | None = Form(default=None, alias="dialog_id"),
    dialog_id_query: UUID | None = Query(default=None, alias="dialog_id"),
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    dialog_id = dialog_id_form or dialog_id_query
    trace_id = getattr(request.state, "traceId", None)
    payload = await file.read()
    try:
        document = OpenAPIService.load_document(payload)
        ingestion_result = OpenAPIService.extract_actions_with_failures(document, source_filename=file.filename)
    except ValueError as exc:
        log_business_event(
            "actions_ingest_rejected",
            trace_id=trace_id,
            user_id=str(current_user.id),
            source_filename=file.filename,
            file_size_bytes=len(payload),
            reason="invalid_openapi_document",
            details=str(exc),
        )
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    action_payloads = ingestion_result["succeeded"] + ingestion_result["failed"]
    if not action_payloads:
        log_business_event(
            "actions_ingest_rejected",
            trace_id=trace_id,
            user_id=str(current_user.id),
            source_filename=file.filename,
            file_size_bytes=len(payload),
            reason="no_supported_operations",
        )
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No supported HTTP operations found in OpenAPI file")

    actions = [Action(user_id=current_user.id, **action_payload) for action_payload in action_payloads]
    session.add_all(actions)
    await session.flush()

    succeeded_actions = [action for action in actions if action.ingest_status == ActionIngestStatus.SUCCEEDED]
    failed_actions = [action for action in actions if action.ingest_status == ActionIngestStatus.FAILED]

    capability_service = CapabilityService(session)
    capabilities = await capability_service.create_from_actions(
        succeeded_actions,
        owner_user_id=current_user.id,
        refresh=False,
    )

    if dialog_id is not None and succeeded_actions:
        action_ids = [action.id for action in succeeded_actions]
        scoped_capabilities = await capability_service.get_capabilities(
            action_ids=action_ids,
            owner_user_id=current_user.id,
            include_all=False,
        )
        dialog_memory = DialogMemoryService()
        await dialog_memory.bind_capabilities(
            str(dialog_id),
            [str(capability.id) for capability in scoped_capabilities],
        )

    await session.commit()

    for action in actions:
        await session.refresh(action)
    for capability in capabilities:
        await session.refresh(capability)

    log_business_event(
        "actions_ingested",
        trace_id=trace_id,
        user_id=str(current_user.id),
        source_filename=file.filename,
        file_size_bytes=len(payload),
        succeeded_count=len(succeeded_actions),
        failed_count=len(failed_actions),
        created_capabilities_count=len(capabilities),
        dialog_id=str(dialog_id) if dialog_id is not None else None,
    )

    return ActionIngestWithCapabilitiesResponse(
        succeeded_count=len(succeeded_actions),
        failed_count=len(failed_actions),
        created_capabilities_count=len(capabilities),
        succeeded_actions=succeeded_actions,
        failed_actions=failed_actions,
        capabilities=capabilities,
    )
