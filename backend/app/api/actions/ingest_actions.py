from __future__ import annotations

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database.session import get_session
from app.models import Action, ActionIngestStatus
from app.schemas.action_sch import ActionIngestResponse
from app.services.openapi_ingestion import extract_actions_with_failures_from_document, load_openapi_document
from app.schemas.capability_sch import ActionIngestWithCapabilitiesResponse
from app.services.capability_ingestion import ingest_openapi_to_capabilities


router = APIRouter(tags=["Actions"])


@router.post("/ingest", response_model=ActionIngestWithCapabilitiesResponse, status_code=status.HTTP_201_CREATED)
async def ingest_actions(
    file: UploadFile = File(...),
    session: AsyncSession = Depends(get_session),
):
    try:
        document = load_openapi_document(payload)
        ingestion_result = extract_actions_with_failures_from_document(document, source_filename=file.filename)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    action_payloads = ingestion_result["succeeded"] + ingestion_result["failed"]
    if not action_payloads:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No supported HTTP operations found in OpenAPI file")

    actions = [Action(**action_payload) for action_payload in action_payloads]
    session.add_all(actions)
    await session.commit()

    for action in actions:
        await session.refresh(action)

    succeeded_actions = [action for action in actions if action.ingest_status == ActionIngestStatus.SUCCEEDED]
    failed_actions = [action for action in actions if action.ingest_status == ActionIngestStatus.FAILED]

    return ActionIngestResponse(
        succeeded_count=len(succeeded_actions),
        failed_count=len(failed_actions),
        succeeded_actions=succeeded_actions,
        failed_actions=failed_actions,
    )
