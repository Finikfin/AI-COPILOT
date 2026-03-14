from __future__ import annotations

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database.session import get_session
from app.models import Action
from app.schemas.action_sch import ActionIngestResponse
from app.services.openapi_ingestion import extract_actions_from_document, load_openapi_document


router = APIRouter(tags=["Actions"])


@router.post("/ingest", response_model=ActionIngestResponse, status_code=status.HTTP_201_CREATED)
async def ingest_actions(
    file: UploadFile = File(...),
    session: AsyncSession = Depends(get_session),
):
    payload = await file.read()
    try:
        document = load_openapi_document(payload)
        action_payloads = extract_actions_from_document(document, source_filename=file.filename)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    if not action_payloads:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No supported HTTP operations found in OpenAPI file")

    actions = [Action(**action_payload) for action_payload in action_payloads]
    session.add_all(actions)
    await session.commit()

    for action in actions:
        await session.refresh(action)

    return ActionIngestResponse(created_count=len(actions), actions=actions)
