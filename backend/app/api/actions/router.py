from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, File, HTTPException, Query, Response, UploadFile, status
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database.session import get_session
from app.models import Action, HttpMethod
from app.schemas.action_sch import ActionIngestResponse, ActionResponse
from app.services.openapi_ingestion import extract_actions_from_document, load_openapi_document


router = APIRouter(prefix="/v1/actions", tags=["Actions"])


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


@router.get("", response_model=list[ActionResponse])
async def list_actions(
    method: HttpMethod | None = Query(default=None),
    source_filename: str | None = Query(default=None),
    search: str | None = Query(default=None, min_length=1),
    session: AsyncSession = Depends(get_session),
):
    query = select(Action).order_by(Action.created_at.desc())

    if method is not None:
        query = query.where(Action.method == method)

    if source_filename:
        query = query.where(Action.source_filename == source_filename)

    if search:
        search_pattern = f"%{search}%"
        query = query.where(
            or_(
                Action.operation_id.ilike(search_pattern),
                Action.path.ilike(search_pattern),
                Action.summary.ilike(search_pattern),
            )
        )

    result = await session.execute(query)
    return result.scalars().all()


@router.get("/{action_id}", response_model=ActionResponse)
async def get_action(
    action_id: UUID,
    session: AsyncSession = Depends(get_session),
):
    action = await session.get(Action, action_id)
    if action is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Action not found")
    return action


@router.delete("/{action_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_action(
    action_id: UUID,
    session: AsyncSession = Depends(get_session),
):
    action = await session.get(Action, action_id)
    if action is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Action not found")

    await session.delete(action)
    await session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
