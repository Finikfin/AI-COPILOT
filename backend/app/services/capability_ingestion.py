from __future__ import annotations

from fastapi import UploadFile
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Action, Capability
from app.services.openapi_ingestion import extract_actions_from_document, load_openapi_document
from app.utils.ollama_client import build_capability_from_action


async def ingest_openapi_to_capabilities(
    file: UploadFile,
    session: AsyncSession,
) -> tuple[list[Action], list[Capability]]:
    payload = await file.read()
    document = load_openapi_document(payload)
    action_payloads = extract_actions_from_document(document, source_filename=file.filename)

    if not action_payloads:
        raise ValueError("No supported HTTP operations found in OpenAPI file")

    actions = [Action(**action_payload) for action_payload in action_payloads]
    session.add_all(actions)
    await session.flush()

    capabilities: list[Capability] = []
    for action in actions:
        capability_payload = build_capability_from_action(action)
        capabilities.append(
            Capability(
                action_id=action.id,
                name=capability_payload["name"],
                description=capability_payload.get("description"),
                input_schema=capability_payload.get("input_schema"),
                output_schema=capability_payload.get("output_schema"),
                data_format=capability_payload.get("data_format"),
                llm_payload=capability_payload.get("llm_payload"),
            )
        )

    session.add_all(capabilities)
    await session.commit()

    for capability in capabilities:
        await session.refresh(capability)

    return actions, capabilities
