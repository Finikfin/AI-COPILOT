from fastapi import APIRouter

from app.api.actions.delete_action import router as delete_action_router
from app.api.actions.get_action import router as get_action_router
from app.api.actions.ingest_actions import router as ingest_actions_router
from app.api.actions.list_actions import router as list_actions_router


router = APIRouter(prefix="/v1/actions", tags=["Actions"])
router.include_router(ingest_actions_router)
router.include_router(list_actions_router)
router.include_router(get_action_router)
router.include_router(delete_action_router)
