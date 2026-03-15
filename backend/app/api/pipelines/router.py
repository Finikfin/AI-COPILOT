from fastapi import APIRouter

from app.api.pipelines.generate import router as generate_router
from app.api.pipelines.get_dialog_history import router as get_dialog_history_router
from app.api.pipelines.list_dialogs import router as list_dialogs_router
from app.api.pipelines.reset_dialog import router as reset_dialog_router
from app.api.pipelines.run import router as run_router


router = APIRouter(prefix="/v1/pipelines", tags=["Pipelines"])
router.include_router(generate_router)
router.include_router(list_dialogs_router)
router.include_router(get_dialog_history_router)
router.include_router(reset_dialog_router)
router.include_router(run_router)
