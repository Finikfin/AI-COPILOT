from fastapi import APIRouter

from app.api.pipelines.generate import router as generate_router
from app.api.pipelines.reset_dialog import router as reset_dialog_router


router = APIRouter(prefix="/v1/pipelines", tags=["Pipelines"])
router.include_router(generate_router)
router.include_router(reset_dialog_router)
