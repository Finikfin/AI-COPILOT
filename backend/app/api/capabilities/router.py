from fastapi import APIRouter

from app.api.capabilities.get_capability import router as get_capability_router
from app.api.capabilities.list_capabilities import router as list_capabilities_router


router = APIRouter(prefix="/v1/capabilities", tags=["Capabilities"])
router.include_router(list_capabilities_router)
router.include_router(get_capability_router)
