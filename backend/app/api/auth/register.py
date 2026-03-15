from fastapi import APIRouter, status

router = APIRouter(prefix="/v1/auth", tags=["Auth"])


@router.post("/register", status_code=status.HTTP_201_CREATED)
async def register():
    return {
        "ok": True,
        "message": "pong",
    }
