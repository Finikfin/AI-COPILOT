from fastapi import APIRouter, status

router = APIRouter(prefix="/v1/auth", tags=["Auth"])


@router.post("/login", status_code=status.HTTP_200_OK)
async def login():
    return {
        "ok": True,
        "message": "pong",
    }
