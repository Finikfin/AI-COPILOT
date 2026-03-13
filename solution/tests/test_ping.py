from httpx import AsyncClient
import pytest
from app.api.ping.router import router as ping_router

@pytest.mark.asyncio
async def test_ping():
    async with AsyncClient(app=ping_router, base_url="http://test") as ac:
        response = await ac.get("/ping")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
