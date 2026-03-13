from httpx import AsyncClient
import pytest
from app.main import app

@pytest.mark.asyncio
async def test_ping():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = await ac.get("/ping")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
