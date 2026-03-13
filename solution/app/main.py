import uuid
import sys
import asyncio
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException

from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from redis import asyncio as aioredis

from app.api.auth.register import router as auth_router
from app.api.auth.login import router as login_router

# from app.utils.error_handlers import validation_exception_handler, http_exception_handler
from app.core.database.init import init_db


if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await init_db()
    except Exception as e:
        print(f"Database initialization error: {e}")
    
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = os.getenv("REDIS_PORT", "6379")
    redis_url = f"redis://{redis_host}:{redis_port}"
    
    redis = None
    try:
        redis = aioredis.from_url(redis_url, encoding="utf8", decode_responses=True)
        FastAPICache.init(RedisBackend(redis), prefix="fastapi-cache")
        print(f"Redis initialized successfully at {redis_url}!")
    except Exception as e:
        print(f"Redis initialization error: {e}")

    yield

    if redis:
        await redis.close()


app = FastAPI(lifespan=lifespan)


# @app.middleware("http")
# async def add_trace_id(request, call_next):
#     trace_id = request.headers.get("X-Trace-Id") or str(uuid.uuid4())
#     request.state.traceId = trace_id
    
#     response = await call_next(request)
#     response.headers["X-Trace-Id"] = trace_id
#     return response


# app.add_exception_handler(RequestValidationError, validation_exception_handler)
# app.add_exception_handler(HTTPException, http_exception_handler)

app.include_router(auth_router, prefix="/api")
app.include_router(login_router, prefix="/api")