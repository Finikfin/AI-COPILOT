import sys
import asyncio
import os
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.exceptions import RequestValidationError

from app.api.ping.router import router as health_router
from app.api.actions.router import router as actions_router
from app.api.capabilities.router import router as capabilities_router
from app.api.pipelines.router import router as pipelines_router
from app.utils.error_handlers import (
    validation_exception_handler,
    http_exception_handler,
    unhandled_exception_handler,
)
from app.core.database.init import init_db

try:
    from fastapi_cache import FastAPICache
    from fastapi_cache.backends.redis import RedisBackend
    from redis import asyncio as aioredis
except ModuleNotFoundError:
    FastAPICache = None
    RedisBackend = None
    aioredis = None

try:
    from app.api.auth.register import router as auth_router
    from app.api.auth.login import router as login_router
except ModuleNotFoundError as exc:
    auth_router = None
    login_router = None
    print(f"Auth routes are disabled: {exc}")

try:
    from app.api.users.get_me import router as get_me_router
    from app.api.users.list_users import router as list_users_router
    from app.api.users.update_me import router as update_me_router
    from app.api.users.update_user import router as update_user_router
    from app.api.users.update_password import router as update_password_router
    from app.api.users.delete_user import router as delete_user_router
except ModuleNotFoundError as exc:
    get_me_router = None
    list_users_router = None
    update_me_router = None
    update_user_router = None
    update_password_router = None
    delete_user_router = None
    print(f"User routes are disabled: {exc}")


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
    redis_url = os.getenv("REDIS_URL", f"redis://{redis_host}:{redis_port}")
    
    redis = None
    if FastAPICache and RedisBackend and aioredis:
        try:
            redis = aioredis.from_url(redis_url, encoding="utf8", decode_responses=True)
            FastAPICache.init(RedisBackend(redis), prefix="fastapi-cache")
            print(f"Redis initialized successfully at {redis_url}!")
        except Exception as e:
            print(f"Redis initialization error: {e}")
    else:
        print("fastapi-cache2 is not installed; Redis cache is disabled.")

    yield

    if redis:
        await redis.close()


app = FastAPI(lifespan=lifespan)


@app.middleware("http")
async def add_trace_id(request, call_next):
    trace_id = request.headers.get("X-Trace-Id") or str(uuid.uuid4())
    request.state.traceId = trace_id
    
    response = await call_next(request)
    response.headers["X-Trace-Id"] = trace_id
    return response


app.add_exception_handler(RequestValidationError, validation_exception_handler)
app.add_exception_handler(HTTPException, http_exception_handler)
app.add_exception_handler(Exception, unhandled_exception_handler)

from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health_router, prefix="/api")
app.include_router(actions_router, prefix="/api")
app.include_router(capabilities_router, prefix="/api")
app.include_router(pipelines_router, prefix="/api")

if auth_router is not None and login_router is not None:
    app.include_router(auth_router, prefix="/api")
    app.include_router(login_router, prefix="/api")

if all(
    router is not None
    for router in (
        get_me_router,
        list_users_router,
        update_me_router,
        update_user_router,
        update_password_router,
        delete_user_router,
    )
):
    app.include_router(get_me_router, prefix="/api/users")
    app.include_router(list_users_router, prefix="/api/users")
    app.include_router(update_me_router, prefix="/api/users")
    app.include_router(update_user_router, prefix="/api/users")
    app.include_router(update_password_router, prefix="/api/users")
    app.include_router(delete_user_router, prefix="/api/users")
