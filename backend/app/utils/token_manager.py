import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import List
from uuid import UUID

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError

from app.core.database.session import get_session
from app.models import User, UserRole

try:
    from jose import JWTError, jwt
except ModuleNotFoundError:
    JWTError = Exception
    jwt = None


JWT_SECRET = os.environ.get("JWT_SECRET", "super_secret_key_123")
JWT_ALG = "HS256"
MOCK_AUTH_ENABLED = os.environ.get("MOCK_AUTH", "true").lower() in {"1", "true", "yes", "on"}
security = HTTPBearer(auto_error=False)


def create_access_token(*, sub: str, role: str) -> tuple[str, int]:
    expires_in = 3600
    if jwt is None:
        return f"mock-token-{role.lower()}-{sub}", expires_in

    expire = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
    payload = {"sub": str(sub), "role": role, "exp": expire}
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALG)
    return token, expires_in


def _build_mock_user() -> User:
    return User(
        id=uuid.uuid5(uuid.NAMESPACE_URL, "mock-admin@example.com"),
        email="mock-admin@example.com",
        full_name="Mock Admin",
        hashed_password="mock",
        role=UserRole.ADMIN,
        is_active=True,
    )


async def get_current_user(
    creds: HTTPAuthorizationCredentials | None = Depends(security),
    session: AsyncSession = Depends(get_session),
) -> User:
    auth_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    # In mock mode we still honor a provided token to keep user isolation.
    if MOCK_AUTH_ENABLED and creds is None:
        mock_user = _build_mock_user()
        existing_user = await session.get(User, mock_user.id)
        if existing_user is not None:
            return existing_user

        session.add(mock_user)
        try:
            await session.commit()
        except IntegrityError:
            await session.rollback()
        existing_user = await session.get(User, mock_user.id)
        return existing_user or mock_user

    if creds is None:
        raise auth_exception

    user = await _resolve_user_from_token(token=creds.credentials, session=session)
    if user is None:
        raise auth_exception

    return user


async def _resolve_user_from_token(token: str, session: AsyncSession) -> User | None:
    user_id: UUID | None = None
    if jwt is not None:
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG])
            user_id_str: str | None = payload.get("sub")
            if user_id_str is None:
                return None
            user_id = UUID(user_id_str)
        except (JWTError, ValueError):
            return None
    else:
        user_id = _parse_mock_token(token)
        if user_id is None:
            return None

    result = await session.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if user is None:
        return None
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_423_LOCKED,
            detail="User account is deactivated",
        )
    return user


def _parse_mock_token(token: str) -> UUID | None:
    if not token.startswith("mock-token-"):
        return None
    parts = token.split("-", 3)
    if len(parts) != 4:
        return None
    _, _, _, raw_user_id = parts
    try:
        return UUID(raw_user_id)
    except ValueError:
        return None


def check_permissions(allowed_roles: List[UserRole]):
    async def role_checker(current_user: User = Depends(get_current_user)):
        if current_user.role not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Not enough permissions",
            )
        return current_user

    return role_checker
