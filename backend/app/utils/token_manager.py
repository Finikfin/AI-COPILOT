import os
from datetime import datetime, timedelta, timezone
from typing import List
from uuid import UUID

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database.session import get_session
from app.models import User, UserRole

try:
    from jose import JWTError, jwt
except ModuleNotFoundError:
    JWTError = Exception
    jwt = None


JWT_SECRET = os.environ.get("JWT_SECRET", "super_secret_key_123")
JWT_ALG = "HS256"
security = HTTPBearer(auto_error=False)


def create_access_token(*, sub: str, role: str) -> tuple[str, int]:
    expires_in = 3600
    if jwt is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="JWT support is not installed",
        )

    expire = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
    payload = {"sub": str(sub), "role": role, "exp": expire}
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALG)
    return token, expires_in


async def get_current_user(
    creds: HTTPAuthorizationCredentials | None = Depends(security),
    session: AsyncSession = Depends(get_session),
) -> User:
    if creds is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if jwt is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="JWT support is not installed",
        )

    token = creds.credentials
    auth_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG])
        user_id_str: str | None = payload.get("sub")
        if user_id_str is None:
            raise auth_exception
        user_id = UUID(user_id_str)
    except (JWTError, ValueError):
        raise auth_exception

    result = await session.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()

    if user is None:
        raise auth_exception

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_423_LOCKED,
            detail="User account is deactivated",
        )

    return user


def check_permissions(allowed_roles: List[UserRole]):
    async def role_checker(current_user: User = Depends(get_current_user)):
        if current_user.role not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Not enough permissions",
            )
        return current_user

    return role_checker
