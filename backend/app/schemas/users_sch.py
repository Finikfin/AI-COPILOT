from typing import Annotated, Optional
import uuid
from datetime import datetime

from pydantic import BaseModel, EmailStr, Field, ConfigDict, field_validator
from app.models import UserRole


class UserBase(BaseModel):
    email: EmailStr
    full_name: Annotated[str | None, Field(max_length=255)] = None


class UserResponse(UserBase):
    id: uuid.UUID
    role: UserRole
    is_active: bool
    created_at: datetime
    updated_at: datetime | None = None

    model_config = ConfigDict(from_attributes=True)


class UserUpdate(BaseModel):
    email: Optional[EmailStr] = None
    full_name: Optional[str] = Field(None, min_length=2, max_length=255)
    role: Optional[UserRole] = None
    is_active: Optional[bool] = None


class UserUpdateMe(BaseModel):
    email: Optional[EmailStr] = None
    full_name: Optional[str] = Field(None, min_length=2, max_length=255)


class PasswordUpdate(BaseModel):
    old_password: str = Field(min_length=8)
    new_password: str = Field(min_length=8)

    @field_validator("new_password")
    @classmethod
    def validate_password_complexity(cls, v: str) -> str:
        if not any(c.isalpha() for c in v) or not any(c.isdigit() for c in v):
            raise ValueError("must contain at least one letter and one digit")
        return v
