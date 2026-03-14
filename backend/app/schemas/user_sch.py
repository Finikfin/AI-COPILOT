from pydantic import BaseModel, EmailStr, ConfigDict
from uuid import UUID
from datetime import datetime
from app.models import UserRole
from typing import Optional

class UserBase(BaseModel):
    email: EmailStr
    full_name: str

class UserUpdate(BaseModel):
    email: Optional[EmailStr] = None
    full_name: Optional[str] = None
    role: Optional[UserRole] = None
    is_active: Optional[bool] = None
    min_approvals_required: Optional[int] = None

class UserResponse(UserBase):
    id: UUID
    role: UserRole
    is_active: bool
    min_approvals_required: int
    created_at: datetime
    
    model_config = ConfigDict(from_attributes=True)

class UserUpdateMe(BaseModel):
    email: Optional[EmailStr] = None
    full_name: Optional[str] = None

class PasswordUpdate(BaseModel):
    old_password: str
    new_password: str
