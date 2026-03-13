from pydantic import BaseModel, EmailStr, Field, field_validator

class RegisterIn(BaseModel):
    email: EmailStr = Field(max_length=254)
    password: str = Field(min_length=8, max_length=72)
    full_name: str = Field(min_length=2, max_length=200, alias="fullName")

    @field_validator("password")
    @classmethod
    def validate_password_complexity(cls, v: str) -> str:
        if not any(c.isalpha() for c in v) or not any(c.isdigit() for c in v):
            raise ValueError("must contain at least one letter and one digit")
        return v

class LoginIn(BaseModel):
    email: EmailStr = Field(max_length=254)
    password: str = Field(min_length=8, max_length=72)