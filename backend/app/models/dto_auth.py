"""
Pydantic DTOs used by the auth router.
"""

from pydantic import BaseModel, EmailStr


class Token(BaseModel):
    """JWT returned by /auth/login & /auth/register"""
    access_token: str
    token_type: str = "bearer"


class UserCreate(BaseModel):
    """Payload expected when a user registers"""
    email: EmailStr
    password: str
    full_name: str | None = None
