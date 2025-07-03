# backend/app/models/user.py
from __future__ import annotations

import uuid
import datetime as dt

from sqlalchemy import Column, String, DateTime
from sqlalchemy.dialects.postgresql import UUID

from ..core.database import Base


class User(Base):
    """
    Minimal tenant-user model.

    ─ id: primary UUID
    ─ email: unique login
    ─ hashed_pw: bcrypt-hashed secret (never store plain text)
    """
    __tablename__ = "user"                 # ← MUST match FK in Company

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    email = Column(String(256), unique=True, index=True, nullable=False)
    full_name = Column(String(256), nullable=True)

    hashed_pw = Column(String(256), nullable=False)

    created_at = Column(
        DateTime(timezone=True),
        default=dt.datetime.utcnow,
        nullable=False,
    )

    # ------------------------------------------------------------------ #
    # convenience helpers
    # ------------------------------------------------------------------ #
    def __repr__(self) -> str:  # pragma: no cover
        return f"<User {self.id} {self.email}>"
