# backend/app/models/company.py
from __future__ import annotations

import uuid
import datetime as dt

from sqlalchemy import Column, String, Text, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import UUID

# ⬇️  FIXED: use relative import so it resolves inside the “backend.app” package
from ..core.database import Base


class Company(Base):
    """Tenant-level metadata & warehouse connection info."""

    __tablename__ = "company"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    owner_id = Column(UUID(as_uuid=True), ForeignKey("user.id"), nullable=False)

    biz_type = Column(String(128), nullable=True)
    description = Column(Text, nullable=True)

    # optional: which external DWH we connect to (snowflake/bigquery/…)
    warehouse_type = Column(String(32), nullable=True)
    snowflake_dsn = Column(Text, nullable=True)  # ⚠️ store encrypted IRL!

    created_at = Column(
        DateTime(timezone=True), default=dt.datetime.utcnow, nullable=False
    )

    # --------------------------------------------------------------------- #
    # Convenience helpers
    # --------------------------------------------------------------------- #
    def __repr__(self) -> str:  # pragma: no cover
        return f"<Company {self.id} ({self.biz_type or 'n/a'})>"
