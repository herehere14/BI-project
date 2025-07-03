# backend/app/routers/dashboard.py
from uuid import UUID
import json

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from ..core.database import get_db
from ..models.dto import KPITile
from .auth import current_user_id         # <- now resolves

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get("/", response_model=list[KPITile])
async def get_dashboard(
    db: AsyncSession = Depends(get_db),
    user_id: UUID = Depends(current_user_id),
):
    rows = await db.execute(
        text(
            """
            SELECT *
            FROM kpi_snapshot
            WHERE owner_id = :uid
            ORDER BY captured_at DESC
            LIMIT 6
            """
        ),
        {"uid": str(user_id)},
    )
    return [
        KPITile(
            label=r.label,
            value=float(r.value),
            delta_pct=float(r.delta_pct),
            spark=json.loads(r.spark or "[]"),
        )
        for r in rows.mappings()
    ]
