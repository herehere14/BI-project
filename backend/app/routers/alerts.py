# backend/app/routers/alerts.py
from uuid import UUID

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from ..core.database import get_db
from ..models.dto import Alert
from .auth import current_user_id         # <- same helper

router = APIRouter(prefix="/alerts", tags=["alerts"])


@router.get("/", response_model=list[Alert])
async def list_alerts(
    db: AsyncSession = Depends(get_db),
    user_id: UUID = Depends(current_user_id),
):
    res = await db.execute(
        text(
            """
            SELECT *
            FROM alerts
            WHERE owner_id = :uid
            ORDER BY ts DESC
            LIMIT 50
            """
        ),
        {"uid": str(user_id)},
    )
    return [Alert(**row) for row in res.mappings()]
