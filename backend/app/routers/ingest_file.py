from fastapi import APIRouter, UploadFile, File, Depends, HTTPException
import pandas as pd, io, json, datetime as dt
from sqlalchemy.ext.asyncio import AsyncSession
from ..core.database import get_db
from ..models.company import Company
router = APIRouter(prefix="/ingest", tags=["ingest"])

@router.post("/file")
async def ingest_file(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(current_user_id),
):
    ext = file.filename.rsplit(".", 1)[-1].lower()
    raw = await file.read()
    if ext in ("csv", "txt"):
        df = pd.read_csv(io.BytesIO(raw))
    elif ext in ("parquet", "pq"):
        df = pd.read_parquet(io.BytesIO(raw))
    elif ext in ("xlsx", "xls"):
        df = pd.read_excel(io.BytesIO(raw))
    else:
        raise HTTPException(415, "Unsupported filetype")

    # Very simple mapping: expect KPI columns
    required = {"label", "value", "delta_pct"}
    if not required.issubset(df.columns):
        raise HTTPException(400, f"File must contain columns: {required}")

    now = dt.datetime.utcnow()
    for _, row in df.iterrows():
        await db.execute(
            "INSERT INTO kpi_snapshot (captured_at,label,value,delta_pct,spark,owner_id)"
            " VALUES (:ts,:label,:val,:pct,'[]',:oid)",
            {"ts": now, "label": row.label, "val": row.value,
             "pct": row.delta_pct, "oid": user_id},
        )
    await db.commit()
    return {"rows": len(df)}
