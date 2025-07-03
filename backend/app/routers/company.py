from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from backend.core.database import get_db
from backend.models.company import Company
from backend.models.dto_company import CompanyCreate, CompanyOut
from uuid import UUID

router = APIRouter(prefix="/company", tags=["company"])

@router.post("/", response_model=CompanyOut)
async def upsert_company(
    payload: CompanyCreate,
    user_id: UUID = Depends(current_user_id),      # write your own auth dep
    db: AsyncSession = Depends(get_db),
):
    company = await db.get(Company, {"owner_id": user_id})
    if not company:
        company = Company(owner_id=user_id, **payload.model_dump())
        db.add(company)
    else:
        for k, v in payload.model_dump().items():
            setattr(company, k, v)
    await db.commit()
    await db.refresh(company)
    return company
