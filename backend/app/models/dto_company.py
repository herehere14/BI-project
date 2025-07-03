from pydantic import BaseModel, Field
class CompanyCreate(BaseModel):
    biz_type: str = Field(..., max_length=128)
    description: str
    snowflake_dsn: str | None = None

class CompanyOut(CompanyCreate):
    id: str
    class Config: orm_mode = True
