from datetime import datetime
from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------#
class KPITile(BaseModel):
    label: str
    value: float
    delta_pct: float
    spark: list[float] = Field(default_factory=list)


class Alert(BaseModel):
    id: int
    ts: datetime
    severity: str
    headline: str
    kpi: str | None = None
    suggested_action: str | None = None


# -- Ask-AI (interactive) ---------------------------------------------------#
class AskAIRequest(BaseModel):
    query: str


class ForecastBand(BaseModel):
    dates: list[str]
    baseline: list[float]
    forecast: list[float]
    lower: list[float]
    upper: list[float]


class ActionCard(BaseModel):
    title: str
    subtitle: str
    cost: float
    roi: float
    cta: str


class AskAIResponse(BaseModel):
    impact_summary: str
    forecast: ForecastBand
    actions: list[ActionCard]
