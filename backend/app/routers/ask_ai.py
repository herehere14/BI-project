from fastapi import APIRouter, HTTPException
from ..models.dto import AskAIRequest, AskAIResponse
from ..services.ai import ask_ai_sync
from ..core.settings import settings

router = APIRouter(prefix="/ask-ai", tags=["ask-ai"])


@router.post("/", response_model=AskAIResponse)
async def ask_ai(req: AskAIRequest):
    try:
        result = await ask_ai_sync(settings.OPENAI_MODEL_4O, req.model_dump())
        return AskAIResponse(**result)
    except TimeoutError:
        raise HTTPException(status_code=504, detail="AI service timeout")
