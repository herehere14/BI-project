"""
Fetch macro news / events every 30 min and publish to Redis so the
`/ws/market` WebSocket can push it live to the dashboard UI.
"""
import json, asyncio, datetime as dt
import redis.asyncio as redis
import openai, httpx, feedparser                      # rss fallback
from backend.core.celery_app import celery_app
from backend.core.settings import settings

openai.api_key = settings.OPENAI_API_KEY
r_pub = redis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=True)

PROMPT = """
You are a news scout for mid-size B2C companies.
Return **exactly** 5 JSON items:
[
  { "id": "...", "type": "news|event", "title": "...", "description": "...",
    "impact": "high|medium|low", "timestamp": "...", "source": "...",
    "confidence": 0-100 }
]
Items must be recent (<24 h) and globally relevant (economy, supply chains,
consumer trends, tech, regulation, climate, logistics disruptions, etc.).
"""

@celery_app.task(name="external.fetch_news", bind=True, max_retries=3, default_retry_delay=60)
def fetch_news(self):
    try:
        resp = openai.chat.completions.create(
            model=settings.OPENAI_MODEL_3O,
            messages=[{"role": "system", "content": PROMPT}],
            temperature=0.3,
        )
        items = json.loads(resp.choices[0].message.content)
    except Exception as e:                              # network / parsing
        raise self.retry(exc=e)

    # fallback – ensure we still publish something
    if not items:
        feed = feedparser.parse("https://www.reutersagency.com/feed/?best-sectors=business-finance&post_type=best")
        items = [{
            "id": entry.id,
            "type": "news",
            "title": entry.title,
            "description": entry.summary[:240],
            "impact": "medium",
            "timestamp": entry.published,
            "source": "Reuters RSS",
            "confidence": 60,
        } for entry in feed.entries[:5]]

    # push to Redis pub/sub
    asyncio.run(r_pub.publish("market", json.dumps(items)))
    # (optional) persist to postgres here
    return {"sent": len(items), "ts": dt.datetime.utcnow().isoformat()}

# schedule (in same file for simplicity)
celery_app.conf.beat_schedule = {
    "external-news-30m": {
        "task": "external.fetch_news",
        "schedule": 60 * 30,          # every 30 min
    }
}
