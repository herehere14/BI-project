"""
1. Nightly snapshot of key SQL views ➜ GPT-4-o ➜ write forecast table.
2. RPC endpoint for interactive /ask-ai queries.
"""
import json, asyncio, datetime, uuid
import redis.asyncio as redis
from openai import AsyncOpenAI
from sqlalchemy import text
from core.database import AsyncSessionLocal
from core.settings import _Settings
from workers.queues import celery_app

cfg = _Settings()
r = redis.from_url(cfg.REDIS_URL, decode_responses=True)
openai = AsyncOpenAI(api_key=cfg.OPENAI_API_KEY)

# ---------------------------------------------------------------------- #
@celery_app.task(name="workers.internal_analyser.snapshot_task")
def snapshot_task() -> None:
    async def _run():
        async with AsyncSessionLocal() as db:
            rows = await db.execute(text("SELECT date, revenue FROM revenue_30d_view"))
            csv_string = "\n".join(f"{r.date},{r.revenue}" for r in rows)
            prompt = (
                "Given this CSV of daily revenue, forecast next 30 days "
                "using seasonality where relevant. "
                "Return JSON: {dates[], baseline[], forecast[], lower[], upper[]}."
            )
            resp = await openai.chat.completions.create(
                model=cfg.OPENAI_MODEL_4O,
                messages=[{"role":"system","content":prompt},
                          {"role":"user","content":csv_string}],
                temperature=0.1,
            )
            js = json.loads(resp.choices[0].message.content)
            await db.execute(
                text("INSERT INTO forecasts (captured_at, data) VALUES (:ts, :d)"),
                dict(ts=datetime.datetime.utcnow(), d=json.dumps(js))
            )
            await db.commit()
    asyncio.run(_run())

# ---------------------------------------------------------------------- #
async def _handle_rpc(msg: dict) -> None:
    payload = msg["payload"]
    query = payload["query"]
    # TODO: Light SQL pull based on query context
    prompt = (
        "You are a decision analyst.\n"
        f"Answer the user's question. Output valid JSON matching schema X.\n"
        "Question: " + query
    )
    resp = await openai.chat.completions.create(
        model=cfg.OPENAI_MODEL_4O,
        messages=[{"role":"system","content":prompt}],
        temperature=0.2,
    )
    await r.lpush(f"resp:{msg['cid']}", resp.choices[0].message.content)

@celery_app.task(name="workers.internal_analyser.run_rpc_listener")
def run_rpc_listener() -> None:
    async def _rpc():
        sub = r.pubsub()
        await sub.subscribe("ai-sync.gpt-4o-mini")
        async for m in sub.listen():
            if m["type"] != "message":
                continue
            await _handle_rpc(json.loads(m["data"]))
    asyncio.run(_rpc())
