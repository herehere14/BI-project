"""
Thin wrappers that talk to the Celery workers through Redis queues.
You can swap for your favourite task-queue later.
"""
import json, uuid
import redis.asyncio as redis
from ..core.settings import settings

channel_sync = "ai-sync"   # RPC style (small queries)

async def ask_ai_sync(model: str, payload: dict) -> dict:
    """
    Fire-and-wait pattern for small interactive queries.
    Worker listens on channel_sync.<model>.
    """
    cid = str(uuid.uuid4())
    msg = {"cid": cid, "payload": payload}
    r: redis.Redis = settings.redis_client
    await r.publish(f"{channel_sync}.{model}", json.dumps(msg))
    # wait loop (≤ 10 s)
    res_key = f"resp:{cid}"
    res = await r.blpop(res_key, timeout=10)
    if not res:
        raise TimeoutError("AI worker timeout")
    return json.loads(res[1])
