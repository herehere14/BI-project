# backend/app/main.py
"""
FastAPI entry-point; wires up HTTP routers and three WebSocket endpoints
(live alerts, market-intel, dashboard heartbeat).

Key points
----------
1.  **Auto-import `backend.app.models.*`** so that all SQLAlchemy models are
    registered on `Base.metadata` **before** we call `init_db()` – this creates
    the tables and prevents “relation … does not exist” errors on first run.
2.  Robust WebSocket helpers (silent close, cooperative `await` loop).
3.  Clean startup / shutdown: database engine disposed and Redis connection
    closed.
"""

from __future__ import annotations

import asyncio
import importlib
import logging
import pkgutil
import random
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from starlette.websockets import WebSocketDisconnect, WebSocketState

from .core.database import init_db, shutdown
from .core.settings import settings
from .routers import alerts, ask_ai, auth, dashboard

# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #
logging.basicConfig(
    level=logging.INFO, format="%(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("main")

# --------------------------------------------------------------------------- #
# FastAPI instance
# --------------------------------------------------------------------------- #
app = FastAPI(
    title="Decision-Intel API",
    version="0.1.0",
    docs_url="/",
    redoc_url=None,
)

# --------------------------------------------------------------------------- #
# CORS  (frontend dev server runs on :5173)
# --------------------------------------------------------------------------- #
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --------------------------------------------------------------------------- #
# Routers
# --------------------------------------------------------------------------- #
app.include_router(dashboard.router)
app.include_router(alerts.router)
app.include_router(ask_ai.router)
app.include_router(auth.router)

# --------------------------------------------------------------------------- #
# Helper: safe WebSocket close
# --------------------------------------------------------------------------- #
async def _safe_close(ws: WebSocket) -> None:
    if ws.application_state != WebSocketState.DISCONNECTED:
        try:
            await ws.close()
        except Exception:  # pragma: no cover
            pass


# --------------------------------------------------------------------------- #
# WebSocket / Alerts channel (Redis pub-sub)
# --------------------------------------------------------------------------- #
@app.websocket("/ws/alerts")
async def alerts_ws(ws: WebSocket) -> None:
    await ws.accept()
    pubsub = settings.redis_client.pubsub()
    await pubsub.subscribe("alerts")
    try:
        while True:
            msg: dict[str, Any] | None = await pubsub.get_message(
                ignore_subscribe_messages=True, timeout=5.0
            )
            if msg and msg["type"] == "message":
                await ws.send_json(msg["data"])
            await asyncio.sleep(0)  # cooperative yield
    except WebSocketDisconnect:
        logger.info("alerts_ws – client disconnected")
    except Exception:  # pragma: no cover
        logger.exception("alerts_ws – unexpected error")
    finally:
        try:
            await pubsub.unsubscribe("alerts")
            await pubsub.close()
        finally:
            await _safe_close(ws)


# --------------------------------------------------------------------------- #
# WebSocket / Market-intel demo feed
# --------------------------------------------------------------------------- #
@app.websocket("/ws/market")
async def market_ws(ws: WebSocket) -> None:
    await ws.accept()
    try:
        while True:
            await asyncio.sleep(5)
            await ws.send_json(
                {
                    "id": str(random.randint(1000, 9999)),
                    "type": random.choice(
                        ["trend", "competitor", "risk", "opportunity"]
                    ),
                    "title": "Demo Market Pulse",
                    "description": "Synthetic market signal for dev",
                    "impact": random.choice(["high", "medium", "low"]),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "source": "demo-feed",
                    "confidence": random.randint(70, 95),
                }
            )
    except WebSocketDisconnect:
        logger.info("market_ws – client disconnected")
    except Exception:  # pragma: no cover
        logger.exception("market_ws – unexpected error")
    finally:
        await _safe_close(ws)


# --------------------------------------------------------------------------- #
# WebSocket / Dashboard keep-alive
# --------------------------------------------------------------------------- #
@app.websocket("/ws/dashboard")
async def dashboard_ws(ws: WebSocket) -> None:
    await ws.accept()
    counter = 0
    try:
        while True:
            await asyncio.sleep(10)
            counter += 1
            await ws.send_json({"ping": counter})
    except WebSocketDisconnect:
        logger.info("dashboard_ws – client disconnected")
    except Exception:  # pragma: no cover
        logger.exception("dashboard_ws – unexpected error")
    finally:
        await _safe_close(ws)


# --------------------------------------------------------------------------- #
# Startup / Shutdown
# --------------------------------------------------------------------------- #
@app.on_event("startup")
async def _startup() -> None:
    # auto-import every module in backend.app.models → register ORM classes
    from . import models as _models

    for _, mod, _ in pkgutil.iter_modules(_models.__path__):
        importlib.import_module(f"{_models.__name__}.{mod}")

    await init_db()
    logger.info("🚀  FastAPI ready – database initialised")


@app.on_event("shutdown")
async def _shutdown() -> None:
    await shutdown()
    await settings.redis_client.close()
    logger.info("👋  Server shutdown complete")
