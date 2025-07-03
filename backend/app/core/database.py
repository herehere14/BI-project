# backend/app/core/database.py
"""
Async-SQLAlchemy setup.

Exposes:
    • engine              – AsyncEngine bound to asyncpg
    • AsyncSessionLocal   – async_sessionmaker factory
    • Base                – declarative base for ORM models
    • get_db()            – FastAPI dependency (yields an AsyncSession)
    • init_db()           – dev helper to auto-create tables
    • shutdown()          – graceful engine disposal on shutdown
"""

from __future__ import annotations

from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import declarative_base

from .settings import settings  # must define DATABASE_URL, etc.

# ------------------------------------------------------------------------- #
# Engine & Session factory
# ------------------------------------------------------------------------- #

engine: AsyncEngine = create_async_engine(
    settings.DATABASE_URL,
    echo=getattr(settings, "DB_ECHO", False),
    pool_pre_ping=True,
)

AsyncSessionLocal: async_sessionmaker[AsyncSession] = async_sessionmaker(
    bind=engine,
    expire_on_commit=False,
)

Base = declarative_base()  # import & subclass this in your models

# ------------------------------------------------------------------------- #
# FastAPI helpers
# ------------------------------------------------------------------------- #

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    **Dependency** – yields a fresh `AsyncSession` per request.

    Usage inside a router:
    ```python
    @router.get("/items")
    async def list_items(db: AsyncSession = Depends(get_db)):
        ...
    ```
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            # 100 % explicit is 100 % clear.
            await session.close()


async def init_db() -> None:
    """
    Dev-utility: create all tables that don’t yet exist.

    In production rely on Alembic migrations instead.
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def shutdown() -> None:
    """Dispose the engine on application shutdown."""
    await engine.dispose()
