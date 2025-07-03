# backend/app/core/settings.py
from functools import cached_property
from pydantic_settings import BaseSettings, SettingsConfigDict
import redis.asyncio as redis


class Settings(BaseSettings):
    # ------------------------------------------------------------------ #
    # Runtime connection strings (override in .env or Docker-Compose)
    # ------------------------------------------------------------------ #
    DATABASE_URL: str = (
        "postgresql+asyncpg://postgres:postgres@localhost:5432/intel"
    )
    REDIS_URL: str = "redis://localhost:6379/0"

    # ------------------------------------------------------------------ #
    # OpenAI
    # ------------------------------------------------------------------ #
    OPENAI_API_KEY: str | None = None       # put this in .env, not in code
    OPENAI_MODEL_3O: str = "gpt-3o-mini"
    OPENAI_MODEL_4O: str = "gpt-4o-mini"

    # ------------------------------------------------------------------ #
    # Optional external data keys
    # ------------------------------------------------------------------ #
    NEWS_API_KEY: str | None = None

    # ------------------------------------------------------------------ #
    # JWT / Auth  ❗ (new)
    # ------------------------------------------------------------------ #
    JWT_SECRET: str = "CHANGE-ME-IN-PROD"
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRES_HOURS: int = 24

    # ------------------------------------------------------------------ #
    # Pydantic config
    # ------------------------------------------------------------------ #
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
    )

    # ------------------------------------------------------------------ #
    # Convenience aliases  ❗ (new)
    # ------------------------------------------------------------------ #
    @property
    def jwt_secret(self) -> str:
        """Backward-compat alias used by older imports."""
        return self.JWT_SECRET

    @property
    def jwt_algorithm(self) -> str:
        return self.JWT_ALGORITHM

    # ------------------------------------------------------------------ #
    # Lazy, cached dependencies (unchanged)
    # ------------------------------------------------------------------ #
    @cached_property
    def redis_client(self) -> redis.Redis:
        """
        One shared async Redis client for the whole app.
        cached_property ensures it’s built once and is *hashable*.
        """
        return redis.from_url(
            self.REDIS_URL,
            encoding="utf-8",
            decode_responses=True,   # str in / str out
        )


# ---------------------------------------------------------------------- #
# Export singleton                                                       #
# ---------------------------------------------------------------------- #
settings = Settings()
