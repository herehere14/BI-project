import os
from celery import Celery
from core.settings import _Settings  # reuse env parsing

cfg = _Settings()  # loads .env in worker container

celery_app = Celery(
    "intel-lite",
    broker=cfg.REDIS_URL,
    backend=cfg.REDIS_URL,
    include=["workers.external_fetcher", "workers.internal_analyser"],
)
celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    beat_schedule={
        "external-fetch-every-15": {
            "task": "workers.external_fetcher.fetch_news_task",
            "schedule": 900,      # seconds
        },
        "internal-snapshot-daily": {
            "task": "workers.internal_analyser.snapshot_task",
            "schedule": 86400,
        },
    },
)
