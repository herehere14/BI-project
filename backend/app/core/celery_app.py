from celery import Celery
from backend.core.settings import settings

celery_app = Celery(__name__)
celery_app.conf.update(
    broker_url=settings.REDIS_URL,
    result_backend=settings.REDIS_URL,
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    beat_scheduler="celery.beat.Scheduler",
)
