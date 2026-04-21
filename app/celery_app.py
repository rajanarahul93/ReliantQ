from celery import Celery
from app.config import settings

celery_app = Celery(
    "worker",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
)

# Priority Queue configuration
celery_app.conf.task_queues = {
    "high_priority": {"exchange": "high_priority", "routing_key": "high_priority"},
    "default": {"exchange": "default", "routing_key": "default"},
    "low_priority": {"exchange": "low_priority", "routing_key": "low_priority"},
}
celery_app.conf.task_default_queue = "default"

# Periodic tasks (Celery Beat)
celery_app.conf.beat_schedule = {
    "reaper-every-60-seconds": {
        "task": "reaper_task",
        "schedule": 60.0,
    },
    "cleanup-every-day": {
        "task": "cleanup_old_jobs_task",
        "schedule": 86400.0, # Once a day
    },
}
