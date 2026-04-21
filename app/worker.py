from app.celery_app import celery_app
# Task imports are necessary for registration
from app.tasks.process_job import process_job
from app.tasks.reaper import reaper_task
from app.tasks.cleanup import cleanup_old_jobs_task

@celery_app.task(name="ping")
def ping():
    return "pong"
