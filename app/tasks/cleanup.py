from app.celery_app import celery_app
from app.database import SessionLocal
from app.models import Job, JobStatus
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

@celery_app.task(name="cleanup_old_jobs_task")
def cleanup_old_jobs_task():
    """
    Deletes SUCCESS or FAILURE jobs older than 30 days.
    """
    db = SessionLocal()
    try:
        retention_days = 30
        threshold = datetime.utcnow() - timedelta(days=retention_days)
        
        # Find and delete
        deleted_count = db.query(Job).filter(
            Job.status.in_([JobStatus.SUCCESS, JobStatus.FAILURE]),
            Job.updated_at < threshold
        ).delete(synchronize_session=False)
        
        db.commit()
        logger.info(f"Housekeeping: Deleted {deleted_count} old jobs.")
        return f"Deleted {deleted_count} jobs."
    except Exception as e:
        db.rollback()
        logger.error(f"Error in cleanup task: {e}")
        raise e
    finally:
        db.close()
