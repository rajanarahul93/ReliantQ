from app.celery_app import celery_app
from app.database import SessionLocal
from app.models import Job, JobStatus, DeadLetterJob
from app.config import settings
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

@celery_app.task(name="reaper_task")
def reaper_task():
    """
    Scans for jobs stuck in PROCESSING for too long and re-queues them.
    """
    db = SessionLocal()
    try:
        timeout_threshold = datetime.utcnow() - timedelta(minutes=settings.STUCK_JOB_TIMEOUT_MINUTES)
        
        # Find stuck jobs
        stuck_jobs = db.query(Job).filter(
            Job.status == JobStatus.PROCESSING,
            Job.updated_at < timeout_threshold
        ).all()
        
        if not stuck_jobs:
            return f"No stuck jobs found. Cleaned up at {datetime.utcnow()}"

        count = 0
        for job in stuck_jobs:
            logger.warning(f"Reaping stuck job {job.id} (Status: {job.status}, Updated: {job.updated_at})")
            
            job.retry_count += 1
            job.updated_at = datetime.utcnow()
            
            if job.retry_count > 3: # Max retries
                job.status = JobStatus.FAILURE
                dlq = DeadLetterJob(
                    job_id=job.id,
                    last_error="Stuck in PROCESSING for too long (Reaped)"
                )
                db.add(dlq)
                logger.info(f"Stuck job {job.id} moved to DLQ.")
            else:
                job.status = JobStatus.RETRYING
                # Re-enqueue
                queue_name = "default"
                if job.priority >= 2:
                    queue_name = "high_priority"
                elif job.priority <= 0:
                    queue_name = "low_priority"
                
                celery_app.send_task(
                    "process_job",
                    args=[str(job.id)],
                    queue=queue_name
                )
                logger.info(f"Stuck job {job.id} re-enqueued to {queue_name}.")
            
            count += 1
        
        db.commit()
        return f"Reaped {count} jobs at {datetime.utcnow()}"

    except Exception as e:
        db.rollback()
        logger.error(f"Error in reaper task: {e}")
        raise e
    finally:
        db.close()
